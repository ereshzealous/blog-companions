package com.rtb.dsp.data;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rtb.dsp.compute.AuctionContext;
import com.rtb.dsp.compute.AuctionContextData;
import com.rtb.dsp.config.RtbProperties;
import com.rtb.dsp.model.BudgetSnapshot;
import com.rtb.dsp.model.CampaignBudget;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Coalesces cache misses outside Caffeine.compute to avoid blocking under a JDK 21 monitor.
 */
@Slf4j
public class MultiTierBudgetCache {

    public enum Tier { L1, L2, DB }

    /** A budget read and the tier that answered it. {@code snapshot} is null for unknown campaigns. */
    public record Lookup(BudgetSnapshot snapshot, Tier tier) {
    }

    private static final String KEY_PREFIX = "campaign:budget:";

    private final Cache<Long, BudgetSnapshot> l1;
    private final Cache<Long, Long> invalidatedAtNanos;
    private final StringRedisTemplate redis;
    private final CampaignBudgetRepository repository;
    private final SingleFlight<Long, Lookup> flights = new SingleFlight<>("budget");
    private final Duration l2Ttl;
    private final Duration waitTimeout;
    private final Counter l2Hits;
    private final Counter dbLoads;
    private final Counter l2Errors;

    public MultiTierBudgetCache(RtbProperties.Cache config, StringRedisTemplate redis,
                                CampaignBudgetRepository repository, MeterRegistry registry) {
        this.redis = redis;
        this.repository = repository;
        this.l2Ttl = config.l2Ttl();
        this.waitTimeout = config.singleFlightTimeout();
        this.l1 = Caffeine.newBuilder()
                .maximumSize(config.l1MaxSize())
                .expireAfterWrite(config.l1Ttl())
                .recordStats()
                .build();
        this.invalidatedAtNanos = Caffeine.newBuilder()
                .expireAfterWrite(config.l1Ttl())
                .maximumSize(config.l1MaxSize())
                .build();
        CaffeineCacheMetrics.monitor(registry, l1, "budget.l1");
        this.l2Hits = registry.counter("rtb.budget.l2.hits");
        this.dbLoads = registry.counter("rtb.budget.db.loads");
        this.l2Errors = registry.counter("rtb.budget.l2.errors");
        registry.gauge("rtb.budget.singleflight.flights", flights, SingleFlight::flights);
        registry.gauge("rtb.budget.singleflight.joined", flights, SingleFlight::joined);
    }

    public Lookup get(long campaignId) throws Exception {
        BudgetSnapshot hit = l1.getIfPresent(campaignId);
        if (hit != null) {
            return new Lookup(hit, Tier.L1);
        }
        return flights.execute(campaignId, () -> loadThrough(campaignId), waitBudget());
    }

    /** Shared loaders must not depend on an individual auction context. */
    private Lookup loadThrough(long campaignId) {
        long started = System.nanoTime();
        Lookup result = readL2(campaignId);
        if (result == null) {
            dbLoads.increment();
            BudgetSnapshot fromDb = repository.findById(campaignId).map(CampaignBudget::toSnapshot).orElse(null);
            if (fromDb == null) {
                return new Lookup(null, Tier.DB);
            }
            writeL2(fromDb);
            result = new Lookup(fromDb, Tier.DB);
        }
        // Return a pre-invalidation load to its waiters without recaching the stale value.
        Long invalidated = invalidatedAtNanos.getIfPresent(campaignId);
        if (invalidated == null || invalidated - started < 0) {
            l1.put(campaignId, result.snapshot());
        }
        return result;
    }

    private Lookup readL2(long campaignId) {
        try {
            String encoded = redis.opsForValue().get(KEY_PREFIX + campaignId);
            if (encoded == null) {
                return null;
            }
            l2Hits.increment();
            return new Lookup(BudgetSnapshot.decode(encoded), Tier.L2);
        } catch (DataAccessException e) {
            l2Errors.increment();
            log.debug("L2 read failed for campaign {}, falling through to the database", campaignId, e);
            return null;
        }
    }

    private void writeL2(BudgetSnapshot snapshot) {
        // +/-10% jitter so keys written in the same burst do not all expire in the same second.
        long ttlMillis = l2Ttl.toMillis();
        long jittered = ttlMillis + ThreadLocalRandom.current().nextLong(-ttlMillis / 10, ttlMillis / 10 + 1);
        try {
            redis.opsForValue().set(KEY_PREFIX + snapshot.campaignId(), snapshot.encode(), Duration.ofMillis(jittered));
        } catch (DataAccessException e) {
            l2Errors.increment();
            log.debug("L2 write failed for campaign {}", snapshot.campaignId(), e);
        }
    }

    private Duration waitBudget() {
        AuctionContextData auction = AuctionContext.currentIfBound().orElse(null);
        if (auction == null) {
            return waitTimeout;
        }
        long remaining = Math.max(0, auction.remainingNanos());
        return remaining < waitTimeout.toNanos() ? Duration.ofNanos(remaining) : waitTimeout;
    }

    public void invalidateLocal(long campaignId) {
        invalidatedAtNanos.put(campaignId, System.nanoTime());
        l1.invalidate(campaignId);
    }

    public static String redisKey(long campaignId) {
        return KEY_PREFIX + campaignId;
    }

    public SingleFlight<Long, Lookup> singleFlight() {
        return flights;
    }

    public long l1Size() {
        return l1.estimatedSize();
    }
}
