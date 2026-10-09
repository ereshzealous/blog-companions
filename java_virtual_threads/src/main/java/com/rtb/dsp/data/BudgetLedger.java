package com.rtb.dsp.data;

import com.rtb.dsp.model.WinNotice;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * The ledger insert and debit share a transaction, making redelivery idempotent.
 * Invalidation runs after commit so a concurrent miss cannot recache the old row.
 */
@Slf4j
public class BudgetLedger {

    private static final String RECORD_WIN = """
            INSERT INTO win_ledger (auction_id, campaign_id, price_micros)
            VALUES (?, ?, ?) ON CONFLICT (auction_id) DO NOTHING
            """;

    // The exchange has already charged us, so spend is recorded even past the cap; the remaining
    // balance floors at zero and the overspend is counted rather than rejected.
    private static final String DEBIT = """
            UPDATE campaign_budget
               SET remaining_micros = GREATEST(remaining_micros - ?, 0),
                   spent_today_micros = spent_today_micros + ?,
                   updated_at = now()
             WHERE campaign_id = ?
            RETURNING remaining_micros
            """;

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final String invalidationChannel;
    private final Counter settled;
    private final Counter duplicates;
    private final Counter exhausted;

    public BudgetLedger(JdbcTemplate jdbc, StringRedisTemplate redis, String invalidationChannel, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.invalidationChannel = invalidationChannel;
        this.settled = registry.counter("rtb.ledger.settled");
        this.duplicates = registry.counter("rtb.ledger.duplicates");
        this.exhausted = registry.counter("rtb.ledger.exhausted");
    }

    @Transactional
    public boolean settle(WinNotice win) {
        if (jdbc.update(RECORD_WIN, win.auctionId(), win.campaignId(), win.clearingPriceMicros()) == 0) {
            duplicates.increment();
            return false;
        }
        Long remaining = jdbc.queryForObject(DEBIT, Long.class,
                win.clearingPriceMicros(), win.clearingPriceMicros(), win.campaignId());
        if (remaining != null && remaining == 0) {
            exhausted.increment();
        }
        settled.increment();
        long campaignId = win.campaignId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                invalidate(campaignId);
            }
        });
        return true;
    }

    private void invalidate(long campaignId) {
        try {
            redis.delete(MultiTierBudgetCache.redisKey(campaignId));
            redis.convertAndSend(invalidationChannel, Long.toString(campaignId));
        } catch (DataAccessException e) {
            // The L1 and L2 TTLs bound staleness when an invalidation is lost.
            log.warn("budget invalidation for campaign {} failed; caches converge on TTL", campaignId, e);
        }
    }
}
