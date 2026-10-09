package com.rtb.dsp.data;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;

/**
 * Redis Pub/Sub can lose invalidations; L1 TTL bounds staleness when a message is missed.
 */
@Slf4j
public class RedisBudgetInvalidationSub implements MessageListener {

    private final MultiTierBudgetCache cache;
    private final Counter received;
    private final Counter malformed;

    public RedisBudgetInvalidationSub(MultiTierBudgetCache cache, MeterRegistry registry) {
        this.cache = cache;
        this.received = registry.counter("rtb.budget.invalidations.received");
        this.malformed = registry.counter("rtb.budget.invalidations.malformed");
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            cache.invalidateLocal(Long.parseLong(body.trim()));
            received.increment();
        } catch (NumberFormatException e) {
            malformed.increment();
            log.warn("ignoring malformed budget invalidation '{}'", body);
        }
    }
}
