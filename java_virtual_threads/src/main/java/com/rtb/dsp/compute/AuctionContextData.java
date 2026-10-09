package com.rtb.dsp.compute;

import java.time.Instant;

/**
 * Uses monotonic time for elapsed checks and an Instant for StructuredTaskScope.joinUntil.
 */
public record AuctionContextData(
        String requestId,
        String exchange,
        String openRtbVersion,
        long ingressNanos,
        long deadlineNanos,
        Instant deadline) {

    public long remainingNanos() {
        return deadlineNanos - System.nanoTime();
    }

    public boolean expired() {
        return remainingNanos() <= 0;
    }

    public long elapsedMicros() {
        return (System.nanoTime() - ingressNanos) / 1_000;
    }
}
