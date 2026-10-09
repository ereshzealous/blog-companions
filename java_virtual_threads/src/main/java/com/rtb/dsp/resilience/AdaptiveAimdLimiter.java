package com.rtb.dsp.resilience;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Adjusts concurrency from windowed P90 latency and downstream drops. A completing request
 * claims each adjustment window with CAS; no timer thread is required.
 */
public final class AdaptiveAimdLimiter {

    public enum Outcome { SUCCESS, DROPPED }

    public record Config(int initialLimit, int minLimit, int maxLimit, Duration targetP90, double backoffRatio,
                         int additiveStep, Duration window, int minWindowSamples) {

        public Config {
            if (minLimit < 1 || maxLimit < minLimit) {
                throw new IllegalArgumentException("need 1 <= minLimit <= maxLimit");
            }
            if (backoffRatio <= 0 || backoffRatio >= 1) {
                throw new IllegalArgumentException("backoffRatio must be in (0, 1)");
            }
            initialLimit = Math.clamp(initialLimit, minLimit, maxLimit);
        }

        public static Config fromLittlesLaw(double arrivalsPerSecond, Duration latency, int minLimit, int maxLimit,
                                            Duration targetP90, double backoffRatio, int additiveStep,
                                            Duration window, int minWindowSamples) {
            int initial = (int) Math.ceil(arrivalsPerSecond * latency.toNanos() / 1e9);
            return new Config(initial, minLimit, maxLimit, targetP90, backoffRatio, additiveStep, window,
                    minWindowSamples);
        }
    }

    private static final long BUCKET_NANOS = 250_000;  // 0.25 ms resolution
    private static final int BUCKETS = 1_000;          // up to 250 ms, then one overflow bucket

    private final Config config;
    private final long targetNanos;
    private final long windowNanos;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLongArray histogram = new AtomicLongArray(BUCKETS + 1);
    private final AtomicInteger windowSamples = new AtomicInteger();
    private final AtomicInteger windowPeakInFlight = new AtomicInteger();
    private final AtomicBoolean droppedInWindow = new AtomicBoolean();
    private final AtomicLong windowStart;
    private final LongAdder rejected = new LongAdder();
    private volatile int limit;
    private volatile long lastP90Nanos;

    public AdaptiveAimdLimiter(Config config) {
        this.config = config;
        this.targetNanos = config.targetP90().toNanos();
        this.windowNanos = config.window().toNanos();
        this.limit = config.initialLimit();
        this.windowStart = new AtomicLong(System.nanoTime());
    }

    public boolean tryAcquire() {
        int current;
        do {
            current = inFlight.get();
            if (current >= limit) {
                rejected.increment();
                return false;
            }
        } while (!inFlight.compareAndSet(current, current + 1));
        int peak = windowPeakInFlight.get();
        if (current + 1 > peak) {
            windowPeakInFlight.compareAndSet(peak, current + 1);
        }
        return true;
    }

    public void release(long latencyNanos, Outcome outcome) {
        inFlight.decrementAndGet();
        int bucket = (int) Math.min(BUCKETS, Math.max(0, latencyNanos) / BUCKET_NANOS);
        histogram.incrementAndGet(bucket);
        windowSamples.incrementAndGet();
        if (outcome == Outcome.DROPPED) {
            droppedInWindow.set(true);
        }
        long now = System.nanoTime();
        long start = windowStart.get();
        if (now - start >= windowNanos && windowStart.compareAndSet(start, now)) {
            adjust();
        }
    }

    private void adjust() {
        boolean dropped = droppedInWindow.get();
        int samples = windowSamples.get();
        if (samples < config.minWindowSamples() && !dropped) {
            return;  // too little signal; keep accumulating into the next window
        }
        droppedInWindow.set(false);
        windowSamples.addAndGet(-samples);
        long[] counts = new long[BUCKETS + 1];
        long total = 0;
        for (int i = 0; i <= BUCKETS; i++) {
            counts[i] = histogram.getAndSet(i, 0);
            total += counts[i];
        }
        int peak = windowPeakInFlight.getAndSet(inFlight.get());
        long p90 = percentile(counts, total, 0.90);
        lastP90Nanos = p90;

        int current = limit;
        int next = current;
        if (dropped || p90 > targetNanos) {
            next = Math.max(config.minLimit(), (int) (current * config.backoffRatio()));
        } else if (peak * 2 >= current) {
            // Do not let idle pods grow a limit they cannot serve during the next burst.
            next = Math.min(config.maxLimit(), current + config.additiveStep());
        }
        limit = next;
    }

    private static long percentile(long[] counts, long total, double q) {
        if (total == 0) {
            return 0;
        }
        long rank = (long) Math.ceil(q * total);
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= rank) {
                return (i + 1) * BUCKET_NANOS;
            }
        }
        return (long) counts.length * BUCKET_NANOS;
    }

    public int limit() {
        return limit;
    }

    public int inFlight() {
        return inFlight.get();
    }

    public long rejected() {
        return rejected.sum();
    }

    public long lastP90Nanos() {
        return lastP90Nanos;
    }

    public Config config() {
        return config;
    }
}
