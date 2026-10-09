package com.rtb.dsp.resilience;

import com.rtb.dsp.resilience.AdaptiveAimdLimiter.Config;
import com.rtb.dsp.resilience.AdaptiveAimdLimiter.Outcome;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class AdaptiveAimdLimiterTest {

    private static final long MS = 1_000_000;
    private static final Duration WINDOW = Duration.ofMillis(50);

    private static Config config(int initial) {
        return new Config(initial, 4, 5_000, Duration.ofMillis(35), 0.7, 8, WINDOW, 10);
    }

    @Test
    void littlesLawSeedsTheInitialLimit() {
        Config c = Config.fromLittlesLaw(30_000, Duration.ofMillis(8), 8, 5_000, Duration.ofMillis(35), 0.7, 8,
                Duration.ofMillis(100), 50);
        assertThat(c.initialLimit()).isEqualTo(240);
    }

    @Test
    void rejectsImmediatelyAtTheLimit() {
        var limiter = new AdaptiveAimdLimiter(config(4));
        for (int i = 0; i < 4; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.rejected()).isEqualTo(1);
    }

    @Test
    void multiplicativeDecreaseWhenP90ExceedsTarget() throws InterruptedException {
        var limiter = new AdaptiveAimdLimiter(config(100));
        runWindow(limiter, 60, 150 * MS);
        assertThat(limiter.limit()).isEqualTo(70);
        runWindow(limiter, 60, 150 * MS);
        assertThat(limiter.limit()).isEqualTo(49);
        assertThat(limiter.lastP90Nanos()).isGreaterThanOrEqualTo(150 * MS);
    }

    @Test
    void additiveIncreaseWhenHealthyAndTheLimitIsInUse() throws InterruptedException {
        var limiter = new AdaptiveAimdLimiter(config(20));
        runWindow(limiter, 20, 2 * MS);
        assertThat(limiter.limit()).isEqualTo(28);
    }

    @Test
    void anIdleLimiterDoesNotDriftUpwards() throws InterruptedException {
        var limiter = new AdaptiveAimdLimiter(config(1_000));
        for (int window = 0; window < 3; window++) {
            Thread.sleep(WINDOW.toMillis() + 10);
            for (int i = 0; i < 20; i++) {
                limiter.tryAcquire();
                limiter.release(2 * MS, Outcome.SUCCESS);
            }
        }
        assertThat(limiter.limit()).isEqualTo(1_000);
    }

    @Test
    void aDroppedRequestBacksOffEvenWithLowLatency() throws InterruptedException {
        var limiter = new AdaptiveAimdLimiter(config(100));
        Thread.sleep(WINDOW.toMillis() + 10);
        limiter.tryAcquire();
        limiter.release(1 * MS, Outcome.DROPPED);
        assertThat(limiter.limit()).isEqualTo(70);
    }

    @Test
    void neverBacksOffBelowTheMinimum() throws InterruptedException {
        var limiter = new AdaptiveAimdLimiter(config(5));
        for (int i = 0; i < 5; i++) {
            runWindow(limiter, 5, 200 * MS, 12);
        }
        assertThat(limiter.limit()).isEqualTo(4);
    }

    /** Holds {@code concurrent} slots at once (so the limit is in use), then releases them. */
    private static void runWindow(AdaptiveAimdLimiter limiter, int concurrent, long latency) throws InterruptedException {
        runWindow(limiter, concurrent, latency, concurrent);
    }

    /**
     * Releases all but one sample inside the current window, waits for the window to expire, then
     * releases the last one so the adjustment sees the whole window at once.
     */
    private static void runWindow(AdaptiveAimdLimiter limiter, int concurrent, long latency, int samples)
            throws InterruptedException {
        int acquired = 0;
        for (int i = 0; i < concurrent; i++) {
            if (limiter.tryAcquire()) {
                acquired++;
            }
        }
        for (int i = 0; i < samples - 1; i++) {
            if (i >= acquired) {
                limiter.tryAcquire();
            }
            limiter.release(latency, Outcome.SUCCESS);
        }
        Thread.sleep(WINDOW.toMillis() + 10);
        if (samples - 1 >= acquired) {
            limiter.tryAcquire();
        }
        limiter.release(latency, Outcome.SUCCESS);
    }
}
