package com.rtb.dsp.data;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicaHealthTrackerTest {

    private final Deque<Object> readings = new ArrayDeque<>();
    private final ReplicaHealthTracker tracker = new ReplicaHealthTracker(() -> {
        Object next = readings.pop();
        if (next instanceof Exception e) {
            throw e;
        }
        return (Duration) next;
    }, Duration.ofMillis(200), Duration.ofSeconds(1), 3, null);

    private void probe(Object... values) {
        for (Object v : values) {
            readings.add(v);
            tracker.probeOnce();
        }
    }

    @Test
    void startsBypassedAndClosesOnlyAfterConsecutiveHealthyProbes() {
        assertThat(tracker.isReplicaUsable()).isFalse();
        probe(ms(10), ms(20));
        assertThat(tracker.isReplicaUsable()).isFalse();
        probe(ms(15));
        assertThat(tracker.isReplicaUsable()).isTrue();
    }

    @Test
    void oneProbeOverThresholdOpensTheCircuit() {
        probe(ms(10), ms(10), ms(10));
        probe(ms(201));
        assertThat(tracker.isReplicaUsable()).isFalse();
        assertThat(tracker.lastLagMillis()).isEqualTo(201);
    }

    @Test
    void aFailedProbeOpensTheCircuitAndResetsTheHealthyStreak() {
        probe(ms(10), ms(10), ms(10));
        probe(new IllegalStateException("standby is not streaming"));
        assertThat(tracker.isReplicaUsable()).isFalse();
        probe(ms(10), ms(10));
        assertThat(tracker.isReplicaUsable()).isFalse();
        probe(ms(10));
        assertThat(tracker.isReplicaUsable()).isTrue();
    }

    private static Duration ms(long millis) {
        return Duration.ofMillis(millis);
    }
}
