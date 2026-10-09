package com.rtb.dsp.data;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Starts bypassed and requires consecutive healthy probes to recover. A failed or lagging
 * probe bypasses the replica immediately; successful queries alone do not establish freshness.
 */
@Slf4j
public class ReplicaHealthTracker implements SmartLifecycle {

    @FunctionalInterface
    public interface LagProbe {
        /** Current replication lag; throws if the replica cannot be measured. */
        Duration measure() throws Exception;
    }

    private final LagProbe probe;
    private final long thresholdNanos;
    private final Duration interval;
    private final int closeAfter;
    private final AtomicInteger consecutiveHealthy = new AtomicInteger();
    private volatile boolean replicaUsable;
    private volatile long lastLagMillis = -1;
    private volatile ScheduledExecutorService scheduler;

    public ReplicaHealthTracker(LagProbe probe, Duration threshold, Duration interval, int closeAfter,
                                MeterRegistry registry) {
        this.probe = probe;
        this.thresholdNanos = threshold.toNanos();
        this.interval = interval;
        this.closeAfter = closeAfter;
        if (registry != null) {
            registry.gauge("rtb.replica.lag.ms", this, t -> t.lastLagMillis);
            registry.gauge("rtb.replica.usable", this, t -> t.replicaUsable ? 1 : 0);
        }
    }

    public boolean isReplicaUsable() {
        return replicaUsable;
    }

    public long lastLagMillis() {
        return lastLagMillis;
    }

    public void probeOnce() {
        try {
            Duration lag = probe.measure();
            lastLagMillis = lag.toMillis();
            if (lag.toNanos() > thresholdNanos) {
                open("lag " + lag.toMillis() + " ms over threshold");
            } else if (consecutiveHealthy.incrementAndGet() >= closeAfter && !replicaUsable) {
                replicaUsable = true;
                log.info("replica closed: {} consecutive probes under threshold (lag {} ms)", closeAfter, lastLagMillis);
            }
        } catch (Exception e) {
            lastLagMillis = -1;
            open("probe failed: " + e.getMessage());
        }
    }

    private void open(String reason) {
        consecutiveHealthy.set(0);
        if (replicaUsable) {
            replicaUsable = false;
            log.warn("replica opened, reads fall back to primary: {}", reason);
        }
    }

    @Override
    public void start() {
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("replica-lag-probe").factory());
        s.scheduleWithFixedDelay(this::probeOnce, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        scheduler = s;
    }

    @Override
    public void stop() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }
}
