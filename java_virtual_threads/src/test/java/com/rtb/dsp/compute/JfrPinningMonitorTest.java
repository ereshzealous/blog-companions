package com.rtb.dsp.compute;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class JfrPinningMonitorTest {

    private final Object lock = new Object();

    /** JDK 24 (JEP 491) removed pinning on synchronized, so this only reproduces on 21-23. */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_21, max = JRE.JAVA_23)
    void reportsVirtualThreadParkedInsideSynchronized() throws Exception {
        var registry = new SimpleMeterRegistry();
        var monitor = new JfrPinningMonitor(registry, Duration.ofMillis(5), 16);
        monitor.start();
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (monitor.totalPinnedEvents() == 0 && System.nanoTime() < deadline) {
                Thread.ofVirtual().start(this::sleepWhileHoldingMonitor).join();
                Thread.sleep(200);
            }
        } finally {
            monitor.stop();
        }

        assertThat(monitor.totalPinnedEvents()).isPositive();
        assertThat(monitor.eventsBySite().keySet())
                .anyMatch(site -> site.startsWith("JfrPinningMonitorTest.sleepWhileHoldingMonitor"));
        assertThat(registry.find("rtb.vthread.pinned").counters()).isNotEmpty();
    }

    /** The waiter never parks, so it raises no pinned event: only jdk.JavaMonitorEnter sees it. */
    @Test
    @EnabledForJreRange(min = JRE.JAVA_21, max = JRE.JAVA_23)
    void reportsVirtualThreadBlockedEnteringAContendedMonitor() throws Exception {
        var monitor = new JfrPinningMonitor(new SimpleMeterRegistry(), Duration.ofMillis(5), 16);
        monitor.start();
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (monitor.totalMonitorBlockedEvents() == 0 && System.nanoTime() < deadline) {
                Thread holder = Thread.ofVirtual().start(this::sleepWhileHoldingMonitor);
                Thread.sleep(5);
                Thread waiter = Thread.ofVirtual().start(() -> {
                    synchronized (lock) {
                        Thread.onSpinWait();
                    }
                });
                holder.join();
                waiter.join();
                Thread.sleep(200);
            }
        } finally {
            monitor.stop();
        }

        assertThat(monitor.totalMonitorBlockedEvents()).isPositive();
    }

    private void sleepWhileHoldingMonitor() {
        synchronized (lock) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
