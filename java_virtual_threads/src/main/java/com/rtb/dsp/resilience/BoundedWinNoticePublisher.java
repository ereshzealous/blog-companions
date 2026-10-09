package com.rtb.dsp.resilience;

import com.rtb.dsp.model.WinNotice;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Keeps Kafka blocking off request threads. The drainer retries unacknowledged batches;
 * queue acceptance is volatile and does not imply durable broker acknowledgment.
 */
@Slf4j
public class BoundedWinNoticePublisher implements SmartLifecycle {

    @FunctionalInterface
    public interface BatchSink {
        /** Returns once every notice in the batch is acknowledged; throws if any is not. */
        void publish(List<WinNotice> batch) throws Exception;
    }

    private final ArrayBlockingQueue<WinNotice> queue;
    private final BatchSink sink;
    private final long flushNanos;
    private final int maxBatch;
    private final Duration offerTimeout;
    private final LongAdder published = new LongAdder();
    private final LongAdder batches = new LongAdder();
    private final Counter rejected;
    private volatile boolean running;
    private volatile Thread drainer;

    public BoundedWinNoticePublisher(int capacity, Duration flushInterval, int maxBatch, Duration offerTimeout,
                                     BatchSink sink, MeterRegistry registry) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.sink = sink;
        this.flushNanos = flushInterval.toNanos();
        this.maxBatch = maxBatch;
        this.offerTimeout = offerTimeout;
        this.rejected = registry.counter("rtb.win_notices.rejected");
        registry.gauge("rtb.win_notices.queue.depth", queue, ArrayBlockingQueue::size);
        registry.gauge("rtb.win_notices.published", published, LongAdder::sum);
    }

    /** Waits at most {@code offerTimeout} for space; false means "tell the caller to retry". */
    public boolean offer(WinNotice notice) throws InterruptedException {
        if (queue.offer(notice, offerTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
            return true;
        }
        rejected.increment();
        return false;
    }

    @Override
    public void start() {
        running = true;
        drainer = Thread.ofPlatform().name("win-notice-drainer").daemon().start(this::drainLoop);
    }

    @Override
    public void stop() {
        running = false;
        Thread t = drainer;
        if (t != null) {
            try {
                t.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        drainer = null;
    }

    @Override
    public boolean isRunning() {
        return drainer != null;
    }

    @Override
    public int getPhase() {
        // Stop after the web server (which stops at a higher phase) so in-flight notices drain.
        return Integer.MAX_VALUE - 2048;
    }

    private void drainLoop() {
        List<WinNotice> batch = new ArrayList<>(maxBatch);
        while (running || !queue.isEmpty()) {
            try {
                collect(batch);
                if (!batch.isEmpty()) {
                    publishWithRetry(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void collect(List<WinNotice> batch) throws InterruptedException {
        WinNotice first = queue.poll(flushNanos, TimeUnit.NANOSECONDS);
        if (first == null) {
            return;
        }
        batch.add(first);
        long deadline = System.nanoTime() + flushNanos;
        while (batch.size() < maxBatch) {
            queue.drainTo(batch, maxBatch - batch.size());
            long left = deadline - System.nanoTime();
            if (batch.size() >= maxBatch || left <= 0) {
                return;
            }
            WinNotice next = queue.poll(left, TimeUnit.NANOSECONDS);
            if (next == null) {
                return;
            }
            batch.add(next);
        }
    }

    private void publishWithRetry(List<WinNotice> batch) throws InterruptedException {
        long backoffMillis = 50;
        while (true) {
            try {
                sink.publish(batch);
                published.add(batch.size());
                batches.increment();
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                log.warn("win-notice batch of {} not acknowledged, retrying in {} ms", batch.size(), backoffMillis, e);
                Thread.sleep(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, 2_000);
            }
        }
    }

    public int depth() {
        return queue.size();
    }

    public long publishedCount() {
        return published.sum();
    }

    public long batchCount() {
        return batches.sum();
    }
}
