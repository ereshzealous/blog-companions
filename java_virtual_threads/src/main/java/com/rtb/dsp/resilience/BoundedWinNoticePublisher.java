package com.rtb.dsp.resilience;

import com.rtb.dsp.model.WinNotice;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keeps Kafka blocking off request threads. Receipts succeed only after broker acknowledgment.
 */
@Slf4j
public class BoundedWinNoticePublisher implements SmartLifecycle {

    @FunctionalInterface
    public interface BatchSink {
        /** Returns once every notice in the batch is acknowledged; throws if any is not. */
        void publish(List<WinNotice> batch) throws Exception;
    }

    private record Pending(WinNotice notice, CompletableFuture<Void> acknowledgment) {
    }

    private final ArrayBlockingQueue<Pending> queue;
    private final Set<Pending> pending = ConcurrentHashMap.newKeySet();
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final BatchSink sink;
    private final long flushNanos;
    private final int maxBatch;
    private final Duration offerTimeout;
    private final Duration responseTimeout;
    private final LongAdder published = new LongAdder();
    private final LongAdder batches = new LongAdder();
    private final Counter rejected;
    private volatile boolean running;
    private volatile Thread drainer;

    public BoundedWinNoticePublisher(int capacity, Duration flushInterval, int maxBatch, Duration offerTimeout,
                                     Duration responseTimeout, BatchSink sink, MeterRegistry registry) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.sink = sink;
        this.flushNanos = flushInterval.toNanos();
        this.maxBatch = maxBatch;
        this.offerTimeout = offerTimeout;
        this.responseTimeout = responseTimeout;
        this.rejected = registry.counter("rtb.win_notices.rejected");
        registry.gauge("rtb.win_notices.queue.depth", queue, ArrayBlockingQueue::size);
        registry.gauge("rtb.win_notices.published", published, LongAdder::sum);
    }

    /** A timed-out receipt may still be delivered; callers must retry with the same auction ID. */
    public CompletableFuture<Void> submit(WinNotice notice) throws InterruptedException {
        lifecycle.readLock().lockInterruptibly();
        try {
            if (!running) {
                return reject("Win publisher is stopped");
            }
            Pending entry = new Pending(notice, new CompletableFuture<>());
            pending.add(entry);
            try {
                if (!queue.offer(entry, offerTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    pending.remove(entry);
                    return reject("Win queue is full");
                }
            } catch (InterruptedException e) {
                pending.remove(entry);
                throw e;
            }
            return entry.acknowledgment().copy().orTimeout(responseTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private CompletableFuture<Void> reject(String reason) {
        rejected.increment();
        return CompletableFuture.failedFuture(new RejectedExecutionException(reason));
    }

    @Override
    public void start() {
        lifecycle.writeLock().lock();
        try {
            if (drainer != null && drainer.isAlive()) {
                return;
            }
            running = true;
            drainer = Thread.ofPlatform().name("win-notice-drainer").daemon().start(this::drainLoop);
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    public void stop() {
        Thread t;
        lifecycle.writeLock().lock();
        try {
            running = false;
            t = drainer;
        } finally {
            lifecycle.writeLock().unlock();
        }
        if (t != null) {
            try {
                t.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (t.isAlive()) {
                    t.interrupt();
                }
                failPending(new RejectedExecutionException("Win publisher stopped before acknowledgment"));
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Stop after the web server (which stops at a higher phase) so in-flight notices drain.
        return Integer.MAX_VALUE - 2048;
    }

    private void drainLoop() {
        List<Pending> batch = new ArrayList<>(maxBatch);
        try {
            while (running || !queue.isEmpty()) {
                collect(batch);
                if (!batch.isEmpty()) {
                    publishWithRetry(batch);
                    batch.clear();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            failPending(new RejectedExecutionException("Win publisher stopped before acknowledgment"));
        }
    }

    private void failPending(Exception failure) {
        pending.forEach(entry -> entry.acknowledgment().completeExceptionally(failure));
        pending.clear();
        queue.clear();
    }

    private void collect(List<Pending> batch) throws InterruptedException {
        Pending first = queue.poll(flushNanos, TimeUnit.NANOSECONDS);
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
            Pending next = queue.poll(left, TimeUnit.NANOSECONDS);
            if (next == null) {
                return;
            }
            batch.add(next);
        }
    }

    private void publishWithRetry(List<Pending> batch) throws InterruptedException {
        List<WinNotice> notices = batch.stream().map(Pending::notice).toList();
        long backoffMillis = 50;
        while (true) {
            try {
                sink.publish(notices);
                published.add(batch.size());
                batches.increment();
                batch.forEach(entry -> {
                    entry.acknowledgment().complete(null);
                    pending.remove(entry);
                });
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
