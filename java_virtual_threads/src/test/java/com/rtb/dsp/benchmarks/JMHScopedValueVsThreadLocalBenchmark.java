package com.rtb.dsp.benchmarks;

import com.rtb.dsp.compute.AuctionContext;
import com.rtb.dsp.compute.AuctionContextData;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * OperationsPerInvocation normalizes batch time and GC allocation to one task.
 * Compare reused-thread bindings separately from virtual-thread creation per task.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"--enable-preview", "-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class JMHScopedValueVsThreadLocalBenchmark {

    static final int TASKS = 100_000;
    static final ThreadLocal<AuctionContextData> THREAD_LOCAL = new ThreadLocal<>();

    private final AuctionContextData auction = new AuctionContextData("bench-1", "exchange-a", "2.6",
            System.nanoTime(), System.nanoTime() + 50_000_000, Instant.now().plusMillis(50));

    @Benchmark
    @OperationsPerInvocation(TASKS)
    public void threadLocal_reusedThread(Blackhole bh) {
        for (int i = 0; i < TASKS; i++) {
            THREAD_LOCAL.set(auction);
            try {
                bh.consume(readThreadLocal(4));
            } finally {
                THREAD_LOCAL.remove();
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TASKS)
    public void scopedValue_reusedThread(Blackhole bh) {
        for (int i = 0; i < TASKS; i++) {
            ScopedValue.where(AuctionContext.CURRENT, auction).run(() -> bh.consume(readScoped(4)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TASKS)
    public long threadLocal_virtualThreadPerTask() throws InterruptedException {
        LongAdder sink = new LongAdder();
        CountDownLatch done = new CountDownLatch(TASKS);
        for (int i = 0; i < TASKS; i++) {
            Thread.startVirtualThread(() -> {
                THREAD_LOCAL.set(auction);
                try {
                    sink.add(readThreadLocal(4));
                } finally {
                    THREAD_LOCAL.remove();
                    done.countDown();
                }
            });
        }
        done.await();
        return sink.sum();
    }

    @Benchmark
    @OperationsPerInvocation(TASKS)
    public long scopedValue_virtualThreadPerTask() throws InterruptedException {
        LongAdder sink = new LongAdder();
        CountDownLatch done = new CountDownLatch(TASKS);
        for (int i = 0; i < TASKS; i++) {
            Thread.startVirtualThread(() -> {
                try {
                    ScopedValue.where(AuctionContext.CURRENT, auction).run(() -> sink.add(readScoped(4)));
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
        return sink.sum();
    }

    private static long readThreadLocal(int depth) {
        if (depth == 0) {
            return THREAD_LOCAL.get().deadlineNanos();
        }
        return readThreadLocal(depth - 1) + depth;
    }

    private static long readScoped(int depth) {
        if (depth == 0) {
            return AuctionContext.CURRENT.get().deadlineNanos();
        }
        return readScoped(depth - 1) + depth;
    }
}
