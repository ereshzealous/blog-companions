package com.rtb.dsp.benchmarks;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rtb.dsp.data.SingleFlight;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Latch setup is excluded from measurement. Counters are per stampede, not per reader.
 * Bystander wake-up lateness measures carrier starvation, including monitor-entry contention.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, batchSize = 1)
@Measurement(iterations = 20, batchSize = 1)
@Fork(value = 1, jvmArgsAppend = {"--enable-preview", "-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class JMHSingleFlightCacheStampedeBenchmark {

    static final int READERS = 10_000;
    static final long HOT_CAMPAIGN = 1L;
    static final long SLA_NANOS = Duration.ofMillis(50).toNanos();

    @Param({"NAIVE", "SINGLE_FLIGHT", "CAFFEINE_COMPUTE"})
    public String strategy;

    @Param({"2", "20"})
    public int queryMillis;

    private SimulatedReplica replica;
    private Cache<Long, Long> l1;
    private SingleFlight<Long, Long> flight;
    private CountDownLatch go;
    private CountDownLatch done;
    private List<Thread> readers;
    private final LongAdder poolTimeouts = new LongAdder();
    private final LongAdder slaMisses = new LongAdder();
    private final AtomicLong bystanderStallNanos = new AtomicLong();
    private volatile boolean stampedeOver;
    private Thread bystander;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Stampede {
        public long dbQueries;
        public long poolTimeouts;
        public long slaMisses;
        public double bystanderStallMs;
    }

    @Setup(Level.Trial)
    public void trial() {
        replica = new SimulatedReplica(40, Duration.ofMillis(queryMillis), Duration.ofMillis(250));
        l1 = Caffeine.newBuilder().maximumSize(20_000).expireAfterWrite(Duration.ofSeconds(10)).build();
        flight = new SingleFlight<>("bench");
    }

    @Setup(Level.Invocation)
    public void parkReaders() throws InterruptedException {
        l1.invalidateAll();
        replica.queries.set(0);
        poolTimeouts.reset();
        slaMisses.reset();
        go = new CountDownLatch(1);
        done = new CountDownLatch(READERS);
        CountDownLatch parked = new CountDownLatch(READERS);
        readers = new ArrayList<>(READERS);
        for (int i = 0; i < READERS; i++) {
            readers.add(Thread.ofVirtual().start(() -> {
                parked.countDown();
                try {
                    go.await();
                    long start = System.nanoTime();
                    read();
                    if (System.nanoTime() - start > SLA_NANOS) {
                        slaMisses.increment();
                    }
                } catch (PoolTimeoutException e) {
                    poolTimeouts.increment();
                    slaMisses.increment();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                } finally {
                    done.countDown();
                }
            }));
        }
        parked.await();
        stampedeOver = false;
        bystanderStallNanos.set(0);
        bystander = Thread.ofVirtual().start(() -> {
            try {
                go.await();
                while (!stampedeOver) {
                    long expected = System.nanoTime() + 1_000_000;
                    Thread.sleep(1);
                    bystanderStallNanos.accumulateAndGet(System.nanoTime() - expected, Math::max);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Benchmark
    public void hotKeyExpires(Stampede metrics) throws InterruptedException {
        go.countDown();
        done.await();
        metrics.dbQueries = replica.queries.get();
        metrics.poolTimeouts = poolTimeouts.sum();
        metrics.slaMisses = slaMisses.sum();
        stampedeOver = true;
        metrics.bystanderStallMs = bystanderStallNanos.get() / 1e6;
    }

    @TearDown(Level.Invocation)
    public void joinReaders() throws InterruptedException {
        for (Thread t : readers) {
            t.join();
        }
        bystander.join();
    }

    private long read() throws Exception {
        Long cached = strategy.equals("CAFFEINE_COMPUTE") ? null : l1.getIfPresent(HOT_CAMPAIGN);
        if (cached != null) {
            return cached;
        }
        return switch (strategy) {
            case "NAIVE" -> {
                long v = replica.load(HOT_CAMPAIGN);
                l1.put(HOT_CAMPAIGN, v);
                yield v;
            }
            case "SINGLE_FLIGHT" -> flight.execute(HOT_CAMPAIGN, () -> {
                long v = replica.load(HOT_CAMPAIGN);
                l1.put(HOT_CAMPAIGN, v);
                return v;
            }, Duration.ofSeconds(5));
            case "CAFFEINE_COMPUTE" -> l1.get(HOT_CAMPAIGN, replica::loadUnchecked);
            default -> throw new IllegalArgumentException(strategy);
        };
    }

    static final class PoolTimeoutException extends RuntimeException {
        PoolTimeoutException() {
            super("connection not available within the pool timeout", null, false, false);
        }
    }

    /** A replica behind a bounded connection pool. Virtual threads unmount while they wait. */
    static final class SimulatedReplica {
        final AtomicLong queries = new AtomicLong();
        private final Semaphore pool;
        private final Duration queryTime;
        private final long poolTimeoutNanos;

        SimulatedReplica(int connections, Duration queryTime, Duration poolTimeout) {
            this.pool = new Semaphore(connections, true);
            this.queryTime = queryTime;
            this.poolTimeoutNanos = poolTimeout.toNanos();
        }

        long load(long campaignId) throws InterruptedException {
            if (!pool.tryAcquire(poolTimeoutNanos, TimeUnit.NANOSECONDS)) {
                throw new PoolTimeoutException();
            }
            try {
                queries.incrementAndGet();
                Thread.sleep(queryTime);
                return 1_000_000_000L + campaignId;
            } finally {
                pool.release();
            }
        }

        Long loadUnchecked(Long campaignId) {
            try {
                return load(campaignId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
