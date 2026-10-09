package com.rtb.dsp.benchmarks;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Each invocation runs a full SurgeSimulation.brownout episode. Auxiliary counters report
 * episode outcomes, including separate admitted-only and all-response latency percentiles.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, batchSize = 1)
@Measurement(iterations = 5, batchSize = 1)
@Fork(value = 1, jvmArgsAppend = {"--enable-preview", "-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class JMHAdaptiveLimiterBenchmark {

    static final int ARRIVALS_PER_SECOND = 10_000;
    static final int DOWNSTREAM_CAPACITY = 256;

    @Param({"STATIC_SEMAPHORE_5000", "AIMD_STEP8_MIN50", "AIMD_STEP32_MIN16"})
    public String gate;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Episode {
        public double p50AdmittedMs;
        public double p99AdmittedMs;
        public double p99AllMs;
        public double timeoutPct;
        public double shedPct;
        public double goodputPct;
        public double peakInFlight;
        public double recoveryMs;
    }

    @Benchmark
    public void latencySurge(Episode episode) throws InterruptedException {
        SurgeSimulation.Result r = switch (gate) {
            case "AIMD_STEP8_MIN50" -> SurgeSimulation.runAimd(ARRIVALS_PER_SECOND, DOWNSTREAM_CAPACITY, 8, 50);
            case "AIMD_STEP32_MIN16" -> SurgeSimulation.runAimd(ARRIVALS_PER_SECOND, DOWNSTREAM_CAPACITY, 32, 16);
            default -> SurgeSimulation.runStatic(ARRIVALS_PER_SECOND, DOWNSTREAM_CAPACITY);
        };
        episode.p50AdmittedMs = r.p50AdmittedMs();
        episode.p99AdmittedMs = r.p99AdmittedMs();
        episode.p99AllMs = r.p99AllMs();
        episode.timeoutPct = r.exchangeTimeoutPercent();
        episode.shedPct = r.shedPercent();
        episode.goodputPct = r.goodputPercent();
        episode.peakInFlight = r.peakInFlight();
        episode.recoveryMs = r.recoveryMs();
    }
}
