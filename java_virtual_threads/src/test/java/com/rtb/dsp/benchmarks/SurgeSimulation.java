package com.rtb.dsp.benchmarks;

import com.rtb.dsp.resilience.AdaptiveAimdLimiter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Arrivals follow a fixed schedule. Latency starts at scheduled arrival, including queueing
 * before admission, to avoid coordinated omission.
 */
public final class SurgeSimulation {

    public interface Gate {
        boolean tryAcquire();

        void release(long latencyNanos);

        int limit();

        int inFlight();
    }

    public record Phase(Duration length, Duration serviceTime) {
    }

    public record Tick(long atMillis, int limit, int inFlight, int queued, long served, long shed) {
    }

    /** What happened to the requests that were due in one 100 ms slice, as the exchange saw it. */
    public record Bucket(long startMillis, int arrivals, int bidInSla, int lateBid, int timedOut, int shed) {
    }

    public record Result(String gate, int requests, double p50AdmittedMs, double p99AdmittedMs, double p999AdmittedMs,
                         double p99AllMs, double exchangeTimeoutPercent, double shedPercent, double goodputPercent,
                         int peakInFlight, double recoveryMs, List<Tick> timeline, List<Bucket> outcomes) {

        String row() {
            return String.format(Locale.ROOT, "%-22s %8d %10.1f %10.1f %10.1f %10.1f %9.1f%% %8.1f%% %9.1f%% %8d %10.0f",
                    gate, requests, p50AdmittedMs, p99AdmittedMs, p999AdmittedMs, p99AllMs, exchangeTimeoutPercent,
                    shedPercent, goodputPercent, peakInFlight, recoveryMs);
        }
    }

    public static Gate staticSemaphore(int permits) {
        Semaphore s = new Semaphore(permits);
        return new Gate() {
            public boolean tryAcquire() {
                return s.tryAcquire();
            }

            public void release(long latencyNanos) {
                s.release();
            }

            public int limit() {
                return permits;
            }

            public int inFlight() {
                return permits - s.availablePermits();
            }
        };
    }

    public static Gate aimd(AdaptiveAimdLimiter limiter) {
        return new Gate() {
            public boolean tryAcquire() {
                return limiter.tryAcquire();
            }

            public void release(long latencyNanos) {
                limiter.release(latencyNanos, AdaptiveAimdLimiter.Outcome.SUCCESS);
            }

            public int limit() {
                return limiter.limit();
            }

            public int inFlight() {
                return limiter.inFlight();
            }
        };
    }

    public static List<Phase> brownout() {
        return List.of(new Phase(Duration.ofMillis(1000), Duration.ofMillis(5)),
                new Phase(Duration.ofMillis(1000), Duration.ofMillis(150)),
                new Phase(Duration.ofMillis(1500), Duration.ofMillis(5)));
    }

    public static AdaptiveAimdLimiter.Config aimdConfig(int arrivalsPerSecond, int additiveStep, int minWindowSamples) {
        return AdaptiveAimdLimiter.Config.fromLittlesLaw(arrivalsPerSecond, Duration.ofMillis(5), 8, 5_000,
                Duration.ofMillis(35), 0.7, additiveStep, Duration.ofMillis(100), minWindowSamples);
    }

    public static Result run(String name, Gate gate, int arrivalsPerSecond, List<Phase> phases, int capacity,
                             Duration sla, Duration tmax) throws InterruptedException {
        long totalNanos = phases.stream().mapToLong(p -> p.length().toNanos()).sum();
        int requests = (int) (arrivalsPerSecond * totalNanos / 1_000_000_000L);
        long interval = 1_000_000_000L / arrivalsPerSecond;
        long[] scheduled = new long[requests];
        long[] latency = new long[requests];
        byte[] outcome = new byte[requests];  // 1 = served, 2 = shed
        Semaphore downstream = new Semaphore(capacity, true);
        AtomicInteger peak = new AtomicInteger();
        List<Tick> timeline = new ArrayList<>();
        long[] counts = new long[2];

        long start = System.nanoTime() + 5_000_000;
        Thread sampler = Thread.ofPlatform().daemon().start(() -> {
            long next = start;
            while (!Thread.currentThread().isInterrupted()) {
                LockSupport.parkNanos(next - System.nanoTime());
                long served = 0, shed = 0;
                for (int i = 0; i < requests; i++) {
                    if (outcome[i] == 1) served++;
                    else if (outcome[i] == 2) shed++;
                }
                timeline.add(new Tick((next - start) / 1_000_000, gate.limit(), gate.inFlight(),
                        downstream.getQueueLength(), served, shed));
                next += 50_000_000;
                if (next - start > totalNanos + 3_000_000_000L) {
                    return;
                }
            }
        });

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            int launched = 0;
            while (launched < requests) {
                long due = Math.min(requests, (System.nanoTime() - start) / interval + 1);
                for (; launched < due; launched++) {
                    int id = launched;
                    long at = start + id * interval;
                    scheduled[id] = at;
                    pool.execute(() -> handle(id, at, start, gate, downstream, phases, latency, outcome, peak));
                }
                LockSupport.parkNanos(200_000);
            }
        }
        sampler.interrupt();
        sampler.join();

        return summarise(name, requests, scheduled, latency, outcome, start, phases, sla, tmax, peak.get(),
                List.copyOf(timeline));
    }

    private static void handle(int id, long at, long origin, Gate gate, Semaphore downstream, List<Phase> phases,
                               long[] latency, byte[] outcome, AtomicInteger peak) {
        long waitUntil = at - System.nanoTime();
        if (waitUntil > 0) {
            LockSupport.parkNanos(waitUntil);
        }
        if (!gate.tryAcquire()) {
            latency[id] = System.nanoTime() - at;
            outcome[id] = 2;
            return;
        }
        int now = gate.inFlight();
        peak.accumulateAndGet(now, Math::max);
        long admitted = System.nanoTime();
        try {
            downstream.acquireUninterruptibly();
            try {
                Thread.sleep(serviceTime(phases, origin));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                downstream.release();
            }
        } finally {
            long done = System.nanoTime();
            gate.release(done - admitted);
            latency[id] = done - at;
            outcome[id] = 1;
        }
    }

    /** The phase is decided by when service starts, so a request queued into the surge gets surge latency. */
    private static Duration serviceTime(List<Phase> phases, long origin) {
        long sinceStart = System.nanoTime() - origin;
        long boundary = 0;
        for (Phase p : phases) {
            boundary += p.length().toNanos();
            if (sinceStart < boundary) {
                return p.serviceTime();
            }
        }
        return phases.getLast().serviceTime();
    }

    private static Result summarise(String name, int requests, long[] scheduled, long[] latency, byte[] outcome,
                                    long start, List<Phase> phases, Duration sla, Duration tmax, int peak,
                                    List<Tick> timeline) {
        long slaNanos = sla.toNanos(), tmaxNanos = tmax.toNanos();
        long[] admitted = new long[requests];
        long[] all = Arrays.copyOf(latency, requests);
        int nAdmitted = 0, shed = 0, timeouts = 0, good = 0;
        for (int i = 0; i < requests; i++) {
            if (outcome[i] == 1) {
                admitted[nAdmitted++] = latency[i];
                if (latency[i] <= slaNanos) good++;
            } else {
                shed++;
            }
            if (latency[i] > tmaxNanos) timeouts++;
        }
        long[] adm = Arrays.copyOf(admitted, nAdmitted);
        Arrays.sort(adm);
        Arrays.sort(all);

        long surgeEnd = start + phases.get(0).length().toNanos() + phases.get(1).length().toNanos();
        double recoveryMs = recovery(scheduled, latency, outcome, start, surgeEnd, slaNanos,
                phases.get(0).length().toNanos());

        return new Result(name, requests, ms(pct(adm, 0.50)), ms(pct(adm, 0.99)), ms(pct(adm, 0.999)),
                ms(pct(all, 0.99)), 100.0 * timeouts / requests, 100.0 * shed / requests, 100.0 * good / requests,
                peak, recoveryMs, timeline, outcomes(scheduled, latency, outcome, start, slaNanos, tmaxNanos));
    }

    /**
     * Time from the end of the surge until a 100 ms bucket of arrivals is again answered with a bid
     * inside the SLA at 90% of the pre-surge rate.
     */
    private static double recovery(long[] scheduled, long[] latency, byte[] outcome, long start, long surgeEnd,
                                   long slaNanos, long baselineNanos) {
        long bucket = 100_000_000;
        int buckets = (int) ((scheduled[scheduled.length - 1] - start) / bucket) + 1;
        long[] arrivals = new long[buckets], good = new long[buckets];
        for (int i = 0; i < scheduled.length; i++) {
            int b = (int) ((scheduled[i] - start) / bucket);
            arrivals[b]++;
            if (outcome[i] == 1 && latency[i] <= slaNanos) good[b]++;
        }
        int baseBuckets = (int) (baselineNanos / bucket);
        double baseline = 0;
        for (int b = 2; b < baseBuckets; b++) {
            baseline += (double) good[b] / Math.max(1, arrivals[b]);
        }
        baseline /= Math.max(1, baseBuckets - 2);
        int first = (int) ((surgeEnd - start) / bucket);
        for (int b = first; b < buckets; b++) {
            if ((double) good[b] / Math.max(1, arrivals[b]) >= 0.9 * baseline) {
                return (b - first) * 100.0;
            }
        }
        return (buckets - first) * 100.0;
    }

    private static List<Bucket> outcomes(long[] scheduled, long[] latency, byte[] outcome, long start, long slaNanos,
                                         long tmaxNanos) {
        long bucket = 100_000_000;
        int n = (int) ((scheduled[scheduled.length - 1] - start) / bucket) + 1;
        int[][] c = new int[n][5];
        for (int i = 0; i < scheduled.length; i++) {
            int b = (int) ((scheduled[i] - start) / bucket);
            c[b][0]++;
            if (outcome[i] != 1) {
                c[b][4]++;
            } else if (latency[i] > tmaxNanos) {
                c[b][3]++;
            } else if (latency[i] > slaNanos) {
                c[b][2]++;
            } else {
                c[b][1]++;
            }
        }
        List<Bucket> out = new ArrayList<>(n);
        for (int b = 0; b < n; b++) {
            out.add(new Bucket(b * 100L, c[b][0], c[b][1], c[b][2], c[b][3], c[b][4]));
        }
        return out;
    }

    private static long pct(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        return sorted[(int) Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1)];
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    public static Result runStatic(int rps, int capacity) throws InterruptedException {
        return run("static semaphore 5000", staticSemaphore(5_000), rps, brownout(), capacity,
                Duration.ofMillis(50), Duration.ofMillis(80));
    }

    public static Result runAimd(int rps, int capacity, int additiveStep, int minWindowSamples)
            throws InterruptedException {
        return run("AIMD +" + additiveStep + "/" + minWindowSamples + " samples",
                aimd(new AdaptiveAimdLimiter(aimdConfig(rps, additiveStep, minWindowSamples))), rps, brownout(),
                capacity, Duration.ofMillis(50), Duration.ofMillis(80));
    }

    public static void main(String[] args) throws Exception {
        int rps = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int capacity = args.length > 1 ? Integer.parseInt(args[1]) : 256;
        Path csv = Path.of(args.length > 2 ? args[2] : "build/results/surge-timeline.csv");

        runStatic(rps, capacity);  // warm-up
        runAimd(rps, capacity, 32, 16);
        Result fixed = runStatic(rps, capacity);
        Result adaptive = runAimd(rps, capacity, 8, 50);
        Result tuned = runAimd(rps, capacity, 32, 16);

        System.out.printf(Locale.ROOT, "%d req/s for 3.5 s, downstream capacity %d, SLA 50 ms, tmax 80 ms%n", rps, capacity);
        System.out.printf(Locale.ROOT, "%-22s %8s %10s %10s %10s %10s %10s %9s %10s %8s %10s%n", "gate", "requests",
                "p50 adm", "p99 adm", "p99.9 adm", "p99 all", "timeouts", "shed", "goodput", "peak", "recovery");
        System.out.println(fixed.row());
        System.out.println(adaptive.row());
        System.out.println(tuned.row());

        Files.createDirectories(csv.toAbsolutePath().getParent());
        StringBuilder out = new StringBuilder("gate,t_ms,limit,in_flight,queued,served,shed\n");
        for (Result r : List.of(fixed, adaptive, tuned)) {
            for (Tick t : r.timeline()) {
                out.append(r.gate()).append(',').append(t.atMillis()).append(',').append(t.limit()).append(',')
                        .append(t.inFlight()).append(',').append(t.queued()).append(',').append(t.served())
                        .append(',').append(t.shed()).append('\n');
            }
        }
        Files.writeString(csv, out);

        StringBuilder buckets = new StringBuilder("gate,bucket_ms,arrivals,bid_in_sla,late_bid,timed_out,shed\n");
        for (Result r : List.of(fixed, adaptive, tuned)) {
            for (Bucket b : r.outcomes()) {
                buckets.append(r.gate()).append(',').append(b.startMillis()).append(',').append(b.arrivals()).append(',')
                        .append(b.bidInSla()).append(',').append(b.lateBid()).append(',').append(b.timedOut()).append(',')
                        .append(b.shed()).append('\n');
            }
        }
        Path outcomesCsv = csv.resolveSibling("surge-outcomes.csv");
        Files.writeString(outcomesCsv, buckets);
        System.out.println("outcomes -> " + outcomesCsv.toAbsolutePath());
        System.out.println("timeline -> " + csv.toAbsolutePath());
    }

    private SurgeSimulation() {
    }
}
