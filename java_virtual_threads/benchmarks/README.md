# Benchmark and test evidence

Open [benchmark-results.html](results/benchmark-results.html) for three experiments, two HTTP smoke runs,
run commands and raw output. On GitHub, download the raw HTML first.

## Read and reproduce

From `java_virtual_threads/`, rebuild the saved report:

```bash
python3 tools/build_benchmark_report.py
```

Shortcut: **`make report`**. Output: **`benchmarks/results/benchmark-results.html`**.

The [generator](../tools/build_benchmark_report.py) reads saved measurements and embeds the charts,
raw output, fonts and styles. It needs only Python 3.10+ and does not rerun benchmarks.
Use `--output PATH` for another destination.

To collect new measurements:

```bash
./gradlew jmh
./gradlew surgeBenchmark
```

For live bidder/Docker metrics and run history, start `python3 tools/benchmark_console.py serve`
and open http://127.0.0.1:8090. See [setup and tests](../README.md#run-locally).

## Recorded observations

| Experiment | Saved observation | Interpretation |
|---|---|---|
| Context binding, new virtual thread per task | ThreadLocal 173.9 ns/task; ScopedValue 177.1 ns/task | Timing error bars overlap; no demonstrated advantage in this task shape |
| Hot-key miss, simulated 20 ms load | Naive: mean 557 queries and 8,890 pool timeouts; SingleFlight: one query, no pool timeouts | Coalescing removes duplicate dependency work |
| Simulated surge, fixed versus tuned gate | Timeouts 21.0% → 1.3%; shedding 12.4% → 32.4%; bids within 50 ms 66.0% → 66.3% | Failure behavior improves substantially; useful completion rate changes little |
| Same-host HTTP smoke tests | 33,164.07 and 32,050.12 requests/s; client p99 69 and 83 ms | Short local baselines, not sustained capacity within the auction deadline |

JMH event counters use the mean of raw episode samples, not the summed event score.
Surge CSVs contain representative episodes, separate from those means. The tuned gate's overall
p99 is about 93 ms; admitted-only p99 remains about 150 ms.

## Files

| File | Contents |
|---|---|
| [benchmark-results.html](results/benchmark-results.html) | Offline report: three experiments, two HTTP smoke runs, commands and raw output |
| [jmh-results.json](results/jmh-results.json) | All 13 JMH configurations, raw samples, error estimates and GC allocation metrics |
| [jmh-human.txt](results/jmh-human.txt) | Human-readable JMH output (workstation path normalized) |
| [jmh-run.log](results/jmh-run.log) | Historical Gradle/JMH execution log |
| [surge-summary.txt](results/surge-summary.txt) | Representative simulation summary |
| [surge-timeline.csv](results/surge-timeline.csv) | Concurrency and admission limit every 50 ms |
| [surge-outcomes.csv](results/surge-outcomes.csv) | Arrival/outcome counts per 100 ms bucket |
| [stack-smoke.md](results/stack-smoke.md) | Historical Compose smoke notes, including physical replication and ledger checks |
| [ab-smoke.txt](results/ab-smoke.txt), [stack-smoke-2.txt](results/stack-smoke-2.txt) | Original ApacheBench output |
| [results-provenance.json](results-provenance.json) | Original and publication hashes; path normalization details |
| [validation/](validation/) | Test results, environment and source hashes for the packaged project |

**Recorded environment:** Apple M5 Pro, 15 cores, 24 GB; macOS 26.5, OpenJDK 21.0.10,
Docker 29.8 and Gradle 8.14.3. Workstation paths in `jmh-human.txt`, `jmh-run.log` and
`surge-summary.txt` were shortened for publication. Measurements are unchanged; provenance records both hashes.

Smoke notes report response-length mismatches. Successful-bid throughput still requires HTTP status
and business-outcome accounting. One run exceeded the 80 ms exchange timeout at client p99.

## What a new run captures

Each JMH or surge run writes `benchmarks/runs/<run-id>/` with:

- A manifest: command, timestamps, run status, host metadata, source hashes and artifact hashes.
- Original JMH JSON/TXT or surge CSV/TXT results.
- `report.json` and a standalone offline `report.html`, rendered from that run's data.

The latest attempt appears at `build/reports/benchmarks/latest.html`. Failed runs never substitute
older results. A forcibly stopped process may leave a `running` manifest; treat it as incomplete.

The console reader supports JMH JSON/JSONL, the supplied surge CSV schemas and ApacheBench TXT.
`-PjmhVerify=true` checks capture and rendering; its short run is not performance evidence.

Every five seconds, the console records bidder Actuator/Prometheus metrics and `rtb-dsp` Docker
stats to `benchmarks/captures/` as JSONL. These describe the bidder JVM, not the forked JMH JVM.
Live HTTP latency is an interval mean; client p99, distributed tracing and fleet telemetry are not implemented.
Runs and captures are ignored by Git and survive `gradlew clean`.

## Capacity remains an open experiment

The **1.2M requests/s fleet target remains unmeasured**. Test multiple instances behind a load balancer
with a separate constant-rate generator. Measure useful completions and client tail latency under steady
load, cold caches, replica lag and slow dependencies. Multiplying laptop throughput by pod count is not proof.
