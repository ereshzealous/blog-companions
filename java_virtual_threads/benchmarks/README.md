# Benchmark and test evidence

`results/` contains the saved measurements and the [HTML results report](results/benchmark-results.html).
Download the raw HTML from GitHub and open it in your browser, or open it from a local clone.
New runs go into ignored `runs/` directories;
continuous local telemetry goes into ignored `captures/` directories. They survive `gradlew clean`.

## Read and reproduce

From the project directory:

```bash
python3 tools/build_benchmark_report.py
# Equivalent: make report
./gradlew clean test
python3 -m unittest discover -s tools -p test_benchmark_console.py
./gradlew jmh
./gradlew surgeBenchmark
```

The [build script](../tools/build_benchmark_report.py) writes `benchmarks/results/benchmark-results.html`
using [the shared renderer](../tools/benchmark_report.py), the recorded result files, and the bundled
[styles and fonts](../tools/report/). Tables, CSS charts and raw output are generated from the evidence;
the result is a standalone HTML file with no runtime network requests or JavaScript.
This rebuild does not rerun any benchmark. An alternate destination can be set with `--output PATH`.

To follow new runs
and see live bidder/Docker metrics, start `python3 tools/benchmark_console.py serve` and open
http://127.0.0.1:8090. The bidder must be running for live JVM/HTTP measurements.

## Recorded observations

| Experiment | Saved observation | Interpretation |
|---|---|---|
| Context binding, new virtual thread per task | ThreadLocal 173.9 ns/task; ScopedValue 177.1 ns/task | Timing error bars overlap; no demonstrated advantage in this task shape |
| Hot-key miss, simulated 20 ms load | Naive: mean 557 queries and 8,890 pool timeouts; SingleFlight: one query, no pool timeouts | Coalescing removes duplicate dependency work |
| Simulated surge, fixed versus tuned gate | Timeouts 21.0% → 1.3%; shedding 12.4% → 32.4%; bids within 50 ms 66.0% → 66.3% | Failure behavior improves substantially; useful completion rate changes little |
| Same-host HTTP smoke tests | 33,164.07 and 32,050.12 requests/s; client p99 69 and 83 ms | Short local baselines, not sustained capacity within the auction deadline |

JMH auxiliary event counters are reported here as the mean of raw episode samples, not as
JMH's summed event score. The separate surge CSV contains representative episodes, not the
samples used to estimate those JMH means. The tuned surge gate's overall p99 is about 93 ms,
while admitted-only p99 remains about 150 ms.

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

The original benchmark machine was an Apple M5 Pro (15 cores), 24 GB, macOS 26.5, OpenJDK
21.0.10, Docker 29.8 and Gradle 8.14.3. Only workstation directory prefixes in `jmh-human.txt`,
`jmh-run.log` and `surge-summary.txt` were replaced with relative paths for publication; measurements are unchanged.

Historical smoke notes attribute ApacheBench failures to response-length differences. Those
notes are retained as recorded; status and business-outcome accounting is still needed before
claiming a successful-bid rate. One smoke run exceeded the 80 ms exchange timeout at client p99.

## What a new run captures

Each `./gradlew jmh` or `./gradlew surgeBenchmark` invocation creates a unique run directory with:

- A manifest: command, timestamps, run status, host metadata, source hashes and artifact hashes.
- Original JMH JSON/TXT or surge CSV/TXT results.
- `report.json` and a standalone offline `report.html`, rendered from that run's data.

The latest attempted run is also exported to `build/reports/benchmarks/latest.html`. A failed
run remains failed; its report never substitutes an earlier successful dataset. If a process
is forcibly killed, its manifest may remain `running` and must be treated as incomplete.

The reader accepts JMH JSON arrays or JSONL containing complete JMH result objects, the supplied
surge CSV schemas, and ApacheBench TXT. It does not infer metrics from arbitrary log files.
`-PjmhVerify=true` selects a short pipeline check, not a performance experiment.

The console samples Actuator/Prometheus metrics and Docker stats every five seconds, writing
JSONL in `captures/`. It restricts Docker sampling to the `rtb-dsp` Compose project. Bidder JVM
telemetry is separate from forked JMH measurements. Live HTTP latency is an interval mean;
client p99, distributed tracing and fleet-wide telemetry are not implemented.

## Capacity remains an open experiment

The **1.2M requests/s fleet target has not been measured**. A credible next test needs a separate
constant-rate generator, several bidder instances behind a load balancer, and outcome-classified
client latency under steady load, cold caches, replica lag and slow dependencies. Scaling the
laptop's peak by a pod count is not evidence that the fleet will meet its deadline.
