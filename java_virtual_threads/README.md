# Java virtual threads: a real-time bidder

A Java 21 POC for scoped context, shared cache loads, adaptive admission and asynchronous win settlement.

**Measured:** ~32–33k HTTP requests/s on one laptop; client p99 **69 ms and 83 ms**.
**Unmeasured:** the **1.2M requests/s** fleet target. These short smoke runs do not establish sustained capacity or successful bids per second.

## Saved results

Download [benchmark-results.html](benchmarks/results/benchmark-results.html) and open it in your browser.
It includes three experiments, two HTTP smoke runs, commands and raw output.

To rebuild it, run from `java_virtual_threads/`:

```bash
python3 tools/build_benchmark_report.py
```

Shortcut: **`make report`**. Output: **`benchmarks/results/benchmark-results.html`**.

Only Python 3.10+ is needed. The [generator](tools/build_benchmark_report.py) reads saved measurements;
it does not rerun benchmarks. The report embeds its fonts and styles and works offline without JavaScript.
See [benchmark methods and evidence](benchmarks/README.md).

## Requirements

- **JDK 21** with `JAVA_HOME` set. The build enables preview APIs; the wrapper supplies Gradle 8.14.3.
- **Docker + Compose v2**, running. Integration tests use Testcontainers.
- **Python 3.10+** on macOS/Linux or WSL2; no Python packages required.
- Internet access for initial dependency and image downloads.
- Optional: `make` for shortcuts, ApacheBench (`ab`) for HTTP smoke tests.

## Run locally

```bash
git clone git@github.com:ereshzealous/blog-companions.git
cd blog-companions/java_virtual_threads
docker compose up -d --wait
./gradlew test
./gradlew bootRun
```

The bidder runs on **8080**. `make all` starts infrastructure, runs Java/Python tests and starts the bidder.

In another terminal, from the same directory:

```bash
curl -fsS http://localhost:8080/actuator/health
curl -i -H 'Content-Type: application/json' -H 'x-openrtb-tmax: 80' \
  --data-binary @examples/bid-request.json http://localhost:8080/openrtb/2.6/bid
python3 tools/benchmark_console.py serve
```

Open **http://127.0.0.1:8090** for bidder/Docker metrics and benchmark history.
Bids return **200**; no-bids return **204**. The 80 ms exchange budget reserves 30 ms for the return path.

| Service | Host port |
|---|---:|
| Bidder / Actuator | 8080 |
| Live console | 8090 |
| PostgreSQL primary / standby | 6100 / 6101 |
| Redis | 6800 |
| Kafka | 9092 |

Compose seeds sample campaigns on first startup and uses demo credentials matching `application.yml`.
To stop, press Ctrl+C in the bidder and console terminals, then run
`docker compose down --remove-orphans` (or `make down-all`). PostgreSQL volumes are retained.

## Tests and benchmarks

Run from `java_virtual_threads/`:

| Task | Command |
|---|---|
| Java tests | `./gradlew clean test` |
| Python checks | `python3 -m unittest discover -s tools -p test_benchmark_console.py` |
| All JMH experiments | `./gradlew jmh` |
| One JMH experiment | `./gradlew jmh -PjmhInclude=ScopedValue` — also accepts `Stampede` or `AdaptiveLimiter` |
| Surge simulation | `./gradlew surgeBenchmark` |
| Short capture check | `./gradlew jmh -PjmhInclude=scopedValue_reusedThread -PjmhVerify=true` |

JMH and surge benchmarks simulate dependencies and need no Docker. Integration tests start their own
containers; their replica-routing checks do not test physical replication. Compose provides a real standby.
The short capture check validates report generation, not performance.

| Output | Location |
|---|---|
| Java test report / JUnit XML | `build/reports/tests/test/index.html` / `build/test-results/test/` |
| Each benchmark run | `benchmarks/runs/<run-id>/` — raw results, manifest, JSON and HTML reports |
| Latest attempted run | `build/reports/benchmarks/latest.html` — failed runs remain marked failed |
| Checked-in test evidence | [benchmarks/validation/](benchmarks/validation/) |

The console discovers new runs automatically. Runs and live captures survive `gradlew clean` and are ignored by Git.
Saved measurements are never overwritten by a benchmark run.

## HTTP smoke test

With the bidder running:

```bash
ab -n 100000 -c 256 -k -p examples/bid-request.json \
  -T application/json http://localhost:8080/openrtb/2.6/bid
```

ApacheBench is a closed-loop client. Classify HTTP status and bid outcomes before treating its rate as useful throughput.
Variable response lengths alone do not identify failed bids.

## Project layout

| Path | Contents |
|---|---|
| `src/main/` | Bidder, configuration, schema and seed data |
| `src/test/` | Tests, JMH benchmarks and surge simulation |
| `docker/`, `docker-compose.yml` | Local infrastructure |
| `gradle/`, `gradlew*`, `*.gradle.kts` | Build and wrapper |
| `tools/` | Console, report generator and evidence readers |
| `examples/` | Request fixture |
| `benchmarks/` | Saved measurements, HTML report and test evidence |

## Limits of this POC

- Admission bounds queues; it cannot make a slow dependency meet the deadline.
- Win notices are acknowledged after an in-memory enqueue, before Kafka confirms durability. A crash can lose accepted notices.
- Cached budgets need an allocation or reservation policy to enforce fleet-wide spend caps.
- The console uses Actuator/Prometheus and Docker stats. It shows mean HTTP latency, not p99, and has no OpenTelemetry collector or trace store.
- Fleet capacity needs a separate constant-rate generator, multiple instances, and useful-completion and tail-latency measurements under failures.
