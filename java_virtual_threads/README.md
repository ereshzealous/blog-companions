# Java virtual threads: a real-time bidder

A runnable Java 21 proof of concept for deadline-bound bidding: virtual threads and scoped
context, shared cache loads, adaptive admission, and asynchronous win settlement.

The saved single-instance HTTP smoke runs reached about **32–33k requests/s**, with client
p99 latencies of **69 ms and 83 ms**. **1.2M requests/s is an unmeasured fleet design target.**
These short local runs do not establish sustained capacity or successful bids per second.

## Requirements

- **JDK 21**, with `JAVA_HOME` pointing to it. The build enables the JDK 21 preview APIs.
- **Docker with Compose v2**, running and accessible to your user. Tests use Testcontainers.
- **Python 3.10+** on macOS or Linux; use WSL2 on Windows. Python tools use only the standard library.
- Internet access for the first Gradle dependency and Docker image downloads.
- Optional: `make` for command shortcuts and ApacheBench (`ab`) for the HTTP smoke command.

The Gradle wrapper pins Gradle 8.14.3; a separate Gradle installation is unnecessary.

## Run locally

```bash
git clone git@github.com:ereshzealous/blog-companions.git
cd blog-companions/java_virtual_threads
java -version
docker compose up -d --wait
./gradlew test
./gradlew bootRun
```

The last command runs the bidder in the foreground on port **8080**. `make all` runs the same
setup, tests (including the Python checks), and bidder startup. Leave that terminal running.

In another terminal, from this directory:

```bash
curl -fsS http://localhost:8080/actuator/health
curl -i -H 'Content-Type: application/json' -H 'x-openrtb-tmax: 80' \
  --data-binary @examples/bid-request.json http://localhost:8080/openrtb/2.6/bid
python3 tools/benchmark_console.py serve
```

Open **http://127.0.0.1:8090** for live bidder/Docker metrics and benchmark history.
An eligible auction returns a bid with HTTP 200; no-bids return 204. The configured auction
budget is 80 ms at the exchange, with 30 ms reserved for the return path.

| Service | Host port |
|---|---:|
| Bidder / Actuator | 8080 |
| Live console | 8090 |
| PostgreSQL primary | 6100 |
| PostgreSQL physical standby | 6101 |
| Redis | 6800 |
| Kafka | 9092 |

Compose initializes the schema and sample campaigns on first startup. Its credentials are
local demo values, paired with `src/main/resources/application.yml`.

Stop the bidder and console with Ctrl+C in their terminals, then run `make down-all` or
`docker compose down --remove-orphans`. PostgreSQL volumes are retained.

## Tests and benchmarks

Run these commands from `java_virtual_threads/`:

```bash
./gradlew clean test
python3 -m unittest discover -s tools -p test_benchmark_console.py
./gradlew jmh
./gradlew jmh -PjmhInclude=ScopedValue
./gradlew jmh -PjmhInclude=Stampede
./gradlew jmh -PjmhInclude=AdaptiveLimiter
./gradlew surgeBenchmark
```

JMH and the surge simulation model dependencies and do not require Compose. The full test
suite starts its own PostgreSQL, Redis and Kafka containers through Testcontainers; its
replica-routing tests do not test physical replication. The Compose topology has a real standby.
Gradle writes the test report to `build/reports/tests/test/index.html` and JUnit XML to
`build/test-results/test/`. Checked-in test evidence is under `benchmarks/validation/`.

Each benchmark invocation writes a unique directory under `benchmarks/runs/`, containing raw
results, a manifest, `report.json` and an offline `report.html`. The live console discovers
new runs automatically. The latest attempted run is also exported to
`build/reports/benchmarks/latest.html`; failed runs are marked as failed.

For a quick check of the capture pipeline:

```bash
./gradlew jmh -PjmhInclude=scopedValue_reusedThread -PjmhVerify=true
```

This short mode verifies capture and rendering; it is not a publishable performance measurement.

## Saved results

See [the evidence guide](benchmarks/README.md) for measured values, source files and limitations.
To view the checked-in measurements without starting Docker or rerunning a benchmark:

```bash
make report
# Open build/reports/benchmarks/saved.html in your browser.
```

The report embeds its data, CSS and JavaScript and works offline. The console uses system fonts
and has no dependency on the blog's images, diagrams or authoring tools.

To repeat a short HTTP smoke run while the bidder is running:

```bash
ab -n 100000 -c 256 -k -p examples/bid-request.json \
  -T application/json http://localhost:8080/openrtb/2.6/bid
```

ApacheBench is a closed-loop client. Save the output and classify responses by HTTP status and
bid outcome before interpreting its request rate as useful throughput. Response bodies contain
variable latency fields, so response-length differences alone do not classify business failures.

## Project layout

```text
src/main/             Bidder code, configuration, database schema and seed data
src/test/             Unit/integration tests, JMH benchmarks and surge simulation
docker/               PostgreSQL primary/standby initialization
docker-compose.yml    Local PostgreSQL, Redis and Kafka topology
gradle/, gradlew*     Pinned Gradle wrapper
tools/                Live console, evidence readers and Python tests
examples/             Request fixture for curl and ApacheBench
benchmarks/results/   Saved component and HTTP measurements
benchmarks/validation/ Checked-in test results and source hashes
```

Local builds, caches, new benchmark runs and live captures are ignored by Git. Saved evidence
is committed deliberately; running a benchmark never overwrites it.

## Limits of this POC

- Admission can contain queue growth but cannot make a slow dependency meet the decision deadline.
- Win-notice HTTP acceptance currently acknowledges an in-memory enqueue before Kafka confirms
  durability. A process crash can lose an accepted notice; ledger deduplication does not fix ingress loss.
- Cached budgets do not enforce a strict fleet-wide spend cap; that needs an allocation or reservation policy.
- The console reads Actuator/Prometheus metrics and Docker stats. It is not an OpenTelemetry
  collector or distributed trace store. Live HTTP latency is an interval mean, not p99.
- A credible capacity claim needs a separate constant-rate load generator, multiple bidder
  instances, and measured useful completions and tail latency under failures.
