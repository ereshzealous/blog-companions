# Packaged project validation — 2026-10-10

The self-contained directory was copied into the target repository and checked independently
of the local authoring workspace.

- **39 Java tests passed**, including three Testcontainers integration tests; no failures or skips.
- **10 Python tests passed**, including standalone HTML export without article assets.
- Docker Compose configuration validated; the relocated bidder, console and four services were checked.
- Offline results render at 1280, 390 and 320 px with no overflow, including expanded raw output.
- A short, single-configuration JMH run completed and automatically produced original results,
  a manifest, normalized JSON and offline HTML. This verifies the pipeline, not performance.

Commands:

```bash
./gradlew --no-daemon clean test
python3 -m unittest discover -s tools -p test_benchmark_console.py
docker compose config --quiet
./gradlew --no-daemon jmh -PjmhInclude=scopedValue_reusedThread -PjmhVerify=true
python3 tools/benchmark_console.py export --input benchmarks/results --output build/reports/benchmarks/saved.html
```

`tests.json` records the Java environment, source hashes and suite totals. `junit/` contains
JUnit XML with test outcomes and timing retained; console streams are omitted and the machine
hostname is normalized. `checks.json` records packaging and capture checks; `html-report.json` records offline browser checks.
The report CSS received a wrapping fix after the Java test run; its final hash is recorded
in `checks.json`. No Java sources changed after the test run. Full reports are
regenerated under `build/` when you run the commands.

Integration tests share one PostgreSQL container between the two connection pools. They test
routing, not physical standby replication. The saved HTTP and JMH measurements in `results/`
come from the earlier benchmark runs and were not replaced by this validation.
