# ConnectedFleet — Apache Arrow in a Kafka → ClickHouse IoT pipeline

A reproducible proof of concept that measures **where Apache Arrow helps, and where it does not**, in an event-driven IoT analytics pipeline:

- PostgreSQL CDC, a transactional outbox, MongoDB change streams, an IoT telemetry producer and a CSV importer;
- Kafka with Debezium for capture and a normalizer that maps everything to canonical topics;
- three comparable lanes into identical ClickHouse databases.

| Lane | What it is | Role |
|---|---|---|
| **A** | Official ClickHouse Kafka Connect Sink | production baseline |
| **B** | Python row worker (typed rows → Native insert) | control |
| **C** | Python Arrow worker (RecordBatch → `pyarrow.compute` → `INSERT … FORMAT ArrowStream`) | experimental candidate |

ConnectedFleet is fictional. Event volumes in this repository are synthetic experiment settings, not production claims.

## Status and evidence

Every claim maps to a saved, validated run under `benchmarks/results/`. The [complete saved-results page](benchmarks/results/index.html) embeds charts, tables, all 48 correctness checks and full original output. Open it locally, or use the [GitHub Pages report](https://ereshzealous.github.io/blog-companions/connectedfleet-iot-arrow-poc/benchmarks/results/index.html) after publication. It rebuilds from saved files with `make public-report`; no services or new measurements are started. The original technical report remains at `benchmarks/results/benchmark-results.html` (`make results-html`), with a `report.html` for each run that left a manifest. A curated summary of what the runs show is
in [`benchmarks/results/REPORT.md`](benchmarks/results/REPORT.md).

| Part | Evidence | Result |
|---|---|---|
| Unit and integration tests | Existing code and archived validation history; no rerun for publication | The separate integration result was 17/17 at `9c6ac78`, before benchmarked `434b2a4`; not re-established at the packaging revision |
| Lab 01 — row vs Arrow at the JSON-to-column boundary | `lab01-20261010T162903Z-nogit` (1 warm-up + 5 reps) | measured |
| Lab 02 — real sources → canonical topics → A/B/C, correctness and fault tests | `lab02-20261010T164622Z-nogit` | 48/48 checks |
| **Lab 03 confirm profile (cited)** | **`lab03-confirm-20261011T031053Z-434b2a4`**: E1–E4, 1M events, 1 warm-up + 5 reps (3 for latency), every timing case on a quiet host | **238/238 validation cases valid**; every Lab 03 number |
| Lab 03 earlier runs (not cited) | smoke `…165506Z-32b2836` (async inserts, D-019), `…174059Z-c39b87f` (host load), `…183128Z-c67d181` (n = 1, before D-024/D-031); confirm `…192702Z-a499a19` (E4 pre-flight), `…194353Z-aa2da9c` (crashed), `…205001Z-9c6ac78` (Kafka client cap, D-031) and ten runs stopped for host load or while the host guard and memory settings were fixed | see each run's `NOTE.md` |
| Lab 03 full profile, E5 interop | not run | no claim is made |

Lab 01 and Lab 02 ran just before the first commit (`faf26dd`) and record no commit SHA. Every Lab 03 run records
its commit; the validator rejects runs without one. `docs/ARCHITECTURE.md` §11 lists design decisions, what the runs
changed, and the fixes made along the way.

## What Arrow does and does not change here

- Kafka messages are JSON bytes. They are decoded and validated **before** any Arrow array exists, by the same code in lanes B and C.
- Arrow changes the *middle* of a worker: typed columnar buffers and vectorized compute, at the cost of a batch **build** step that Lab 01 times separately.
- ClickHouse parses an `ArrowStream` body into its own columns. This is not end-to-end zero-copy.
- Arrow does not fix CDC ordering, deletes, outbox semantics or delivery guarantees. Every custom lane here is **at-least-once**: a ClickHouse acknowledgement followed by a Kafka offset commit is not one atomic step.

## Publication boundary

This directory is the standalone code companion: source, infrastructure, technical documentation, saved evidence and a static results page. Medium articles, article previews, Excalidraw artwork, authoring tools and local review records live outside this project and are not part of the GitHub package.

The original local repository was packaged from `60c93d87d6b0bfe69aef39bc596b2360dd7ed21c`; the cited measurements were taken at `434b2a4`. A publication commit does not create new performance evidence. Publication copies replace local workstation path prefixes with `<POC>`, `<WORKSPACE>` or `<HOME>`; [provenance](benchmarks/publication-provenance.json) records original and publication hashes.

## Prerequisites

| Need | For | Notes |
|---|---|---|
| `uv` ≥ 0.11, Python 3.12 | all labs | `uv python install 3.12` if missing; versions are locked in `uv.lock` |
| `make`, `git` | all labs | GNU Make 3.81+ |
| Docker + Compose v2 | Labs 02 and 03 | Every recorded run used a **7.7 GiB** Docker VM (15 CPUs): enough for the smoke and confirm profiles. The full profile (10M events) needs 12–16 GiB. ≥ 20 GB free disk for images and data. ClickHouse runs in a 3 GiB container with `infra/clickhouse/config.d/connectedfleet.xml` (75% memory ratio, small caches, D-033). Stop unrelated containers and heavy jobs before timing runs: Lab 03 waits for a quiet host before every timing case and re-runs a case that other work overlapped (`host_guard` in the workload file, D-032). |
| Free ports | Labs 02 and 03 | 9092 (Kafka), 8083 (Connect), 5432 (PostgreSQL), 27017 (MongoDB), 8123 / 9000 (ClickHouse) |

`make doctor` checks all of this read-only and changes nothing.

## Quick start

```bash
# Lab 01 — no Docker needed
make doctor
make test                    # unit tests (no Docker)
make lab-01                  # LAB01_PROFILE=quick|default|full

# Labs 02 and 03 — Docker Compose stack
cp .env.example .env         # local-only credentials (make up does this if .env is missing)
make up                      # build images, start Kafka/Connect/PostgreSQL/MongoDB/ClickHouse, wait until healthy
make init                    # topics, 3 ClickHouse DBs, Mongo users, connectors RUNNING, versions.json
make seed                    # deterministic source data
make smoke                   # Lab 02 with small volumes
make lab-02                  # full Lab 02 scenario and fault tests
git commit -am "…"           # Lab 03 records the commit SHA; runs from a dirty tree fail validation
make lab-03 PROFILE=smoke    # harness proof (E1–E4), ~25 min
make lab-03 PROFILE=confirm  # the cited evidence: 1M events, 5 reps, tuned sink, DLQ recovery, ~2 h on a quiet host
make report                  # re-validate saved runs, aggregate CSVs + summary
```

Expected output: each lab prints a summary table and the path of its evidence directory, and exits non-zero if any parity, correctness or validation check fails.

## Make targets

| Target | What it does | Touches |
|---|---|---|
| `help` | list targets | — |
| `doctor` | read-only host check | — |
| `sync` | create `.venv` exactly from `uv.lock` | `.venv/` |
| `test` | unit tests only (`tests/unit`) | — |
| `lab-01` | Lab 01 (`LAB01_PROFILE=quick\|default\|full`) | `benchmarks/results/lab-01/` |
| `verify-lab-01` | `test` + quick Lab 01 | as above |
| `up` | build images, start infrastructure, wait for health | Docker |
| `init` | idempotent topics, ClickHouse DDL, Mongo users, connector registration, `versions.json` | Docker services |
| `seed` | deterministic source rows and documents (idempotent upserts) | PostgreSQL, MongoDB |
| `status` | connector states, topic sizes, row counts per lane DB | — |
| `smoke` / `lab-02` | Lab 02 (small / full) | `benchmarks/results/lab-02/` |
| `lab-03 PROFILE=smoke\|confirm\|full` / `benchmark` | Lab 03 experiments | `benchmarks/results/runs/`, `aggregate/` |
| `report` | re-validate every saved Lab 03 run and regenerate aggregates (no Docker) | `benchmarks/results/` |
| `public-report` | rebuild the public HTML reading page from saved files, using Python standard library only | `benchmarks/results/index.html` |
| `verify-all` | unit tests, Lab 01, integration tests, Lab 02, Lab 03 smoke | everything above |
| `logs` | follow container logs (`SERVICE=<name>`) | — |
| `down` | stop containers; **data is kept** | Docker |
| `clean` | delete Python caches, then **asks** (type `delete`) before removing containers and volumes | Docker volumes on confirmation |

## Lab 01 — the JSON-to-column boundary

Runs a row path and an Arrow path over the same seeded telemetry fixture. The fixture has fixed injection positions for:

- nulls and absent fields,
- `battery_pct = 20` / `temperature_c = 40.0` boundaries,
- eleven kinds of malformed record,
- Unicode ids,
- late events.

It checks that both paths agree on valid/reject counts, alerts, per-event risk flags and per-site aggregates, then times each phase separately: **decode, validate, build (Arrow only), transform, serialize**. Each (size, batch, path) runs in a fresh process for memory measurement.

- Risk rule: `battery_pct < 20 AND temperature_c > 40` with SQL (Kleene) null semantics, as in `pyarrow.compute.and_kleene` and ClickHouse `and`.
- Evidence: `benchmarks/results/lab-01/runs/<run_id>/` with `run.json` (manifest), `summary.csv`, `phases.csv`, `memory.csv`, `parity.json`, `buffers.json` (real pyarrow buffer layout) and `summary.txt`.
- It is a single-process microbenchmark: it says nothing about Kafka or ClickHouse throughput.

## Lab 02 — the pipeline

| Source | Mechanism | Raw topic | Canonical topic → table |
|---|---|---|---|
| PostgreSQL `device_registry` | Debezium PostgreSQL (`cf-pg-registry`, slot `cf_registry_slot`) | `cfraw.pg.public.device_registry` | `cf.device-registry.v1` → `device_registry_versions` (+ `_current`) |
| PostgreSQL `outbox_event` | Debezium PostgreSQL + Outbox Event Router (`cf-pg-outbox`) | `cfraw.outbox.device` | `cf.lifecycle.v1` → `lifecycle_events` |
| IoT gateway | seeded producer (`acks=all`, idempotent) | `cfraw.telemetry.gateway` | `cf.telemetry.v1` → `telemetry_fact` |
| CSV diagnostics | bounded-chunk importer, id = file hash + row | `cfraw.diagnostics.csv` | `cf.diagnostics.v1` → `diagnostic_events` |
| MongoDB `device_state` | Debezium MongoDB change streams (`cf-mongo-state`) | `cfraw.mongo.connectedfleet.device_state` | `cf.device-state.v1` → `device_state_versions` (+ `_current`) |

- **Contracts:** `contracts/*.schema.json` (JSON Schema 2020-12) with examples. Every event carries `event_id, device_id, event_time, emitted_at, source_system, schema_version`; CDC topics add `operation, is_deleted, version`.
- **Normalizer:** maps raw → canonical, keys by `device_id`, routes rejects to `cf.dlq.normalizer.v1` with source offsets, and commits offsets only after producing.
- **ClickHouse:** `infra/clickhouse/schema.sql` is applied identically to `bench_sink`, `bench_row` and `bench_arrow`. Facts are MergeTree; state is versioned history plus `ReplacingMergeTree(version, is_deleted)` "current" tables. Merges are eventual, so read them with `FINAL`. ClickHouse enforces no uniqueness: count logical rows with `uniqExact(event_id)`.
- **Lanes B and C** share `src/connectedfleet/lanes/loop.py`: bounded poll, flush timeout, retry with backoff, DLQ, and offset commit after the ClickHouse acknowledgement. `FAULT=after_insert_before_commit` crashes a worker inside that window to prove at-least-once behaviour.

### Prove the sources are real

Run these after `make up init seed` (credentials from `.env`):

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "UPDATE device_registry SET firmware_version='3.2.2', updated_at=now() WHERE device_id=(SELECT min(device_id) FROM device_registry);"
make status                                    # connector states, topic sizes, row counts per lane
curl -s localhost:8083/connector-plugins       # Debezium PostgreSQL/MongoDB and ClickHouse Sink must be listed
curl -s localhost:8083/connectors?expand=status
```

Lab 02 automates the full set: insert, update and delete; committed and rolled-back outbox transactions; Mongo insert, update and delete; CSV re-import; a poison record; a connector restart; and the crash window. Every check is written to `benchmarks/results/lab-02/runs/<run_id>/run.json`.

## Lab 03 — benchmarks

`benchmarks/workloads/{smoke,confirm,full}.yaml` define:

| Experiment | What it compares |
|---|---|
| E1 | direct ingestion, A/B/C, plus a live-latency mode |
| E2 | transform isolation, B vs C, plus a 2×2 compute × insert-format matrix and dense / null-heavy / string-heavy shapes |
| E3 | CSV backfill: direct HTTP CSV upload (streamed, parsed by ClickHouse) vs Python `csv` + Native vs `pyarrow.csv` + ArrowStream, with client and server cost |
| E4 | correctness under replay, the crash window and a mid-stream poison record, for lane A as three connector variants; DLQ recovery; source-level cases linked from Lab 02 |
| E5 | optional interoperability with Polars and DuckDB (`uv sync --group interop`) |

Lanes run sequentially, in rotating order, from one per-run single-partition topic. Destination tables are truncated per lane, and CPU limits are equal.

`benchmarks/validate.py` rejects runs with:

- a missing commit or version metadata;
- too few repetitions;
- misreported status;
- wrong row, null or risk counts;
- schema drift;
- impossible percentiles;
- content hashes that differ across lanes.

The full profile's oversized cases are recorded as `NOT_RUN` with a reason.

Layout: `benchmarks/results/runs/<run_id>/` (manifest, per-case results, validation) and `benchmarks/results/aggregate/` (throughput, latency, memory, phase cost). Only curated, reproducible evidence is committed; raw logs are git-ignored.

## Configuration and secrets

- `.env.example` lists every variable, with local-development defaults. `.env` is git-ignored; never commit real credentials.
- Connector JSON files reference credentials as `{{ENV:NAME}}` and are rendered at registration time.
- Pinned versions:
  - `compose.yaml`: Kafka 4.2.2, Debezium Connect 3.6.3.Final with ClickHouse Sink 1.6.0, PostgreSQL 17.11, MongoDB 7.0.43 (8.x refuses Linux kernels ≥ 6.19, e.g. Docker Desktop's VM), ClickHouse 26.3.47.6;
  - `pyproject.toml` / `uv.lock`: Python dependencies.
- `make init` writes the versions of the running stack to `versions.json`.

## Correctness caveats

- **At-least-once.** Lanes B and C can insert a batch twice if they crash between the ClickHouse acknowledgement and the offset commit. Lane A runs with `exactlyOnce=false`.
- **Outbox.** A committed change always has a committed event; that alone is not end-to-end exactly-once.
- **MongoDB.** `change_streams_update_full` looks the document up after the change, so an update event may already contain a later update.
- **Deletes.** Under `REPLICA IDENTITY DEFAULT`, PostgreSQL deletes carry only the key; descriptive fields are null in the canonical delete event.
- **Ordering.** Kafka orders per partition only. Nothing here orders events across topics.

## Troubleshooting

- `make doctor` first: most failures are Docker memory or port conflicts.
- `make logs SERVICE=connect` for connector errors; `curl -s localhost:8083/connectors/<name>/status` for task traces.
- `make down` keeps data; `make clean` removes caches and only deletes volumes after you type `delete`.

## Repository layout

```
contracts/          canonical topic schemas + examples
connectors/         Kafka Connect connector configs
docs/               ARCHITECTURE.md
infra/              Dockerfiles, PostgreSQL/MongoDB/ClickHouse init
src/connectedfleet/ codec, contracts, normalize, lanes, producers, sources, clickhouse, lab01, lab02, bench
labs/               lab entry points
benchmarks/         workloads, run/validate/report, results
tests/unit          no Docker
tests/integration   needs the stack (marked `integration`)
```

## License

Apache-2.0.
