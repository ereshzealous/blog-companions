# ConnectedFleet POC — Architecture

Status legend used in this document:
**[implemented]** code exists in this repository · **[validated locally]** exercised by a recorded lab run on one laptop (run ids in §9; evidence under `benchmarks/results/`) · **[design]** decided, not yet coded.
"Validated locally" means single-node Docker Compose at smoke/confirm scale. It is not a production certification.

## 1. Purpose and the question being tested

ConnectedFleet is a fictional industrial IoT platform. The POC asks one narrow question:
**in a Kafka → ClickHouse pipeline, where does an Arrow RecordBatch stage pay for itself, and where is it just extra conversion?**

Three things are deliberately *not* claimed:
- Arrow does not replace Kafka, Debezium, the canonical contracts or ClickHouse tuning.
- Kafka messages are JSON bytes. They are decoded before any Arrow array exists, and no buffer travels unchanged from Kafka to ClickHouse storage.
- A ClickHouse acknowledgement plus a Kafka offset commit is not one atomic action. Every custom lane is at-least-once.

## 2. Runtime topology

```
 sources                       capture                 Kafka (KRaft, 1 broker)          lanes                      ClickHouse
 ───────                       ───────                 ───────────────────────          ─────                      ──────────
 PostgreSQL device_registry ─► Debezium PG (slot 1) ─► cfraw.pg.public.device_registry ┐
 PostgreSQL outbox_event    ─► Debezium PG (slot 2)                                    │
                               + Outbox Event Router ► cfraw.outbox.device             │   normalizer      ┌► A official sink ─► bench_sink
 IoT gateway (simulated)    ─► telemetry producer ──► cfraw.telemetry.gateway          ├─► (decode/map/ ─► cf.*.v1 ─┼► B row worker ────► bench_row
 CSV diagnostics files      ─► CSV importer ────────► cfraw.diagnostics.csv            │    type/validate) │        └► C Arrow worker ──► bench_arrow
 MongoDB rs0 device_state   ─► Debezium Mongo ──────► cfraw.mongo.connectedfleet.device_state ┘   └► cf.dlq.normalizer.v1
```

Cassandra is out of scope (future extension, disabled; it never appears in the active path).

### 2.1 Services and pinned versions (Compose) [validated locally: Lab 02]

| Service | Image / build | Ports (host) | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:4.2.2` | 9092 | KRaft combined broker+controller, RF=1, `auto.create.topics.enable=false`; topics created by `make init` |
| `connect` | build `infra/connect/Dockerfile` FROM `quay.io/debezium/connect:3.6.3.Final` + ClickHouse Kafka Connect Sink `v1.6.0` | 8083 | JsonConverter, `schemas.enable=false`; `connector.client.config.override.policy=All` so benchmark runs can set `max.poll.records` per connector |
| `postgres` | `postgres:17.11` | 5432 | `wal_level=logical`, two publications, two replication slots |
| `mongodb` | `mongo:7.0.43` (8.x refuses Linux kernels ≥ 6.19, see D-016 in the workspace log) | 27017 | single-member replica set `rs0` with keyFile auth (required for change streams + auth) |
| `clickhouse` | `clickhouse/clickhouse-server:26.3.47.6` | 8123 (HTTP), 9000 (native) | three identical databases `bench_sink`, `bench_row`, `bench_arrow`; `infra/clickhouse/config.d/connectedfleet.xml` sizes memory and caches for the 3 GiB container (D-033) |
| `app` | build `infra/app/Dockerfile` (`python:3.12.13-slim` + uv 0.11.7, `uv sync --frozen`) | — | runs the normalizer, producers, row/Arrow workers and E3 backfill inside the Docker VM, so every lane shares one clock and one cgroup model. Lab orchestration (`labs/*.py`) runs on the host via `uv run` and talks to published ports |

`make init` writes `versions.json` from the running stack (compose image tags, Connect `/` and `/connector-plugins`, ClickHouse `SELECT version()`, PostgreSQL/MongoDB server versions, client library versions). Lab manifests embed it.

### 2.2 Resource guidance

Lab 01 needs no Docker. Lab 02: container memory limits sum to ≈ 12 GiB with all lanes running; 12–16 GiB recommended (smaller VMs untested). Lab 03 full: 16 GiB, ≥ 6 CPUs. Benchmark lanes get **equal CPU limits** (`cpus: 2.0`) on `connect` (lane A), `worker-row` and `worker-arrow`; memory limits differ by runtime (JVM vs CPython) and are reported, not hidden.

## 3. Topics

Raw topics are named by connector/producer configuration and are **not** the canonical contract.

| Topic | Producer | Partitions | Key |
|---|---|---:|---|
| `cfraw.pg.public.device_registry` | Debezium PostgreSQL connector `cf-pg-registry` (`topic.prefix=cfraw.pg`) | 3 | Debezium key `{device_id}` |
| `cfraw.outbox.device` | Debezium PostgreSQL connector `cf-pg-outbox` + `EventRouter` (`route.topic.replacement=cfraw.outbox.${routedByValue}`) | 3 | `aggregateid` (= device_id) |
| `cfraw.mongo.connectedfleet.device_state` | Debezium MongoDB connector `cf-mongo-state` (`topic.prefix=cfraw.mongo`) | 3 | Debezium key `{id}` |
| `cfraw.telemetry.gateway` | `connectedfleet.producers.telemetry` | 6 | device id |
| `cfraw.diagnostics.csv` | `connectedfleet.producers.csv_import` | 3 | device id |
| `cf.telemetry.v1` … `cf.diagnostics.v1` | normalizer | 6 (telemetry), 3 (others) | `device_id` |
| `cf.dlq.normalizer.v1`, `cf.dlq.sink.v1`, `cf.dlq.lane-b.v1`, `cf.dlq.lane-c.v1` | normalizer / sink / workers | 1 | original key |
| `cfbench.<run_id>.telemetry` | Lab 03 benchmark producer | 6 | `device_id` |

Kafka guarantees ordering only within a partition. Keying by `device_id` keeps one device's events ordered; nothing orders events *across* topics. Joins in ClickHouse use event time and versions, never arrival order.

### 3.1 Raw gateway format (deliberately different from canonical)

```json
{"msg_id":"gw-7f3a-000001057","dev":"sensor-01042","ts":1791534600000,"sent":1791534600100,
 "m":{"t_dC":436,"bat":17,"rssi":-78},"fw":"3.2.1","site":"factory-12"}
```
Temperature arrives in deci-degrees Celsius and times as epoch milliseconds. The normalizer owns the conversion to `cf.telemetry.v1` (°C, RFC 3339 UTC).

## 4. Canonical contracts [implemented: schemas]

`contracts/*.schema.json` (JSON Schema 2020-12) plus `contracts/examples/`. Every event carries
`event_id, device_id, event_time, emitted_at, source_system, schema_version`. CDC topics add `operation, is_deleted, version`.

| Topic | Stable `event_id` | `version` (ordering) |
|---|---|---|
| `cf.telemetry.v1` | gateway `msg_id` | — |
| `cf.lifecycle.v1` | outbox row `id` (UUID) | — |
| `cf.device-registry.v1` | `pgreg-<lsn>-<device_id>` | commit LSN (`source.lsn`) |
| `cf.device-state.v1` | `mgstate-<cluster seconds>-<ord>-<device_id>` | `(seconds << 32) \| ord` |
| `cf.diagnostics.v1` | `csv-<sha256[:16]>-<row_number>` | — |

Null policy: an absent optional field and an explicit `null` both become a typed null. Validators count them separately.
Timestamps are UTC milliseconds. Unknown extra fields are tolerated (forward compatible) and counted.
Telemetry decoding/validation lives in `connectedfleet.codec.telemetry` and is shared by every custom lane.

## 5. Capture details [validated locally: Lab 02]

**PostgreSQL registry CDC.** Table `device_registry(device_id text PK, customer_id, site_id, firmware_version, state, updated_at timestamptz)`.
Publication `cf_registry_pub FOR TABLE device_registry`, slot `cf_registry_slot`, `plugin.name=pgoutput`, `publication.autocreate.mode=disabled`, `snapshot.mode=initial`.
Delete events carry only the key under `REPLICA IDENTITY DEFAULT`. The normalizer emits `is_deleted=true` with null descriptive fields and drops the following tombstone.

**Transactional outbox.** Table `outbox_event(id uuid PK, aggregatetype text, aggregateid text, type text, payload jsonb, occurred_at timestamptz)`, publication `cf_outbox_pub`, slot `cf_outbox_slot`, separate connector `cf-pg-outbox` with `transforms=outbox` (`io.debezium.transforms.outbox.EventRouter`).
`connectedfleet.sources.registry_app` changes `device_registry` **and** inserts the outbox row in one SQL transaction. A rolled-back transaction produces neither row, so no event.
The outbox is insert-only during tests. This guarantees that a committed business change has a committed event. It does **not** give end-to-end exactly-once: the router, the normalizer and the lanes can still redeliver, and consumers rely on the stable outbox `id`.

**MongoDB device state.** Replica set `rs0`, collection `connectedfleet.device_state` with `_id = device_id`.
`capture.mode=change_streams_update_full`: updates carry a full document looked up *after* the change, so it may already contain a later update (documented race). Deletes carry only the key (no pre-image).
Snapshot then streaming; the connector's stored resume token makes restarts resume without re-emitting.

**Telemetry producer.** Seeded; per-device key; `acks=all`, `enable.idempotence=true`; bounded rate (`--rate`), `--count`, low-volume smoke and larger benchmark profiles. Event time is generated; `sent` is the real send clock.

**CSV importer.** Reads files in bounded chunks with `csv`; `file_sha256 + row_number` is the logical id; re-importing the same file produces identical ids (logical duplicates). A *separate* direct-load path exists for Lab 03 E3 and never goes through Kafka.

## 6. Normalizer [validated locally: Lab 02]

One Python consumer group (`cf-normalizer`) subscribes to the five raw topics:

1. Poll a bounded batch.
2. Decode and map per source. Tombstones are skipped and counted.
3. Validate against the contract.
4. Produce canonical records keyed by `device_id` with an idempotent producer.
5. Send rejects to `cf.dlq.normalizer.v1`. Headers carry the source topic, partition, offset, error code and a redacted detail. No payload fields are logged.
6. `flush()`, then commit the consumed offsets.

Offsets are committed only after canonical and DLQ produces are acknowledged, so a crash replays (at-least-once) and never loses data. A poison message is DLQ'd once and committed past, so it can't retry forever.

## 7. ClickHouse model [validated locally: Lab 02, Lab 03 → `infra/clickhouse/schema.sql`]

The same DDL is applied to `bench_sink`, `bench_row` and `bench_arrow`:

| Table | Engine | Sort key | Notes |
|---|---|---|---|
| `telemetry_fact` | MergeTree, `PARTITION BY toYYYYMM(event_time)` | `(device_id, event_time, event_id)` | append-only; `ingested_at DateTime64(3,'UTC') DEFAULT now64(3)` for observability/latency |
| `telemetry_enriched` | MergeTree | same | E2 only (B and C): telemetry + `risk_flag Nullable(Bool)` |
| `lifecycle_events` | MergeTree | `(device_id, event_time, event_id)` | append-only log |
| `diagnostic_events` | MergeTree | `(device_id, event_time, event_id)` | keeps `file_sha256`, `row_number` to separate logical from physical duplicates |
| `device_registry_versions` | MergeTree | `(device_id, version, event_id)` | full history incl. delete markers |
| `device_registry_current` | ReplacingMergeTree(`version`, `is_deleted`) fed by a materialized view | `device_id` | merges are eventual; read with `FINAL` |
| `device_state_versions` / `device_state_current` | as above | as above | same pattern for Mongo state |

ClickHouse does not enforce foreign keys or uniqueness. Duplicates from at-least-once delivery are physically present until queried with `uniqExact(event_id)` / `LIMIT 1 BY event_id`. An optional insert-level mitigation (`insert_deduplication_token` with `non_replicated_deduplication_window`) is an E4 experiment, not a default.

Types: `DateTime64(3,'UTC')` ↔ Arrow `timestamp[ms, UTC]`; `Nullable(UInt8)` ↔ Arrow `uint8` with validity bitmap; `LowCardinality(Nullable(String))` ↔ Arrow `string` (dictionary encoding optional, to be measured).

## 8. The three lanes

All three read the **same canonical topics** (Lab 02) or the **same per-run benchmark topic** (Lab 03) into their own database.

| | A — official sink | B — row worker | C — Arrow worker |
|---|---|---|---|
| Runtime | Kafka Connect (JVM), ClickHouse Kafka Connect Sink | CPython, `confluent-kafka` | CPython, `confluent-kafka` |
| Decode | connector JsonConverter (schemaless) | shared `connectedfleet.codec` | shared `connectedfleet.codec` |
| In-memory form | Connect records | typed tuples | `pyarrow.RecordBatch` |
| Transform | none (E1); not applicable to E2 | Python per row | `pyarrow.compute` kernels |
| Insert | connector's own insert path (`exactlyOnce=false`, at-least-once) | `clickhouse-connect` insert (Native format over HTTP) | HTTP `INSERT INTO db.t (cols) FORMAT ArrowStream` with an IPC stream body |
| Offset commit | Connect framework after the sink's `put`/flush | manual, after ClickHouse acknowledges | manual, after ClickHouse acknowledges |
| Role | production baseline | control | candidate |

B and C share one consumer loop (`connectedfleet.lanes.loop`): the same poll size, flush timeout, commit point, retry and DLQ policy. Only the sink class differs, so differences come from representation and insert format, not from loop mechanics.

**Crash window (B and C).** `insert acknowledged → (crash) → offset commit`. On restart the batch is consumed and inserted again: a physical duplicate with the same `event_id`. Lab 02 injects this crash on purpose (`FAULT=after_insert_before_commit`) and records physical vs logical counts.

## 9. Labs

| Lab | Entry | Needs | Proves |
|---|---|---|---|
| 01 | `make lab-01` [validated locally: `lab01-20261010T162903Z-nogit`, 5 reps] | Python only | row/Arrow parity; per-phase cost of decode, validate, build, transform, serialize; real buffer layout |
| 02 | `make lab-02` [validated locally: `lab02-20261010T164622Z-nogit`, 48/48 checks] | full Compose stack | real CDC/outbox/Mongo/telemetry/CSV capture → canonical topics → A/B/C → equal logical content in three DBs; rollback, delete, restart, duplicate, DLQ cases |
| 03 | `make lab-03 PROFILE=smoke\|confirm\|full` [validated locally: smoke `lab03-smoke-20261010T183128Z-c67d181`; confirm runs listed in `benchmarks/results/manifest.json`] | full Compose stack | controlled E1–E4 experiments with run manifests, validator and report (E5 optional, not run) |

### 9.1 Lab 02 scenario [validated locally]

1. Preflight: every container healthy; Connect `/connector-plugins` lists `PostgresConnector`, `MongoDbConnector` and `ClickHouseSinkConnector`; all connectors `RUNNING`.
2. Seed: 5 registry devices (snapshot), 5 Mongo documents, CSV file `fixtures/diagnostics/batch-001.csv` (20 rows).
3. Mutate:
   - PostgreSQL: insert, update, delete.
   - Outbox: 3 committed transactions and 1 rolled back.
   - MongoDB: insert, partial update, delete.
   - Telemetry: 500 events.
   - CSV: import the same file twice.
4. Wait until the canonical topic counts reach their expected values, then until each lane's tables reach their expected logical counts.
5. Verify across the three databases:
   - Logical counts and an order-independent content hash per table: `groupBitXor(cityHash64(<contract columns>))` over `LIMIT 1 BY event_id`.
   - Deleted devices absent from `*_current FINAL`.
   - Rollback event absent.
   - CSV physical rows = 2 × logical rows.
6. Faults:
   - Lane B crash after insert, before commit → restart → physical > logical, logical unchanged.
   - Poison raw record → `cf.dlq.normalizer.v1`.
   - Connector restart → no gap.
7. Evidence goes to `benchmarks/results/lab-02/runs/<run_id>/` (manifest, checks, counts, versions).

### 9.2 Lab 03 experiments [validated locally; full profile not run]

- **E1 direct ingest:** canonical telemetry, no transform, A/B/C sequentially, fresh topic and databases per run, same batch size per comparison.
- **E2 transform isolation:** B vs C only, identical risk rule. Optional 2×2 (row/Arrow compute × Native/ArrowStream insert), labeling every extra conversion.
- **E3 CSV backfill:** direct HTTP CSV upload, streamed and parsed by ClickHouse (`INSERT … FROM input(…) FORMAT CSVWithNames`) vs Python `csv` + Native vs `pyarrow.csv` streaming + ArrowStream; same file hash. Every insert carries a `log_comment`, so server CPU, query time and peak memory come from `system.query_log` next to client cost.
- **E4 correctness/failure:** replay from offsets, crash at the commit boundary, re-import, CDC update/delete, outbox rollback, a mid-stream poison record. Lane A runs as three connector variants (`bench/sinkcfg.py`): as first benchmarked (connector buffer), tuned (no connector buffer; Kafka fetch batching) and skip (`input_format_allow_errors_num`). The sink dead-letters a failed batch as a whole, so E4 also recovers the dead-lettered rows with `python -m connectedfleet.recovery` and checks every good row arrives once. Physical vs logical duplicates per lane.
- **E5 interoperability (optional):** hand the same RecordBatch to Polars/DuckDB. Report copies or conversions only when measured.

Timing cases run under a host-load guard (`bench/hostload.py`, `host_guard` in the workload file): each E1/E2/E3 case waits until other work on the host (whole-host CPU minus Docker's VM, Docker Desktop and the harness) has stayed under the limit, samples it while the case runs, and is re-run if other work exceeded it; discarded attempts are listed in the manifest.

Metrics per run: validated queryable rows/s, both steady (rows of inserts 2..k over the first-to-last insert window, from ClickHouse's own `ingested_at`) and wall-clock (lane start to last row, 0.2 s completion poll); p50/p95/p99 of `ingested_at − emitted_at` (producer runs in the Docker VM, so it shares the ClickHouse clock); per-phase timings for B/C; container CPU seconds and peak memory from the Docker stats API; Kafka consumer lag; ClickHouse parts and insert counts from `system.part_log`/`system.parts`; bytes on the wire.
`benchmarks/validate.py` rejects:
- wrong event counts or checksums
- p99 < p95
- missing SHA/version metadata
- status claims that disagree with validation

## 10. Repository layout

```
contracts/            JSON Schema per canonical topic + examples          [implemented]
docs/ARCHITECTURE.md  this document                                        [implemented]
src/connectedfleet/
  contracts/          executable contracts, all five topics               [implemented]
  codec/              shared decode/validate (telemetry + canonical)      [implemented]
  fixtures/           seeded generators                                   [implemented: telemetry]
  lab01/              Lab 01 paths, runner, worker, report                [implemented]
  instrumentation/    env/version capture, memory                         [implemented]
  producers/ normalize/ lanes/ clickhouse/ sources/ lab02/ bench/      [validated locally]
  recovery.py         DLQ re-validation and replay                        [validated locally: E4]
  ops.py connect.py kafka_admin.py kafka_io.py compose.py topics.py config.py
infra/                Dockerfiles (connect, app), PostgreSQL/MongoDB init, ClickHouse DDL [validated locally]
connectors/           Kafka Connect connector JSON ({{ENV:..}} placeholders)            [validated locally]
labs/                 lab01 / lab02 / lab03 entry points                                [validated locally]
benchmarks/           workloads (smoke/confirm/full), run/validate/report, results/      [validated locally; full not run]
tests/unit            no Docker                                                         [passing: `make test`]
tests/integration     marked `integration`, need the stack                              [passing against the stack]
```

## 11. Implementation notes, deviations from the first design, and what the runs changed

| Topic | Decision | Why |
|---|---|---|
| Where labs run | Lab orchestrators run on the host (`uv run`). Normalizer, producers, lane workers and the E3 backfill run in the `app` image. | The host can drive compose, the Docker API and the sources; workloads that are measured stay inside the Docker VM (one clock, cgroup limits). Host MongoDB clients use `directConnection=true` because the replica-set member name `mongodb:27017` only resolves inside the network. |
| `make down` | `docker compose stop` (containers and data kept); `make clean` asks before `down -v`. | Kafka logs live in the container filesystem; removing containers while PostgreSQL slots survive would desynchronise Debezium offsets. |
| Lab 02 isolation | No truncation. Every expectation is scoped by a run tag (`dev-<tag>-NN`, `gw-<tag>-…`, uuid5 outbox ids, CSV file hash). Cross-lane equivalence is checked on whole tables. | Lanes keep their committed offsets across runs, so history is identical in all three databases; per-run filters keep the checks exact. |
| Content hash | `GROUP BY key`, `min()` per column, `formatRowNoNewline('TSV', …)`, XOR of `cityHash64`. Benchmarks also exclude `emitted_at` (producer send time). | Order- and duplicate-independent; NULL-safe; live-mode runs produce the same workload at different send times. |
| Lane A float parsing | Sink `clickhouseSettings` adds `precise_float_parsing=1` (plus best-effort ISO timestamps and JSON bools as numbers). | JSONEachRow text→Float64 must match the binary floats lanes B/C send, or the cross-lane hash would differ for a parsing reason. Verified: Lab 02 content hashes match across all three lanes. |
| Lane A batch size | Benchmark connectors set `consumer.override.max.poll.records`, `bufferCount`, `bufferFlushTime`, `ignorePartitionsWhenBatching=true`; the harness reports the *effective* batch (rows / distinct `ingested_at`). | The sink inserts per `put()` and per partition by default; the effective batch makes any remaining difference visible instead of assumed. Measured in Lab 03: the connector flushes its last partial buffer only on new records or on Kafka Connect's offset flush (60 s), so a burst's tail can wait up to a minute. The `sink_tuned` variant drops the connector buffer and batches with Kafka fetch settings instead. |
| Benchmark topics | One partition per benchmark topic, one consumer per lane, lane order rotated per repetition, pipeline connectors paused. | Equal input and parallelism for A/B/C; the partition count is recorded and can be raised in the workload file. |
| Lane C insert | Raw HTTP `INSERT … FORMAT ArrowStream` via a persistent stdlib connection, not `clickhouse_connect.insert_arrow`. | `insert_arrow` sends the Arrow *file* format; the experiment needs the streaming format and an explicit column list. |
| Lane B insert | `clickhouse-connect` `insert()` with explicit column types (Native over HTTP), `compress=False` like lane C. | Native encoding happens inside the client, so for B the serialize phase is part of `insert`; reports compare serialize+insert. |
| Absent vs null on the canonical wire | The normalizer drops telemetry measurement keys that were absent upstream. | Keeps the distinction observable downstream; typed storage still maps both to NULL. |
| MongoDB Debezium user | `readAnyDatabase` + `clusterMonitor`. | POC simplification; least-privilege roles not yet verified (VERIFY). |
| E4 source-level cases | CSV re-import, PostgreSQL delete/update, outbox rollback and Mongo update/delete are executed by Lab 02 and linked into the Lab 03 correctness matrix by run id. | Avoids re-implementing source mutations; the matrix shows the source run id and never marks an unexecuted case as passed. |
| E5 | Optional `interop` dependency group; host process; timings only, buffer copying is not asserted. | Cheap to add; honest about what it can and cannot show. Not run: no cross-engine claim is made. |
| Synchronous inserts (D-019) | Every lane insert sends `async_insert=0`, as the official sink does. | ClickHouse 26.3 defaults to async inserts with a ≥ 50 ms adaptive wait; lanes B/C first inherited it (58 ms vs 3.4 ms per insert in `system.query_log`), which made run 1 unfair. |
| Worker flush timer (review fix, a499a19) | With an empty buffer the loop polls for at most the flush timeout, and the flush deadline counts from the start of that poll. | `consume(n, timeout)` blocks the whole timeout unless n records arrive, so a 250 ms flush behaved like ~0.75 s before the fix. Runs before a499a19 carry the old behaviour. |
| Sink DLQ granularity | The official sink dead-letters a failed batch as a whole (documented); `recovery.py` re-validates and replays the good records. | One bad record cost 499 good rows in E4; they were recoverable from the DLQ, not lost. |
| Worker Kafka prefetch (D-031) | The workers' librdkafka consumer sets `fetch.queue.backoff.ms=10` (`lanes/worker.py` CONSUMER_CONFIG). `make lab-03` rebuilds the worker image first; manifests record its id and build time. | librdkafka's defaults (`queued.min.messages=100000`, backoff 1,000 ms) capped both workers near 100K rows/s at 1M events while the tuned sink's fetch was sized to the batch. A consume-only probe (`bench/consume_probe.py`, evidence in `benchmarks/results/probes/`) drained 1M records at ~135K/s with the defaults and 0.8–1.0M/s with the setting. |
| Host-load guard (D-032) | Per timing case: wait for a quiet host, measure other host work while the case runs, re-run a case that overlapped it (up to 6 attempts). The validator excludes a case still busy after the last attempt; the run stays valid while every group keeps the profile's minimum clean repetitions. Interrupted (`RUNNING`) runs are invalid. | The machine is shared: native builds and benchmarks started during runs and slowed every lane, the official sink included. A start-of-run idle check, then `ps %cpu` and per-process sampling all missed load; whole-host CPU (psutil) minus our own processes does not. |
| ClickHouse memory (D-033) | `max_server_memory_usage_to_ram_ratio` 0.75, mark/index/uncompressed caches of 64–256 MiB, information-level logging. | With defaults (90% of a 3 GiB container, multi-GiB caches, trace logging) the server was OOM-killed by the cgroup during background merges. |
