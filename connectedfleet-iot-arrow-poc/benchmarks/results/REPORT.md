# Lab 03 — curated results

Cited run: **`lab03-confirm-20261011T031053Z-434b2a4`** (POC commit `434b2a4`, clean tree; evidence committed in
`7ce8cf5`). Profile `confirm`: 1,000,000 events per throughput case, one warm-up and five measured repetitions
(three for live latency), lane order rotated, every timing case run on a quiet host. **238 of 238 validation cases
valid.** A validation case is one lane, method or repetition that passed the validator's checks (counts, null counts,
content hashes, schemas, percentiles); it is not a count of successful failure scenarios.

Every number below is a median over the measured repetitions with min–max in brackets, read from
`aggregate/*.csv` rows with `git_sha` starting `434b2a4` (or from the run's `run.json`). Nothing is pooled across commits.

## Verdict

- **Plain Kafka → ClickHouse: the official ClickHouse Kafka Connect Sink, tuned.** With its connector buffer off and
  Kafka's fetch sized to the batch it was the fastest lane, 28% (batch 1,000) and 45% (batch 10,000) above the Arrow
  worker, with no code to own. As first configured, with its own buffer, it was 12–16× slower.
- **Custom Python processing: Arrow end to end.** The Arrow worker beat the row worker by 26–27% without a transform and
  29–30% with one, with 22–27% less worker CPU. The gain is in building and sending the batch, not in the transform.
  Converting Arrow back to Python lists for a row-oriented client was the slowest option.
- **CSV backfill: a direct HTTP upload parsed by ClickHouse.** Fastest, 1.7× pyarrow; it moves the CPU to the server.
- **Correctness does not depend on the representation.** The sink dead-letters whole batches (recoverable by replaying
  its DLQ); the workers reject only the bad record. Never use the sink's skip-errors setting: it drops good rows silently.

## E1 — whole lanes, no transform (1,000,000 dense events)

| Batch | Lane | Steady rows/s | End to end rows/s | Rows per insert |
|---|---|---:|---:|---:|
| 1,000 | A · sink (connector buffer) | 16.7K (16.7–16.7K) | 16.6K (16.6–16.6K) | 1,001 |
| 1,000 | A · tuned | **202.0K** (186.6–207.6K) | **197.3K** (179.9–198.1K) | 996 |
| 1,000 | B · row worker | 123.8K (119.5–129.7K) | 114.2K (109.2–121.8K) | 1,000 |
| 1,000 | C · Arrow worker | 157.6K (154.1–158.7K) | 146.2K (144.9–149.3K) | 1,000 |
| 10,000 | A · sink (connector buffer) | 16.5K (16.5–16.5K) | 16.6K (16.6–33.2K) | 10,101 |
| 10,000 | A · tuned | **267.2K** (251.2–274.7K) | **256.8K** (237.7–257.4K) | 9,615 |
| 10,000 | B · row worker | 146.5K (125.5–148.9K) | 133.9K (116.4–136.5K) | 10,000 |
| 10,000 | C · Arrow worker | 183.9K (177.5–189.8K) | 168.8K (161.8–174.6K) | 10,000 |

Worker CPU per million events: B 6.74 / 6.85 s, C 4.95 / 5.32 s (batch 1,000 / 10,000). Lane A's figure is the whole
Kafka Connect container (JVM), a different scope: 5.07 / 4.89 s (sink), 4.28 / 4.41 s (tuned). Peak container memory:
B 166 / 148 MiB, C 112 / 139 MiB, Kafka Connect 1.2–1.4 GiB. Phase time per million events, batch 10,000 (B → C):
decode 1,726 → 1,680 ms, validate 1,275 → 1,295 ms, build 747 → 371 ms, serialize — → 30 ms (B encodes inside its
insert), insert 1,330 → 463 ms.

## E1 live — latency at 1,000 events/s, batch 1,000 (three repetitions)

| Flush | Lane | p50 | p95 | p99 | max | events > 5 s | max lag |
|---|---|---:|---:|---:|---:|---:|---:|
| 250 ms | A · sink | 143 ms | 268 ms | 340 ms | 24.3 s | 31 | n/m |
| 250 ms | A · tuned | 145 ms | 261 ms | 496 ms | 695 ms | 0 | n/m |
| 250 ms | B · row | 149 ms | 268 ms | 491 ms | 690 ms | 0 | 254 |
| 250 ms | C · Arrow | 147 ms | 266 ms | 401 ms | 600 ms | 0 | 257 |
| 1 s | A · sink | 511 ms | 984 ms | 24.3 s | 24.5 s | 264 | n/m |
| 1 s | A · tuned | 489 ms | 898 ms | 935 ms | 1.0 s | 0 | n/m |
| 1 s | B · row | 524 ms | 974 ms | 1.0 s | 1.0 s | 0 | 1,013 |
| 1 s | C · Arrow | 523 ms | 973 ms | 1.0 s | 1.0 s | 0 | 990 |

After the worker flush-timer fix the flush timeout sets latency in every tuned lane. The buffered sink's last partial
batch waits for Kafka Connect's 60 s offset flush. Lag n/m: Kafka Connect commits sink offsets every 60 s.

## E2 — compute representation × insert format (1,000,000 events, batch 10,000, risk rule)

| Steady rows/s | dense | null-heavy (40% nulls) | text-heavy |
|---|---:|---:|---:|
| row + Native (lane B) | 143.1K (140.2–143.9K) | 142.9K (142.0–143.8K) | 133.2K (129.0–134.8K) |
| row + ArrowStream | 153.4K (142.9–156.8K) | 152.6K (151.9–156.6K) | 144.5K (143.3–146.0K) |
| Arrow + Native (`to_pylist`) | 105.5K (100.9–106.1K) | 106.0K (105.7–106.9K) | 100.4K (99.0–100.9K) |
| **Arrow + ArrowStream (lane C)** | **184.3K** (177.6–187.1K) | **186.4K** (184.8–188.7K) | **172.1K** (168.6–173.2K) |
| Worker CPU-s per million, B → C | 7.02 → 5.27 | 7.04 → 5.23 | 7.22 → 5.34 |

Arrow end to end: +29% / +30% / +29% over row + Native. Insert format alone: +7% / +7% / +8%. Arrow + Native:
−26% / −26% / −25%; its `to_pylist` serialize phase takes 2.84–2.88 s per million events. The transform itself takes
52–55 ms per million as Python and 22–24 ms as Arrow kernels.

## E3 — CSV backfill (seeded file; batch 10,000 for the Python paths)

| Rows | File | Method | Load time | Rows/s | Client CPU-s | Server CPU-s | Server peak |
|---:|---|---|---:|---:|---:|---:|---:|
| 1,000,000 | plain | direct HTTP upload, parsed by ClickHouse | **0.46 s** (0.45–0.47) | 2.16M | 0.05 | 0.66 | 391 MiB |
| 1,000,000 | plain | pyarrow CSV → ArrowStream | 0.78 s (0.75–0.83) | 1.29M | 0.39 | 0.35 | 9 MiB |
| 1,000,000 | plain | csv module → Native | 1.96 s (1.92–2.00) | 0.51M | 1.65 | 0.29 | 7 MiB |
| 1,000,000 | gzip | direct HTTP upload | 0.52 s (0.51–0.53) | 1.94M | 0.01 | 0.72 | 391 MiB |
| 1,000,000 | gzip | pyarrow | 0.82 s (0.77–0.93) | 1.22M | 0.51 | 0.36 | 9 MiB |
| 1,000,000 | gzip | csv module | 2.15 s (2.13–2.18) | 0.47M | 1.82 | 0.32 | 7 MiB |

The 100,000-row file shows the same order (`aggregate/e3-backfill.csv`). Server CPU is `OSCPUVirtualTimeMicroseconds`
of the tagged insert queries in `system.query_log`; client CPU is the loading process (`getrusage`). The uncompressed
ArrowStream payload for 1M rows was 148.7 MiB against 82.6 MiB of CSV.

## E4 — correctness and failure (5,000 events, batch 500)

| Scenario | A · sink | A · tuned | A · skip | B · row | C · Arrow |
|---|---|---|---|---|---|
| One malformed record mid-stream | 4,501 stored; DLQ +500 (499 good) | 4,501 stored; DLQ +500 (499 good) | 4,502 stored; **498 good rows missing, no DLQ record** | 5,000; DLQ +1 | 5,000; DLQ +1 |
| Replay the DLQ (`python -m connectedfleet.recovery`) | 998 of 1,000 records replayed, 2 quarantined; **5,000 of 5,000** | — | — | — | — |
| Replay from offset 0, new group | logical unchanged, physical doubled | — | — | logical 5,000, physical 10,000 | logical 5,000, physical 10,000 |
| Crash after insert, before commit | n/a (Kafka Connect owns offsets) | — | — | 5,500 physical / 5,000 logical | 5,500 physical / 5,000 logical |
| 90% nulls (2,000 events) | exact | — | — | exact | exact |
| CSV re-import, PostgreSQL and MongoDB update/delete, outbox rollback | pass (Lab 02) | — | — | pass (Lab 02) | pass (Lab 02) |

## How it was measured

- **Steady rate:** rows of inserts 2..k ÷ (last − first insert time), from ClickHouse's own `ingested_at`.
  **End to end:** rows ÷ (lane start → last queryable row), start-up included, 0.2 s completion poll.
- **Same input for every lane:** one seeded topic per workload, one partition, one consumer; lanes run one at a time into
  emptied tables; synchronous inserts everywhere (`async_insert=0`); equal CPU limits (2 cores).
- **Comparable Kafka clients:** the tuned sink sizes its fetch to the batch; the workers set `fetch.queue.backoff.ms=10` (D-031).
- **Host-load guard (D-032):** before each timing case the harness waits until other work on the host (whole-host user
  CPU minus Docker's VM, Docker Desktop and the harness, including its finished child processes) has stayed under 1.5
  cores for 60 s; it samples during the case and re-runs a case that exceeded that. In the cited run it discarded 30
  attempts and waited 26 minutes; every discarded attempt is listed in `run.json` → `host_guard`.
- **Validator:** META, REPS, STATUS, ERROR, COUNT, CHECKSUM, RISK, NULLS, SCHEMA, PCTL, HOST
  (`src/connectedfleet/bench/validate.py`). `make report` re-validates every saved run with the current rules.

## What changed the conclusions

- **D-019** — custom lanes inherited ClickHouse's default async inserts (~58 ms per insert); every lane now inserts synchronously.
- **D-024** — the worker loop polled up to 0.5 s before starting its flush timer; fixed, latency re-measured.
- **D-025** — lane A runs as three connector variants; the sink's batch-level DLQ is recovered by replay.
- **D-026** — the direct CSV path streams the file, and E3 reports server CPU from the query log.
- **D-029** — a crash no longer loses measured cases (per-experiment checkpoints); checksums group only when duplicates exist.
- **D-031** — librdkafka's prefetch defaults capped both workers near 100K rows/s; the workers set `fetch.queue.backoff.ms=10`
  (probe: `probes/kafka-consume-ceiling-20261010-055e989.jsonl`).
- **D-032** — per-case host-load guard on a shared laptop; the stopped runs document how it was made accurate.
- **D-033** — ClickHouse configured for its 3 GiB container (75% memory ratio, small caches) after an OOM kill.

## Runs

| Run | Status | Use |
|---|---|---|
| `lab03-confirm-20261011T031053Z-434b2a4` | 238/238 valid | **every Lab 03 number** |
| `lab03-smoke-20261010T165506Z-32b2836` | superseded (async inserts) | evidence of D-019 only |
| `lab03-smoke-20261010T174059Z-c39b87f` | host load | correctness only |
| `lab03-smoke-20261010T183128Z-c67d181` | smoke, n = 1, before D-024/D-031 | history |
| `lab03-confirm-20261010T192702Z-a499a19` | E4 pre-flight | history |
| `lab03-confirm-20261010T194353Z-aa2da9c` | crashed in E3 | not cited |
| `lab03-confirm-20261010T205001Z-9c6ac78` | stopped after E1 (Kafka client cap); invalid (`RUNNING`) | evidence of D-031 |
| `…214922Z-2d8587e`, `…220213Z-1f53350` | host load | not cited |
| `…230335Z-59bc304`, `…234600Z-7e220a0`, `…001919Z-d1904b5`, `…005033Z-8a28e84`, `…020918Z-c5e6ab5`, `…022202Z-ae7a455`, `…025237Z-1855aee` | stopped while the host guard was being corrected (8a28e84: the host never became quiet) | not cited |
| `…025753Z-e0e29b1` | stopped (checksum memory) | not cited |

Each stopped run keeps a `NOTE.md` with the reason.

## Limits

- One laptop (Apple arm64, 15 cores; Docker VM 7.7 GiB); one node of each service; no replication or high availability.
- Single-threaded CPython workers against a JVM connector; a JVM or Rust worker would shift the balance.
- One event shape plus null-heavy and text-heavy variants; one simple transform.
- 1,000,000 events per run; the 10M `full` profile was not run. Batches of 100 rows were not re-measured at this commit.
- E5 (Polars/DuckDB interop) not run. The host guard cannot attribute other work's disk I/O.

## Reproduce

```bash
make up init                  # stack, topics, tables, connectors (needs ~12 GiB for Docker)
make lab-02                   # correctness first
make lab-03 PROFILE=confirm   # rebuilds the worker image, then E1-E4; ~2 h on a quiet machine
make report                   # re-validate every run, regenerate aggregate/*.csv and report/summary.txt
```
