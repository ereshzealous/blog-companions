# Blog Companions

Source code and runnable reference clusters for my engineering blog posts.

Each subdirectory is a self-contained, end-to-end demo paired with the article that explains it. Every demo here actually runs — `git clone`, `make all`, look at the result.

## Demos

| # | Demo | What it shows | Article |
|---|------|---------------|---------|
| 1 | [`otel_kubernetes_demo/`](./otel_kubernetes_demo) | A production pattern for hybrid OpenTelemetry collector deployments on Kubernetes — sidecar **and** DaemonSet running side-by-side, verified by a 154-span end-to-end trace across gRPC → Kafka → Postgres → WebSocket. | _link to Medium post when published_ |
| 2 | [`cache_stampede_poc/`](./cache_stampede_poc) | Why one expired hot key can take down the database behind a healthy cache — eight experiments run twice: a deterministic simulator on a virtual clock, and a live Redis 7.4 + PostgreSQL 16 lab where `pg_stat_statements` counts the origin calls. | _link to Medium post when published_ |
| 3 | [`queue_overload_poc/`](./queue_overload_poc) | Why a durable queue can be perfectly healthy while the customer waits — a 10-minute spike that takes an hour to repay, run twice: a deterministic simulator with 34 predictions registered before the run, and a live Kafka KRaft + PostgreSQL 16 lab where 8× the consumers buy 1.05× the throughput. | _link to Medium post when published_ |
| 4 | [`cdc_recovery_poc/`](./cdc_recovery_poc) | Why a CDC connector can report `RUNNING` while the data users query is half an hour stale — a PostgreSQL 18 → Debezium 3.6 → Kafka 4.3 → ClickHouse 26.8 pipeline broken at every durable boundary under 20,000 changes/sec, then reconciled key by key: 2,650,391 rows with 0 discrepancies, and one unrecoverable case that fails loudly instead of pretending to resume. | _link to Medium post when published_ |

## Conventions across all demos

- One top-level `Makefile` per demo with `make all` (full setup) and `make down-all` (full teardown).
- One `README.md` per demo with prerequisites, the command sequence, and the expected result.
- No hidden state — if it isn't in the repo, it doesn't run.

---

*Maintainer: [@ereshzealous](https://github.com/ereshzealous) · Issues and PRs welcome.*
