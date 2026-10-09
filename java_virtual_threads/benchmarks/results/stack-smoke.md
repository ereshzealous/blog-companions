# Full-stack smoke run (docker compose + bootRun)

Machine: Apple M5 Pro (15 cores), 24 GB, macOS 26.5, OpenJDK 21.0.10 (Homebrew), Docker 29.8.
Stack: `docker compose up -d` (Postgres 16 primary :6100 + physical streaming replica :6101,
Redis 7.2 :6800, Kafka 3.9.1 KRaft :9092) and `./gradlew bootRun`. Recorded 2026-10-09.

## Replication
- `pg_stat_replication` on the primary: `state=streaming`, `sync_state=async`, `replay_lag=00:00:00.000045`.
- Replica: `pg_is_in_recovery()=t`, WAL receiver `streaming`, 400 campaigns replicated.

## Bid path
| request | status | total | budget lookup | tier |
|---|---|---|---|---|
| first bid (cold JVM, cold caches) | 200 | 46.7 ms | 22.1 ms | DB |
| second bid | 200 | 0.61 ms | 0.11 ms | L1 |
| floor above every campaign | 204 | | | |
| win notice `GET /win` | 202 | | | |
| bid after the win settled | 200 | 3.2 ms | 2.7 ms | DB (L1 evicted by Pub/Sub, L2 deleted after commit) |

`win_ledger` contained the settled auction; `rtb_budget_invalidations_received_total=1`.

## Load (ab, single-threaded client on the same laptop, keep-alive, c=256)
- 100,000 requests, 33,164 req/s; p50 5 ms, p90 7 ms, p99 69 ms, max 585 ms (closed loop, client-side queueing included).
- All 99,712 "failed" requests are `ab` Length mismatches (the JSON latency fields vary); Micrometer counted 140,003 HTTP 200 on the bid endpoint across both runs.
- L1: 120,005 hits, 4 misses. Limiter settled at 336 (seed 240); 0 sheds. `jdk.VirtualThreadPinned` >= 5 ms: 0 events. App RSS 389 MB.

## Replica lag breaker (pg_wal_replay_pause on the replica, writes on the primary)
- Before: lag 0 ms, replica usable.
- Replay paused: probe reported `lag 26308 ms over threshold` and opened; reads fell back to the primary (`routes_fallback` 1 -> 2).
  The figure is the age of the last replayed commit (26 s before), not the size of the gap: replay-timestamp lag measures staleness.
- Replay resumed: closed after 3 consecutive probes under 200 ms (lag 0 ms).

## Second run (after adding jdk.JavaMonitorEnter streaming and eager Hikari pools)
Same stack and machine. Warm-up of 20,000 requests, then:
- 100,000 requests, 32,050 req/s; p50 5 ms, p90 7 ms, p99 83 ms, max 344 ms. All "failed" requests are `ab` Length mismatches; 120,000 HTTP 200 counted.
- `rtb_vthread_pinned`: 0 events.
- `rtb_vthread_monitor_blocked_total{site="NioChannel.close:83"} 2` — log: `jdk.JavaMonitorEnter for 8 ms at NioChannel.close:83` (Tomcat 10.1.42's connection-close path).
- Limiter settled at 304; 0 sheds.
Raw output: [stack-smoke-2.txt](stack-smoke-2.txt).
