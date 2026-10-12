# Superseded: lanes B/C used asynchronous inserts

The run passed every validation rule (119/119 cases), but its lane A vs lane B/C comparison
is confounded. **Do not cite it as the A/B/C throughput or latency result.** The superseding
run is the next Lab 03 smoke run, at the commit that adds `SYNC_INSERT` (decision D-019).

What went wrong (found 2026-10-10 from `system.query_log` after the run):

| Lane | Insert settings sent | Server-side `AsyncInsertQuery` | Mean server insert time |
|---|---|---|---|
| A, official sink (JSONEachRow) | `async_insert=0` (sent by the connector) | 0 | 3.4 ms (2,924 inserts) |
| B, row worker (Native) | none, so the server default applied | 1 | 57.7 ms (3,447 inserts) |
| C, Arrow worker (ArrowStream) | none, so the server default applied | 1 | 58.1 ms (4,145 inserts) |

ClickHouse 26.3.47.6 defaults to `async_insert=1`, `wait_for_async_insert=1` and
`async_insert_use_adaptive_busy_timeout=1` (`async_insert_busy_timeout_min_ms=50`). So every
insert from a single-writer custom lane waited in the async queue for at least ~50 ms. That
fixed cost dominated lanes B/C at small batches and hid compute differences between B and C.

Still usable from this run, labelled as such:

- B vs C relative results. Both paid the same async wait, but the wait dilutes compute differences.
- The cost of the default: ~55 ms per insert for a single writer.
- The E3 direct-load figures. CSVWithNames inserts were not async (`AsyncInsertQuery=0`).

Two harness issues were also found here and fixed in the same commit:

- **E4 sink poison race.** The poison record was produced last, alone in the sink's trailing
  partial buffer. The harness deleted the connector as soon as 5,000 rows had arrived, before
  that buffer was flushed. Result: "DLQ +0", marked `partial` (decision D-020).
- **Lane A lag shown as 0.** Kafka Connect commits sink offsets every 60 s
  (`offset.flush.interval.ms` default). During a 10 s burst there were no committed offsets,
  and the 0 came from after the burst. Lag is now computed over the produce window only, and is
  "n/m" when unobservable (decision D-021).
