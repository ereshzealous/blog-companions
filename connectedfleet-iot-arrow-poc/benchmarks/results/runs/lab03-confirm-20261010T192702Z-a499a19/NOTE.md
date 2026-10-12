# Pre-flight: E4 only, at a499a19

This run exercised the new E4 code paths before the long confirm run: the three lane-A connector variants
(`bench/sinkcfg.py`) and DLQ recovery (`connectedfleet.recovery`). It validates (14/14 cases).
The machine was idle: `host-load.txt` has no foreign-container samples.

Results:
- `sink`: one bad record mid-stream dead-lettered its 500-record batch (499 good rows); 4,501 of 5,000 rows were stored.
  Recovery read 1,000 DLQ records (two passes), replayed 998 and quarantined the 2 copies of the poison record.
  The table then held 5,000 of 5,000 rows. The dead-lettered rows were recoverable, not lost.
- `sink_tuned`: same outcome, 4,501 of 5,000 with DLQ +500. Removing the connector buffer does not change
  batch-level dead-lettering, which ClickHouse documents.
- `sink_skip` (ClickHouse `input_format_allow_errors_num=1000`): **4,502 of 5,000 stored, DLQ +0, poison not stored.
  498 good rows disappeared with no DLQ record.**
  - In `system.query_log`, the batch holding the poison (query 389b3ec5-…) read 1 row and wrote 1 row out of 500,
    with no error.
  - Reproduced directly against the same ClickHouse with 500 JSONEachRow records and a bad second row:
    - newline-separated: 499 of 500 stored;
    - the same records concatenated with no newlines: 1 of 500 stored.
  - So the skip setting only drops the bad row when it can resync on a newline. The connector's body behaves like
    the concatenated case. That is inferred from the exact match; the body itself was not captured.
  - Conclusion: this "hardening" turns a recoverable batch failure into silent data loss.
- Lanes B and C: 5,000 of 5,000 with DLQ +1. Crash window: 5,000 logical, 5,500 physical. 90% nulls: preserved.

Found during this run: three benchmark connectors (`cfbench-a-*`), leaked by two aborted runs at 16:52–16:54Z
(sampler crash, fixed in 32b2836), had idled in the Connect JVM through every later run. Measured cost at idle:
0.41 vs 0.36 CPU-s per 30 s with and without them, about 0.2% of one core and about 1% of lane A's container CPU
per smoke case. Negligible, but disclosed. Fixed in the next commit: connectors are deleted in a `finally` block,
and any stale `cfbench-*` connector is removed at run start (recorded in the manifest).
