# Aborted: no manifest, no results

This confirm-profile run (aa2da9c) completed E1 and E2 (186 cases, 0 errors) and most of E3. It then crashed at
20:44:27Z. Only `stdout.log` survives; it is kept for the record.

- Cause 1, the query: E3's content hash grouped 1M CSV rows by key across 10 columns to remove duplicates.
  That needs about 1.3 GiB, and ClickHouse (3 GiB container, 2.69 GiB resident at the time) refused with
  `MEMORY_LIMIT_EXCEEDED`. Each E3 load goes into an emptied table, so its rows are unique and the grouping was
  unnecessary.
- Cause 2, the harness: an exception in any experiment aborted the run before `write()`. The manifest was only
  written at the end, so every measured case was lost.

Fixed in the next commit (D-029), test first:

- `content_hash(dedupe=False)` hashes rows directly in constant memory. It gives the same hash as the
  de-duplicating form when rows are unique; an integration test checks that against ClickHouse.
- E3 de-duplicates only if a load actually produced duplicates, and the de-duplicating form may spill to disk.
- `run_experiments` records a failing experiment and carries on with the rest. The manifest is checkpointed after
  every experiment. A run with a failed experiment ends with status `PARTIAL`.

The log's wall-clock rates are not used anywhere: steady rates, latency, CPU and phase costs were never saved for
this run.
