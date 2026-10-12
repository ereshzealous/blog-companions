# Correctness evidence only: timing measured under heavy unrelated host load

This run validates (119/119 cases) at commit c39b87f, which includes the fixes in decisions D-019 to D-021:
synchronous inserts in every lane, a mid-stream E4 poison record, and lag measured over the produce window.

**Do not cite its throughput, CPU or latency numbers.** Unrelated Docker containers on the same laptop
(user benchmark jobs, not part of this POC) used a mean of 881% and a peak of 1,658% CPU across 40 samples.
The machine has 15 cores. The samples are in `host-load.txt`, taken every 30 s while the run was active.
Lanes run one after another while that load varied, so a lane can look slower only because it ran at a
busier moment. Examples of the distortion:

- the official sink measured 7.7K steady rows/s at batch 1,000, against 38K in run 32b2836 with the same settings;
- in E2, string-heavy data came out faster than dense data, the reverse of every other measurement.

What this run does establish, because correctness does not depend on CPU contention:

- **E4, one mid-stream poison record (batch 500).** The official sink dead-lettered the whole batch holding it:
  DLQ +500, 499 of them good records. 4,501 of 5,000 good rows were stored.
  Lanes B and C dead-lettered only the poison and stored 5,000 of 5,000.
- **Replay from offset 0 with a new consumer group.** All lanes re-inserted exactly what their first pass
  inserted: logical rows unchanged, physical rows doubled.
- **Crash after the ClickHouse ack, before the commit (B and C).** No loss after the restart:
  5,000 logical rows, 5,500 physical rows.
- **Null-heavy edge (90% nulls).** All three lanes preserved the null counts exactly.
- **Synchronous inserts.** The fix is in effect: `AsyncInsertQuery` = 0 for every lane.

The git_dirty flag is true because untracked result files existed. The code was exactly commit c39b87f.
