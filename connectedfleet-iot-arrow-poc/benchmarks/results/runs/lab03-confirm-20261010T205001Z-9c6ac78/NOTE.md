# Stopped on purpose after E1: the custom lanes were throttled by a Kafka client default

This confirm-profile run (9c6ac78) completed E1: 80 cases, 0 errors, idle machine (host-load.txt has no foreign
samples). It was interrupted two minutes into E2 (21:35Z; console.log has the interrupt) and its manifest stays at status `RUNNING`, so the validator
excludes it. It is kept because it shows the problem.

What E1 showed (1M events, 5 measured repetitions, medians from run.json):

| Lane | Steady rows/s, batch 1,000 | Steady rows/s, batch 10,000 | Worker CPU-s |
|---|---|---|---|
| A · sink (connector buffer) | 16.7K | 16.5K | — (JVM container scope) |
| A · tuned | 190.2K | 273.1K | — |
| B · row | 102.2K | 107.1K | 6.52 / 7.19 |
| C · Arrow | 102.3K | 107.2K | 5.17 / 5.48 |

B and C reached the same rate while using only 50–70% of one core, and E2's warm-ups (four different compute and
insert combinations) all landed at 90–101K rows/s. A shared ceiling, not the lanes' own work, was setting the rate.

Cause: librdkafka (2.15.1) defaults `queued.min.messages = 100000` and `fetch.queue.backoff.ms = 1000`. Once 100,000
messages are prefetched, the next fetch is postponed by a full second. A consumer that processes faster than that waits.
Measured with `connectedfleet.bench.consume_probe` on a 1M-event topic, consuming only (no decode, no insert):
about 136K records/s with the defaults against 1.0–1.16M records/s with `fetch.queue.backoff.ms = 10`.

Lane A's tuned connector had its Kafka fetch settings sized to the batch; the workers had not. Comparing them this way
would have been unfair to B and C, and E2 could not separate compute from insert format under the cap.

Fixed in the next commit (D-031): the workers set `fetch.queue.backoff.ms = 10` (CONSUMER_CONFIG in
`lanes/worker.py`, unit-tested); `make lab-03` rebuilds the worker image first, and each run records the image id and
build time. The whole confirm profile (E1–E4) is re-run at the fixed commit. Nothing from this run is cited as a result.
