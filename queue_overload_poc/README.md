# Queue Overload POC

Evidence for *Your Queue Is Durable. Your System Is Still Overloaded*.

This POC exists to make one claim falsifiable: **a queue cannot create downstream capacity, so when
arrival exceeds service it converts a throughput deficit into time.** Every number in the article
comes from here.

---

## 1 · The problem this exists to answer

A flash sale opens. Checkout accepts orders and publishes them to a topic; a consumer group writes
fulfilment records to PostgreSQL. Traffic doubles for ten minutes.

Nothing fails. The broker is healthy, no message is lost, no consumer errors, the database p99 is
flat. **And a customer who ordered at 14:12 is still waiting at 14:22.**

That is the situation this POC reproduces, and the question it answers is the one you actually face
at 2 a.m.: *of the things you could do about it, which ones do anything?*

Six instincts, six measurements. Four of them turn out to change nothing at all:

| The instinct | What the POC measured | Verdict |
|---|---|---|
| "The queue will absorb it" | Backlog grew to **3.6M**, oldest message **5 minutes** old, broker never unhealthy | It absorbed the messages and stored the *time* |
| "Traffic is back to normal, we recovered" | Worst wait **10 min**; backlog not empty for another **60 min** | A 10-minute spike costs an hour |
| "Add more consumers" | 16x the fleet: throughput ratio **1.0000**, round-trip **5 ms → 80 ms** | Zero extra orders; the queue just moved into threads |
| "Cap concurrency to protect the database" | Dependency back to **5 ms** — backlog **bit-for-bit identical** | Protects the dependency, does nothing for the backlog |
| "Prioritise critical work" | Critical at **0 s**, total backlog **unchanged** | Reorders the debt; does not reduce it |
| "Shed optional work at the door" | Backlog **3.6M → 198K**, **zero** customer orders shed | The only tested lever that bounded it |

**The fix this POC proposes**, in one line: *a concurrency budget protects the dependency, priority
decides who waits, and bounding how much waiting exists means changing a rate — more sustainable
capacity, or less admitted work — so size recovery headroom on purpose, alert on message age rather
than depth, and defer optional work before the debt compounds.*

### The worked example behind those numbers

```
normal arrival     5,000 orders/sec      lambda_n
service capacity   6,000 orders/sec      mu   (C* = 30 concurrent x 5 ms)
flash sale        12,000 orders/sec      lambda_s
spike duration    10 minutes             D
```

The general results, which the tests verify across four unrelated parameter sets so they are not
artefacts of these particular numbers:

```
B_peak         = (lambda_s - mu) * D                 = 3,600,000
W_max          = B_peak / mu                         =   600 s = 10 min
T_recovery     = B_peak / (mu - lambda_n)            = 3,600 s = 60 min
T_recovery / D = (lambda_s - mu) / (mu - lambda_n)   = 6
```

`lambda_s = 2 * mu` is a tidy special case — there `W_max = D` — but it is a special case, and
`test_the_two_by_capacity_case_is_a_special_case_not_the_rule` fails if it is treated as general.

**Recovery headroom is the repayment rate on latency debt** — `mu - lambda_n`, here 1,000/s. It is
the number almost nobody chooses deliberately, and it sets how long customers stay late after the
worst is over.

## 2 · Tier 1 vs Tier 2

| | Tier 1 — deterministic backlog | Tier 2 — live lab |
|---|---|---|
| Stack | Python 3, fixed timestep, no randomness | Kafka (KRaft) + consumer service + PostgreSQL |
| Runs in | under a second | minutes, under Docker Compose |
| Establishes | the behaviour is **exact and reproducible, within the model** | the same behaviour is **corroborated on real software** |
| Ground truth | modelled counters + registered predictions | consumer-group lag, explicit `enqueued_at`, `pg_stat_activity` |
| Status | **built, run, captured** | **built, run, captured** |

Tier 1 is the source of every number the article quotes. Tier 2's job is to show the same *shape* on
software the reader already runs — not to benchmark anything.

## 3 · Architecture

![Two tiers over one logical pipeline: Tier 1 is a deterministic Python simulation with a traffic profile, an admission policy, a FIFO broker queue per class, a priority scheduler, a consumption policy and a bounded-capacity dependency; Tier 2 is the same path on real Kafka and PostgreSQL.](docs/poc-two-tiers.png)

```
TrafficProfile ─> AdmissionPolicy ─> BrokerQueue ─> Scheduler ─> ConsumptionPolicy ─> Downstream
  arrival rate     defers optional    FIFO per class  critical    workers, budget     C* real slots
  over time        on queue age (S7)  exact age       first (S6)  (S4 / S5)           contention
```

- `backlog/queue.py` — the durable log, one per class. Batched by arrival tick, so a long run at
  12,000/s stays exact without allocating tens of millions of objects. Strictly FIFO, so the head
  *is* the oldest message: age is read directly rather than estimated.
- `backlog/downstream.py` — the fulfilment dependency as `C*` **real service slots**. A worker that
  finds every slot busy waits. There is no latency formula in the file: response time is an emergent
  property of contention, and `test_littles_law_holds_on_the_pool` checks `N = X * R` against it
  rather than assuming it. The header records why a retrograde USL curve was rejected.
- `backlog/engine.py` — the tick loop, traffic profile, scheduler, admission and consumption
  policies, and the three separately-named recovery measurements.
- `backlog/scenarios.py` — the runs and their **pre-registered predictions**.
- `backlog/verify.py` — derived and cross-scenario metrics, and the prediction checker.

### The live lab

```
loadgen.py ──produce──> Kafka (KRaft, 8 partitions) ──poll──> consumer.py ──> PostgreSQL 16
  own process             one topic per run                    worker pool      C* slots,
  paced, carries          fresh consumer group                 + semaphore      pg_sleep holds
  enqueued_at                                                                   the slot
```

Three details are deliberate, and each one is a claim the article makes:

**The dependency's useful concurrency is a real semaphore.** `C*` slots, each held for a real
`pg_sleep` inside the transaction. A worker beyond `C*` blocks, exactly as it would on a saturated
connection pool. That is what makes "more consumers do not create capacity" a measurement.

**Age comes from an explicit `enqueued_at` in the payload.** Kafka's record timestamp defaults to
`CreateTime` (producer clock) and `LogAppendTime` would measure broker residence. Neither is the
end-to-end queue delay an SLO cares about, so the lab carries its own and says which it is.

**Offsets are committed after the rows are written, never on a timer,** and prefetch is bounded by
`max_poll_records` — the naive run lets it reach 4,096, the bounded run holds it at 64. In
production you would hold that bound with `pause()`/`resume()`; `consumer.py` explains why this lab
does not, and what went wrong when it did.

## 4 · Scenario matrix

Every message carries a class in every run. S1–S5 ignore it; S6 turns on the scheduler and nothing
else; S7 turns on admission and nothing else. The control runs **before** the treatment.

| Run | The one change | Question it answers |
|---|---|---|
| S1 | arrival 5,000/s | Does the model behave below capacity? |
| S2 | arrival 12,000/s | What does a durable queue do when arrival exceeds service? |
| S3 | + recovery phase | The spike is over. Is the system? |
| S4 | workers 30 → 480 | Does adding consumers create downstream capacity? |
| S5 | + downstream cap | Does bounding concurrency protect the dependency? |
| S6 | + priority scheduler *(control)* | What does prioritising critical work change on its own? |
| S7 | + admission control | Can backpressure bound the debt priority only rearranges? |
| S4b | + caller deadline *(optional)* | What happens past a caller deadline? Not part of the main proof. |

## 5 · Prerequisites

| Need | Version | Used by | Check |
|---|---|---|---|
| Python | 3.9 or newer | both tiers | `python3 --version` |
| pytest | any recent | the 52 unit tests | `python3 -m pytest --version` |
| Docker + Compose | any recent, daemon running | Tier 2 only | `docker compose version` |

```bash
python3 -m pip install pytest        # the only dependency you may need to install
```

Tier 1 needs nothing else: no virtualenv, no requirements file, no network at run time. Tier 2 pulls
`apache/kafka:3.8.0` and `postgres:16-alpine` the first time and builds one small image; the Python
clients (`kafka-python-ng`, `psycopg`) are installed inside that image, never on your machine.

## 6 · Run it

One command runs everything and captures it into a folder you name:

```bash
./run.sh results
```

| Option | What it does |
|---|---|
| `./run.sh results` | Tier 1 + Tier 2 into `results/` — about 3 minutes |
| `./run.sh results-1` | another run into its own folder, so you can compare two runs |
| `./run.sh results --tier1` | Tier 1 only, no Docker needed — about 3 seconds |
| `./run.sh results --keep` | leave the live lab running afterwards (inspect Kafka/PostgreSQL yourself) |
| `./run.sh results --force` | reuse a folder name, overwriting what was there |
| `./run.sh results --open` | open the HTML report when it finishes |
| `./run.sh --help` | the same list |

It refuses to write into `captured-output/`, `reports/` or any source directory — those hold the
capture the article quotes, and an experiment should never overwrite the published evidence. Each
run folder gets its own `.gitignore` containing `*`, so a run is never committed by accident.

The individual pieces still work on their own:

```bash
python3 run.py --scenario all --save      # Tier 1 only, writes captured-output/
python3 run.py --scenario S3 S7           # one or two scenarios
python3 run.py --sweep                    # just the consumer sweep table
python3 -m pytest -q tests                # the 52 unit tests
./run_all.sh                              # Tier 2, step 0 to teardown
make tier1 | make tier2 | make run | make down-all
python3 report.py results                 # rebuild the HTML report from a results folder
```

## 7 · What a run produces

```
results/
├── report.html     open this first — the visual report
├── RESULT.md       the same verdict in Markdown
├── run.log         full transcript
├── tier1/          summary.txt · summary.json · S1…S4b-samples.csv · pytest.txt
└── tier2/          00-prerequisites … 11-verify-clean · REPORT.md
```

`report.html` is one self-contained file — no network, no assets, no JavaScript. It opens with a
PASS/FAIL verdict and five headline numbers, then the backlog comparison, the measured age curve,
the consumer sweep split into service time versus waiting, every scenario with its predictions, and
the live results table.

## 8 · What success looks like

```
8/8 scenarios passed · 34 predictions held · pytest: 52 passed
deterministic (same numbers on a second run): yes
steps with a non-zero exit: 0 · Tier 2 semantic assertions: 18/18 passed · containers left: 0
== PASS
```

A run passes when all of these hold; `run.sh` checks them and sets its exit code accordingly.

| Check | Expected | Why it matters |
|---|---|---|
| Tier 1 scenarios | `8/8 scenarios passed` | every scenario asserts its own claim rather than printing numbers |
| Tier 1 predictions | 34 | each one registered *before* the run |
| Unit tests | `52 passed` | model properties, the queue arithmetic swept over parameters, determinism |
| Determinism | identical `summary.json` on a second run | the simulation is evidence, not a sample |
| Tier 2 steps | every step `exit=0` | the lab built, ran and tore down cleanly |
| Live assertions | `18/18 passed` | the same shapes hold on real Kafka and PostgreSQL |
| Teardown | `containers_left= 0` | nothing left running on your machine |

## 9 · Reproduce the article numbers

| Article claim | Run | Metric | Value |
|---|---|---|---|
| "backlog grows to 3.6M" | S2 | `peak_depth` | 3,600,000 |
| "the worst wait is 10 minutes" | S3 | `max_wait_s` | 600.1 |
| "age is back inside SLO at +55 min" | S3 | `time_to_age_slo_s` | 3,299.7 |
| "the backlog clears at +60 min" | S3 | `time_to_backlog_zero_s` | 3,599.9 |
| "repayment is 6× the spike" | S3 | `debt_ratio` | 6.0 |
| "16× the fleet, 0% more throughput" | S4 | `throughput_vs_s2` | 1.0000 |
| "16× the round-trip" | S4 | `peak_downstream_response_ms` | 80.0 |
| "the budget holds in-flight at 30" | S5 | `peak_downstream_in_flight` | 30 |
| "and the backlog is unmoved" | S5 | `peak_depth` | 3,600,000 |
| "priority reorders but does not bound" | S6 | `peak_depth` | 3,600,000 |
| "critical stays timely under priority" | S6 | `peak_oldest_age_critical_s` | 0.0 |
| "deferrable absorbs the delay" | S6 | `peak_oldest_age_deferrable_s` | 545.4 |
| "admission bounds the backlog" | S7 | `peak_depth` | 197,940 |
| "paid for by deferring optional work" | S7 | `deferred` | 3,742,200 |
| "no customer order is shed" | S7 | `deferred_critical` | 0 |

```bash
python3 -c "
import json; s={x['id']:x['derived'] for x in json.load(open('captured-output/summary.json'))['scenarios']}
print(s['S3']['max_wait_s'], s['S4']['throughput_vs_s2'], s['S6']['peak_depth'], s['S7']['peak_depth'])"
```

From the article root, `python3 tools/validate.py` re-checks every quoted number against this file.

## 10 · Where to verify

- **A prediction was registered before the run:** `backlog/scenarios.py`, in version control.
- **The prediction held:** `captured-output/summary.json` → `scenarios[].predictions[].ok`.
- **The run is deterministic:** `tests/test_tier1.py::test_runs_are_byte_identical`.
- **Nothing was lost:** `tests/test_tier1.py::test_no_message_is_ever_lost`, every run.
- **The arithmetic is not a coincidence:** `test_peak_backlog_is_the_rate_deficit_times_duration` and
  `test_recovery_follows_the_general_formula`, each parametrised over four rate/duration sets that
  include ratios where the old `mu / (mu - lambda_n)` form gives the wrong answer.
- **Response time is emergent, not asserted:** `test_littles_law_holds_on_the_pool` and
  `test_service_time_never_changes_only_waiting_does`.

## 11 · What the POC proves

- **S2** — the modelled queue keeps accepting work and conserves every message while age climbs:
  `enqueued == processed + final_depth` exactly, in every run.
- **S3** — traffic recovery does not erase backlog. Worst wait 600.1 s; age back inside the SLO at
  +55 min; backlog actually empty at +60 min — three different numbers, none of them "recovered".
- **S4** — consumer scaling does not create capacity. 16× the fleet returned a throughput ratio of
  1.0000 and a round-trip of 5 ms → 80 ms. The *service* time never moved; the extra 75 ms is
  waiting for a slot.
- **S5** — a concurrency budget protects the dependency, and that is *all* it does. The backlog is
  bit-for-bit identical to S2.
- **S6** *(control)* — priority holds critical work at 0 s while deferrable absorbs 545.4 s, and the
  total backlog is unchanged. **Priority reorders debt; it does not bound it.**
- **S7** — one flag different from S6: backlog 3,600,000 → 197,940, paid for by deferring 3,742,200
  optional messages and shedding zero customer orders.
- **S4b** *(optional)* — past the deadline each message costs 1.649 attempts, 60.6% of capacity does
  new work, and the queue drains at 0.625× the 30-worker rate. The writes still land; the cost is
  wasted capacity, not lost work.

### Where the POC corrected me

An earlier version combined priority and admission in one run and credited the result to shedding.
The control shows that was wrong: **priority alone already keeps critical work at zero wait.** A
second registered prediction also failed — I expected peak age under 90 s and measured 545.4 s,
because deferring *new* optional work does nothing for optional work *already queued*. Both are
published in the article rather than buried here.

## 12 · What the POC does NOT prove

- It is **not a benchmark**. No throughput numbers for any product.
- It says nothing about **Kafka durability**: no broker crash, no replication failure, no
  crash-recovery test, no persistence guarantee. The word *durable* in the article's title rests on
  documented broker semantics, not on this POC.
- Tier 1's dependency is **a model** with a stated capacity (`C*` slots at a fixed service time). It
  establishes the logic given that capacity; Tier 2 is what corroborates the shape on real software.
- It says nothing about **Kafka tuning**, partitioning, consumer-group internals, or exactly-once.
- It is **not a universal queueing model**: strict FIFO, one service-time class, no batching effects,
  no multi-region behaviour.
- The **6× ratio is a property of these rates.** The formula generalises; the number does not.


## 13 · Files

```
queue_overload_poc/
├── run.sh                 one command: both tiers -> a results folder, PASS/FAIL, exit code
├── run.py                 Tier 1 entry point (--scenario, --save, --out)
├── run_all.sh             Tier 2: step 0 -> teardown, one report per step
├── report.py              a results folder -> one self-contained report.html
├── Makefile               make tier1 | tier2 | run | down-all
├── backlog/               the deterministic model
├── tests/                 52 pytest cases
├── live/                  docker-compose.yml · Dockerfile · init.sql
│                          loadgen.py · consumer.py · verify_results.py
├── captured-output/       the Tier 1 capture the article quotes
├── reports/               the Tier 2 capture the article quotes
└── docs/                  the figures used above
```

`results*/` folders are run artifacts and are gitignored; each one also writes its own `.gitignore`
containing `*`, so a run can never be committed by accident whatever you name it.
