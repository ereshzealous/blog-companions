# Cache Stampede POC

## What this demonstrates

Four questions, answered by measurement rather than argument:

1. How badly can **one expired key** amplify origin traffic?
2. How much does **local vs fleet-wide coalescing** reduce it?
3. What happens when the **whole cache is useless**?
4. Can **retries or late refreshes** reintroduce the failure?

It demonstrates behaviour with **counts and bounds** — origin calls, peak concurrency, stale responses,
recovery time. **It is not a benchmark** and makes no performance claims. All parameters are a worked
example (1,000 callers, 100 pods, a 200 ms origin, a 5-minute fresh TTL, a 60-second stale window).

## Prerequisites

| Need | Version | Used by | Check |
|---|---|---|---|
| Python | 3.12 or newer | both tiers (the virtual clock uses `asyncio.run(loop_factory=…)`) | `python3 --version` |
| pytest | any recent | the 16 unit tests | `python3 -m pytest --version` |
| Docker + Compose | any recent, daemon running | Tier 2 only | `docker compose version` |

```bash
python3 -m pip install pytest        # the only dependency you may need to install
```

Nothing else: no virtualenv, no requirements file, no network access at run time. Tier 2 pulls
`redis:7.4-alpine` and `postgres:16-alpine` the first time and builds one small image.

## Run it

One command runs everything and captures it into a folder you name:

```bash
./run.sh results
```

| Option | What it does |
|---|---|
| `./run.sh results` | Tier 1 + Tier 2 into `results/` — about 2½ minutes |
| `./run.sh results-1` | another run into its own folder, so you can compare two runs |
| `./run.sh results --tier1` | Tier 1 only, no Docker needed — about 45 seconds |
| `./run.sh results --keep` | leave the live lab running afterwards (inspect Redis/PostgreSQL yourself) |
| `./run.sh results --force` | reuse a folder name, overwriting what was there |
| `./run.sh results --open` | open the HTML report when it finishes |
| `./run.sh --help` | the same list |

It refuses to write into `captured-output/` or `reports/` — those hold the capture the article
quotes, and an experiment should never overwrite the published evidence.

The individual pieces still work on their own if you prefer:

```bash
python3 run.py --scenario all --save      # Tier 1 only, writes captured-output/
python3 run.py --scenario S3 S6           # one or two scenarios
python3 -m pytest -q tests                # the unit tests
./run_all.sh                              # Tier 2, step 0 to teardown
make tier1 | make tier2 | make down-all   # the same, via make
python3 report.py results                 # rebuild the HTML report from a results folder
```

## What a run produces

```
results/
├── report.html     open this first — the visual report
├── RESULT.md       the same verdict in Markdown
├── run.log         full transcript
├── tier1/          summary.txt · summary.json · pytest.txt · prerequisites.txt
└── tier2/          00-prerequisites … 11-verify-clean · REPORT.md
```

`report.html` is one self-contained file — no network, no assets, no JavaScript. It opens with a
PASS/FAIL verdict and four headline numbers, then the origin-load chart, the cold-cache recovery
curves, the expiry histogram, every experiment with its assertions, the live-lab table, and an index
of the files each number came from.

## How to read the results

A run passes when all of these hold; `run.sh` checks them and sets its exit code accordingly.

| Check | Expected | Why it matters |
|---|---|---|
| Tier 1 scenarios | `8/8 scenarios passed` (23 assertions) | every scenario asserts its own claim rather than printing numbers |
| Unit tests | `16 passed` | assertions, determinism, and the version-guard rule |
| Determinism | identical `summary.json` on a second run | the simulation is evidence, not a sample |
| Tier 2 steps | every step `exit=0` | the lab built, ran and tore down cleanly |
| Live assertions | `10/10 passed` | the same claims hold on real Redis and PostgreSQL |
| Teardown | `containers_left= 0` | nothing left running on your machine |

### What the numbers say

The headline: **the same 1,000 callers, one expired key, and four designs.**

| Design | Origin loads (simulated) | Live PostgreSQL calls | What it fixes, and what it does not |
|---|---|---|---|
| no protection | 1,000 | 569–1,000 across runs | nothing — one needed refresh became a load per caller |
| local singleflight | 100 | 100 | collapses per process; the count grows every time you scale out |
| fleet refresh lease | 1 | 1 | one refresh for the whole fleet — but 999 callers wait ~210 ms for it |
| stale-while-revalidate | 1 | 1 | nobody waits (p99 ~1 ms simulated, ~60 ms live); the cost is bounded staleness |

Four more findings complete the picture:

- **Jitter fixes the wrong problem well.** 100,000 keys expiring together became a peak of 8,523 per
  5-second bucket — but only when the jitter is subtracted (`max_ttl − rand(0, 60 s)`). The additive
  form spreads just as evenly and pushed all 100,000 keys past the freshness limit.
- **A cold cache is a capacity event.** Per-pod limits allowed 391 concurrent origin loads (live: 218);
  one aggregate budget of 20 held it at 20 (live: 20). The budget also sets recovery: 90% hit ratio
  after 27 s at N=20, after 12 s at N=50.
- **Bounded degradation is the goal, not universal success.** At N=20 the live lab served 1,687 of
  3,000 requests and deliberately refused 1,313 rather than letting everything queue.
- **Retries are origin work.** With the origin down, 3,000 attempts became 1,100 calls under a 10%
  per-client retry budget.
- **Refresh ownership does not order writes.** A refresher that stalls past its lease overwrote a
  newer price with an older one on real Redis; the version-aware write refused it (v11 kept, not v10).

Live counts for the naive run vary between runs (569–1,000 observed) because real arrival timing
varies — that is why every quoted figure comes from the deterministic tier, and why the live verifier
reports the naive run without asserting it. Every *bound* — 100, 1, 1, ≤20 concurrent — held on every
run.

## Architecture

![Tier 1 execution architecture: run.py selects a scenario, which builds a Policy and fires a burst of 1,000 callers over 100 Pod objects, each with a local singleflight table and a retry budget; the read path reaches SharedCache and branches fresh, stale or absent; the refresh path runs through the local gate, the aggregate origin gate and the 200 ms pricing origin; a virtual clock drives every timer; metrics feed 23 scenario assertions and 16 pytest cases.](docs/figure-16-tier1-runtime.png)

Tier 1 is `run.py` on a virtual clock. A scenario builds a `Policy` — the only thing that differs between
runs — then fires `burst()`: 1,000 callers at one instant, round-robin over 100 `Pod` objects. Each pod owns
its singleflight table and retry budget; `SharedCache` owns the entry, the `SET NX PX` lease and the
version-aware write; `OriginGate` admits or sheds; `PricingOrigin` counts calls and peak concurrency.
`vclock.py` drives arrival, expiry, lease expiry, retry backoff and the 200 ms origin latency, so the run is
deterministic. S5 sits outside the request path: it is a TTL distribution over 100,000 keys.

## Live architecture

![Tier 2 live architecture: a load generator fires 1,000 concurrent GETs at four app processes hosting 25 logical pods each; the apps use Redis 7.4 for the cached value, the SET NX PX refresh lease and a version-aware Lua write; misses go to pricing-svc with its admission semaphore and 250 ms queue timeout, then to PostgreSQL 16 where each read runs pg_sleep(0.2) and pg_stat_statements is the independent witness; run_all.sh runs fourteen steps and verify_results.py runs 10 semantic assertions.](docs/figure-17-tier2-live.png)

Tier 2 is the same shape on real infrastructure. Four app processes host 25 logical pods each — **4 real app
processes hosting 100 logical coalescing scopes** — and the load generator routes `pod = i % 100` to
`APPS[pod // 25]`. Redis 7.4 holds the value (`fresh_until` inside it, `stale_until` as the key's TTL), the
refresh lease and the Lua compare-and-set used by the stale-set test. `pricing-svc` holds the admission
semaphore; with one replica in this lab, that semaphore is the aggregate budget. PostgreSQL runs
`pg_sleep(0.2)` per price read, and `pg_stat_statements` counts those calls independently of the application.

## How evidence is produced

![From claim to evidence: the article claim that fleet coordination collapses duplicate refreshes leads to experiment S3, the one change of a fleet refresh lease, the Tier 1 result of one origin call against an assertion of at most two, the Tier 2 run on real Redis and PostgreSQL, the PostgreSQL witness of one price query, the live assertion in verify_results.py, and the captured artifacts, ending in the published sentence: 1,000 callers to 1 origin load.](docs/figure-18-evidence-chain.png)

Every number quoted in the article is the last link of a chain like this one: a claim, one experiment, one
change, a Tier 1 count with its assertion, a Tier 2 run, an independent PostgreSQL count, a live assertion,
and the file that holds the result.

## Reproducing the article's published numbers

`./run.sh <folder>` writes into a folder of its own and never touches the committed capture. The
published numbers — the ones quoted in the accompanying article — live in `captured-output/` and
`reports/`, produced by:

```bash
python3 run.py --scenario all --save      # -> captured-output/
./run_all.sh                              # -> reports/
```

Run those two only if you intend to replace the published capture. Origin calls in Tier 2 are counted
by PostgreSQL's `pg_stat_statements`, not by the application, so a passing run is never the
application marking its own homework.

## What each scenario proves

| # | Claim in the article | Setup | Assertion | Captured |
|---|---|---|---|---|
| S1 | Correlated misses multiply origin load | 1,000 callers, one expired key, no protection | `origin_calls >= 900` | 1,000 loads |
| S2 | Local singleflight is per process | same burst across 100 pods, each coalescing | `origin_calls <= active_pods` | 100 loads, 900 waiters |
| S3 | A fleet refresh lease gives ~1 load | `SET refresh:key token NX PX 5000`; losers poll the cache | `origin_calls <= 2` | 1 load; p99 210 ms (waiters wait) |
| S4 | Stale-while-revalidate decouples users from refresh | entry past `fresh_until`, inside `stale_until` | 1,000 stale, 1 refresh, p99 < origin | 1 load; p99 1 ms |
| S5 | Jitter spreads many keys, inside the freshness envelope | 100,000 keys: fixed vs `300 − rand(0,60)` vs `300 + rand(0,60)` | peak ≤ 1.3 × mean; max TTL ≤ 300 s | 100,000 → 8,523 per 5 s; `+rand` puts all 100,000 past 300 s |
| S6 | Every limit has a scope; the budget sets recovery | cold cache, 60,000 requests over 30 s, 5,000 Zipf keys | aggregate N holds; per-pod limits do not | per-pod: 391 concurrent; N=20: 20 (90% hits at 27 s); N=50: 50 (12 s) |
| S7 | Retries are origin work | origin down, 3 attempts each vs a 10% per-pod retry budget | unbounded = 3×; budget ≤ 1.1× | 3,000 → 1,100 calls |
| S8 | The refresh lease is an efficiency lock only if duplicate refreshes are harmless | refresher 1 stalls past its lease; price changes; refresher 2 writes; refresher 1 writes late | plain SET regresses; versioned write holds | v10 (₹69,999) vs v11 (₹64,999) |

S7's 10% ratio is the concrete example Google SRE describes, used here to test the mechanism;
a real budget comes from your downstream capacity and SLOs.

## Inside the live lab

Driving it by hand, if you want to poke at it:

```bash
docker compose -f live/docker-compose.yml up -d --build --wait
docker compose -f live/docker-compose.yml run --rm -T loadgen python loadgen.py all    # or L1 | L2 | L3 | L4 | L6 | L8
docker compose -f live/docker-compose.yml exec -T postgres psql -U postgres -d pricing \
  -c "SELECT calls, left(query,60) FROM pg_stat_statements WHERE query LIKE '%prices p%';"
docker compose -f live/docker-compose.yml down -v
```

Topology (`live/docker-compose.yml`):

- `redis` (7.4): real TTL expiry; the value carries `fresh_until`, the key's TTL is `stale_until`.
- `postgres` (16): `prices` table; each price read runs `pg_sleep(0.2)` to model a slow query.
  `pg_stat_statements` is the ground truth for origin calls.
- `pricing`: the origin service, with an admission semaphore (the aggregate budget: there is one replica).
- `app1`–`app4`: **four real application processes simulating 100 coalescing scopes** (25 logical
  pods each, each with its own singleflight table). Modes: `naive | local | fleet | swr`.
- `loadgen`: fires 1,000 concurrent GETs at the expiry instant, or a cold-cache stream.

Captured run (`reports/REPORT.md`):

```
run                                          requests     ok  pg calls  max conc  waiters  stale  p99 ms
L1 naive                                         1000   1000       572       182        0      0    1833
L2 local singleflight (100 scopes)               1000   1000       100       100      900      0     281
L3 fleet refresh lease (Redis SET NX PX)         1000   1000         1         1      999      0     289
L4 stale-while-revalidate                        1000   1000         1         1        0   1000      65
L6 cold cache · no origin budget                 3000   3000       994       195        -      -       -
L6 cold cache · origin budget N=20               3000   1690       320        20        -      -       -
```

Naive L1 was repeated five more times and made 1,000 PG calls each time; across the six runs the
range is **572–1,000**. Real arrival timing varies (the first run after start-up delivered the burst
more slowly, so later requests found the refreshed key); the simulation does not vary.

`pg_sleep(0.2)` demonstrates slow-origin concurrency and call amplification. It is **not** a model
of PostgreSQL saturation.

### A lab note: why the load generator avoids httpx

At 1,000 concurrent connections, httpx spent ~5.5 ms of CPU per request in this lab, while raw
asyncio sockets took ~0.06 ms. That turned a simultaneous burst into a 5-second trickle, so later
requests found the refreshed key and the stampede disappeared. `live/minihttp.py` is a 40-line
HTTP/1.1 client used by both the load generator and the app → pricing hop. A load generator that
cannot generate simultaneity cannot demonstrate correlated misses.

## Files

```
cache_stampede_poc/
├── run.sh                   one run -> one results folder, with a verdict
├── report.py                results folder -> self-contained report.html
├── run.py                   Tier 1 CLI + summary table
├── run_all.sh               step 0 → teardown, reports/ + REPORT.md
├── Makefile                 make all | tier1 | tier2 | down-all | clean
├── docs/                    the three architecture figures used above
├── stampede/
│   ├── vclock.py            deterministic virtual-time asyncio loop
│   ├── model.py             SharedCache, PricingOrigin, OriginGate, RetryBudget, Policy, Pod
│   └── scenarios.py         S1–S8 with their assertions
├── tests/test_scenarios.py  16 pytest cases
├── captured-output/         summary.txt, summary.json, pytest.txt
├── reports/                 00-prerequisites … 11-verify-clean, REPORT.md
└── live/                    docker-compose.yml, Dockerfile, init.sql, app.py, pricing_svc.py, loadgen.py, minihttp.py
```

## What this POC does not claim

- No latency benchmarks.
- No Redis Cluster, failover or Redlock behaviour.
- No multi-region.
- No production traffic.
- No universal TTL, budget or retry values.
- The virtual-clock loop uses two private asyncio attributes, which is fine for a teaching tool and not for production.
