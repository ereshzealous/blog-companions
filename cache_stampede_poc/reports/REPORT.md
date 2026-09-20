# POC run report

Generated 2026-09-20 03:35 UTC by run_all.sh. Worked-example parameters; not a benchmark.

## Tier 1 · deterministic simulation
```
scenario                   requests cache_hits cache_misses origin_calls max_origin_concurrency coalesced_waiters stale_served result 
-------------------------- -------- ---------- ------------ ------------ ---------------------- ----------------- ------------ ------ 
S1 naive                   1,000    0          1,000        1,000        1,000                  0                 0            PASS   
S2 local singleflight      1,000    0          1,000        100          100                    900               0            PASS   
S3 fleet refresh owner     1,000    0          1,000        1            1                      999               0            PASS   
S4 stale-while-revalidate  1,000    0          0            1            1                      0                 1,000        PASS   
S5 TTL jitter              100,000  -          -            -            -                      -                 -            PASS   
S6 cold cache              60,000   45,329     14,671       3,032        20                     54                0            PASS   
S7 retry amplification     1,000    0          1,000        1,100        50                     0                 0            PASS   
S8 stale-set race          2        0          2            2            1                      0                 0            PASS   
```

pytest: 16 passed in 28.70s

## Tier 2 · live lab (4 app processes hosting 100 coalescing scopes, Redis 7, PostgreSQL 16)
```
SUMMARY
run                                          requests     ok  pg calls  max conc  waiters  stale  p99 ms
L1 naive                                         1000   1000       664       188        0      0    1863
L2 local singleflight (100 scopes)               1000   1000       100       100      900      0     274
L3 fleet refresh lease (Redis SET NX PX)         1000   1000         1         1      999      0     298
L4 stale-while-revalidate                        1000   1000         1         1        0   1000      75
L6 cold cache · no origin budget                 3000   3000       990       197        -      -       -
L6 cold cache · origin budget N=20               3000   1703       320        20        -      -       -
L8 stale-set race (real Redis)                      2      2         0         0        -      -       -
L8 stale-set race · plain SET -> v10 ₹69,999 · version-aware write -> v11 ₹64,999

```

Origin calls are PostgreSQL pg_stat_statements counts of the price query (ground truth).

Naive (L1) repeated five times: real arrival timing varies, so the count varies:
```
L1 naive                                         1000   1000      1000       805        0      0    1535
L1 naive                                         1000   1000      1000      1000        0      0     874
L1 naive                                         1000   1000      1000      1000        0      0     879
L1 naive                                         1000   1000      1000      1000        0      0     894
L1 naive                                         1000   1000      1000      1000        0      0     872
```

## Verification

Tier 1: 23 scenario assertions · 16 passed (pytest)

Tier 2 (live Redis + PostgreSQL):
```
Tier 2 semantic assertions: 10/10 passed
```

## Steps
- `00-prerequisites.txt` · exit=0
- `01-clean-slate.txt` · exit=0
- `02-tier1-sim.txt` · exit=0
- `03-tier1-pytest.txt` · exit=0
- `04-lab-build-up.txt` · exit=0
- `05-lab-status.txt` · exit=0
- `06-live-all.txt` · exit=0
- `06b-live-naive-repeats.txt` · exit=0
- `06c-live-verify.txt` · exit=0
- `07-inspect-postgres.txt` · exit=0
- `08-inspect-redis.txt` · exit=0
- `09-lab-logs.txt` · exit=0
- `10-lab-down.txt` · exit=0
- `11-verify-clean.txt` · exit=0
