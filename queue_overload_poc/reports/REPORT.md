# POC run report

Generated 2026-09-20 17:18 UTC by run_all.sh. Worked-example parameters; not a benchmark.

## Tier 1 · deterministic simulation
```
  peak backlog                        0   (critical 0 · deferrable 0)
  peak oldest age                   0.0 s
  worst wait                        0.0 s   (0.0 s)
  time to backlog zero            never
  peak backlog                3,600,000   (critical 1,617,600 · deferrable 1,977,000)
  peak oldest age                 300.0 s
  worst wait                      300.0 s   (5.0 min)
  time to backlog zero            never
  peak backlog                3,599,900   (critical 1,619,685 · deferrable 1,980,215)
  peak oldest age                 600.0 s
  worst wait                      600.1 s   (10.0 min)
  time to backlog zero         60.0 min   (6.0x the spike)
  peak backlog                3,600,000   (critical 1,617,600 · deferrable 1,977,000)
  peak oldest age                 300.0 s
  worst wait                      300.0 s   (5.0 min)
  time to backlog zero            never
  peak backlog                3,600,000   (critical 1,617,600 · deferrable 1,977,000)
  peak oldest age                 300.0 s
  worst wait                      300.0 s   (5.0 min)
  time to backlog zero            never
  peak backlog                3,600,000   (critical 0 · deferrable 3,594,600)
  peak oldest age                 545.4 s
  worst wait                      545.4 s   (9.1 min)
  time to backlog zero            never
  peak backlog                  197,940   (critical 0 · deferrable 197,940)
  peak oldest age                 567.0 s
  worst wait                      330.1 s   (5.5 min)
  time to backlog zero            never
  peak backlog                5,174,431   (critical 2,325,232 · deferrable 2,841,681)
  peak oldest age                 431.2 s
```

pytest: 52 passed in 1.46s

## Tier 2 · live lab (Kafka KRaft + PostgreSQL 16, scaled to a laptop)

Rates are scaled: the dependency has C* real slots and each write holds one for a real
PostgreSQL `pg_sleep`. The lab reproduces the shape, it does not benchmark either product.

```
SUMMARY
run  name                      rate/s  workers  processed   thr/s   rt p95  svc p50  prefetch   age p95  deferred
-----------------------------------------------------------------------------------------------------------------
L1   healthy baseline             176        8       2112   189.2     31.2     28.0        64     993.8         0
L2   sustained overload           640        8       7680   268.9     32.1     27.9        64   18465.0         0
L3   naive consumer scaling       640       64       7680   282.8     34.3     27.3      3918   16152.8         0
L4   bounded consumption          640       64       7680   270.4    230.9     28.3        64   18005.7         0
L5   priority only (control)      640       64       7680   271.8    228.3     28.2        64   18773.3         0
L6   priority + admission         640       64       3860   243.9    158.5     28.2        64    5587.5      3820

```

Queue delay is measured from an explicit `enqueued_at` in the payload — end-to-end queue
delay, not Kafka's `CreateTime` and not broker residence.

## Verification

Tier 1: 34 registered predictions · 52 passed (pytest)

Tier 2 (live Kafka + PostgreSQL):
```
Tier 2 semantic assertions: 18/18 passed
```

## Steps
- `00-prerequisites.txt` · exit=0
- `01-clean-slate.txt` · exit=0
- `02-tier1-sim.txt` · exit=0
- `03-tier1-pytest.txt` · exit=0
- `04-lab-build-up.txt` · exit=0
- `05-lab-topic.txt` · exit=0
- `06-live-all.txt` · exit=0
- `06b-live-verify.txt` · exit=0
- `07-inspect-postgres.txt` · exit=0
- `08-inspect-kafka.txt` · exit=0
- `09-lab-logs.txt` · exit=0
- `10-lab-down.txt` · exit=0
- `11-verify-clean.txt` · exit=0
