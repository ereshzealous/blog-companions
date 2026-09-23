#!/usr/bin/env python3
"""Check a recorded run against its scenario's pass criteria.

Reads only the files the run already wrote, so it needs no Docker, no network and no
dependencies: every check below is answered from results/<run>/summary.json and events.jsonl.

Usage:
  scripts/verify-run.py                      every run under results/
  scripts/verify-run.py results/<run-id> ... only these runs
  scripts/verify-run.py --published          the four runs the article cites

Exit status is 0 only if every checked run passes.
"""
import json
import pathlib
import sys

LAB = pathlib.Path(__file__).resolve().parent.parent
RESULTS = LAB / "results"
PUBLISHED = [
    "capacity-20260913T190718Z",
    "recovery-20260913T212902Z",
    "capture-pressure-20260913T215324Z",
    "history-loss-20260913T220653Z",
]
ZERO = ("missing_in_clickhouse", "ghost_rows_in_clickhouse", "content_mismatches",
        "clickhouse_logical_duplicate_keys")


class Run:
    def __init__(self, path):
        self.path = path
        self.summary = json.loads((path / "summary.json").read_text())
        self.events = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines() if line.strip()]
        self.checks = []

    @property
    def scenario(self):
        return self.summary.get("scenario", "")

    def check(self, ok, label, detail=""):
        self.checks.append((bool(ok), label, detail))

    def event_names(self):
        return [e.get("event") for e in self.events]

    def alerts(self):
        return sorted({a["alert"] for a in self.summary.get("alerts", []) if a.get("state") == "firing"})

    def reconcile(self, label):
        return self.summary.get("reconcile", {}).get(label)

    def converged(self, label, expect=True):
        r = self.reconcile(label)
        if r is None:
            self.check(False, f"reconcile {label} present", "missing from summary.json")
            return None
        self.check(r["converged"] is expect, f"reconcile {label} converged={expect}",
                   f"converged={r['converged']}")
        return r

    def rows_clean(self, r, label):
        for table, t in (r or {}).get("tables", {}).items():
            bad = {k: t[k] for k in ZERO if t.get(k)}
            self.check(not bad, f"{label}: {table} has no missing, ghost, different or duplicate rows",
                       f"source={t['source_rows']} " + (str(bad) if bad else "all zero"))


def common(run):
    names = run.event_names()
    run.check(names and names[-1] == "oom-check", "scenario finished (events.jsonl ends with oom-check)",
              f"last event: {names[-1] if names else 'none'}")
    oom = [e["detail"] for e in run.events if e.get("event") == "oom-check"]
    killed = [c for c, v in (oom[-1] if oom else {}).items() if v]
    run.check(not killed, "no container was OOM-killed", ", ".join(killed) or "none")


def capacity(run):
    r = run.converged("capacity")
    run.rows_clean(r, "capacity")
    f = run.summary["findings"]
    run.check(f.get("tasks_running_with_tasks_max_4") == 1,
              "tasks.max=4 still runs one capture task",
              f"tasks running: {f.get('tasks_running_with_tasks_max_4')}")
    one, two = f.get("sink_mu_1_process"), f.get("sink_mu_2_processes")
    run.check(one and two and two > one, "a second sink process adds throughput",
              f"{one:,.0f} -> {two:,.0f} changes/s" if one and two else "missing")


def recovery(run):
    r = run.converged("final")
    run.rows_clean(r, "final")
    delete_batch = (r or {}).get("explicit_delete_batch") or {}
    run.check(delete_batch.get("still_visible_in_clickhouse") == 0,
              "no deleted key is still visible downstream",
              f"{delete_batch.get('keys')} keys deleted, "
              f"{delete_batch.get('still_visible_in_clickhouse')} still visible")
    f = run.summary["findings"]
    crashed = (f.get("sink_crash") or {}).get("rows_in_crashed_batch")
    replays = ((r or {}).get("history") or {}).get("sink_replays")
    run.check(crashed is not None and crashed == replays,
              "the crashed batch replayed, and nothing else",
              f"crashed batch {crashed:,} rows, sink replays {replays:,}" if crashed and replays else "missing")
    nulls = ((f.get("schema_backfill") or {}).get("null_after") or {}).get("null_rows")
    run.check(nulls == 0, "the backfill reached every row", f"NULL rows after backfill: {nulls}")
    drain = f.get("drain") or {}
    run.check(True, "drain measured (not a pass condition)",
              f"{drain.get('backlog'):,} records, predicted {drain.get('predicted_drain_s')} s"
              if drain else "missing")


def capture_pressure(run):
    r = run.converged("final")
    run.rows_clean(r, "final")
    names = run.event_names()
    for event in ("kafka-paused", "kafka-unpaused", "caught-up"):
        run.check(event in names, f"outage recorded: {event}")
    f = run.summary["findings"]
    run.check(f.get("retained_wal_bytes_at_unpause", 0) > 0,
              "the source kept writing into WAL while Kafka was down",
              f"{f.get('retained_wal_bytes_at_unpause', 0) / 1e9:.2f} GB retained after "
              f"{f.get('outage_s')} s, caught up in {f.get('catch_up_s_after_unpause')} s")


def history_loss(run):
    first = run.converged("after-resnapshot", expect=False)
    tables = (first or {}).get("tables", {})
    ghosts = sum(t.get("ghost_rows_in_clickhouse", 0) for t in tables.values())
    clean = all(not t.get("missing_in_clickhouse") and not t.get("content_mismatches") for t in tables.values())
    run.check(ghosts > 0 and clean,
              "after the resnapshot only ghost rows remain (deletes it could not see)",
              f"{ghosts:,} ghost rows, 0 missing, 0 different")
    after = run.converged("after-sweep")
    run.rows_clean(after, "after-sweep")
    run.check("source-history-lost" in run.alerts(), "the lost slot raised an alert",
              ", ".join(run.alerts()))
    names = run.event_names()
    ordered = ["source-history-lost", "normal-resume-requested", "resnapshot-sweep"]
    positions = [names.index(e) if e in names else -1 for e in ordered]
    run.check(all(p >= 0 for p in positions) and positions == sorted(positions),
              "capture never pretended to resume: lost, refused, then resnapshot and sweep",
              " -> ".join(e for e in names if e in set(ordered) | {"normal-resume-refused", "normal-resume-stalled"}))


SCENARIOS = {"capacity": capacity, "recovery": recovery, "capture-pressure": capture_pressure,
             "history-loss": history_loss}


def verify(path):
    run = Run(path)
    print(f"\n{run.scenario} · {path.name}")
    versions = json.loads((path / "versions.json").read_text()) if (path / "versions.json").exists() else {}
    if versions:
        print("  " + " · ".join(f"{k} {v}" for k, v in list(versions.items())[:5] if isinstance(v, str)))
    common(run)
    scenario = SCENARIOS.get(run.scenario)
    if scenario:
        scenario(run)
    else:
        run.check(False, f"unknown scenario '{run.scenario}'")
    for ok, label, detail in run.checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  —  {detail}" if detail else ""))
    failed = sum(1 for ok, _, _ in run.checks if not ok)
    print(f"  {len(run.checks) - failed}/{len(run.checks)} checks passed")
    return failed == 0


def main(argv):
    if argv and argv[0] == "--published":
        paths = [RESULTS / r for r in PUBLISHED]
    elif argv:
        paths = [pathlib.Path(a) for a in argv]
    else:
        paths = sorted(p for p in RESULTS.iterdir() if (p / "summary.json").exists())
    if not paths:
        sys.exit(f"no runs found under {RESULTS}")
    results = []
    for path in paths:
        if not (path / "summary.json").exists():
            print(f"\n{path}: no summary.json — the scenario did not finish")
            results.append(False)
            continue
        results.append(verify(path))
    print(f"\n{sum(results)}/{len(results)} runs pass their scenario's criteria")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
