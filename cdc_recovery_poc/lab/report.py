#!/usr/bin/env python3
"""Turn one run's raw evidence into summary.json and report.md.

Usage: report.py <capacity|recovery|capture-pressure|history-loss>

Every number is derived from files in results/<run-id>/ (collector samples, generator and sink counters,
scenario events, reconciliation) or from ClickHouse's change_history table. Nothing is typed in by hand.
"""
import json
import statistics
import sys

import clickhouse_connect

from common import ch_kwargs, run_dir

RUN = run_dir()


def jsonl(name):
    try:
        with open(RUN / name) as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


EVENTS = jsonl("events.jsonl")
METRICS = jsonl("metrics.jsonl")
GENERATOR = jsonl("generator.jsonl")
ALERTS = jsonl("alerts.jsonl")
T0 = EVENTS[0]["ts_ms"] if EVENTS else (METRICS[0]["ts_ms"] if METRICS else 0)


def ev(name, which=0):
    matches = [e for e in EVENTS if e["event"] == name]
    return matches[which] if len(matches) > abs(which) else None


def rel(ts):
    return None if ts is None else round((ts - T0) / 1000, 1)


def window(rows, start, end):
    return [r for r in rows if start <= r["ts_ms"] <= end]


def rate(samples, key):
    """Least-squares slope of a counter per second over the samples."""
    pts = [(s["ts_ms"] / 1000, s[key]) for s in samples if s.get(key) is not None]
    if len(pts) < 3:
        return None
    mx = statistics.fmean(p[0] for p in pts)
    my = statistics.fmean(p[1] for p in pts)
    den = sum((p[0] - mx) ** 2 for p in pts)
    return round(sum((p[0] - mx) * (p[1] - my) for p in pts) / den, 1) if den else None


def mean(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None]
    return round(statistics.fmean(vals), 1) if vals else None


def peak(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None]
    return max(vals) if vals else None


def load_reconcile(label):
    try:
        return json.loads((RUN / f"reconcile-{label}.json").read_text())
    except FileNotFoundError:
        return None


def series(step_s=10):
    """Collector samples downsampled for charts: lag, oldest unapplied age, WAL retained, source and Kafka rates."""
    out, bucket = [], None
    for s in METRICS:
        b = (s["ts_ms"] - T0) // (step_s * 1000)
        if b != bucket:
            out.append({"t_s": b * step_s, "sink_lag": s.get("sink_lag"), "oldest_unapplied_age_ms": s.get("oldest_unapplied_age_ms"),
                        "retained_wal_bytes": s.get("retained_wal_bytes"), "unconfirmed_wal_bytes": s.get("unconfirmed_wal_bytes"), "kafka_end_offsets": s.get("kafka_end_offsets"),
                        "task_state": s.get("task_state"), "wal_status": s.get("wal_status")})
            bucket = b
    for prev, cur in zip(out, out[1:]):
        if prev["kafka_end_offsets"] is not None and cur["kafka_end_offsets"] is not None:
            cur["kafka_ingest_per_s"] = round((cur["kafka_end_offsets"] - prev["kafka_end_offsets"]) / step_s)
    gen = {(g["ts_ms"] - T0) // (step_s * 1000) * step_s: g["changes_per_s"] for g in GENERATOR}
    for point in out:
        point["source_changes_per_s"] = gen.get(point["t_s"])
    return out


def freshness(ch, start_ms, end_ms, step_s=10):
    rows = ch.query(f"""
        SELECT intDiv(toUnixTimestamp64Milli(applied_at) - {T0}, {step_s * 1000}) * {step_s} AS t_s, count(),
               quantilesExact(0.5, 0.95, 0.99)(toUnixTimestamp64Milli(applied_at) - source_ts_ms)
        FROM cdc.change_history
        WHERE snapshot = 'false' AND toUnixTimestamp64Milli(applied_at) BETWEEN {start_ms} AND {end_ms}
        GROUP BY t_s ORDER BY t_s""").result_rows
    return [{"t_s": t, "applied": n, "p50_ms": q[0], "p95_ms": q[1], "p99_ms": q[2]} for t, n, q in rows]


def freshness_window(ch, start_ms, end_ms):
    n, q = ch.query(f"""
        SELECT count(), quantilesExact(0.5, 0.95, 0.99)(toUnixTimestamp64Milli(applied_at) - source_ts_ms)
        FROM cdc.change_history
        WHERE snapshot = 'false' AND toUnixTimestamp64Milli(applied_at) BETWEEN {start_ms} AND {end_ms}""").result_rows[0]
    return {"applied": n, "p50_ms": q[0], "p95_ms": q[1], "p99_ms": q[2]} if n else None


def capacity(summary, ch):
    phases = {}
    names = ["fill-tasks1", "catchup-tasks1", "fill-tasks4", "catchup-tasks4", "c1", "c2"]
    for name in names:
        s, e = ev(f"phase-{name}-start"), ev(f"phase-{name}-end")
        if not (s and e):
            continue
        m = window(METRICS, s["ts_ms"] + 10000, e["ts_ms"])
        if name.startswith("catchup"):
            # Offsets confirm on a flush interval, so the phase end lags the real catch-up: fit only while Kafka grew.
            grew = [b for a, b in zip(m, m[1:]) if (b.get("kafka_end_offsets") or 0) > (a.get("kafka_end_offsets") or 0)]
            trimmed = [x for x in m if grew and x["ts_ms"] <= grew[-1]["ts_ms"]]
            # A fast drain can finish inside a couple of samples, leaving nothing to fit. Keep the
            # whole phase window rather than reporting null: a coarse rate beats no measurement.
            m = trimmed if len(trimmed) >= 3 else window(METRICS, s["ts_ms"], e["ts_ms"])
        p = {"seconds": round(rel(e["ts_ms"]) - rel(s["ts_ms"]), 1),
             "source_changes_per_s": mean(window(GENERATOR, s["ts_ms"] + 10000, e["ts_ms"]), "changes_per_s"),
             "kafka_records_per_s": rate(m, "kafka_end_offsets"),
             "task_count": peak(m, "task_count")}
        if name in ("c1", "c2"):
            start = s["ts_ms"] + 15000
            n, first, last = ch.query(f"""SELECT count(), toUnixTimestamp64Milli(min(applied_at)), toUnixTimestamp64Milli(max(applied_at))
                FROM cdc.change_history WHERE toUnixTimestamp64Milli(applied_at) BETWEEN {start} AND {e['ts_ms']}""").result_rows[0]
            p["sink_applied_per_s"] = round(n / ((last - first) / 1000), 1) if n and last > first else None
            p["sink_lag_slope_per_s"] = (lambda r: -r if r is not None else None)(rate(m, "sink_lag"))
        phases[name] = p
    summary["phases"] = phases
    g = lambda k, f: phases.get(k, {}).get(f)
    summary["findings"] = {
        "capture_drain_tasks_max_1_records_per_s": g("catchup-tasks1", "kafka_records_per_s"),
        "capture_drain_tasks_max_4_records_per_s": g("catchup-tasks4", "kafka_records_per_s"),
        "tasks_running_with_tasks_max_4": g("catchup-tasks4", "task_count"),
        "source_write_rate_during_fill": g("fill-tasks1", "source_changes_per_s"),
        "capture_rate_while_source_writing": g("fill-tasks1", "kafka_records_per_s"),
        "sink_mu_1_process": g("c1", "sink_applied_per_s"),
        "sink_mu_2_processes": g("c2", "sink_applied_per_s"),
        "peak_unconfirmed_wal_bytes": peak(METRICS, "unconfirmed_wal_bytes"),
    }


def recovery(summary, ch):
    base_s, base_e = ev("baseline-start"), ev("source-ddl")
    restore, drained, steady = ev("clickhouse-restored"), ev("backlog-drained"), ev("steady-state-after-recovery")
    kill, running = ev("connect-worker-killed"), ev("capture-task-running")
    crash = ev("sink-crash-after-insert-before-commit")
    end = ev("generator-stopped")
    f = summary["findings"] = {}
    if base_s and base_e:
        f["freshness_baseline"] = freshness_window(ch, base_s["ts_ms"] + 30000, base_e["ts_ms"])
    if restore and drained:
        f["freshness_worst_10s_bucket_p99_ms"] = max((b["p99_ms"] for b in freshness(ch, base_s["ts_ms"], drained["ts_ms"])), default=None)
        f["drain"] = {**restore["detail"], "observed_drain_s": drained["detail"]["observed_drain_s"]}
    if drained and steady:
        f["freshness_after_recovery"] = freshness_window(ch, drained["ts_ms"] + 60000, steady["ts_ms"])
    # Peaks cover the test itself, from baseline on; the initial snapshot load is not a fault.
    tested = window(METRICS, base_s["ts_ms"], end["ts_ms"] if end else 10**15) if base_s else METRICS
    f["peak_sink_lag_records"] = peak(tested, "sink_lag")
    f["peak_oldest_unapplied_age_ms"] = peak(tested, "oldest_unapplied_age_ms")
    f["peak_retained_wal_bytes"] = peak(tested, "retained_wal_bytes")
    f["peak_wal_status"] = sorted({m.get("wal_status") for m in tested if m.get("wal_status")})
    if kill:
        dead = kill["detail"].get("worker") or ""
        after = [m for m in METRICS if m["ts_ms"] > kill["ts_ms"]]
        back = next((m for m in after if m.get("task_state") == "RUNNING" and not str(m.get("task_worker") or "").startswith(dead)), None)
        before_back = [m for m in after if not back or m["ts_ms"] < back["ts_ms"]]
        stale = [m for m in before_back if m.get("task_state") == "RUNNING" and str(m.get("task_worker") or "").startswith(dead)]
        f["worker_kill"] = {
            "worker": dead,
            "capture_resumed_after_s": round(rel(back["ts_ms"]) - rel(kill["ts_ms"]), 1) if back else None,
            "status_said_running_on_dead_worker_s": round((stale[-1]["ts_ms"] - kill["ts_ms"]) / 1000, 1) if stale else 0,
            "states_seen": sorted({str(m.get("task_state")) for m in before_back}),
            "resumed_on": back.get("task_worker") if back else None,
            "peak_unconfirmed_wal_bytes_during_outage": peak(before_back, "unconfirmed_wal_bytes"),
        }
    if crash:
        f["sink_crash"] = {"rows_in_crashed_batch": crash["rows"], "offsets": crash["offsets"]}
    f["unmapped_field_events"] = [e for e in EVENTS if e["event"] == "sink-unmapped-fields"]
    nb, na = ev("new-column-null-before-backfill"), ev("new-column-null-after-backfill")
    f["schema_backfill"] = {"null_before": nb["detail"] if nb else None, "null_after": na["detail"] if na else None}
    sig = ev("incremental-snapshot-signal")
    if sig:
        n, first, last = ch.query(f"""
            SELECT count(), toUnixTimestamp64Milli(min(applied_at)), toUnixTimestamp64Milli(max(applied_at))
            FROM cdc.change_history WHERE op = 'r' AND applied_at >= fromUnixTimestamp64Milli({sig['ts_ms']})""").result_rows[0]
        f["incremental_snapshot"] = {"reads_applied": n, "seconds_from_signal_to_last_read": round((last - sig["ts_ms"]) / 1000, 1) if n else None}
    throttles = [e["detail"] for e in EVENTS if e["event"] == "clickhouse-throttled"]
    f["clickhouse_throttle_steps"] = throttles
    summary["freshness_series"] = freshness(ch, T0, end["ts_ms"]) if end else []


def capture_pressure(summary, ch):
    base, pause, unpause, caught = ev("baseline-start"), ev("kafka-paused"), ev("kafka-unpaused"), ev("caught-up")
    f = summary["findings"] = {}
    if pause and unpause:
        m = window(METRICS, pause["ts_ms"] + 5000, unpause["ts_ms"])
        f["outage_s"] = rel(unpause["ts_ms"]) - rel(pause["ts_ms"])
        f["wal_growth_bytes_per_s"] = rate(m, "retained_wal_bytes")
        f["source_changes_per_s_during_outage"] = mean(window(GENERATOR, pause["ts_ms"] + 5000, unpause["ts_ms"]), "changes_per_s")
        f["source_changes_per_s_baseline"] = mean(window(GENERATOR, base["ts_ms"] + 20000, pause["ts_ms"]), "changes_per_s") if base else None
        f["retained_wal_bytes_at_unpause"] = unpause["detail"].get("retained_wal_bytes")
        f["task_states_during_outage"] = sorted({x.get("task_state") for x in m if x.get("task_state")})
    if caught:
        f["catch_up_s_after_unpause"] = caught["detail"].get("seconds_after_unpause")
    f["peak_retained_wal_bytes"] = peak(window(METRICS, base["ts_ms"], 10**15) if base else METRICS, "retained_wal_bytes")


def history_loss(summary, ch):
    f = summary["findings"] = {}
    for name in ("capture-stopped", "source-history-lost", "normal-resume-requested", "normal-resume-stalled", "normal-resume-refused",
                 "normal-resume-not-refused", "normal-resume-still-stalled", "recovery-start", "resnapshot-start",
                 "resnapshot-reads-applied", "resnapshot-sweep"):
        e = ev(name)
        if e:
            f[name] = {"t_s": rel(e["ts_ms"]), **({"detail": e["detail"]} if e.get("detail") else {})}
    f["alerts"] = [{"t_s": rel(a["ts_ms"]), "alert": a["alert"], "state": a["state"]} for a in ALERTS]


def main():
    scenario = sys.argv[1]
    ch = clickhouse_connect.get_client(**ch_kwargs("admin"))
    summary = {"scenario": scenario, "run_id": RUN.name, "timeline": [{"t_s": rel(e["ts_ms"]), "event": e["event"], "detail": e.get("detail")} for e in EVENTS]}
    try:
        summary["versions"] = json.loads((RUN / "versions.json").read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        summary["versions"] = None
    {"capacity": lambda: capacity(summary, ch), "recovery": lambda: recovery(summary, ch),
     "capture-pressure": lambda: capture_pressure(summary, ch), "history-loss": lambda: history_loss(summary, ch)}[scenario]()
    summary["reconcile"] = {p.stem.removeprefix("reconcile-"): json.loads(p.read_text()) for p in sorted(RUN.glob("reconcile-*.json"))}
    summary["alerts"] = [{"t_s": rel(a["ts_ms"]), "alert": a["alert"], "state": a["state"]} for a in ALERTS]
    summary["series"] = series()
    (RUN / "summary.json").write_text(json.dumps(summary, indent=1))

    lines = [f"# {scenario} · {RUN.name}", "", "Generated by lab/report.py from this directory's raw files.", "", "## Findings", ""]
    lines += [f"- **{k}**: `{json.dumps(v)}`" for k, v in summary["findings"].items()]
    lines += ["", "## Timeline", ""] + [f"- {t['t_s']:>7} s · {t['event']}" + (f" · `{json.dumps(t['detail'])}`" if t.get("detail") else "") for t in summary["timeline"]]
    for label, r in summary["reconcile"].items():
        lines += ["", f"## Reconciliation · {label}", "", f"- converged: **{r['converged']}**"]
        for table, t in r["tables"].items():
            lines.append(f"- {table}: source {t['source_rows']} · ClickHouse logical {t['clickhouse_logical_rows']} · physical {t['clickhouse_physical_rows']}"
                         f" · missing {t['missing_in_clickhouse']} · ghost {t['ghost_rows_in_clickhouse']} · content mismatches {t['content_mismatches']}"
                         f" · logical duplicate keys {t['clickhouse_logical_duplicate_keys']}")
        lines.append(f"- history: `{json.dumps(r['history'])}`")
        if r.get("version_ordering"):
            lines.append(f"- version ordering (source position vs Kafka offset): `{json.dumps(r['version_ordering'])}`")
        if r.get("explicit_delete_batch"):
            lines.append(f"- explicit delete batch: `{json.dumps(r['explicit_delete_batch'])}`")
    (RUN / "report.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(summary["findings"], indent=1))


if __name__ == "__main__":
    main()
