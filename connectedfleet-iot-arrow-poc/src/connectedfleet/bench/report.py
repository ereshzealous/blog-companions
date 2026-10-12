"""Lab 03 reporting: per-run metrics CSVs and cross-run aggregates from validated cases only.

Aggregates never pool runs measured on different POC commits: every group key starts
with the git SHA. Warm-up cases and invalid cases are excluded. Numbers are copied from
the saved manifests; nothing is interpolated or smoothed.

Outputs (benchmarks/results/):
  runs/<run_id>/metrics.csv, phase-metrics.csv          per run
  aggregate/throughput.csv latency.csv memory.csv phase-cost.csv e3-backfill.csv e4-correctness.csv
  report/summary.txt validated-metrics.csv validation.json
  manifest.json                                          index of runs
"""

from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

PHASES = ("decode", "validate", "build", "transform", "serialize", "insert")


def _median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def flatten(manifest: dict, case: dict, validity: dict | None) -> dict:
    m = case.get("measurements") or {}
    cfg = case.get("config") or {}
    workload = (manifest.get("workloads") or {}).get(case.get("workload_key")) or {}
    lat = m.get("latency_ms") or {}
    res = m.get("resources") or {}
    counts = m.get("counts") or {}
    logical = counts.get("logical") or m.get("rows_loaded")
    phases = m.get("phases_ns") or {}
    batches, window_s = m.get("insert_batches"), m.get("insert_window_s")
    # k inserts span k-1 intervals, so only the rows of the last k-1 batches landed inside the window.
    steady = (logical * (batches - 1) / batches / window_s) if logical and batches and batches > 1 and window_s else None
    row = {
        "run_id": manifest["run_id"],
        "git_sha": (manifest.get("code") or {}).get("git_sha"),
        "profile": manifest.get("profile"),
        "case_id": case["case_id"],
        "experiment": case["experiment"],
        "lane": case["lane"],
        "compute": (case.get("variant") or {}).get("compute"),
        "insert": (case.get("variant") or {}).get("insert"),
        "mode": case.get("mode"),
        "scenario": cfg.get("scenario"),
        "warmup": case.get("warmup"),
        "repetition": case.get("repetition"),
        "workload_key": case.get("workload_key"),
        "event_count": workload.get("count") or (workload.get("file") or {}).get("rows"),
        "shape": cfg.get("shape") or ("string_heavy" if workload.get("string_heavy") else None),
        "batch_size": cfg.get("batch_size"),
        "flush_timeout_ms": cfg.get("flush_timeout_ms"),
        "compression": cfg.get("compression"),
        "logical_rows": logical,
        "physical_rows": counts.get("physical"),
        "wall_s": m.get("wall_s"),
        "wall_rows_per_s": m.get("wall_rows_per_s") or m.get("rows_per_s"),
        "insert_window_rows_per_s": m.get("insert_window_rows_per_s"),
        "steady_rows_per_s": steady,
        "effective_batch_rows": m.get("effective_batch_rows"),
        "insert_batches": m.get("insert_batches"),
        "parts_active": m.get("parts_active"),
        "p50_ms": lat.get("p50_ms"),
        "p95_ms": lat.get("p95_ms"),
        "p99_ms": lat.get("p99_ms"),
        "max_ms": lat.get("max_ms"),
        "over_5s": lat.get("over_5s"),
        "max_lag": (m.get("lag") or {}).get("max_lag"),
        "cpu_s": res.get("cpu_s") if res else m.get("client_cpu_s"),
        "cpu_scope": res.get("scope") if res else "client process",
        "peak_mem_bytes": res.get("peak_mem_bytes"),
        "max_rss_bytes": res.get("max_rss_bytes") or m.get("client_max_rss_bytes"),
        "payload_bytes": m.get("payload_bytes") or m.get("body_bytes"),
        "dlq_delta": m.get("dlq_delta"),
        "poison_in_table": m.get("poison_in_table"),
        "first_pass_logical": (m.get("first_pass_counts") or {}).get("logical"),
        "first_pass_physical": (m.get("first_pass_counts") or {}).get("physical"),
        "recovery_read": (m.get("recovery") or {}).get("read"),
        "recovery_replayed": (m.get("recovery") or {}).get("replayed"),
        "recovery_quarantined": (m.get("recovery") or {}).get("quarantined"),
        "recovery_quarantined_ids": " ".join((m.get("recovery") or {}).get("quarantined_ids") or []) or None,
        "server_cpu_s": (m.get("server") or {}).get("cpu_s"),
        "server_query_s": (m.get("server") or {}).get("query_s"),
        "server_peak_mem_bytes": (m.get("server") or {}).get("peak_mem_bytes"),
        "valid": (validity or {}).get("valid"),
        "invalid_reasons": "; ".join((validity or {}).get("reasons", [])),
    }
    for phase in PHASES:
        row[f"{phase}_ms"] = (phases.get(phase) / 1e6) if phases.get(phase) is not None else None
    return row


def write_run_metrics(run_dir: Path, manifest: dict, validation: dict) -> None:
    by_id = {c["case_id"]: c for c in validation["cases"]}
    rows = [flatten(manifest, c, by_id.get(c["case_id"])) for c in manifest.get("cases", [])]
    _write_csv(run_dir / "metrics.csv", rows)
    _write_csv(run_dir / "phase-metrics.csv",
               [{k: r[k] for k in ("case_id", "experiment", "lane", "compute", "insert", "batch_size", "shape", "logical_rows",
                                   *[f"{p}_ms" for p in PHASES])} for r in rows if r.get("decode_ms") is not None])


def _load_runs(results_root: Path) -> list[tuple[dict, dict]]:
    runs = []
    for run_file in sorted((results_root / "runs").glob("lab03-*/run.json")):
        manifest = json.loads(run_file.read_text())
        # always re-validate with the current rules; the run's own validation.json is the harness's snapshot
        from connectedfleet.bench.validate import validate_run

        runs.append((manifest, validate_run(manifest)))
    return runs


def _aggregate(rows: list[dict], keys: tuple[str, ...], metrics: tuple[str, ...]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(k) for k in keys)].append(row)
    out = []
    for key, members in sorted(groups.items(), key=lambda kv: [str(x) for x in kv[0]]):
        agg = dict(zip(keys, key))
        agg["n_measured"] = len(members)
        agg["run_ids"] = " ".join(sorted({m["run_id"] for m in members}))
        for metric in metrics:
            values = [m[metric] for m in members if m.get(metric) is not None]
            agg[f"{metric}_median"] = _median(values)
            agg[f"{metric}_min"] = min(values) if values else None
            agg[f"{metric}_max"] = max(values) if values else None
        out.append(agg)
    return out


def e4_matrix(manifest: dict, rows: list[dict]) -> list[dict]:
    """Correctness matrix rows: (scenario, lane) -> status from what was actually executed."""
    workloads = manifest.get("workloads") or {}
    matrix = []
    for r in rows:
        if r["experiment"] != "E4" or not r.get("scenario"):
            continue
        w = workloads.get(r["workload_key"]) or {}
        count, poison = w.get("count"), w.get("poison_records", 0)
        logical, physical = r.get("logical_rows"), r.get("physical_rows")
        scenario = r["scenario"]
        if scenario == "happy_path_with_poison":
            dlq = r.get("dlq_delta")
            stored = r.get("poison_in_table") or 0
            ok_rows = logical == count and not stored
            ok_dlq = dlq == poison
            status = "pass" if ok_rows and ok_dlq else ("partial" if ok_rows or ok_dlq else "fail")
            note = f"logical {logical}/{count}; DLQ +{dlq} (poison sent {poison})"
            if dlq is not None and dlq > poison:
                note += f"; {dlq - poison} good records dead-lettered with the poison (batch-level failure)"
            if stored:
                note += f"; poison stored in the table ({stored} rows, value coerced)"
            if dlq == 0 and poison and ok_rows:
                note += "; bad record skipped by ClickHouse without a DLQ record (silently dropped)"
            if count is not None and logical is not None and dlq is not None:
                silent = (count - logical) - max(dlq - poison, 0)
                if silent > 0:
                    note += f"; {silent} good rows missing with no DLQ record (silently dropped)"
        elif scenario == "replay_new_group":
            first_logical, first_physical = r.get("first_pass_logical"), r.get("first_pass_physical")
            if first_physical is not None:
                # Replay re-inserts whatever the first pass inserted: logical unchanged, physical doubled.
                status = "pass" if logical == first_logical and physical == 2 * first_physical else "fail"
                note = (f"logical {logical}, physical {physical} (first pass logical {first_logical}, physical "
                        f"{first_physical}): at-least-once duplicates")
            else:
                status = "pass" if logical == count and physical == 2 * count else "fail"
                note = (f"logical {logical}, physical {physical} (expected logical {count}, physical {2 * count}): "
                        "at-least-once duplicates")
        elif scenario == "dlq_recovery":
            ids = (r.get("recovery_quarantined_ids") or "").split()
            only_poison = bool(ids) and all(i.startswith("poison-") for i in ids)
            status = "pass" if logical == count and only_poison else "fail"
            note = (f"replayed {r.get('recovery_replayed')} of {r.get('recovery_read')} DLQ records, quarantined "
                    f"{r.get('recovery_quarantined')} ({' '.join(ids)}); logical {logical}/{count}")
        elif scenario == "crash_after_insert_before_commit":
            status = "pass" if logical == count and (physical or 0) > count else "fail"
            note = f"after restart logical {logical}, physical {physical}: duplicate rows from the crash window"
        elif scenario == "schema_null_edge":
            status = "pass" if r.get("valid") else "fail"
            note = "null counts preserved exactly" if r.get("valid") else r.get("invalid_reasons")
        else:
            status, note = "unknown", ""
        matrix.append({"run_id": r["run_id"], "git_sha": r["git_sha"], "scenario": scenario, "lane": r["lane"],
                       "status": status, "executed": True, "note": note, "source": r["case_id"]})
    for item in manifest.get("not_run") or []:
        if item.get("experiment") == "E4" and item.get("scenario"):
            matrix.append({"run_id": manifest["run_id"], "git_sha": (manifest.get("code") or {}).get("git_sha"),
                           "scenario": item["scenario"], "lane": item.get("lane", "all"),
                           "status": "not_applicable" if "NOT_APPLICABLE" in item["reason"] else "not_run",
                           "executed": False, "note": item["reason"], "source": None})
    link = (manifest.get("workloads") or {}).get("E4-lab02-link")
    if link:
        names = {
            "csv_reimport": "CSV re-import",
            "pg_delete_update": "registry current state after FINAL",
            "outbox_rollback": "rolled-back outbox event absent",
            "mongo_update_delete": "mongo current state after FINAL",
        }
        lane_names = {"sink": "sink", "row": "row", "arrow": "arrow"}
        for scenario, prefix in names.items():
            for lane in lane_names:
                hits = [c for c in link["checks"] if c.get("lane") == lane and str(c.get("name", "")).startswith(prefix)]
                status = ("pass" if hits and all(c["ok"] for c in hits) else "fail") if hits else "not_run"
                matrix.append({"run_id": manifest["run_id"], "git_sha": (manifest.get("code") or {}).get("git_sha"),
                               "scenario": scenario, "lane": lane, "status": status, "executed": bool(hits),
                               "note": f"from Lab 02 run {link['lab02_run_id']}", "source": link["lab02_run_id"]})
    return matrix


def regenerate(results_root: Path) -> dict:
    runs = _load_runs(results_root)
    all_rows, valid_rows, matrix, index = [], [], [], []
    validation_all = []
    for manifest, validation in runs:
        by_id = {c["case_id"]: c for c in validation["cases"]}
        rows = [flatten(manifest, c, by_id.get(c["case_id"])) for c in manifest.get("cases", [])]
        all_rows += rows
        valid_rows += [r for r in rows if r["valid"] and not r["warmup"]]
        matrix += e4_matrix(manifest, [r for r in rows if not r["warmup"]])
        validation_all.append(validation["run"])
        index.append({"run_id": manifest["run_id"], "profile": manifest.get("profile"),
                      "git_sha": (manifest.get("code") or {}).get("git_sha"), "status": manifest.get("status"),
                      "valid": validation["run"]["valid"], "cases": validation["run"]["cases_total"],
                      "cases_valid": validation["run"]["cases_valid"], "ended_at_utc": manifest.get("ended_at_utc")})

    agg_dir, report_dir = results_root / "aggregate", results_root / "report"
    throughput_rows = [r for r in valid_rows if r["experiment"] in ("E1", "E2") and r["mode"] == "drain"]
    _write_csv(agg_dir / "throughput.csv", _aggregate(
        throughput_rows, ("git_sha", "profile", "experiment", "lane", "compute", "insert", "event_count", "shape", "batch_size"),
        ("wall_rows_per_s", "insert_window_rows_per_s", "steady_rows_per_s", "effective_batch_rows", "parts_active")))
    _write_csv(agg_dir / "latency.csv", _aggregate(
        [r for r in valid_rows if r["mode"] == "live"], ("git_sha", "profile", "lane", "batch_size", "flush_timeout_ms"),
        ("p50_ms", "p95_ms", "p99_ms", "max_ms", "over_5s", "max_lag")))
    memory_rows = []
    for r in valid_rows:
        if r["experiment"] in ("E1", "E2") and r.get("cpu_s") is not None and r.get("logical_rows"):
            memory_rows.append({**r, "cpu_s_per_million": r["cpu_s"] / r["logical_rows"] * 1e6})
    _write_csv(agg_dir / "memory.csv", _aggregate(
        memory_rows, ("git_sha", "profile", "experiment", "lane", "compute", "insert", "event_count", "shape", "batch_size", "cpu_scope"),
        ("cpu_s_per_million", "peak_mem_bytes", "max_rss_bytes")))
    phase_rows = []
    for r in valid_rows:
        if r.get("decode_ms") is not None and r.get("logical_rows"):
            scale = 1e6 / r["logical_rows"]
            phase_rows.append({**r, **{f"{p}_ms_per_million": (r[f"{p}_ms"] or 0) * scale for p in PHASES}})
    _write_csv(agg_dir / "phase-cost.csv", _aggregate(
        phase_rows, ("git_sha", "profile", "experiment", "lane", "compute", "insert", "event_count", "shape", "batch_size"),
        tuple(f"{p}_ms_per_million" for p in PHASES)))
    _write_csv(agg_dir / "e3-backfill.csv", _aggregate(
        [r for r in valid_rows if r["experiment"] == "E3"], ("git_sha", "profile", "lane", "event_count", "compression", "batch_size"),
        ("wall_s", "wall_rows_per_s", "cpu_s", "max_rss_bytes", "payload_bytes", "server_cpu_s", "server_query_s",
         "server_peak_mem_bytes")))
    _write_csv(agg_dir / "e4-correctness.csv", matrix)
    _write_csv(report_dir / "validated-metrics.csv", valid_rows)
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "validation.json").write_text(json.dumps(validation_all, indent=2))
    (results_root / "manifest.json").write_text(json.dumps({"lab03_runs": index}, indent=2))
    (report_dir / "summary.txt").write_text(summary_text(index, valid_rows, all_rows, matrix), encoding="utf-8")
    return {"runs": len(index), "valid_rows": len(valid_rows)}


def summary_text(index: list[dict], valid_rows: list[dict], all_rows: list[dict], matrix: list[dict]) -> str:
    lines = ["ConnectedFleet Lab 03 — validated benchmark summary", ""]
    if not index:
        lines.append("No Lab 03 runs found. Nothing has been measured yet.")
        return "\n".join(lines) + "\n"
    lines.append("Runs:")
    for r in index:
        lines.append(f"  {r['run_id']}  profile={r['profile']}  sha={str(r['git_sha'])[:12]}  valid={r['valid']}  "
                     f"cases {r['cases_valid']}/{r['cases']}")
    invalid = [r for r in all_rows if not r["valid"]]
    lines += ["", f"Cases used in aggregates: {len(valid_rows)} (valid, non-warm-up); excluded invalid: {len(invalid)}"]
    for r in invalid[:20]:
        lines.append(f"  excluded {r['case_id']} ({r['experiment']}/{r['lane']}): {r['invalid_reasons'][:160]}")
    lines += ["", "E1/E2 drain throughput (validated rows/s, median of repetitions; wall includes lane start-up; steady = rows of batches 2..k / (last - first insert)):"]
    groups: dict[tuple, list] = defaultdict(list)
    for r in valid_rows:
        if r["experiment"] in ("E1", "E2") and r["mode"] == "drain":
            groups[(r["experiment"], r["event_count"], r["shape"], r["batch_size"], r["lane"], r["compute"], r["insert"])].append(r)
    for key, members in sorted(groups.items(), key=lambda kv: [str(x) for x in kv[0]]):
        exp, n, shape, batch, lane, compute, insert = key
        wall = _median([m["wall_rows_per_s"] for m in members])
        steady = _median([m["steady_rows_per_s"] for m in members])
        eff = _median([m["effective_batch_rows"] for m in members])
        label = lane + (f":{compute}-{insert}" if compute else "")
        lines.append(f"  {exp} n={n} {shape or ''} batch={batch} {label:<26} wall={_fmt(wall)}  steady={_fmt(steady)}  "
                     f"effective batch={_fmt(eff)}  (reps={len(members)})")
    live = [r for r in valid_rows if r["mode"] == "live"]
    if live:
        lines += ["", "E1 live latency (ms, ingested_at - emitted_at, same Docker VM clock):"]
        for r in live:
            lines.append(f"  {r['lane']:<6} batch={r['batch_size']} flush={r['flush_timeout_ms']}ms p50={_fmt(r['p50_ms'])} "
                         f"p95={_fmt(r['p95_ms'])} p99={_fmt(r['p99_ms'])} max={_fmt(r['max_ms'])} >5s={r['over_5s']} "
                         f"max lag={'n/m' if r['max_lag'] is None else r['max_lag']}")
    if matrix:
        lines += ["", "E4 correctness / failure matrix:"]
        for m in matrix:
            lines.append(f"  {m['scenario']:<34} {m['lane']:<6} {m['status']:<15} {m['note'][:110]}")
    lines += ["", "Reading guide: A (official sink) vs C compares two systems (JVM connector + JSONEachRow vs CPython + ArrowStream);",
              "B vs C isolates representation/compute/insert format on shared decode code. One local machine; not production scaling."]
    return "\n".join(lines) + "\n"


def _fmt(value) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.0f}" if abs(value) >= 100 else f"{value:.2f}"
