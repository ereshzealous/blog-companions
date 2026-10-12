"""Validate a Lab 03 run manifest. A case that fails any rule is excluded from every aggregate.

Rejection rules (prompts/03_BENCHMARK_LAB03.md Part C):
  META      missing POC commit SHA or version metadata
  REPS      fewer measured repetitions than the profile requires (smoke >= 1, full >= 3)
  STATUS    run/case status contradicts what was actually recorded (misreported status)
  ERROR     case raised an error or the lane did not complete / exited non-zero
  COUNT     validated logical rows != workload event count            (E1, E2, E3)
  CHECKSUM  content hash differs between lanes/methods of one workload (E1, E2, E3)
  RISK      E2 risk-flag counts differ from the deterministic expectation
  NULLS     null counts differ from the workload expectation           (E1, E2, E4 null edge)
  SCHEMA    destination table columns/types differ from the contract
  PCTL      impossible percentiles (p99 < p95 or p95 < p50)
  HOST      a timing case still overlapped other work on the host after every re-run (hostload.HostGuard);
            the case is excluded, and the run stays valid while every group keeps the profile's minimum repetitions
Warnings (kept, labelled): dirty working tree, single repetition.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict

from connectedfleet.contracts import canonical

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
MIN_REPS = {"smoke": 1, "confirm": 3, "full": 3}
INGESTED = ("ingested_at", "DateTime64(3, 'UTC')")


def expected_schema(table: str) -> list[list[str]]:
    cols = [list(c) for c in canonical.TELEMETRY.clickhouse_columns]
    if table == "telemetry_enriched":
        cols.append(["risk_flag", "Nullable(Bool)"])
    return cols + [list(INGESTED)]


def _table_of(case: dict) -> str:
    return "telemetry_enriched" if case.get("config", {}).get("transform") == "risk" else "telemetry_fact"


def validate_run(manifest: dict) -> dict:
    run_reasons: list[str] = []
    run_warnings: list[str] = []
    code = manifest.get("code") or {}
    if not SHA_RE.match(str(code.get("git_sha") or "")):
        run_reasons.append("META: POC git commit SHA missing (commit before measuring)")
    if code.get("git_dirty"):
        run_warnings.append("dirty working tree: results describe uncommitted code (label as historical)")
    versions = manifest.get("versions") or {}
    for key in ("python", "pyarrow"):
        if not versions.get(key):
            run_reasons.append(f"META: version '{key}' missing")
    if not isinstance(versions.get("stack"), dict):
        run_reasons.append("META: stack versions missing (run `make init` to write versions.json)")

    spec = manifest.get("workload_spec") or {}
    profile = manifest.get("profile", spec.get("profile"))
    reps = int(spec.get("repetitions", 0))
    if reps < MIN_REPS.get(profile, 1):
        run_reasons.append(f"REPS: profile {profile} requires >= {MIN_REPS.get(profile, 1)} measured repetitions, got {reps}")
    if reps == 1:
        run_warnings.append("single measured repetition: no spread available (smoke evidence only)")

    cases = manifest.get("cases") or []
    statuses = {c.get("status") for c in cases}
    if manifest.get("status") == "RUNNING":
        run_reasons.append("STATUS: run did not finish (manifest is a checkpoint of an interrupted run)")
    if manifest.get("status") == "COMPLETED" and statuses - {"COMPLETED"}:
        run_reasons.append("STATUS: run marked COMPLETED but some cases errored (misreported status)")

    workloads = manifest.get("workloads") or {}
    case_results = {c["case_id"]: {"case_id": c["case_id"], "reasons": [], "warnings": []} for c in cases}

    def reject(case: dict, reason: str) -> None:
        case_results[case["case_id"]]["reasons"].append(reason)

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in cases:
        m = case.get("measurements") or {}
        exp = case.get("experiment")
        if case.get("status") != "COMPLETED":
            reject(case, f"ERROR: {case.get('error') or case.get('status')}")
            continue
        if case.get("status") == "COMPLETED" and not m:
            reject(case, "STATUS: marked COMPLETED without measurements")
            continue
        runtime = m.get("lane_runtime") or {}
        scenario = (case.get("config") or {}).get("scenario")
        if exp in ("E1", "E2") or (exp == "E4" and scenario == "schema_null_edge"):
            if runtime and runtime.get("complete") is False:
                reject(case, "ERROR: lane did not reach the expected row count")
            if runtime.get("exit_code") not in (None, 0):
                reject(case, f"ERROR: worker exit code {runtime.get('exit_code')}")
        workload = workloads.get(case.get("workload_key")) or {}
        if exp in ("E1", "E2"):
            logical = (m.get("counts") or {}).get("logical")
            if logical != workload.get("count"):
                reject(case, f"COUNT: logical rows {logical} != workload {workload.get('count')}")
        if exp in ("E1", "E2") or (exp == "E4" and scenario == "schema_null_edge"):
            if m.get("nulls") is not None and workload.get("expected_nulls") is not None and m["nulls"] != workload["expected_nulls"]:
                reject(case, f"NULLS: observed {m['nulls']} != expected {workload['expected_nulls']}")
        if exp == "E2" and workload.get("expected_risk") is not None and m.get("risk_counts") != workload["expected_risk"]:
            reject(case, f"RISK: observed {m.get('risk_counts')} != expected {workload['expected_risk']}")
        if m.get("schema") is not None and exp in ("E1", "E2", "E4"):
            observed = [list(x) for x in m["schema"]]
            if observed != expected_schema(_table_of(case)):
                reject(case, "SCHEMA: destination columns/types differ from the contract")
        host = m.get("host") or {}
        if host.get("busy"):
            reject(case, f"HOST: {host.get('max_native_cpu_pct', 0):.0f}% native CPU during the case "
                         f"({host.get('top')}) after {host.get('attempt')} attempts")
        lat = m.get("latency_ms")
        if lat and lat.get("p50_ms") is not None:
            if not (lat["p50_ms"] <= lat["p95_ms"] <= lat["p99_ms"]):
                reject(case, f"PCTL: impossible percentiles {lat}")
        if exp == "E3":
            rows_expected = (workload.get("file") or {}).get("rows")
            if m.get("rows_loaded") != rows_expected or (m.get("counts") or {}).get("logical") != rows_expected:
                reject(case, f"COUNT: loaded {m.get('rows_loaded')} != file rows {rows_expected}")
        if exp in ("E1", "E2", "E3") and m.get("content_hash"):
            groups[(exp, case.get("workload_key"))].append(case)

    for (exp, key), members in groups.items():
        hashes = {c["measurements"]["content_hash"] for c in members}
        if len(hashes) > 1:
            for c in members:
                reject(c, f"CHECKSUM: content hashes differ across lanes/methods for {key}: {sorted(hashes)}")

    # Host-load exclusions (HOST only) do not invalidate the run if every measured group keeps enough repetitions.
    host_only = {cid for cid, r in case_results.items() if r["reasons"] and all(x.startswith("HOST:") for x in r["reasons"])}
    measured: dict[tuple, list[dict]] = defaultdict(list)
    for case in cases:
        if case.get("experiment") in ("E1", "E2", "E3") and not case.get("warmup"):
            key = (case.get("experiment"), case.get("mode"), case.get("workload_key"), case.get("lane"),
                   json.dumps(case.get("variant") or {}, sort_keys=True), json.dumps(case.get("config") or {}, sort_keys=True))
            measured[key].append(case)
    need = MIN_REPS.get(profile, 1)
    for key, members in measured.items():
        clean = sum(1 for c in members if not case_results[c["case_id"]]["reasons"])
        if clean < need and any(c["case_id"] in host_only for c in members):
            run_reasons.append(f"REPS: {key[0]} {key[2]} {key[3]} {key[4]} has {clean} clean repetitions after host-load "
                               f"exclusions (needs {need})")
    if host_only:
        run_warnings.append(f"HOST: {len(host_only)} case(s) excluded because other work overlapped them after every re-run")

    out_cases = []
    for case in cases:
        r = case_results[case["case_id"]]
        r["valid"] = not r["reasons"] and not run_reasons
        r["warmup"] = case.get("warmup", False)
        out_cases.append(r)
    blocking = [c for c in out_cases if not c["valid"] and c["case_id"] not in host_only]
    return {
        "run": {"run_id": manifest.get("run_id"), "valid": not run_reasons and not blocking,
                "reasons": run_reasons, "warnings": run_warnings,
                "cases_total": len(out_cases), "cases_valid": sum(c["valid"] for c in out_cases)},
        "cases": out_cases,
    }
