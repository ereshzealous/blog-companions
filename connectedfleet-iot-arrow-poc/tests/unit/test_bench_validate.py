"""validate.py rejection rules, exercised with SYNTHETIC manifests built here (never stored under results/)."""

import copy

import pytest

from connectedfleet.bench.validate import expected_schema, validate_run

SHA = "a" * 40


def synthetic_manifest() -> dict:
    """A minimal, internally consistent, clearly synthetic run with two E1 lanes and one live case."""
    schema = expected_schema("telemetry_fact")
    workload = {"count": 1000, "expected_nulls": {"temperature_c": 40, "battery_pct": 41, "signal_dbm": 39},
                "expected_risk": {"true": 10, "false": 950, "unknown": 40}}

    def case(cid, lane, mode="drain", latency=None):
        return {"case_id": cid, "experiment": "E1", "lane": lane, "variant": {}, "mode": mode, "warmup": False,
                "repetition": 0, "workload_key": "W", "config": {"batch_size": 100, "transform": "none"},
                "status": "COMPLETED", "error": None,
                "measurements": {"counts": {"logical": 1000, "physical": 1000}, "content_hash": "h/1000",
                                 "nulls": dict(workload["expected_nulls"]), "schema": schema,
                                 "latency_ms": latency, "lane_runtime": {"complete": True, "exit_code": 0}}}

    return {
        "run_id": "lab03-SYNTHETIC", "profile": "smoke", "status": "COMPLETED",
        "code": {"git_sha": SHA, "git_dirty": False},
        "versions": {"python": "3.12.13", "pyarrow": "25.0.1", "stack": {"clickhouse": "x"}},
        "workload_spec": {"profile": "smoke", "repetitions": 1},
        "workloads": {"W": workload},
        "cases": [case("E1-0000", "sink"), case("E1-0001", "arrow"),
                  case("E1-0002", "row", "live", {"p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0, "samples": 1000})],
    }


def _reasons(result, case_id):
    return next(c["reasons"] for c in result["cases"] if c["case_id"] == case_id)


def test_synthetic_baseline_is_valid():
    result = validate_run(synthetic_manifest())
    assert result["run"]["valid"], result
    assert "single measured repetition" in " ".join(result["run"]["warnings"])


def test_missing_commit_sha_rejects_the_run():
    m = synthetic_manifest()
    m["code"]["git_sha"] = None
    result = validate_run(m)
    assert not result["run"]["valid"] and any(r.startswith("META") for r in result["run"]["reasons"])


def test_missing_stack_versions_rejects_the_run():
    m = synthetic_manifest()
    m["versions"].pop("stack")
    assert not validate_run(m)["run"]["valid"]


def test_wrong_event_count_rejects_case():
    m = synthetic_manifest()
    m["cases"][1]["measurements"]["counts"]["logical"] = 999
    assert any(r.startswith("COUNT") for r in _reasons(validate_run(m), "E1-0001"))


def test_checksum_mismatch_rejects_every_lane_of_the_workload():
    m = synthetic_manifest()
    m["cases"][0]["measurements"]["content_hash"] = "other/1000"
    result = validate_run(m)
    for cid in ("E1-0000", "E1-0001", "E1-0002"):
        assert any(r.startswith("CHECKSUM") for r in _reasons(result, cid))


def test_null_rate_change_rejects_case():
    m = synthetic_manifest()
    m["cases"][0]["measurements"]["nulls"]["battery_pct"] = 0
    assert any(r.startswith("NULLS") for r in _reasons(validate_run(m), "E1-0000"))


def test_impossible_percentiles_reject_case():
    m = synthetic_manifest()
    m["cases"][2]["measurements"]["latency_ms"] = {"p50_ms": 10.0, "p95_ms": 50.0, "p99_ms": 40.0, "samples": 1000}
    assert any(r.startswith("PCTL") for r in _reasons(validate_run(m), "E1-0002"))


def test_schema_drift_rejects_case():
    m = synthetic_manifest()
    m["cases"][1]["measurements"]["schema"] = m["cases"][1]["measurements"]["schema"][:-2]
    assert any(r.startswith("SCHEMA") for r in _reasons(validate_run(m), "E1-0001"))


def test_misreported_run_status_is_rejected():
    m = synthetic_manifest()
    m["cases"].append({**copy.deepcopy(m["cases"][0]), "case_id": "E1-0003", "status": "ERROR", "error": "boom",
                       "measurements": {}})
    result = validate_run(m)
    assert any(r.startswith("STATUS") for r in result["run"]["reasons"])
    assert any(r.startswith("ERROR") for r in _reasons(result, "E1-0003"))


@pytest.mark.parametrize(("profile", "reps", "valid"), [("full", 1, False), ("full", 3, True), ("smoke", 1, True)])
def test_repetition_requirements(profile, reps, valid):
    m = synthetic_manifest()
    m["profile"] = profile
    m["workload_spec"] = {"profile": profile, "repetitions": reps}
    assert validate_run(m)["run"]["valid"] is valid


def test_incomplete_lane_is_rejected():
    m = synthetic_manifest()
    m["cases"][0]["measurements"]["lane_runtime"]["complete"] = False
    assert any(r.startswith("ERROR") for r in _reasons(validate_run(m), "E1-0000"))
