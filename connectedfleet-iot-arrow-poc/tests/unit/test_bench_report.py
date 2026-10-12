"""report.py aggregation: only valid, non-warm-up cases; grouped per commit; E4 matrix from executed cases.

All manifests here are SYNTHETIC and written to pytest's tmp_path only.
"""

import csv
import json

from connectedfleet.bench import report
from connectedfleet.bench.validate import validate_run

from test_bench_validate import synthetic_manifest


def _write(tmp_path, manifest):
    run_dir = tmp_path / "runs" / manifest["run_id"]
    run_dir.mkdir(parents=True)
    (run_dir / "run.json").write_text(json.dumps(manifest))
    validation = validate_run(manifest)
    (run_dir / "validation.json").write_text(json.dumps(validation))
    report.write_run_metrics(run_dir, manifest, validation)
    return run_dir


def _rows(path):
    with path.open() as handle:
        return list(csv.DictReader(handle))


def test_empty_results_say_nothing_was_measured(tmp_path):
    (tmp_path / "runs").mkdir()
    report.regenerate(tmp_path)
    assert "Nothing has been measured yet" in (tmp_path / "report" / "summary.txt").read_text()


def test_aggregates_exclude_warmups_and_invalid_cases(tmp_path):
    m = synthetic_manifest()
    m["cases"][0]["measurements"].update({"wall_rows_per_s": 1000.0, "insert_window_rows_per_s": 2000.0})
    m["cases"][1]["measurements"].update({"wall_rows_per_s": 1500.0, "insert_window_rows_per_s": 2500.0})
    warm = json.loads(json.dumps(m["cases"][1]))
    warm.update({"case_id": "E1-0009", "warmup": True})
    warm["measurements"]["wall_rows_per_s"] = 1.0
    m["cases"].append(warm)
    run_dir = _write(tmp_path, m)
    report.regenerate(tmp_path)
    throughput = _rows(tmp_path / "aggregate" / "throughput.csv")
    arrow = [r for r in throughput if r["lane"] == "arrow"]
    assert len(arrow) == 1 and float(arrow[0]["wall_rows_per_s_median"]) == 1500.0 and arrow[0]["n_measured"] == "1"
    assert arrow[0]["git_sha"] == "a" * 40
    assert (run_dir / "metrics.csv").is_file()
    index = json.loads((tmp_path / "manifest.json").read_text())
    assert index["lab03_runs"][0]["run_id"] == "lab03-SYNTHETIC"


def test_invalid_run_contributes_nothing(tmp_path):
    m = synthetic_manifest()
    m["code"]["git_sha"] = None
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    assert (tmp_path / "aggregate" / "throughput.csv").read_text() == ""
    assert "excluded" in (tmp_path / "report" / "summary.txt").read_text()


def test_e4_matrix_statuses(tmp_path):
    m = synthetic_manifest()
    m["workloads"]["E4"] = {"count": 100, "poison_records": 1}
    base = {"experiment": "E4", "variant": {}, "mode": "scenario", "warmup": False, "repetition": 0,
            "workload_key": "E4", "status": "COMPLETED", "error": None}
    m["cases"] = [
        {**base, "case_id": "E4-0000", "lane": "row", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 100, "physical": 100}, "dlq_delta": 1}},
        {**base, "case_id": "E4-0001", "lane": "sink", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 60, "physical": 60}, "dlq_delta": 41}},
        {**base, "case_id": "E4-0002", "lane": "row", "config": {"scenario": "replay_new_group"},
         "measurements": {"counts": {"logical": 100, "physical": 200}}},
        {**base, "case_id": "E4-0003", "lane": "arrow", "config": {"scenario": "crash_after_insert_before_commit"},
         "measurements": {"counts": {"logical": 100, "physical": 120}}},
    ]
    m["not_run"] = [{"experiment": "E4", "lane": "sink", "scenario": "crash_after_insert_before_commit",
                     "reason": "NOT_APPLICABLE: framework-managed offsets"}]
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    matrix = {(r["scenario"], r["lane"]): r["status"] for r in _rows(tmp_path / "aggregate" / "e4-correctness.csv")}
    assert matrix[("happy_path_with_poison", "row")] == "pass"
    assert matrix[("happy_path_with_poison", "sink")] == "fail"
    assert matrix[("replay_new_group", "row")] == "pass"
    assert matrix[("crash_after_insert_before_commit", "arrow")] == "pass"
    assert matrix[("crash_after_insert_before_commit", "sink")] == "not_applicable"


def test_e4_matrix_reports_batch_level_poison_loss_and_relative_replay(tmp_path):
    """Official-sink behaviour observed 2026-10-10: one mid-stream poison dead-letters its whole batch."""
    m = synthetic_manifest()
    m["workloads"]["E4"] = {"count": 5000, "poison_records": 1}
    base = {"experiment": "E4", "variant": {}, "mode": "scenario", "warmup": False, "repetition": 0,
            "workload_key": "E4", "status": "COMPLETED", "error": None}
    m["cases"] = [
        {**base, "case_id": "E4-0000", "lane": "sink", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 4501, "physical": 4501}, "dlq_delta": 500, "poison_in_table": 0}},
        {**base, "case_id": "E4-0001", "lane": "sink", "config": {"scenario": "replay_new_group"},
         "measurements": {"counts": {"logical": 4501, "physical": 9002},
                          "first_pass_counts": {"logical": 4501, "physical": 4501}}},
        {**base, "case_id": "E4-0002", "lane": "arrow", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 5001, "physical": 5001}, "dlq_delta": 0, "poison_in_table": 1}},
        {**base, "case_id": "E4-0003", "lane": "row", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 5000, "physical": 5000}, "dlq_delta": 1, "poison_in_table": 0}},
    ]
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    rows = {(r["scenario"], r["lane"]): r for r in _rows(tmp_path / "aggregate" / "e4-correctness.csv")}
    sink = rows[("happy_path_with_poison", "sink")]
    assert sink["status"] == "fail"
    assert "499 good records dead-lettered with the poison" in sink["note"]
    assert rows[("replay_new_group", "sink")]["status"] == "pass"
    arrow = rows[("happy_path_with_poison", "arrow")]
    assert arrow["status"] == "fail" and "poison stored in the table" in arrow["note"]
    assert rows[("happy_path_with_poison", "row")]["status"] == "pass"


def test_steady_rate_counts_only_the_batches_inside_the_insert_window():
    """k inserts span k-1 intervals: logical / window overstates the rate by k/(k-1) (11% at 10 inserts)."""
    m = synthetic_manifest()
    measured = m["cases"][0]["measurements"]
    measured.update({"insert_window_s": 0.9, "insert_batches": 10, "insert_window_rows_per_s": 1000 / 0.9})
    row = report.flatten(m, m["cases"][0], None)
    assert row["steady_rows_per_s"] == 900 / 0.9  # 9 of 10 batches landed after the first one
    single = report.flatten(m, {**m["cases"][1], "measurements": {**measured, "insert_batches": 1}}, None)
    assert single["steady_rows_per_s"] is None  # one insert has no window


def test_throughput_aggregate_carries_the_steady_rate(tmp_path):
    m = synthetic_manifest()
    for case in m["cases"]:
        case["measurements"].update({"insert_window_s": 0.9, "insert_batches": 10})
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    rows = _rows(tmp_path / "aggregate" / "throughput.csv")
    assert rows and float(rows[0]["steady_rows_per_s_median"]) == 1000.0


def test_e4_matrix_dlq_recovery_and_silent_skip(tmp_path):
    m = synthetic_manifest()
    m["workloads"]["E4"] = {"count": 5000, "poison_records": 1}
    base = {"experiment": "E4", "variant": {}, "mode": "scenario", "warmup": False, "repetition": 0,
            "workload_key": "E4", "status": "COMPLETED", "error": None}
    m["cases"] = [
        {**base, "case_id": "E4-0000", "lane": "sink", "config": {"scenario": "dlq_recovery"},
         "measurements": {"counts": {"logical": 5000, "physical": 10000},
                          "recovery": {"read": 1000, "replayed": 998, "quarantined": 2,
                                       "quarantined_ids": ["poison-20261014-0"]}}},
        {**base, "case_id": "E4-0001", "lane": "sink_skip", "config": {"scenario": "happy_path_with_poison"},
         "measurements": {"counts": {"logical": 5000, "physical": 5000}, "dlq_delta": 0, "poison_in_table": 0}},
    ]
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    rows = {(r["scenario"], r["lane"]): r for r in _rows(tmp_path / "aggregate" / "e4-correctness.csv")}
    rec = rows[("dlq_recovery", "sink")]
    assert rec["status"] == "pass" and "replayed 998 of 1000" in rec["note"]
    skip = rows[("happy_path_with_poison", "sink_skip")]
    assert skip["status"] == "partial" and "without a DLQ record" in skip["note"]


def test_stale_benchmark_connectors_are_found_and_pipeline_ones_are_not():
    from connectedfleet.bench.run import stale_bench_connectors

    names = ["cf-pg-outbox", "cfbench-a-afdd0504", "cf-clickhouse-sink", "cfbench-a-871a216e", "cf-mongo-state"]
    assert stale_bench_connectors(names) == ["cfbench-a-871a216e", "cfbench-a-afdd0504"]


def test_e4_matrix_counts_good_rows_lost_without_a_dlq_record(tmp_path):
    """sink_skip (2026-10-11): logical 4502/5000 with DLQ +0 and the poison not stored: 498 good rows vanished."""
    m = synthetic_manifest()
    m["workloads"]["E4"] = {"count": 5000, "poison_records": 1}
    m["cases"] = [{"case_id": "E4-0000", "experiment": "E4", "lane": "sink_skip", "variant": {}, "mode": "scenario",
                   "warmup": False, "repetition": 0, "workload_key": "E4", "status": "COMPLETED", "error": None,
                   "config": {"scenario": "happy_path_with_poison"},
                   "measurements": {"counts": {"logical": 4502, "physical": 4502}, "dlq_delta": 0, "poison_in_table": 0}}]
    _write(tmp_path, m)
    report.regenerate(tmp_path)
    row = _rows(tmp_path / "aggregate" / "e4-correctness.csv")[0]
    assert row["status"] == "fail" and "498 good rows missing with no DLQ record" in row["note"]


def test_experiment_loop_records_a_failure_and_keeps_going():
    """A crash in one experiment lost a whole confirm run (2026-10-10): the manifest was only written at the end."""
    from connectedfleet.bench.run import run_experiments

    calls, checkpoints = [], []

    def boom():
        calls.append("E3")
        raise RuntimeError("memory limit exceeded")

    experiments = [("E1", lambda: calls.append("E1")), ("E3", boom), ("E4", lambda: calls.append("E4"))]
    failed = run_experiments(experiments, enabled=lambda e: True, log=lambda m: None,
                             checkpoint=lambda: checkpoints.append(len(calls)))
    assert calls == ["E1", "E3", "E4"] and len(checkpoints) == 3
    assert failed == [{"experiment": "E3", "reason": "ERROR: RuntimeError: memory limit exceeded"}]
