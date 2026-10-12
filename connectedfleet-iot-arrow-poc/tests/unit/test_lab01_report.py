"""End-to-end Lab 01 run in-process on a tiny fixture: evidence files, parity, honest language."""

import csv
import json
import re

from connectedfleet.lab01.orchestrate import run_lab

FORBIDDEN = re.compile(r"zero[- ]?copy|exactly[- ]once|production[- ]ready", re.IGNORECASE)


def _tiny_config():
    return {"sizes": [2_400], "batches": [100, 1_000], "warmups": 0, "repetitions": 1, "seed": 11, "profile": "test"}


def test_lab_writes_complete_evidence(tmp_path):
    exit_code, run_dir, text = run_lab(_tiny_config(), results_root=tmp_path, in_process=True)
    assert exit_code == 0, text
    for name in ("run.json", "phases.csv", "summary.csv", "memory.csv", "parity.json", "buffers.json", "summary.txt"):
        assert (run_dir / name).is_file(), name

    manifest = json.loads((run_dir / "run.json").read_text())
    assert manifest["status"] == "PASS"
    assert manifest["config"]["in_process"] is True
    assert set(manifest["sections"]) == {"measured", "illustrative", "observed_structure"}
    assert manifest["workload"]["2400"]["sha256"]

    latest = json.loads((tmp_path / "latest.json").read_text())
    assert latest["run_id"] == manifest["run_id"]


def test_row_path_has_no_build_phase_in_csv(tmp_path):
    _, run_dir, _ = run_lab(_tiny_config(), results_root=tmp_path, in_process=True)
    with (run_dir / "summary.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert not [r for r in rows if r["path"] == "row" and r["phase"] == "build"]
    assert [r for r in rows if r["path"] == "arrow" and r["phase"] == "build"]


def test_report_avoids_unsupported_claims(tmp_path):
    _, run_dir, text = run_lab(_tiny_config(), results_root=tmp_path, in_process=True)
    assert not FORBIDDEN.search(text)
    assert not FORBIDDEN.search((run_dir / "run.json").read_text())
    assert "not pipeline, Kafka or ClickHouse throughput" in text
