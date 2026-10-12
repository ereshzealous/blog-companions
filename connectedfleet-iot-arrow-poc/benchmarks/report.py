"""Regenerate Lab 03 aggregates from saved, validated runs: `python benchmarks/report.py` (same as `make report`).

Reads benchmarks/results/runs/*/run.json; writes aggregate/*.csv, report/summary.txt,
report/validated-metrics.csv, report/validation.json and manifest.json. No Docker needed.
"""

from pathlib import Path

from connectedfleet.bench.report import regenerate

RESULTS = Path(__file__).resolve().parent / "results"

if __name__ == "__main__":
    print(regenerate(RESULTS))
    print((RESULTS / "report" / "summary.txt").read_text())
