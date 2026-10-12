"""Re-validate saved Lab 03 runs: `python benchmarks/validate.py [runs/<run_id>/run.json ...]`.

Exit code 1 if any run (or any of its cases) fails a rejection rule; see
src/connectedfleet/bench/validate.py for the rules. No Docker needed.
"""

import json
import sys
from pathlib import Path

from connectedfleet.bench.validate import validate_run

RESULTS = Path(__file__).resolve().parent / "results"

if __name__ == "__main__":
    paths = [Path(p) for p in sys.argv[1:]] or sorted((RESULTS / "runs").glob("lab03-*/run.json"))
    if not paths:
        print("no Lab 03 runs found")
        raise SystemExit(0)
    failed = 0
    for path in paths:
        result = validate_run(json.loads(path.read_text()))
        path.with_name("validation.json").write_text(json.dumps(result, indent=2))
        run = result["run"]
        print(f"{run['run_id']}: valid={run['valid']} cases {run['cases_valid']}/{run['cases_total']} reasons={run['reasons']}")
        for case in result["cases"]:
            if not case["valid"] and case["reasons"]:
                print(f"   {case['case_id']}: {'; '.join(case['reasons'])[:200]}")
        failed += not run["valid"]
    raise SystemExit(1 if failed else 0)
