"""Run the Lab 03 benchmark suite: `python benchmarks/run.py --profile smoke|full [--only E1,E4]`.

Same as `make lab-03`. Needs the stack (`make up init`) and a passing Lab 02.
Writes benchmarks/results/runs/<run_id>/ and refreshes aggregate/ and report/.
"""

from connectedfleet.bench.run import main

if __name__ == "__main__":
    raise SystemExit(main())
