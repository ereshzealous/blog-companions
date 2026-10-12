"""Lab 01 entry point: `make lab-01` (or `uv run python labs/lab01_memory_boundary.py --help`).

Runs the row path and the Arrow path over the same deterministic telemetry
fixture, checks that their outputs are identical, and times each phase of the
JSON-to-column boundary separately. No Docker, Kafka or database is needed.
Evidence is written to benchmarks/results/lab-01/runs/<run_id>/.
"""

from connectedfleet.lab01.orchestrate import main

if __name__ == "__main__":
    raise SystemExit(main())
