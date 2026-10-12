"""Lab 02 entry point: `make lab-02` (full scenario) or `make smoke` (smaller, same checks).

Prerequisites: `make up` and `make init` (real connectors registered and RUNNING).
Drives PostgreSQL (registry + transactional outbox, incl. a rollback), MongoDB
(insert/partial update/delete), the telemetry gateway producer (with poison
records) and the CSV importer (same file twice). Waits for the normalizer and
lanes A/B/C, verifies every lane database, checks cross-lane equivalence, then
injects a lane-B crash between ClickHouse ack and Kafka commit and restarts a
Debezium connector. Evidence: benchmarks/results/lab-02/runs/<run_id>/run.json.
Exit code 0 only if every check passed.
"""

import argparse

from connectedfleet.config import POC_ROOT, Settings, load_dotenv
from connectedfleet.lab02.scenario import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--smoke", action="store_true", help="smaller volumes; identical checks")
    parser.add_argument("--skip-faults", action="store_true",
                        help="PARTIAL MODE: skip crash-window and connector-restart checks (recorded in the manifest as not run)")
    args = parser.parse_args()
    load_dotenv(POC_ROOT / ".env")
    raise SystemExit(run(Settings.from_env(), smoke=args.smoke, skip_faults=args.skip_faults))
