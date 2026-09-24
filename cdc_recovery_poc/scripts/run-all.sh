#!/usr/bin/env bash
# Run every scenario in order, then verify and report.
#
#   1. capacity          measures capture capacity and sink capacity (mu) with no faults
#   2. recovery test     every fault under live load, using the mu the capacity run measured
#   3. capture pressure  Kafka unavailable for 240 s while the application keeps writing
#   4. history loss      the expected failure: the slot loses the history capture needs
#
# Each scenario starts from a destroyed environment, so the runs are independent.
# Progress goes to results/run-all-<UTC>.log as well as the terminal.
#
# Usage: scripts/run-all.sh [LAMBDA] [START]
#   LAMBDA  change rate, default 20000/s
#   START   scenario to start from: capacity (default), recovery, pressure, history-loss.
#           Starting later reuses the mu measured by the most recent capacity run.
set -euo pipefail
cd "$(dirname "$0")/.."

LAMBDA=${1:-20000}
START=${2:-capacity}
STARTED=$(date -u +%Y%m%dT%H%M%SZ)
LOG="results/run-all-$STARTED.log"
mkdir -p results
exec > >(tee -a "$LOG") 2>&1

say() { printf '\n=== %s · %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
run_id_of() { cat control/run-id; }

say "run-all started; lambda=$LAMBDA; log $LOG"
[[ -f .env ]] || scripts/init-env.sh

ran=()
if [[ "$START" == "capacity" ]]; then
  say "1/4 capacity"
  scenarios/01-capacity.sh
  CAPACITY=$(run_id_of)
else
  CAPACITY=$(ls -d results/capacity-* 2>/dev/null | sort | tail -1)
  CAPACITY=${CAPACITY#results/}
  [[ -f "results/$CAPACITY/summary.json" ]] || { echo "no finished capacity run to take mu from" >&2; exit 1; }
  say "starting at $START; taking mu from $CAPACITY"
fi
MU=$(python3 -c "
import json
f = json.load(open('results/$CAPACITY/summary.json'))['findings']
print(int(f['sink_mu_2_processes']))")
say "mu=$MU changes/s"
started=0
if [[ "$START" == "capacity" ]]; then started=1; ran+=("$CAPACITY"); fi

if [[ "$START" == "recovery" ]]; then started=1; fi
if (( started )); then
  say "2/4 recovery test (lambda=$LAMBDA, mu=$MU)"
  LAMBDA=$LAMBDA MU=$MU scenarios/02-recovery-test.sh
  ran+=("$(run_id_of)")
fi

if [[ "$START" == "pressure" ]]; then started=1; fi
if (( started )); then
  say "3/4 capture pressure (lambda=$LAMBDA)"
  LAMBDA=$LAMBDA scenarios/03-capture-pressure.sh
  ran+=("$(run_id_of)")
fi

if [[ "$START" == "history-loss" ]]; then started=1; fi
if (( started )); then
  say "4/4 history loss (lambda=$LAMBDA)"
  LAMBDA=$LAMBDA scenarios/04-history-loss.sh
  ran+=("$(run_id_of)")
fi

say "stopping the lab"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true

paths=(); for r in "${ran[@]}"; do paths+=("results/$r"); done

say "verifying"
scripts/verify-run.py "${paths[@]}" || true

say "building the visual report"
scripts/build-report.py "${paths[@]}"

say "done: ${ran[*]}"
