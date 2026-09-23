#!/usr/bin/env bash
# Step 0 -> teardown. Runs Tier 1 (simulation + tests) and Tier 2 (live Kafka + PostgreSQL),
# writing one report per step to reports/ and a summary to reports/REPORT.md.
#   ./run_all.sh [--keep]     keep = leave the lab running afterwards
set -uo pipefail
cd "$(dirname "$0")"
R="${REPORTS_DIR:-reports}"; CAP="${CAPTURED_DIR:-captured-output}"
mkdir -p "$R"; rm -f "$R"/*.txt "$R"/REPORT.md; FAILED=0
LAB="docker compose -f live/docker-compose.yml"

step() {
  local n="$1" name="$2"; shift 2
  echo "== $n $name"
  { echo "\$ $*"; echo; "$@"; local rc=$?; echo; echo "exit=$rc"; } > "$R/$n-$name.txt" 2>&1
  tail -1 "$R/$n-$name.txt"
  grep -q "^exit=0$" "$R/$n-$name.txt" || FAILED=$((FAILED+1))
}

step 00 prerequisites  bash -c 'docker --version; docker compose version; python3 --version; python3 -c "import pytest; print(\"pytest\", pytest.__version__)"'
step 01 clean-slate    $LAB down -v --remove-orphans
step 02 tier1-sim      python3 run.py --scenario all --save --out "$CAP"
step 03 tier1-pytest   python3 -m pytest -q tests
step 04 lab-build-up   $LAB up -d --build --wait kafka postgres
step 05 lab-topic      $LAB run --rm -T setup
step 06 live-all       $LAB run --rm -T --build runner python loadgen.py all
step 06b live-verify   python3 live/verify_results.py "$R/06-live-all.txt"
step 07 inspect-postgres $LAB exec -T postgres psql -U postgres -d fulfilment -c \
  "SELECT run_id, cls, count(*) AS rows, max(queue_ms) AS max_queue_ms, round(avg(queue_ms)) AS avg_queue_ms FROM fulfilment GROUP BY run_id, cls ORDER BY run_id, cls;"
step 08 inspect-kafka  bash -c "$LAB exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list | head -20"
step 09 lab-logs       $LAB logs --tail 20

if [[ "${1:-}" != "--keep" ]]; then
  step 10 lab-down     $LAB down -v --remove-orphans
  step 11 verify-clean bash -c "$LAB ps -a; docker ps -a --filter name=queue-overload-lab --format '{{.Names}}' | wc -l | xargs echo containers_left="
fi

{
  echo "# POC run report"
  echo
  echo "Generated $(date -u '+%Y-%m-%d %H:%M UTC') by run_all.sh. Worked-example parameters; not a benchmark."
  echo
  echo "## Tier 1 · deterministic simulation"
  echo '```'; grep -E '^(RESULT|  peak backlog|  peak oldest age|  worst wait|  time to backlog zero)' "$R/02-tier1-sim.txt" | head -30; echo '```'
  echo; grep -E "passed|failed" "$R/03-tier1-pytest.txt" | tail -1 | sed 's/^/pytest: /'
  echo
  echo "## Tier 2 · live lab (Kafka KRaft + PostgreSQL 16, scaled to a laptop)"
  echo
  echo "Rates are scaled: the dependency has C* real slots and each write holds one for a real"
  echo "PostgreSQL \`pg_sleep\`. The lab reproduces the shape, it does not benchmark either product."
  echo
  echo '```'; sed -n '/^SUMMARY/,$p' "$R/06-live-all.txt" | sed '/^exit=/d'; echo '```'
  echo
  echo "Queue delay is measured from an explicit \`enqueued_at\` in the payload — end-to-end queue"
  echo "delay, not Kafka's \`CreateTime\` and not broker residence."
  echo
  echo "## Verification"
  echo
  echo "Tier 1: $(grep -c '\[ok\]' "$R/02-tier1-sim.txt") registered predictions · $(grep -oE '[0-9]+ passed' "$R/03-tier1-pytest.txt" | tail -1) (pytest)"
  echo
  echo "Tier 2 (live Kafka + PostgreSQL):"
  echo '```'; sed -n '/^ok  \|^FAIL \|^note /p;/semantic assertions/p' "$R/06b-live-verify.txt"; echo '```'
  echo
  echo "## Steps"
  for f in "$R"/[0-9]*.txt; do echo "- \`$(basename "$f")\` · $(tail -1 "$f")"; done
} > "$R/REPORT.md"

echo "report: $R/REPORT.md"
if [[ $FAILED -gt 0 ]]; then echo "FAILED steps: $FAILED"; exit 1; fi
echo "all steps passed"
