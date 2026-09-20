#!/usr/bin/env bash
# Step 0 -> teardown. Runs Tier 1 (simulation + tests) and Tier 2 (live lab), writing one report per
# step to reports/ and a summary to reports/REPORT.md.  Usage: ./run_all.sh [--keep]  (keep = leave lab up)
set -uo pipefail
cd "$(dirname "$0")"
R="${REPORTS_DIR:-reports}"; CAP="${CAPTURED_DIR:-captured-output}"; mkdir -p "$R"; rm -f "$R"/*.txt "$R"/REPORT.md; FAILED=0
LAB="docker compose -f live/docker-compose.yml"
step() { local n="$1" name="$2"; shift 2; echo "== $n $name"; { echo "\$ $*"; echo; "$@"; local rc=$?; echo; echo "exit=$rc"; } > "$R/$n-$name.txt" 2>&1; tail -1 "$R/$n-$name.txt"; grep -q "^exit=0$" "$R/$n-$name.txt" || FAILED=$((FAILED+1)); }

step 00 prerequisites  bash -c 'docker --version; docker compose version; python3 --version; python3 -c "import pytest; print(\"pytest\", pytest.__version__)"'
step 01 clean-slate    $LAB down -v --remove-orphans
step 02 tier1-sim      python3 run.py --scenario all --save --out "$CAP"
step 03 tier1-pytest   python3 -m pytest -q tests
step 04 lab-build-up   $LAB up -d --build --wait
step 05 lab-status     $LAB ps
step 06 live-all       $LAB run --rm -T --build loadgen python loadgen.py all
step 06b live-naive-repeats bash -c "for i in 1 2 3 4 5; do $LAB run --rm -T loadgen python loadgen.py L1 | grep '^L1'; done"
step 06c live-verify   python3 live/verify_results.py "$R/06-live-all.txt"
step 07 inspect-postgres $LAB exec -T postgres psql -U postgres -d pricing -c "SELECT calls, round(mean_exec_time::numeric,1) AS mean_ms, left(query,60) AS query FROM pg_stat_statements WHERE query LIKE '%prices p%' ORDER BY calls DESC LIMIT 5;"
step 08 inspect-redis  bash -c "$LAB exec -T redis redis-cli INFO keyspace; $LAB exec -T redis redis-cli --scan --pattern 'price:*' | head -5; $LAB exec -T redis redis-cli PTTL price:sku-00000"
step 09 lab-logs       $LAB logs --tail 20
if [[ "${1:-}" != "--keep" ]]; then
  step 10 lab-down     $LAB down -v --remove-orphans
  step 11 verify-clean bash -c "$LAB ps -a; docker ps -a --filter name=stampede-lab --format '{{.Names}}' | wc -l | xargs echo containers_left="
fi

{
  echo "# POC run report"
  echo
  echo "Generated $(date -u '+%Y-%m-%d %H:%M UTC') by run_all.sh. Worked-example parameters; not a benchmark."
  echo
  echo "## Tier 1 · deterministic simulation"
  echo '```'; sed -n '/^scenario/,/^S8/p' "$R/02-tier1-sim.txt"; echo '```'
  echo; grep -E "passed|failed" "$R/03-tier1-pytest.txt" | tail -1 | sed 's/^/pytest: /'
  echo
  echo "## Tier 2 · live lab (4 app processes hosting 100 coalescing scopes, Redis 7, PostgreSQL 16)"
  echo '```'; sed -n '/^SUMMARY/,$p' "$R/06-live-all.txt" | sed '/^exit=/d'; echo '```'
  echo
  echo "Origin calls are PostgreSQL pg_stat_statements counts of the price query (ground truth)."
  echo
  echo "Naive (L1) repeated five times: real arrival timing varies, so the count varies:"
  echo '```'; grep '^L1' "$R/06b-live-naive-repeats.txt"; echo '```'
  echo
  echo "## Verification"
  echo
  echo "Tier 1: $(grep -c '\[ok\]' "$R/02-tier1-sim.txt") scenario assertions · $(grep -oE '[0-9]+ passed' "$R/03-tier1-pytest.txt" | tail -1) (pytest)"
  echo
  echo "Tier 2 (live Redis + PostgreSQL):"
  echo '```'; sed -n '/ok  \|FAIL \|note /p;/semantic assertions/p' "$R/06c-live-verify.txt"; echo '```'
  echo
  echo "## Steps"
  for f in "$R"/[0-9]*.txt; do echo "- \`$(basename "$f")\` · $(tail -1 "$f")"; done
} > "$R/REPORT.md"
echo "report: $R/REPORT.md"
if [[ $FAILED -gt 0 ]]; then echo "FAILED steps: $FAILED"; exit 1; fi
echo "all steps passed"
