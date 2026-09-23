#!/usr/bin/env bash
# Run the POC and capture everything into one results folder.
#
#   ./run.sh results            Tier 1 + Tier 2 -> ./results/
#   ./run.sh results-1          the same, into ./results-1/   (compare runs side by side)
#   ./run.sh results --tier1    Tier 1 only, no Docker needed
#   ./run.sh results --keep     leave the live lab running afterwards
#   ./run.sh results --force    overwrite an existing results folder
#   ./run.sh results --open     open the HTML report when it finishes
#
# The folder is created under the POC root, whatever directory you invoke this from.
# captured-output/ and reports/ (the published evidence) are never touched.
set -uo pipefail
cd "$(dirname "$0")"

OUT=""; TIER1_ONLY=0; KEEP=""; FORCE=0; OPEN=0
for a in "$@"; do
  case "$a" in
    --tier1|--tier1-only) TIER1_ONLY=1 ;;
    --keep)   KEEP="--keep" ;;
    --force)  FORCE=1 ;;
    --open)   OPEN=1 ;;
    -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option: $a" >&2; exit 2 ;;
    *)  if [[ -n "$OUT" ]]; then echo "give one results folder, not two: $OUT and $a" >&2; exit 2; fi; OUT="$a" ;;
  esac
done
[[ -z "$OUT" ]] && { echo "usage: ./run.sh <results-folder> [--tier1] [--keep] [--force] [--open]" >&2; exit 2; }
case "$OUT" in
  captured-output|reports|docs|live|backlog|tests) echo "refusing to write to $OUT: that is repo content, not a run. Pick another name." >&2; exit 2 ;;
  /*) echo "give a folder name, not an absolute path" >&2; exit 2 ;;
esac
if [[ -e "$OUT" && $FORCE -eq 0 ]]; then
  echo "$OUT already exists. Use a new name (results-1, results-2 ...) or pass --force." >&2; exit 2
fi
[[ $FORCE -eq 1 ]] && rm -rf "$OUT"
mkdir -p "$OUT/tier1" "$OUT/tier2"
printf '*\n' > "$OUT/.gitignore"      # a run folder is an artifact: never commit it
LOG="$OUT/run.log"
say() { echo "$@" | tee -a "$LOG"; }
started=$(date -u '+%Y-%m-%d %H:%M:%S UTC'); t0=$SECONDS

say "== queue overload POC -> $OUT/   ($started)"

# step 0 · prerequisites
say ""; say "-- step 0 · prerequisites"
{ python3 --version; python3 -c "import pytest; print('pytest', pytest.__version__)"; } > "$OUT/tier1/prerequisites.txt" 2>&1
PRE_OK=$?
if [[ $TIER1_ONLY -eq 0 ]]; then
  { docker --version; docker compose version; docker info --format 'daemon ok'; } >> "$OUT/tier1/prerequisites.txt" 2>&1 || PRE_OK=1
fi
if [[ $PRE_OK -ne 0 ]]; then
  say "   FAILED — see $OUT/tier1/prerequisites.txt"
  say "   need python 3.9+, pytest, and (for Tier 2) a running Docker"; exit 1
fi
sed 's/^/   /' "$OUT/tier1/prerequisites.txt" | tee -a "$LOG"

# tier 1
say ""; say "-- Tier 1 · deterministic simulation (no Docker)"
python3 run.py --scenario all --save --out "$OUT/tier1" > "$OUT/tier1/run.txt" 2>&1
T1_SIM=$?
python3 -m pytest -q tests > "$OUT/tier1/pytest.txt" 2>&1
T1_TEST=$?
SCEN=$(grep -oE '[0-9]+/[0-9]+ scenarios passed' "$OUT/tier1/run.txt" | tail -1)
PREDS=$(grep -c '\[ok\]' "$OUT/tier1/run.txt")
TESTS=$(grep -oE '[0-9]+ passed' "$OUT/tier1/pytest.txt" | tail -1)
say "   ${SCEN:-no scenario line} · ${PREDS} predictions held · pytest: ${TESTS:-did not report}"

# determinism: the same scenarios twice must give byte-identical json
python3 run.py --scenario all --save --out "$OUT/tier1/.repeat" > /dev/null 2>&1
if cmp -s "$OUT/tier1/summary.json" "$OUT/tier1/.repeat/summary.json"; then DETERMINISTIC=yes; else DETERMINISTIC=NO; fi
rm -rf "$OUT/tier1/.repeat"
say "   deterministic (same numbers on a second run): $DETERMINISTIC"

# tier 2
T2_RUN=0; LIVE_ASSERT=""; LEFTOVER=""; BADSTEPS=""
if [[ $TIER1_ONLY -eq 1 ]]; then
  say ""; say "-- Tier 2 · skipped (--tier1)"
  rmdir "$OUT/tier2" 2>/dev/null
else
  say ""; say "-- Tier 2 · live Kafka + PostgreSQL lab (a few minutes)"
  REPORTS_DIR="$OUT/tier2" CAPTURED_DIR="$OUT/tier1" ./run_all.sh $KEEP >> "$LOG" 2>&1
  T2_RUN=$?
  LIVE_ASSERT=$(grep -oE 'Tier 2 semantic assertions: [0-9]+/[0-9]+ passed' "$OUT/tier2/06b-live-verify.txt" 2>/dev/null | tail -1)
  LEFTOVER=$(grep -oE 'containers_left= *[0-9]+' "$OUT/tier2/11-verify-clean.txt" 2>/dev/null | tail -1 | grep -oE '[0-9]+$')
  BADSTEPS=$(grep -L '^exit=0$' "$OUT/tier2"/[0-9]*.txt 2>/dev/null | wc -l | tr -d ' ')
  say "   steps with a non-zero exit: ${BADSTEPS:-?} · ${LIVE_ASSERT:-no assertion line} · containers left: ${LEFTOVER:-?}"
  [[ -f "$OUT/tier2/06-live-all.txt" ]] && sed -n '/^SUMMARY/,$p' "$OUT/tier2/06-live-all.txt" | sed '/^exit=/d;/^$/d' | sed 's/^/   /' | tee -a "$LOG"
fi

# verdict
ok=1
[[ "$SCEN" == "8/8 scenarios passed" ]] || ok=0
[[ "$TESTS" == "52 passed" ]] || ok=0
[[ "$DETERMINISTIC" == "yes" ]] || ok=0
[[ $T1_SIM -eq 0 && $T1_TEST -eq 0 ]] || ok=0
if [[ $TIER1_ONLY -eq 0 ]]; then
  [[ $T2_RUN -eq 0 ]] || ok=0
  [[ "$LIVE_ASSERT" == *"passed" && "$LIVE_ASSERT" != *"0/"* ]] || ok=0
  [[ -n "$KEEP" || "${LEFTOVER:-1}" == "0" ]] || ok=0
fi
elapsed=$((SECONDS - t0))

{
  echo "# POC run · $OUT"
  echo
  echo "Started $started · took ${elapsed}s · $( [[ $TIER1_ONLY -eq 1 ]] && echo "Tier 1 only" || echo "Tier 1 + Tier 2" )"
  echo
  echo "| check | expected | got |"
  echo "|---|---|---|"
  echo "| Tier 1 scenarios | 8/8 scenarios passed | ${SCEN:-—} |"
  echo "| Tier 1 predictions | 34 | ${PREDS:-—} |"
  echo "| Tier 1 tests | 52 passed | ${TESTS:-—} |"
  echo "| Tier 1 determinism | yes | $DETERMINISTIC |"
  if [[ $TIER1_ONLY -eq 0 ]]; then
    echo "| Tier 2 steps | all exit 0 | $( [[ $T2_RUN -eq 0 ]] && echo "all exit 0" || echo "${BADSTEPS:-?} step(s) failed" ) |"
    echo "| Tier 2 semantics | all passed | ${LIVE_ASSERT:-—} |"
    echo "| Teardown | 0 containers | ${LEFTOVER:-—} |"
  fi
  echo
  echo "**$( [[ $ok -eq 1 ]] && echo PASS || echo FAIL )**"
  echo
  if [[ -f "$OUT/tier1/summary.txt" ]]; then
    echo "## Tier 1"; echo; echo '```'; sed -n '1,18p' "$OUT/tier1/summary.txt"; echo '```'; echo
  fi
  if [[ -f "$OUT/tier2/06-live-all.txt" ]]; then
    echo "## Tier 2 · live (Kafka KRaft + PostgreSQL 16, scaled)"; echo
    echo '```'; sed -n '/^SUMMARY/,$p' "$OUT/tier2/06-live-all.txt" | sed '/^exit=/d'; echo '```'; echo
    echo "Semantic assertions:"; echo
    echo '```'; sed -n '/^ok  \|^FAIL \|^note /p;/semantic assertions/p' "$OUT/tier2/06b-live-verify.txt" 2>/dev/null; echo '```'; echo
  fi
  echo "## Files"
  echo
  echo '```'
  find "$OUT" -type f | sort | sed "s|^$OUT/||"
  echo '```'
} > "$OUT/RESULT.md"

# visual report. Failures are reported, not swallowed: a silent `> /dev/null`
# here hid a broken report through a whole debugging session.
say ""
if python3 report.py "$OUT" > "$OUT/report.log" 2>&1; then
  say "   report: $OUT/report.html"
  [[ $OPEN -eq 1 ]] && { command -v open >/dev/null && open "$OUT/report.html"; }
else
  say "   report FAILED to build — see $OUT/report.log"; ok=0
fi

say ""
if [[ $ok -eq 1 ]]; then say "== PASS · ${elapsed}s · summary: $OUT/RESULT.md"
else say "== FAIL · ${elapsed}s · see $OUT/RESULT.md and $LOG"; fi
exit $(( ok == 1 ? 0 : 1 ))
