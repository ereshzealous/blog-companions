#!/usr/bin/env bash
# Shared helpers for lab scenarios. Scenario scripts source this file.
set -euo pipefail
LAB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$LAB"
set -a
# shellcheck disable=SC1091
source .env
set +a
DC=(docker compose --profile load)

ms() { python3 -c 'import time; print(time.time_ns() // 1_000_000)'; }
say() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

new_run() {
  RUN_ID="$1-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "results/$RUN_ID" control
  echo "$RUN_ID" > control/run-id
  say "run $RUN_ID"
}

# event <name> [json-object]: timestamped entry in results/<run>/events.jsonl
event() {
  local detail="${2:-}"
  [[ -n "$detail" ]] || detail='{}'
  printf '{"ts_ms":%s,"event":"%s","detail":%s}\n' "$(ms)" "$1" "$detail" | tee -a "results/$RUN_ID/events.jsonl"
}

pg() { "${DC[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U lab_admin -d devices -Atq -c "$1"; }
ch() { "${DC[@]}" exec -T clickhouse clickhouse-client --user lab --password "$CLICKHOUSE_PASSWORD" -q "$1"; }
tool() { "${DC[@]}" exec -T tools python "$@"; }
connectctl() { tool connectctl.py "$@"; }
cid() { docker compose ps -aq "$1"; }

# start_worker <service>: start one Connect worker and wait for its REST endpoint.
# Workers start one at a time: two JVMs scanning the plugin path at once have exhausted heap during startup.
# A worker that exits during startup gets one more attempt; both attempts are recorded as events.
start_worker() {
  local w=$1 attempt deadline state
  for attempt in 1 2; do
    docker compose up -d "$w"
    deadline=$(( $(date +%s) + 180 ))
    while :; do
      if tool -c "import requests; requests.get('http://$w:8083/', timeout=3).raise_for_status()" >/dev/null 2>&1; then
        event connect-worker-ready "{\"worker\":\"$w\",\"attempt\":$attempt}"
        return 0
      fi
      state=$(docker inspect -f '{{.State.Status}}' "$(cid "$w")" 2>/dev/null || echo missing)
      if [[ "$state" == "exited" || "$state" == "missing" ]] || (( $(date +%s) >= deadline )); then
        say "$w did not start (attempt $attempt, state $state)"
        docker logs --tail 5 "$(cid "$w")" 2>&1 | cut -c1-200 || true
        event connect-worker-start-failed "{\"worker\":\"$w\",\"attempt\":$attempt,\"state\":\"$state\"}"
        break
      fi
      sleep 3
    done
  done
  say "giving up: $w did not start"
  return 1
}

set_rate() {
  echo "$1" > control/generator-rate
  event generator-rate "{\"rate\":$1}"
}

# last <python expression over m>: evaluate against the latest collector sample
last() {
  python3 - "results/$RUN_ID/metrics.jsonl" "$1" <<'EOF'
import json, sys
m = None
with open(sys.argv[1]) as f:
    for line in f:
        m = json.loads(line)
print(eval(sys.argv[2], {}, {"m": m or {}}))
EOF
}

# wait_until <timeout_s> <description> <python expression over m>
# End a phase when a metric stops moving. Capture is caught up when Kafka's end offsets stop growing.
# The slot's confirmed position is not a usable signal here: once the source goes idle, Debezium has
# nothing left to acknowledge, so unconfirmed WAL plateaus at whatever the last fill left behind.
wait_flat() {
  local timeout=$1 desc=$2 field=$3 stable=${4:-30} deadline value previous="" since now
  deadline=$(( $(date +%s) + timeout ))
  since=$(date +%s)
  while :; do
    value=$(last "m.get('$field')" 2>/dev/null || echo None)
    now=$(date +%s)
    if [[ "$value" != "$previous" ]]; then previous=$value; since=$now; fi
    if [[ -n "$previous" && "$previous" != "None" && "$previous" != "0" ]] && (( now - since >= stable )); then
      say "reached: $desc (${field} flat at ${previous} for ${stable}s)"; return 0
    fi
    if (( now >= deadline )); then say "timeout after ${timeout}s: $desc"; return 1; fi
    sleep 3
  done
}

wait_until() {
  local timeout=$1 desc=$2 expr=$3 deadline
  deadline=$(( $(date +%s) + timeout ))
  until [[ "$(last "bool($expr)" 2>/dev/null)" == "True" ]]; do
    if (( $(date +%s) >= deadline )); then say "timeout after ${timeout}s: $desc"; return 1; fi
    sleep 3
  done
  say "reached: $desc"
}

incremental_snapshot() {
  local collections id
  collections=$(printf '"ops.%s",' "$@")
  collections="[${collections%,}]"
  id="snap-$(ms)"
  pg "INSERT INTO ops.debezium_signal (id, type, data) VALUES ('$id', 'execute-snapshot', '{\"data-collections\": $collections, \"type\": \"incremental\"}')"
  event incremental-snapshot-signal "{\"signal_id\":\"$id\",\"collections\":$collections}"
}

# Wait until ClickHouse current state holds as many live rows as the source (initial load or resnapshot done).
wait_loaded() {
  local timeout=${1:-1800} deadline src dst
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    src=$(pg "SELECT (SELECT count(*) FROM ops.device_registry) + (SELECT count(*) FROM ops.device_status)")
    dst=$(ch "SELECT (SELECT count() FROM cdc.device_registry_current FINAL WHERE is_deleted = 0) + (SELECT count() FROM cdc.device_status_current FINAL WHERE is_deleted = 0)")
    say "load: source=$src clickhouse=$dst"
    [[ "$dst" -ge "$src" ]] && return 0
    (( $(date +%s) < deadline )) || { say "timeout waiting for load"; return 1; }
    sleep 15
  done
}

record_versions() {
  {
    echo "{"
    echo "\"postgres\": \"$(pg 'SHOW server_version')\","
    echo "\"clickhouse\": \"$(ch 'SELECT version()')\","
    echo "\"kafka_image\": \"apache/kafka:$KAFKA_VERSION\","
    echo "\"debezium_image\": \"quay.io/debezium/connect:$DEBEZIUM_VERSION\","
    echo "\"connect\": $(connectctl versions),"
    echo "\"docker_cpus\": $(docker info --format '{{.NCPU}}'), \"docker_memory_bytes\": $(docker info --format '{{.MemTotal}}'),"
    echo "\"host\": \"$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)\""
    echo "}"
  } > "results/$RUN_ID/versions.json"
}

# oom_check: record whether any lab container was killed by the OOM killer during the run
oom_check() {
  local report="{" first=1 c name oom
  for c in $(docker compose --profile load ps -aq); do
    name=$(docker inspect -f '{{.Name}}' "$c" | tr -d /)
    oom=$(docker inspect -f '{{.State.OOMKilled}}' "$c")
    (( first )) || report+=","
    report+="\"$name\":$oom"
    first=0
  done
  event oom-check "$report}"
}
