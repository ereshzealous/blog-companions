#!/usr/bin/env bash
# Expected failure: the source history the connector needs is removed while capture is stopped.
# The proof is not that recovery is automatic. It is that the loss is detected, capture does not resume from a position
# whose history is gone, and state converges only after an explicit resnapshot and a sweep for deletes it could not see.
# Debezium 3.6.2 does not fail fast on a lost slot. A lost slot has no confirmed_flush_lsn, which PostgresConnection
# treats as a slot still being created: it retries every 2 s for 900 attempts while Kafka Connect reports the connector
# RUNNING with no tasks, then throws. This script observes that whole window instead of assuming a fast failure.
# Usage: LAMBDA=<changes/s> scenarios/04-history-loss.sh
source "$(dirname "$0")/lib.sh"
LAMBDA=${LAMBDA:?set LAMBDA}
KEEP=${MAX_SLOT_WAL_KEEP_SIZE:-1GB}
RESUME_OBSERVE_S=${RESUME_OBSERVE_S:-1900}
"$LAB/scenarios/00-up.sh" history-loss
RUN_ID=$(cat control/run-id)

set_rate "$LAMBDA"
docker compose --profile load up -d generator
event baseline-start "{\"lambda\":$LAMBDA}"
sleep 90

say "bounding slot WAL retention to $KEEP, then stopping capture"
pg "ALTER SYSTEM SET max_slot_wal_keep_size = '$KEEP'"
pg "SELECT pg_reload_conf()" >/dev/null
event slot-retention-bounded "{\"max_slot_wal_keep_size\":\"$KEEP\"}"
connectctl stop
connectctl wait STOPPED 120
event capture-stopped "{\"confirmed_flush_lsn\":\"$(pg "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'cdc_devices_clickhouse'")\"}"

say "application keeps writing; checkpoint until the slot loses its history"
deadline=$(( $(date +%s) + 1800 ))
until [[ "$(pg "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'cdc_devices_clickhouse'")" == "lost" ]]; do
  (( $(date +%s) < deadline )) || { say "slot never reached lost"; exit 1; }
  sleep 20
  pg "CHECKPOINT"
done
event source-history-lost "{\"wal_status\":\"lost\",\"invalidation_reason\":\"$(pg "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = 'cdc_devices_clickhouse'")\"}"

# resume_status <since>: connector and task state, Debezium's slot-retry log lines since <since>, and any failure trace.
resume_status() {
  local status retries last
  status=$(connectctl status 2>/dev/null) || status='{"connector":{"state":"UNKNOWN"},"tasks":[]}'
  retries=$("${DC[@]}" logs --no-color --since "$1" connect-1 connect-2 2>/dev/null | grep -c "Cannot obtain valid replication slot" || true)
  last=$("${DC[@]}" logs --no-color --since "$1" connect-1 connect-2 2>/dev/null | grep "Cannot obtain valid replication slot" | tail -1 | sed 's/^[^|]*| //' | cut -c1-260 || true)
  python3 - "$status" "${retries:-0}" "$last" <<'PY'
import json, sys
s = json.loads(sys.argv[1])
tasks = s.get("tasks") or []
traces = [s.get("connector", {}).get("trace") or ""] + [t.get("trace") or "" for t in tasks]
lines = [l.strip() for t in traces for l in t.splitlines() if "Exception" in l or "Caused by" in l]
print(json.dumps({"connector_state": s.get("connector", {}).get("state"), "task_states": [t.get("state") for t in tasks],
                  "slot_retry_log_lines": int(sys.argv[2]), "last_slot_retry_log": sys.argv[3], "trace": "\n".join(lines)[:1500]}))
PY
}

say "attempting normal resume; observing for up to ${RESUME_OBSERVE_S}s"
SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
RESUME_S=$(date +%s)
connectctl resume
event normal-resume-requested "{\"observe_s\":$RESUME_OBSERVE_S}"
outcome=still-stalled stalled_recorded=0 st='{}'
while (( $(date +%s) - RESUME_S < RESUME_OBSERVE_S )); do
  sleep 15
  st=$(resume_status "$SINCE")
  verdict=$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print("failed" if d["connector_state"] == "FAILED" or "FAILED" in d["task_states"] else "task-running" if "RUNNING" in d["task_states"] else "no-task")' "$st")
  if [[ "$verdict" == failed ]]; then outcome=refused; break; fi
  if [[ "$verdict" == task-running ]]; then outcome=not-refused; break; fi
  if (( ! stalled_recorded && $(date +%s) - RESUME_S >= 180 )); then
    event normal-resume-stalled "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d["seconds_after_resume"]=int(sys.argv[2]); print(json.dumps(d))' "$st" "$(( $(date +%s) - RESUME_S ))")"
    stalled_recorded=1
  fi
done
event "normal-resume-$outcome" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d["seconds_after_resume"]=int(sys.argv[2]); print(json.dumps(d))' "$st" "$(( $(date +%s) - RESUME_S ))")"
sleep 30
event evidence-window-closed

say "operator recovery: reset offsets, drop the lost slot, resnapshot"
event recovery-start
connectctl stop
connectctl wait STOPPED 120
connectctl reset-offsets
pg "SELECT pg_drop_replication_slot('cdc_devices_clickhouse')" >/dev/null
pg "ALTER SYSTEM RESET max_slot_wal_keep_size"
pg "SELECT pg_reload_conf()" >/dev/null
SOURCE_ROWS=$(pg "SELECT (SELECT count(*) FROM ops.device_registry) + (SELECT count(*) FROM ops.device_status)")
RESNAPSHOT_MS=$(ms)
event resnapshot-start "{\"ts_ms\":$RESNAPSHOT_MS,\"source_rows\":$SOURCE_ROWS}"
connectctl resume
connectctl wait RUNNING 300
# Ghost rows inflate ClickHouse row counts, so completion is judged by snapshot reads applied since the resume.
deadline=$(( $(date +%s) + 3600 ))
until (( $(ch "SELECT countIf(op = 'r') FROM cdc.change_history WHERE applied_at >= fromUnixTimestamp64Milli($RESNAPSHOT_MS)") * 1000 >= SOURCE_ROWS * 995 )); do
  (( $(date +%s) < deadline )) || { say "resnapshot did not complete"; exit 1; }
  sleep 15
done
event resnapshot-reads-applied "{\"reads\":$(ch "SELECT countIf(op = 'r') FROM cdc.change_history WHERE applied_at >= fromUnixTimestamp64Milli($RESNAPSHOT_MS)")}"

set_rate 0
docker compose --profile load stop generator
event generator-stopped
tool reconcile.py wait-quiet 90
tool reconcile.py compare after-resnapshot

say "sweep: rows not re-emitted since the resnapshot began were deleted while history was lost"
for t in device_registry device_status; do
  cols=$(ch "SELECT arrayStringConcat(groupArray(name), ', ') FROM system.columns WHERE database = 'cdc' AND table = '${t}_current' AND name NOT IN ('version', 'is_deleted', 'sink_id', 'applied_at')")
  ch "INSERT INTO cdc.${t}_current (${cols}, version, is_deleted, sink_id)
      SELECT ${cols}, version + 1, 1, 'resnapshot-sweep' FROM cdc.${t}_current FINAL
      WHERE is_deleted = 0 AND applied_at < fromUnixTimestamp64Milli(${RESNAPSHOT_MS})"
done
event resnapshot-sweep "{\"swept_registry\":$(ch "SELECT count() FROM cdc.device_registry_current WHERE sink_id = 'resnapshot-sweep'"),\"swept_status\":$(ch "SELECT count() FROM cdc.device_status_current WHERE sink_id = 'resnapshot-sweep'")}"
tool reconcile.py compare after-sweep
oom_check
tool report.py history-loss
