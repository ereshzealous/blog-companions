#!/usr/bin/env bash
# The Recovery Test: every fault below happens while live changes continue.
#   schema change -> contract v2 -> incremental snapshot backfill -> ClickHouse throttled -> Connect worker killed
#   -> sink crashes after insert, before offset commit -> explicit delete batch -> restore -> drain -> reconcile
# Usage: LAMBDA=<changes/s> MU=<measured sink capacity> scenarios/02-recovery-test.sh
source "$(dirname "$0")/lib.sh"
LAMBDA=${LAMBDA:?set LAMBDA from the capacity run}
MU=${MU:?set MU (sink drain capacity, changes/s) from the capacity run}
CH_THROTTLE_CPUS=${CH_THROTTLE_CPUS:-0.5}
THROTTLE_S=${THROTTLE_S:-600}
"$LAB/scenarios/00-up.sh" recovery
RUN_ID=$(cat control/run-id)
CH=$(cid clickhouse)
NCPU=$(docker info --format '{{.NCPU}}')

say "baseline at ${LAMBDA} changes/s"
set_rate "$LAMBDA"
GENERATOR_WORKERS=${GENERATOR_WORKERS} docker compose --profile load up -d generator
event baseline-start "{\"lambda\":$LAMBDA,\"mu_measured\":$MU}"
sleep 180

say "schema change: device_registry.firmware_channel"
pg "ALTER TABLE ops.device_registry ADD COLUMN firmware_channel text NOT NULL DEFAULT 'stable'"
touch control/generator-v2
event source-ddl '{"ddl":"ALTER TABLE ops.device_registry ADD COLUMN firmware_channel text NOT NULL DEFAULT '"'"'stable'"'"'"}'
sleep 45
ch "ALTER TABLE cdc.device_registry_current ADD COLUMN IF NOT EXISTS firmware_channel Nullable(String)"
echo 2 > control/contract-version
event contract-v2 '{"clickhouse":"ADD COLUMN firmware_channel Nullable(String)"}'
sleep 30

NULLS=$(ch "SELECT countIf(firmware_channel IS NULL), count() FROM cdc.device_registry_current FINAL WHERE is_deleted = 0" | tr '\t' ',')
event new-column-null-before-backfill "{\"null_rows\":${NULLS%%,*},\"live_rows\":${NULLS##*,},\"source_value\":\"stable\"}"
say "incremental snapshot of device_registry (backfills firmware_channel for rows no event has touched)"
incremental_snapshot device_registry
sleep 45

say "throttling ClickHouse until the sink falls behind the live rate (${THROTTLE_S}s in total)"
THROTTLE_START=$(date +%s)
for cpus in ${CH_THROTTLE_STEPS:-0.5 0.3 0.2 0.12 0.08}; do
  docker update --cpus "$cpus" "$CH" >/dev/null
  lag0=$(last "m.get('sink_lag') or 0")
  sleep 45
  lag1=$(last "m.get('sink_lag') or 0")
  growth=$(( (lag1 - lag0) / 45 ))
  event clickhouse-throttled "{\"cpus\":$cpus,\"lag_growth_per_s\":$growth}"
  (( growth > LAMBDA / 4 )) && break
done
sleep 45

WORKER=$(connectctl task-worker)
say "killing Connect worker that runs the capture task: $WORKER"
event connect-worker-killed "{\"worker\":\"$WORKER\",\"slot_confirmed_flush_lsn\":\"$(last "m.get('confirmed_flush_lsn')")\"}"
docker kill "$(cid "$WORKER")" >/dev/null
sleep 60

say "arming sink crash after insert, before offset commit"
touch control/crash-sink-after-insert
event sink-crash-armed
sleep 60

say "explicit delete batch of 5000 devices"
pg "SELECT coalesce(json_agg(device_id), '[]') FROM (SELECT device_id FROM ops.device_registry WHERE device_id <= ${SEED_REGISTRY_ROWS} ORDER BY random() LIMIT 5000) d" > control/deleted-batch.json
pg "DELETE FROM ops.device_registry WHERE device_id IN (SELECT (jsonb_array_elements_text('$(cat control/deleted-batch.json)'::jsonb))::bigint)"
event delete-batch '{"keys":5000}'

wait_until 900 "capture task running on a surviving worker" "m.get('task_state') == 'RUNNING' and not str(m.get('task_worker') or '').startswith('$WORKER')"
event capture-task-running "{\"worker\":\"$(last "m.get('task_worker')")\"}"
say "restarting killed worker $WORKER"
docker start "$(cid "$WORKER")" >/dev/null
event connect-worker-restarted "{\"worker\":\"$WORKER\"}"

REMAIN=$(( THROTTLE_S - ($(date +%s) - THROTTLE_START) ))
(( REMAIN > 0 )) && sleep "$REMAIN"

BACKLOG=$(last "m.get('sink_lag')")
LIVE=$(python3 -c "import json; rows=[json.loads(l) for l in open('results/$RUN_ID/generator.jsonl')][-6:]; print(round(sum(r['changes_per_s'] for r in rows)/len(rows)))")
PREDICTED=$(python3 -c "print(round($BACKLOG / ($MU - $LIVE)) if $MU > $LIVE else -1)")
docker update --cpus "$NCPU" "$CH" >/dev/null
RESTORE_MS=$(ms)
event clickhouse-restored "{\"backlog\":$BACKLOG,\"lambda_observed\":$LIVE,\"mu_measured\":$MU,\"predicted_drain_s\":$PREDICTED}"

wait_until 3600 "backlog drained (oldest unapplied change < 5 s)" "(m.get('oldest_unapplied_age_ms') or 0) < 5000 and m.get('sink_lag', 10**9) < $LAMBDA * 5"
event backlog-drained "{\"observed_drain_s\":$(( ($(ms) - RESTORE_MS) / 1000 )),\"predicted_drain_s\":$PREDICTED}"
sleep 180
event steady-state-after-recovery
NULLS=$(ch "SELECT countIf(firmware_channel IS NULL), count() FROM cdc.device_registry_current FINAL WHERE is_deleted = 0" | tr '\t' ',')
event new-column-null-after-backfill "{\"null_rows\":${NULLS%%,*},\"live_rows\":${NULLS##*,}}"

say "stop load, wait for quiescence, reconcile"
set_rate 0
docker compose --profile load stop generator
event generator-stopped
tool reconcile.py wait-quiet 90
tool reconcile.py compare final
oom_check
tool report.py recovery
