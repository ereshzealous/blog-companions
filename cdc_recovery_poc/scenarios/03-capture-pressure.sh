#!/usr/bin/env bash
# Capture pressure: Kafka is paused while the application keeps writing.
# Where does the data wait? In PostgreSQL WAL retained by the replication slot. How fast does that reservoir fill?
# Usage: LAMBDA=<changes/s> scenarios/03-capture-pressure.sh
source "$(dirname "$0")/lib.sh"
LAMBDA=${LAMBDA:?set LAMBDA}
OUTAGE_S=${OUTAGE_S:-240}
"$LAB/scenarios/00-up.sh" capture-pressure
RUN_ID=$(cat control/run-id)

set_rate "$LAMBDA"
docker compose --profile load up -d generator
event baseline-start "{\"lambda\":$LAMBDA}"
sleep 120

event kafka-paused "{\"retained_wal_bytes\":$(last "m.get('retained_wal_bytes') or 0")}"
docker pause "$(cid kafka)" >/dev/null
sleep "$OUTAGE_S"
event kafka-unpaused "{\"retained_wal_bytes\":$(last "m.get('retained_wal_bytes') or 0")}"
docker unpause "$(cid kafka)" >/dev/null
RESUME_MS=$(ms)

wait_until 1800 "capture and sink caught up" "(m.get('oldest_unapplied_age_ms') or 0) < 5000 and (m.get('unconfirmed_wal_bytes') or 0) < 256*1024*1024 and m.get('task_state') == 'RUNNING'"
event caught-up "{\"seconds_after_unpause\":$(( ($(ms) - RESUME_MS) / 1000 ))}"
sleep 120

set_rate 0
docker compose --profile load stop generator
event generator-stopped
tool reconcile.py wait-quiet 90
tool reconcile.py compare final
oom_check
tool report.py capture-pressure
