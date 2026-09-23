#!/usr/bin/env bash
# Fresh environment: destroy volumes, start infrastructure, register the connector, wait for the initial snapshot.
# Usage: scenarios/00-up.sh <run-name>
source "$(dirname "$0")/lib.sh"

say "resetting environment"
"${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf control && mkdir -p control
new_run "${1:-setup}"

say "starting PostgreSQL, Kafka, ClickHouse (seeding ${SEED_REGISTRY_ROWS} + ${SEED_STATUS_ROWS} rows)"
docker compose up -d --wait postgres kafka clickhouse
docker compose up -d tools collector
start_worker connect-1
start_worker connect-2
record_versions
event environment-ready "$(cat "results/$RUN_ID/versions.json" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)))')"

event connector-register '{"snapshot.mode":"initial"}'
connectctl register
docker compose up -d sink
connectctl wait RUNNING 300
wait_loaded 1800
event initial-load-complete
