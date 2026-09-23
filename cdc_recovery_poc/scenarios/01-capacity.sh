#!/usr/bin/env bash
# Measure each plane on its own before injecting faults. Each comparison changes one thing.
#   1. Capture capacity of one PostgreSQL connector: build a WAL backlog with the sink stopped, stop writes,
#      and measure how fast the task drains it into Kafka with no source load (tasks.max = 1).
#   2. The same fill and drain with tasks.max = 4, plus the task count Connect actually runs.
#   3. Sink capacity (mu): drain the Kafka backlog with 1 sink process, then 2.
source "$(dirname "$0")/lib.sh"
"$LAB/scenarios/00-up.sh" capacity
RUN_ID=$(cat control/run-id)
FILL_RATE=${FILL_RATE:-150000}
FILL_S=${FILL_S:-90}

docker compose stop sink
set_rate 0
GENERATOR_WORKERS=${CAPACITY_WORKERS:-10} docker compose --profile load up -d generator

fill_and_drain() {
  local tasks=$1
  say "tasks.max=$tasks: fill a WAL backlog at up to ${FILL_RATE}/s for ${FILL_S}s, then drain it with no source load"
  connectctl register "tasks.max=$tasks"
  sleep 10
  connectctl wait RUNNING 120
  sleep 10
  connectctl status > "results/$RUN_ID/status-tasks-max-$tasks.json"
  event "phase-fill-tasks$tasks-start" "{\"rate\":$FILL_RATE}"
  set_rate "$FILL_RATE"
  sleep "$FILL_S"
  set_rate 0
  event "phase-fill-tasks$tasks-end"
  event "phase-catchup-tasks$tasks-start" "{\"tasks.max\":$tasks}"
  wait_until 1800 "capture caught up with tasks.max=$tasks" "(m.get('unconfirmed_wal_bytes') or 0) < 64*1024*1024"
  event "phase-catchup-tasks$tasks-end"
}
fill_and_drain 1
fill_and_drain 4
connectctl register tasks.max=1
docker compose --profile load stop generator
event generator-stopped
sleep 70

say "C1: drain the Kafka backlog with 1 sink process"
event phase-c1-start '{"sink_processes":1}'
docker compose up -d --scale sink=1 sink
sleep 90
event phase-c1-end
say "C2: drain with 2 sink processes"
event phase-c2-start '{"sink_processes":2}'
docker compose up -d --scale sink=2 sink
wait_until 3600 "backlog drained" "m.get('sink_lag', 1) == 0"
event phase-c2-end

tool reconcile.py wait-quiet 60
tool reconcile.py compare capacity
oom_check
tool report.py capacity
