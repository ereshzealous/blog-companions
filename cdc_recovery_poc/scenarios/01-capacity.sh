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
FILL_S=${FILL_S:-120}       # capture runs during this window: measures capture rate under live writes
RESERVOIR_S=${RESERVOIR_S:-120}  # capture paused during this window: builds the WAL backlog to drain

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
  event "phase-fill-tasks$tasks-end"

  # Capture can keep up with the generator on a fast machine, and then no backlog forms and there is
  # nothing to drain. Stopping capture while the source keeps writing builds the reservoir on purpose,
  # so the drain measures capture throughput rather than the generator's.
  say "tasks.max=$tasks: pause capture for ${RESERVOIR_S}s while the source keeps writing"
  connectctl stop
  connectctl wait STOPPED 180
  event "phase-reservoir-tasks$tasks-start" "{\"seconds\":$RESERVOIR_S}"
  sleep "$RESERVOIR_S"
  set_rate 0
  event "phase-reservoir-tasks$tasks-end"
  connectctl resume
  connectctl wait RUNNING 300

  event "phase-catchup-tasks$tasks-start" "{\"tasks.max\":$tasks}"
  wait_flat 1800 "capture caught up with tasks.max=$tasks" kafka_end_offsets 30
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
