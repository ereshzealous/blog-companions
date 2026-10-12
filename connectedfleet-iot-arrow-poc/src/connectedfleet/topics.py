"""Kafka topic names and partition counts (single source for init, producers, normalizer and lanes)."""

from __future__ import annotations

# Raw topics: names come from connector / producer configuration (docs/ARCHITECTURE.md §3).
RAW_PG_REGISTRY = "cfraw.pg.public.device_registry"
RAW_OUTBOX = "cfraw.outbox.device"
RAW_MONGO_STATE = "cfraw.mongo.connectedfleet.device_state"
RAW_TELEMETRY = "cfraw.telemetry.gateway"
RAW_DIAGNOSTICS = "cfraw.diagnostics.csv"
RAW_TOPICS = (RAW_PG_REGISTRY, RAW_OUTBOX, RAW_MONGO_STATE, RAW_TELEMETRY, RAW_DIAGNOSTICS)

# Canonical topics (the contract) -> ClickHouse table.
TELEMETRY = "cf.telemetry.v1"
LIFECYCLE = "cf.lifecycle.v1"
DEVICE_REGISTRY = "cf.device-registry.v1"
DEVICE_STATE = "cf.device-state.v1"
DIAGNOSTICS = "cf.diagnostics.v1"
CANONICAL_TOPICS = (TELEMETRY, LIFECYCLE, DEVICE_REGISTRY, DEVICE_STATE, DIAGNOSTICS)

TOPIC_TABLE = {
    TELEMETRY: "telemetry_fact",
    LIFECYCLE: "lifecycle_events",
    DEVICE_REGISTRY: "device_registry_versions",
    DEVICE_STATE: "device_state_versions",
    DIAGNOSTICS: "diagnostic_events",
}

# Raw topic -> canonical topic it is normalized into.
RAW_TO_CANONICAL = {
    RAW_PG_REGISTRY: DEVICE_REGISTRY,
    RAW_OUTBOX: LIFECYCLE,
    RAW_MONGO_STATE: DEVICE_STATE,
    RAW_TELEMETRY: TELEMETRY,
    RAW_DIAGNOSTICS: DIAGNOSTICS,
}

DLQ_NORMALIZER = "cf.dlq.normalizer.v1"
DLQ_SINK = "cf.dlq.sink.v1"
DLQ_LANE = {"row": "cf.dlq.lane-b.v1", "arrow": "cf.dlq.lane-c.v1"}
DLQ_TOPICS = (DLQ_NORMALIZER, DLQ_SINK, *DLQ_LANE.values())

PARTITIONS = {
    RAW_PG_REGISTRY: 3,
    RAW_OUTBOX: 3,
    RAW_MONGO_STATE: 3,
    RAW_TELEMETRY: 6,
    RAW_DIAGNOSTICS: 3,
    TELEMETRY: 6,
    LIFECYCLE: 3,
    DEVICE_REGISTRY: 3,
    DEVICE_STATE: 3,
    DIAGNOSTICS: 3,
    **{t: 1 for t in DLQ_TOPICS},
}

LANE_DATABASES = {"sink": "bench_sink", "row": "bench_row", "arrow": "bench_arrow"}


def bench_topic(run_id: str, case: str) -> str:
    """Per-run, per-case benchmark topic so every lane reads the identical offset range."""
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in f"{run_id}-{case}")
    return f"cfbench.{safe}.telemetry"
