"""Canonical telemetry contract `cf.telemetry.v1`.

This module is the executable form of `contracts/cf.telemetry.v1.schema.json`.
Both the row path and the Arrow path import their types, ranges and the risk
rule from here, so the two cannot drift apart.

Null policy (frozen):
  * An optional measurement that is absent and one that is explicitly `null`
    both become a typed null. The validator counts them separately so the
    difference stays observable.
  * `risk_flag = battery_pct < 20 AND temperature_c > 40` uses Kleene
    (SQL three-valued) logic, the same semantics as ClickHouse `and` and
    `pyarrow.compute.and_kleene`:
        unknown AND false -> false
        unknown AND true  -> unknown (null)
  * Boundaries are strict: battery_pct == 20 or temperature_c == 40 is not risky.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

TOPIC = "cf.telemetry.v1"
SCHEMA_VERSION = 1

# Risk predicate thresholds (strict inequalities).
BATTERY_RISK_BELOW = 20
TEMPERATURE_RISK_ABOVE = 40.0


@dataclass(frozen=True)
class Range:
    low: float
    high: float


# Physically plausible ranges. Values outside are rejected as OUT_OF_RANGE,
# not silently clamped.
TEMPERATURE_RANGE = Range(-60.0, 150.0)
BATTERY_RANGE = Range(0, 100)
SIGNAL_RANGE = Range(-150, 0)

MAX_ID_LENGTH = 64

REQUIRED_STRING_FIELDS = ("event_id", "device_id", "source_system")
REQUIRED_TIME_FIELDS = ("event_time", "emitted_at")
OPTIONAL_MEASUREMENTS = ("temperature_c", "battery_pct", "signal_dbm")
OPTIONAL_STRINGS = ("firmware_version", "site_id")
KNOWN_FIELDS = frozenset(
    REQUIRED_STRING_FIELDS
    + REQUIRED_TIME_FIELDS
    + ("schema_version",)
    + OPTIONAL_MEASUREMENTS
    + OPTIONAL_STRINGS
)

# Order of the typed row tuple produced by validation. Row path and Arrow path
# both consume this exact order.
TYPED_FIELDS = (
    "event_id",
    "device_id",
    "event_time_ms",
    "emitted_at_ms",
    "source_system",
    "schema_version",
    "temperature_c",
    "battery_pct",
    "signal_dbm",
    "firmware_version",
    "site_id",
)

# Arrow schema for a validated telemetry batch. Timestamps are UTC
# milliseconds; ClickHouse maps timestamp[ms, UTC] to DateTime64(3, 'UTC').
ARROW_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("emitted_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("source_system", pa.string(), nullable=False),
        pa.field("schema_version", pa.uint8(), nullable=False),
        pa.field("temperature_c", pa.float64(), nullable=True),
        pa.field("battery_pct", pa.uint8(), nullable=True),
        pa.field("signal_dbm", pa.int16(), nullable=True),
        pa.field("firmware_version", pa.string(), nullable=True),
        pa.field("site_id", pa.string(), nullable=True),
    ]
)

# The enriched batch both paths serialize: projection plus the risk flag.
ENRICHED_FIELDS = (
    "event_id",
    "device_id",
    "event_time",
    "site_id",
    "temperature_c",
    "battery_pct",
    "risk_flag",
)

ENRICHED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("site_id", pa.string(), nullable=True),
        pa.field("temperature_c", pa.float64(), nullable=True),
        pa.field("battery_pct", pa.uint8(), nullable=True),
        pa.field("risk_flag", pa.bool_(), nullable=True),
    ]
)


def kleene_and(left: bool | None, right: bool | None) -> bool | None:
    """SQL three-valued AND. `None` means unknown."""
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return True


def risk_flag(battery_pct: int | None, temperature_c: float | None) -> bool | None:
    """Row-at-a-time reference implementation of the frozen risk rule."""
    low_battery = None if battery_pct is None else battery_pct < BATTERY_RISK_BELOW
    hot = None if temperature_c is None else temperature_c > TEMPERATURE_RISK_ABOVE
    return kleene_and(low_battery, hot)
