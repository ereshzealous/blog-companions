"""Executable contracts for all five canonical topics.

One `TopicContract` per topic defines field order, validation rules, the Arrow
type and the ClickHouse column type. The normalizer validates its output with
it, the row and Arrow lanes decode with it, and `infra/clickhouse/schema.sql`
is checked against it by unit tests. `cf.telemetry.v1` reuses the frozen Lab 01
types from `contracts.telemetry`, so both stay identical.

Typed row conventions (shared by every lane):
  timestamp -> int epoch milliseconds (UTC)    bool -> int 0/1 (ClickHouse UInt8)
  enum      -> str                             missing optional -> None
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from connectedfleet import topics
from connectedfleet.contracts import telemetry as tel

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)

STRING, TIMESTAMP, INT, FLOAT, BOOL, ENUM = "string", "timestamp", "int", "float", "bool", "enum"


@dataclass(frozen=True)
class Field:
    name: str
    kind: str
    nullable: bool
    ch_type: str
    arrow_type: pa.DataType
    low: float | None = None
    high: float | None = None
    choices: tuple[str, ...] | None = None
    max_len: int = 64


@dataclass(frozen=True)
class TopicContract:
    topic: str
    table: str
    fields: tuple[Field, ...]

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema([pa.field(f.name, f.arrow_type, nullable=f.nullable) for f in self.fields])

    @property
    def clickhouse_columns(self) -> list[tuple[str, str]]:
        """(name, type) in insert order; `ingested_at` is server-side DEFAULT and never sent."""
        return [(f.name, f.ch_type) for f in self.fields]


class ContractError(Exception):
    def __init__(self, code: str, field: str | None = None) -> None:
        super().__init__(f"{code}:{field}")
        self.code = code
        self.field = field


def _ts(name: str, nullable: bool = False) -> Field:
    ch = "DateTime64(3, 'UTC')"
    return Field(name, TIMESTAMP, nullable, f"Nullable({ch})" if nullable else ch, pa.timestamp("ms", tz="UTC"))


def _str(name: str, nullable: bool = True, low_cardinality: bool = False, max_len: int = 64) -> Field:
    base = "Nullable(String)" if nullable else "String"
    ch = f"LowCardinality({base})" if low_cardinality else base
    return Field(name, STRING, nullable, ch, pa.string(), max_len=max_len)


def _enum(name: str, choices: tuple[str, ...], nullable: bool = False) -> Field:
    base = "Nullable(String)" if nullable else "String"
    return Field(name, ENUM, nullable, f"LowCardinality({base})", pa.string(), choices=choices)


def _int(name: str, ch: str, arrow_type: pa.DataType, low: int, high: int, nullable: bool = True) -> Field:
    return Field(name, INT, nullable, f"Nullable({ch})" if nullable else ch, arrow_type, low=low, high=high)


def _float(name: str, low: float, high: float, nullable: bool = True) -> Field:
    return Field(name, FLOAT, nullable, "Nullable(Float64)" if nullable else "Float64", pa.float64(), low=low, high=high)


def _bool(name: str) -> Field:
    return Field(name, BOOL, False, "UInt8", pa.uint8())


COMMON = (
    _str("event_id", nullable=False),
    _str("device_id", nullable=False),
    _ts("event_time"),
    _ts("emitted_at"),
    _str("source_system", nullable=False, low_cardinality=True),
    _int("schema_version", "UInt8", pa.uint8(), 1, 1, nullable=False),
)

CDC = (
    _enum("operation", ("snapshot", "insert", "update", "delete")),
    _bool("is_deleted"),
    _int("version", "UInt64", pa.uint64(), 0, 2**64 - 1, nullable=False),
)

TELEMETRY = TopicContract(
    topics.TELEMETRY,
    "telemetry_fact",
    COMMON
    + (
        _float("temperature_c", tel.TEMPERATURE_RANGE.low, tel.TEMPERATURE_RANGE.high),
        _int("battery_pct", "UInt8", pa.uint8(), 0, 100),
        _int("signal_dbm", "Int16", pa.int16(), -150, 0),
        _str("firmware_version", low_cardinality=True),
        _str("site_id", low_cardinality=True),
    ),
)

LIFECYCLE = TopicContract(
    topics.LIFECYCLE,
    "lifecycle_events",
    COMMON
    + (
        _enum("event_type", ("DeviceActivated", "FirmwareUpdated", "DeviceAssigned", "DeviceDeactivated")),
        _enum("aggregate_type", ("device",)),
        _str("firmware_version"),
        _str("site_id"),
        _str("customer_id"),
        _str("reason", max_len=256),
    ),
)

DEVICE_REGISTRY = TopicContract(
    topics.DEVICE_REGISTRY,
    "device_registry_versions",
    COMMON
    + CDC
    + (
        _str("customer_id"),
        _str("site_id"),
        _str("firmware_version"),
        _enum("state", ("registered", "active", "maintenance", "retired"), nullable=True),
        _ts("updated_at", nullable=True),
    ),
)

DEVICE_STATE = TopicContract(
    topics.DEVICE_STATE,
    "device_state_versions",
    COMMON
    + CDC
    + (
        _str("config_profile"),
        _int("sampling_interval_s", "UInt32", pa.uint32(), 1, 86_400),
        _enum("reporting_mode", ("normal", "power_saving", "diagnostic"), nullable=True),
        _str("desired_firmware"),
        _bool("full_document"),
    ),
)

DIAGNOSTICS = TopicContract(
    topics.DIAGNOSTICS,
    "diagnostic_events",
    COMMON
    + (
        Field("file_sha256", STRING, False, "String", pa.string(), max_len=64),
        _int("row_number", "UInt32", pa.uint32(), 1, 2**32 - 1, nullable=False),
        Field("error_code", STRING, False, "LowCardinality(String)", pa.string(), max_len=32),
        _enum("severity", ("info", "warning", "error", "critical")),
        _int("battery_pct", "UInt8", pa.uint8(), 0, 100),
        _float("temperature_c", tel.TEMPERATURE_RANGE.low, tel.TEMPERATURE_RANGE.high),
        _str("firmware_version"),
        _str("note", max_len=512),
    ),
)

CONTRACTS: dict[str, TopicContract] = {
    c.topic: c for c in (TELEMETRY, LIFECYCLE, DEVICE_REGISTRY, DEVICE_STATE, DIAGNOSTICS)
}

# E2 enriched table: telemetry + risk flag computed by the custom lanes.
ENRICHED_EXTRA = (Field("risk_flag", BOOL, True, "Nullable(Bool)", pa.bool_()),)


def parse_timestamp_ms(value: object, name: str) -> int:
    if type(value) is not str:
        raise ContractError("BAD_TIMESTAMP", name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ContractError("BAD_TIMESTAMP", name) from None
    if parsed.tzinfo is None:
        raise ContractError("BAD_TIMESTAMP", name)
    return (parsed - _EPOCH) // _ONE_MS


def format_timestamp_ms(epoch_ms: int) -> str:
    moment = _EPOCH + timedelta(milliseconds=epoch_ms)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{epoch_ms % 1000:03d}Z"


def _coerce(field: Field, value: object) -> object:
    kind = field.kind
    if kind == STRING or kind == ENUM:
        if type(value) is not str:
            raise ContractError("BAD_TYPE", field.name)
        if not field.nullable and not value:
            raise ContractError("BAD_TYPE", field.name)
        if len(value) > field.max_len:
            raise ContractError("TOO_LONG", field.name)
        if kind == ENUM and value not in field.choices:
            raise ContractError("BAD_ENUM", field.name)
        return value
    if kind == TIMESTAMP:
        return parse_timestamp_ms(value, field.name)
    if kind == INT:
        if type(value) is not int:
            raise ContractError("BAD_TYPE", field.name)
        if not (field.low <= value <= field.high):
            raise ContractError(
                "UNSUPPORTED_SCHEMA_VERSION" if field.name == "schema_version" else "OUT_OF_RANGE", field.name
            )
        return value
    if kind == FLOAT:
        if type(value) is not float and type(value) is not int:
            raise ContractError("BAD_TYPE", field.name)
        number = float(value)
        if not math.isfinite(number) or not (field.low <= number <= field.high):
            raise ContractError("OUT_OF_RANGE", field.name)
        return number
    if kind == BOOL:
        if type(value) is not bool:
            raise ContractError("BAD_TYPE", field.name)
        return int(value)
    raise ContractError("UNKNOWN_KIND", field.name)


def validate(contract: TopicContract, obj: dict) -> tuple:
    """Validate one decoded canonical event into a typed row tuple in contract field order."""
    row = []
    for field in contract.fields:
        value = obj.get(field.name)
        if value is None:
            if not field.nullable:
                raise ContractError("MISSING_REQUIRED", field.name)
            row.append(None)
            continue
        row.append(_coerce(field, value))
    return tuple(row)


def to_json_value(contract: TopicContract, row: tuple) -> dict:
    """Inverse of `validate`: typed row -> canonical JSON object (timestamps as RFC 3339 UTC)."""
    out = {}
    for field, value in zip(contract.fields, row):
        if value is not None and field.kind == TIMESTAMP:
            value = format_timestamp_ms(value)
        elif value is not None and field.kind == BOOL:
            value = bool(value)
        out[field.name] = value
    return out
