"""Decode and validate canonical telemetry JSON (`cf.telemetry.v1`).

Both custom processing paths call these two functions, so JSON decoding and
validation cost the same in the row path and the Arrow path. Arrow only
enters *after* validation; decoding JSON is not something Arrow removes here.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from connectedfleet.contracts import telemetry as contract

# Reject reason codes (stable; used in tests and reports).
MALFORMED_JSON = "MALFORMED_JSON"
NOT_AN_OBJECT = "NOT_AN_OBJECT"
MISSING_REQUIRED = "MISSING_REQUIRED"
BAD_TYPE = "BAD_TYPE"
BAD_TIMESTAMP = "BAD_TIMESTAMP"
OUT_OF_RANGE = "OUT_OF_RANGE"
TOO_LONG = "TOO_LONG"
UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ABSENT = 0
_NULL = 1
_ONE_MS = timedelta(milliseconds=1)


@dataclass(frozen=True)
class Reject:
    index: int
    reason: str
    field: str | None = None


@dataclass
class FieldStats:
    """Absent vs explicit-null counts for optional fields, plus unknown fields."""

    absent: dict[str, int] = field(default_factory=dict)
    explicit_null: dict[str, int] = field(default_factory=dict)
    unknown_fields: int = 0

    def merge(self, other: FieldStats) -> None:
        for name, count in other.absent.items():
            self.absent[name] = self.absent.get(name, 0) + count
        for name, count in other.explicit_null.items():
            self.explicit_null[name] = self.explicit_null.get(name, 0) + count
        self.unknown_fields += other.unknown_fields


class _Invalid(Exception):
    def __init__(self, reason: str, field_name: str | None) -> None:
        self.reason = reason
        self.field_name = field_name


def decode(lines: list[bytes], first_index: int = 0) -> tuple[list[tuple[int, dict]], list[Reject]]:
    """JSON-decode one batch of lines. Returns (index, object) pairs and rejects."""
    decoded: list[tuple[int, dict]] = []
    rejects: list[Reject] = []
    loads = json.loads
    for offset, line in enumerate(lines):
        index = first_index + offset
        try:
            obj = loads(line)
        except ValueError:
            rejects.append(Reject(index, MALFORMED_JSON))
            continue
        if type(obj) is not dict:
            rejects.append(Reject(index, NOT_AN_OBJECT))
            continue
        decoded.append((index, obj))
    return decoded, rejects


def _required_string(obj: dict, name: str) -> str:
    value = obj.get(name)
    if value is None:
        raise _Invalid(MISSING_REQUIRED, name)
    if type(value) is not str or not value:
        raise _Invalid(BAD_TYPE, name)
    if len(value) > contract.MAX_ID_LENGTH:
        raise _Invalid(TOO_LONG, name)
    return value


def _timestamp_ms(obj: dict, name: str) -> int:
    value = obj.get(name)
    if value is None:
        raise _Invalid(MISSING_REQUIRED, name)
    if type(value) is not str:
        raise _Invalid(BAD_TIMESTAMP, name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _Invalid(BAD_TIMESTAMP, name) from None
    if parsed.tzinfo is None:
        # A timestamp without an offset is ambiguous; the contract requires UTC-aware values.
        raise _Invalid(BAD_TIMESTAMP, name)
    return (parsed - _EPOCH) // _ONE_MS


def _optional_float(obj: dict, name: str, rng: contract.Range, notes: list) -> float | None:
    if name not in obj:
        notes.append((_ABSENT, name))
        return None
    value = obj[name]
    if value is None:
        notes.append((_NULL, name))
        return None
    kind = type(value)
    if kind is not float and kind is not int:  # bool is rejected: type(True) is bool
        raise _Invalid(BAD_TYPE, name)
    number = float(value)
    if not math.isfinite(number) or not (rng.low <= number <= rng.high):
        raise _Invalid(OUT_OF_RANGE, name)
    return number


def _optional_int(obj: dict, name: str, rng: contract.Range, notes: list) -> int | None:
    if name not in obj:
        notes.append((_ABSENT, name))
        return None
    value = obj[name]
    if value is None:
        notes.append((_NULL, name))
        return None
    if type(value) is not int:  # 17.5, "17" and True are all BAD_TYPE
        raise _Invalid(BAD_TYPE, name)
    if not (rng.low <= value <= rng.high):
        raise _Invalid(OUT_OF_RANGE, name)
    return value


def _optional_string(obj: dict, name: str, notes: list) -> str | None:
    if name not in obj:
        notes.append((_ABSENT, name))
        return None
    value = obj[name]
    if value is None:
        notes.append((_NULL, name))
        return None
    if type(value) is not str:
        raise _Invalid(BAD_TYPE, name)
    if len(value) > contract.MAX_ID_LENGTH:
        raise _Invalid(TOO_LONG, name)
    return value


def validate(decoded: list[tuple[int, dict]]) -> tuple[list[tuple], list[Reject], FieldStats]:
    """Type and range-check decoded objects into typed row tuples.

    Row tuple order is `contract.TYPED_FIELDS`. Invalid records are rejected
    whole (never partially repaired), with the first failing field recorded.
    """
    rows: list[tuple] = []
    rejects: list[Reject] = []
    stats = FieldStats()
    known = contract.KNOWN_FIELDS
    for index, obj in decoded:
        notes: list = []
        try:
            version = obj.get("schema_version")
            if version is None:
                raise _Invalid(MISSING_REQUIRED, "schema_version")
            if type(version) is not int or version != contract.SCHEMA_VERSION:
                raise _Invalid(UNSUPPORTED_SCHEMA_VERSION, "schema_version")
            row = (
                _required_string(obj, "event_id"),
                _required_string(obj, "device_id"),
                _timestamp_ms(obj, "event_time"),
                _timestamp_ms(obj, "emitted_at"),
                _required_string(obj, "source_system"),
                version,
                _optional_float(obj, "temperature_c", contract.TEMPERATURE_RANGE, notes),
                _optional_int(obj, "battery_pct", contract.BATTERY_RANGE, notes),
                _optional_int(obj, "signal_dbm", contract.SIGNAL_RANGE, notes),
                _optional_string(obj, "firmware_version", notes),
                _optional_string(obj, "site_id", notes),
            )
        except _Invalid as invalid:
            rejects.append(Reject(index, invalid.reason, invalid.field_name))
            continue
        # Absent/null observations count only for accepted rows.
        for kind, name in notes:
            counts = stats.absent if kind == _ABSENT else stats.explicit_null
            counts[name] = counts.get(name, 0) + 1
        if not known.issuperset(obj):
            stats.unknown_fields += sum(1 for key in obj if key not in known)
        rows.append(row)
    return rows, rejects, stats
