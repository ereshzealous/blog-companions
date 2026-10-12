"""Pure mapping functions: one raw source record -> one canonical event dict (or a skip).

No I/O here; `normalize.service` feeds Kafka records in and produces the
results, and unit tests feed hand-written Debezium/gateway/CSV envelopes.

Every mapper returns either a canonical JSON object (validated afterwards
against `contracts.canonical`) or `SKIP` (e.g. Debezium tombstones), or raises
`MappingError` with a stable code. Error details name fields, never values,
so DLQ headers and logs carry no payload data.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from connectedfleet import topics
from connectedfleet.contracts.canonical import format_timestamp_ms

SKIP = None
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)

_PG_OPS = {"r": "snapshot", "c": "insert", "u": "update", "d": "delete"}
_MONGO_OPS = {"r": "snapshot", "c": "insert", "u": "update", "d": "delete"}


class MappingError(Exception):
    def __init__(self, code: str, field: str | None = None) -> None:
        super().__init__(f"{code}:{field}")
        self.code = code
        self.field = field


def _require(obj: dict, name: str):
    if not isinstance(obj, dict) or obj.get(name) is None:
        raise MappingError("MISSING_FIELD", name)
    return obj[name]


def _epoch_ms_to_iso(value: object, name: str) -> str:
    if type(value) is not int:
        raise MappingError("BAD_TIMESTAMP", name)
    return format_timestamp_ms(value)


def _iso_to_ms_iso(value: object, name: str) -> str | None:
    """Re-format a Debezium ZonedTimestamp / timestamptz string to millisecond RFC 3339 UTC."""
    if value is None:
        return None
    if type(value) is not str:
        raise MappingError("BAD_TIMESTAMP", name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise MappingError("BAD_TIMESTAMP", name) from None
    if parsed.tzinfo is None:
        raise MappingError("BAD_TIMESTAMP", name)
    return format_timestamp_ms((parsed - _EPOCH) // _ONE_MS)


# --------------------------------------------------------------------------- gateway telemetry
def map_gateway(value: dict | None) -> dict:
    """Raw gateway JSON (deci-degrees, epoch ms, short keys) -> cf.telemetry.v1."""
    if not isinstance(value, dict):
        raise MappingError("NOT_AN_OBJECT")
    measurements = value.get("m")
    if measurements is None:
        measurements = {}
    if not isinstance(measurements, dict):
        raise MappingError("BAD_TYPE", "m")
    event = {
        "event_id": _require(value, "msg_id"),
        "device_id": _require(value, "dev"),
        "event_time": _epoch_ms_to_iso(_require(value, "ts"), "ts"),
        "emitted_at": _epoch_ms_to_iso(_require(value, "sent"), "sent"),
        "source_system": "iot-gateway",
        "schema_version": 1,
    }
    # Absent stays absent and null stays null; the contract treats both as typed null.
    if "t_dC" in measurements:
        raw = measurements["t_dC"]
        if raw is None:
            event["temperature_c"] = None
        elif type(raw) is int:
            event["temperature_c"] = round(raw / 10.0, 1)
        else:
            raise MappingError("BAD_TYPE", "m.t_dC")
    if "bat" in measurements:
        event["battery_pct"] = measurements["bat"]
    if "rssi" in measurements:
        event["signal_dbm"] = measurements["rssi"]
    if "fw" in value:
        event["firmware_version"] = value["fw"]
    if "site" in value:
        event["site_id"] = value["site"]
    return event


# --------------------------------------------------------------------------- PostgreSQL registry CDC
def map_pg_registry(key: dict | None, value: dict | None) -> dict | None:
    """Debezium PostgreSQL envelope (JsonConverter, schemas disabled) -> cf.device-registry.v1."""
    if value is None:
        return SKIP  # tombstone after a delete
    op = value.get("op")
    if op not in _PG_OPS:
        raise MappingError("BAD_OPERATION", "op")
    source = _require(value, "source")
    lsn = source.get("lsn")
    if type(lsn) is not int:
        raise MappingError("MISSING_FIELD", "source.lsn")
    row = value.get("before") if op == "d" else value.get("after")
    if not isinstance(row, dict):
        # A delete under REPLICA IDENTITY DEFAULT still carries the key in `before`; fall back to the message key.
        row = key if (op == "d" and isinstance(key, dict)) else None
    if row is None:
        raise MappingError("MISSING_FIELD", "before" if op == "d" else "after")
    device_id = _require(row, "device_id")
    deleted = op == "d"
    return {
        "event_id": f"pgreg-{lsn}-{device_id}",
        "device_id": device_id,
        "event_time": _epoch_ms_to_iso(source.get("ts_ms"), "source.ts_ms"),
        "emitted_at": _epoch_ms_to_iso(value.get("ts_ms"), "ts_ms"),
        "source_system": "postgres.public.device_registry",
        "schema_version": 1,
        "operation": _PG_OPS[op],
        "is_deleted": deleted,
        "version": lsn,
        "customer_id": None if deleted else row.get("customer_id"),
        "site_id": None if deleted else row.get("site_id"),
        "firmware_version": None if deleted else row.get("firmware_version"),
        "state": None if deleted else row.get("state"),
        "updated_at": None if deleted else _iso_to_ms_iso(row.get("updated_at"), "after.updated_at"),
    }


# --------------------------------------------------------------------------- transactional outbox
def _header(headers: dict[str, bytes] | None, name: str) -> str | None:
    if not headers or headers.get(name) is None:
        return None
    raw = headers[name]
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    # Header values written through JsonConverter may be JSON-quoted.
    if text.startswith('"') and text.endswith('"'):
        text = json.loads(text)
    return text


def map_outbox(key: object, value: dict | None, headers: dict[str, bytes] | None, timestamp_ms: int | None) -> dict | None:
    """Outbox Event Router message -> cf.lifecycle.v1.

    connectors/cf-pg-outbox.json places `id`, `type` and `occurred_at` in the
    envelope (`table.fields.additional.placement`), so the value is
    {"payload": {...}, "eventId": ..., "eventType": ..., "occurredAt": ...}.
    The router also writes the event id to the `id` header; it is used as a fallback.
    """
    if value is None:
        return SKIP
    if not isinstance(value, dict):
        raise MappingError("NOT_AN_OBJECT")
    payload = value.get("payload", value)
    if isinstance(payload, str):  # payload not expanded (table.expand.json.payload=false)
        try:
            payload = json.loads(payload)
        except ValueError:
            raise MappingError("BAD_PAYLOAD_JSON", "payload") from None
    if not isinstance(payload, dict):
        raise MappingError("BAD_TYPE", "payload")
    event_id = value.get("eventId") or _header(headers, "id")
    if not event_id:
        raise MappingError("MISSING_FIELD", "eventId")
    event_type = value.get("eventType") or _header(headers, "eventType")
    if not event_type:
        raise MappingError("MISSING_FIELD", "eventType")
    device_id = payload.get("device_id") or (key if isinstance(key, str) else None)
    if not device_id:
        raise MappingError("MISSING_FIELD", "payload.device_id")
    occurred = value.get("occurredAt")
    if occurred is None:
        raise MappingError("MISSING_FIELD", "occurredAt")
    if timestamp_ms is None:
        raise MappingError("MISSING_FIELD", "kafka.timestamp")
    return {
        "event_id": str(event_id),
        "device_id": device_id,
        "event_time": _iso_to_ms_iso(occurred, "occurredAt"),
        "emitted_at": format_timestamp_ms(timestamp_ms),
        "source_system": "registry-app.outbox",
        "schema_version": 1,
        "event_type": event_type,
        "aggregate_type": "device",
        "firmware_version": payload.get("firmware_version"),
        "site_id": payload.get("site_id"),
        "customer_id": payload.get("customer_id"),
        "reason": payload.get("reason"),
    }


# --------------------------------------------------------------------------- MongoDB device state
def _ext_json_value(value: object) -> object:
    """Unwrap MongoDB Extended JSON scalars ({"$numberInt": "30"} etc.)."""
    if isinstance(value, dict) and len(value) == 1:
        (tag, inner), = value.items()
        if tag in ("$numberInt", "$numberLong"):
            return int(inner)
        if tag == "$numberDouble":
            return float(inner)
        if tag == "$oid":
            return inner
    return value


def _mongo_key_id(key: object) -> str | None:
    """Debezium MongoDB key payload: {"id": "\"sensor-01\""} for string _id (JSON-encoded)."""
    if not isinstance(key, dict) or key.get("id") is None:
        return None
    raw = key["id"]
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return str(raw)
    decoded = _ext_json_value(decoded)
    return decoded if isinstance(decoded, str) else json.dumps(decoded, sort_keys=True)


_STATE_FIELDS = ("config_profile", "sampling_interval_s", "reporting_mode", "desired_firmware")


def map_mongo_state(key: object, value: dict | None) -> dict | None:
    """Debezium MongoDB change event -> cf.device-state.v1 (capture.mode=change_streams_update_full)."""
    if value is None:
        return SKIP
    op = value.get("op")
    if op not in _MONGO_OPS:
        raise MappingError("BAD_OPERATION", "op")
    source = _require(value, "source")
    ts_ms = source.get("ts_ms")
    if type(ts_ms) is not int:
        raise MappingError("MISSING_FIELD", "source.ts_ms")
    # Snapshot reads carry ord = -1 (Debezium 3.6); (seconds << 32) | -1 would be -1. Use 0 so a snapshot
    # version is valid and sorts before any streamed change (ord >= 1) in the same second.
    ordinal = max(int(source.get("ord") or 0), 0)
    seconds = ts_ms // 1000

    document = None
    if value.get("after"):
        try:
            document = json.loads(value["after"]) if isinstance(value["after"], str) else value["after"]
        except ValueError:
            raise MappingError("BAD_DOCUMENT_JSON", "after") from None
    device_id = _mongo_key_id(key) or (document and _ext_json_value(document.get("_id")))
    if not device_id:
        raise MappingError("MISSING_FIELD", "_id")

    fields = dict.fromkeys(_STATE_FIELDS)
    full_document = False
    if op != "d":
        if isinstance(document, dict):
            full_document = True
            for name in _STATE_FIELDS:
                fields[name] = _ext_json_value(document.get(name))
        elif op == "u":
            # Full-document lookup can return nothing (document deleted before lookup). Keep the patch only.
            update = value.get("updateDescription") or {}
            updated = update.get("updatedFields")
            try:
                patch = json.loads(updated) if isinstance(updated, str) else (updated or {})
            except ValueError:
                raise MappingError("BAD_DOCUMENT_JSON", "updateDescription.updatedFields") from None
            for name in _STATE_FIELDS:
                if name in patch:
                    fields[name] = _ext_json_value(patch[name])
        else:
            raise MappingError("MISSING_FIELD", "after")

    return {
        "event_id": f"mgstate-{seconds}-{ordinal}-{device_id}",
        "device_id": device_id,
        "event_time": format_timestamp_ms(ts_ms),
        "emitted_at": _epoch_ms_to_iso(value.get("ts_ms"), "ts_ms"),
        "source_system": "mongodb.connectedfleet.device_state",
        "schema_version": 1,
        "operation": _MONGO_OPS[op],
        "is_deleted": op == "d",
        "version": (seconds << 32) | int(ordinal),
        **fields,
        "full_document": full_document,
    }


# --------------------------------------------------------------------------- CSV diagnostics
def _csv_optional_int(row: dict, name: str) -> int | None:
    raw = row.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        raise MappingError("BAD_TYPE", f"row.{name}") from None


def _csv_optional_float(row: dict, name: str) -> float | None:
    raw = row.get(name)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        raise MappingError("BAD_TYPE", f"row.{name}") from None


def _csv_optional_str(row: dict, name: str) -> str | None:
    raw = row.get(name)
    return None if raw is None or raw == "" else raw


def map_csv_row(value: dict | None) -> dict:
    """Raw CSV importer message {file_sha256, row_number, imported_at, row{...strings}} -> cf.diagnostics.v1."""
    if not isinstance(value, dict):
        raise MappingError("NOT_AN_OBJECT")
    sha = _require(value, "file_sha256")
    row_number = _require(value, "row_number")
    row = _require(value, "row")
    if type(row_number) is not int:
        raise MappingError("BAD_TYPE", "row_number")
    diagnosed = _iso_to_ms_iso(_require(row, "diagnosed_at"), "row.diagnosed_at")
    return {
        "event_id": f"csv-{str(sha)[:16]}-{row_number}",
        "device_id": _require(row, "device_id"),
        "event_time": diagnosed,
        "emitted_at": _iso_to_ms_iso(_require(value, "imported_at"), "imported_at"),
        "source_system": "csv-import",
        "schema_version": 1,
        "file_sha256": sha,
        "row_number": row_number,
        "error_code": _require(row, "error_code"),
        "severity": _require(row, "severity"),
        "battery_pct": _csv_optional_int(row, "battery_pct"),
        "temperature_c": _csv_optional_float(row, "temperature_c"),
        "firmware_version": _csv_optional_str(row, "firmware_version"),
        "note": _csv_optional_str(row, "note"),
    }


def map_record(topic: str, key: object, value: object, headers: dict | None = None, timestamp_ms: int | None = None):
    """Dispatch by raw topic. Returns (canonical_topic, event dict) or (canonical_topic, SKIP)."""
    canonical_topic = topics.RAW_TO_CANONICAL.get(topic)
    if canonical_topic is None:
        raise MappingError("UNKNOWN_TOPIC", topic)
    if topic == topics.RAW_TELEMETRY:
        return canonical_topic, map_gateway(value)
    if topic == topics.RAW_PG_REGISTRY:
        return canonical_topic, map_pg_registry(key, value)
    if topic == topics.RAW_OUTBOX:
        return canonical_topic, map_outbox(key, value, headers, timestamp_ms)
    if topic == topics.RAW_MONGO_STATE:
        return canonical_topic, map_mongo_state(key, value)
    return canonical_topic, map_csv_row(value)
