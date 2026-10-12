"""Raw envelopes -> canonical events, including deletes, tombstones, patch-only updates and poison input.

Envelopes follow the shapes in the Debezium 3.6 documentation (JsonConverter, schemas disabled).
"""

import json

import pytest

from connectedfleet import topics
from connectedfleet.contracts import canonical
from connectedfleet.kafka_io import Record
from connectedfleet.normalize import mappers
from connectedfleet.normalize.service import dlq_record, process, run_batch, Stats

T0 = 1_791_534_600_000  # 2026-10-09T08:30:00Z


def _valid(topic, event):
    return canonical.validate(canonical.CONTRACTS[topic], event)


# ----------------------------------------------------------------- gateway
def test_gateway_converts_units_and_times():
    event = mappers.map_gateway({"msg_id": "gw-a-1", "dev": "sensor-1", "ts": T0, "sent": T0 + 100,
                                 "m": {"t_dC": 436, "bat": 17, "rssi": -78}, "fw": "3.2.1", "site": "factory-12"})
    assert event["temperature_c"] == 43.6
    assert event["event_time"] == "2026-10-09T08:30:00.000Z"
    assert event["emitted_at"] == "2026-10-09T08:30:00.100Z"
    _valid(topics.TELEMETRY, event)


def test_gateway_keeps_absent_and_null_distinct():
    event = mappers.map_gateway({"msg_id": "gw-a-2", "dev": "d", "ts": T0, "sent": T0, "m": {"t_dC": None}})
    assert event["temperature_c"] is None
    assert "battery_pct" not in event and "signal_dbm" not in event


@pytest.mark.parametrize("bad", [{"ts": "yesterday"}, {"msg_id": None}, {"m": [1, 2]}, {"m": {"t_dC": "hot"}}])
def test_gateway_poison_raises_mapping_error(bad):
    raw = {"msg_id": "gw-a-3", "dev": "d", "ts": T0, "sent": T0, "m": {}}
    with pytest.raises(mappers.MappingError):
        mappers.map_gateway({**raw, **bad})


# ----------------------------------------------------------------- PostgreSQL CDC
def _pg(op, before=None, after=None, lsn=24023128):
    return {"before": before, "after": after, "op": op, "ts_ms": T0 + 120,
            "source": {"lsn": lsn, "ts_ms": T0, "snapshot": "true" if op == "r" else "false", "db": "connectedfleet"}}


ROW = {"device_id": "dev-1", "customer_id": "acme", "site_id": "factory-03", "firmware_version": "3.2.1",
       "state": "active", "updated_at": "2026-10-09T08:30:00.123456Z"}


@pytest.mark.parametrize(("op", "operation"), [("r", "snapshot"), ("c", "insert"), ("u", "update")])
def test_pg_registry_operations(op, operation):
    event = mappers.map_pg_registry({"device_id": "dev-1"}, _pg(op, after=ROW))
    assert event["operation"] == operation and event["is_deleted"] is False
    assert event["event_id"] == "pgreg-24023128-dev-1" and event["version"] == 24023128
    assert event["updated_at"] == "2026-10-09T08:30:00.123Z"  # microseconds truncated to the contract's milliseconds
    _valid(topics.DEVICE_REGISTRY, event)


def test_pg_delete_with_key_only_before_image():
    event = mappers.map_pg_registry({"device_id": "dev-2"}, _pg("d", before={"device_id": "dev-2"}, lsn=30000000))
    assert event["is_deleted"] is True and event["operation"] == "delete"
    assert event["customer_id"] is None and event["state"] is None
    _valid(topics.DEVICE_REGISTRY, event)


def test_pg_tombstone_is_skipped():
    assert mappers.map_pg_registry({"device_id": "dev-2"}, None) is mappers.SKIP


def test_pg_missing_lsn_is_rejected():
    envelope = _pg("u", after=ROW)
    del envelope["source"]["lsn"]
    with pytest.raises(mappers.MappingError):
        mappers.map_pg_registry({"device_id": "dev-1"}, envelope)


# ----------------------------------------------------------------- outbox
def test_outbox_envelope_placement():
    value = {"payload": {"device_id": "dev-1", "firmware_version": "3.2.1"}, "eventId": "5b0f6f0e-0000-0000-0000-000000000001",
             "eventType": "FirmwareUpdated", "occurredAt": "2026-10-09T08:31:12.000000Z"}
    event = mappers.map_outbox("dev-1", value, {}, T0)
    assert event["event_id"] == value["eventId"] and event["event_type"] == "FirmwareUpdated"
    assert event["event_time"] == "2026-10-09T08:31:12.000Z"
    _valid(topics.LIFECYCLE, event)


def test_outbox_header_fallback_and_string_payload():
    value = {"payload": json.dumps({"device_id": "dev-9", "reason": "decommissioned"}), "eventType": "DeviceDeactivated",
             "occurredAt": "2026-10-09T08:31:12Z"}
    event = mappers.map_outbox("dev-9", value, {"id": b'"5b0f6f0e-0000-0000-0000-000000000002"'}, T0)
    assert event["event_id"] == "5b0f6f0e-0000-0000-0000-000000000002"
    assert event["reason"] == "decommissioned"


def test_outbox_without_event_type_is_rejected():
    with pytest.raises(mappers.MappingError):
        mappers.map_outbox("dev-1", {"payload": {"device_id": "dev-1"}, "eventId": "x", "occurredAt": "2026-10-09T08:31:12Z"}, {}, T0)


# ----------------------------------------------------------------- MongoDB
def _mongo(op, after=None, update=None, ordinal=3):
    return {"after": after, "updateDescription": update, "op": op, "ts_ms": T0 + 210,
            "source": {"ts_ms": T0, "ord": ordinal, "db": "connectedfleet", "collection": "device_state"}}


KEY = {"id": '"dev-1"'}
DOC = json.dumps({"_id": "dev-1", "config_profile": "cold-chain", "sampling_interval_s": {"$numberInt": "30"},
                  "reporting_mode": "normal", "desired_firmware": "3.2.1"})


def test_mongo_insert_with_extended_json():
    event = mappers.map_mongo_state(KEY, _mongo("c", after=DOC))
    assert event["device_id"] == "dev-1" and event["sampling_interval_s"] == 30
    assert event["full_document"] is True and event["operation"] == "insert"
    assert event["version"] == ((T0 // 1000) << 32) | 3
    assert event["event_id"] == f"mgstate-{T0 // 1000}-3-dev-1"
    _valid(topics.DEVICE_STATE, event)


def test_mongo_snapshot_read_with_negative_ord_gets_valid_version():
    """Debezium 3.6 emits source.ord = -1 for snapshot reads (observed in Lab 02, 2026-10-10).

    (seconds << 32) | -1 == -1, which violated `version >= 0` and dead-lettered every snapshot document.
    """
    event = mappers.map_mongo_state(KEY, _mongo("r", after=DOC, ordinal=-1))
    assert event["operation"] == "snapshot"
    assert event["version"] == (T0 // 1000) << 32
    assert event["event_id"] == f"mgstate-{T0 // 1000}-0-dev-1"
    streamed = mappers.map_mongo_state(KEY, _mongo("u", after=DOC, ordinal=1))
    assert event["version"] < streamed["version"]  # a snapshot sorts before changes in the same second
    _valid(topics.DEVICE_STATE, event)


def test_mongo_patch_only_update_keeps_unknown_fields_null():
    update = {"updatedFields": json.dumps({"sampling_interval_s": 60}), "removedFields": None, "truncatedArrays": None}
    event = mappers.map_mongo_state(KEY, _mongo("u", after=None, update=update))
    assert event["full_document"] is False
    assert event["sampling_interval_s"] == 60 and event["config_profile"] is None
    _valid(topics.DEVICE_STATE, event)


def test_mongo_delete_and_tombstone():
    event = mappers.map_mongo_state(KEY, _mongo("d"))
    assert event["is_deleted"] is True and event["full_document"] is False
    _valid(topics.DEVICE_STATE, event)
    assert mappers.map_mongo_state(KEY, None) is mappers.SKIP


# ----------------------------------------------------------------- CSV
CSV_RAW = {"file_sha256": "9f2c4e1a7b3d5f60" + "0" * 48, "row_number": 7, "imported_at": "2026-10-09T09:00:00.000Z",
           "row": {"device_id": "sensor-00026", "diagnosed_at": "2026-09-08T01:15:59Z", "error_code": "E-RADIO-004",
                   "severity": "info", "battery_pct": "", "temperature_c": "-4.5", "firmware_version": "", "note": "sensor recalibrated"}}


def test_csv_row_maps_empty_strings_to_null():
    event = mappers.map_csv_row(CSV_RAW)
    assert event["event_id"] == "csv-9f2c4e1a7b3d5f60-7"
    assert event["battery_pct"] is None and event["temperature_c"] == -4.5 and event["firmware_version"] is None
    _valid(topics.DIAGNOSTICS, event)


def test_csv_bad_number_is_rejected():
    raw = json.loads(json.dumps(CSV_RAW))
    raw["row"]["battery_pct"] = "seventeen"
    with pytest.raises(mappers.MappingError):
        mappers.map_csv_row(raw)


# ----------------------------------------------------------------- service-level behaviour
def _record(topic, value, key=b"dev-1", offset=0, headers=None):
    return Record(topic, 0, offset, key, None if value is None else json.dumps(value).encode(), headers or {}, T0)


def test_unknown_topic_is_a_mapping_error():
    with pytest.raises(mappers.MappingError):
        mappers.map_record("nope", None, {})


def test_process_drops_absent_telemetry_keys_and_keys_by_device():
    rec = _record(topics.RAW_TELEMETRY, {"msg_id": "gw-a-9", "dev": "sensor-7", "ts": T0, "sent": T0, "m": {"bat": 5}})
    topic, key, payload = process(rec)
    body = json.loads(payload)
    assert topic == topics.TELEMETRY and key == b"sensor-7"
    assert "temperature_c" not in body and body["battery_pct"] == 5


class _FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value, headers))


def test_batch_routes_poison_to_dlq_without_payload_values_and_commits_past_it():
    good = _record(topics.RAW_TELEMETRY, {"msg_id": "gw-a-10", "dev": "sensor-7", "ts": T0, "sent": T0, "m": {}}, offset=4)
    poison = _record(topics.RAW_TELEMETRY, {"msg_id": "gw-a-11", "dev": "secret-device", "ts": "yesterday", "sent": T0}, offset=5)
    tomb = _record(topics.RAW_PG_REGISTRY, None, offset=9)
    producer, stats = _FakeProducer(), Stats()
    offsets = run_batch([good, poison, tomb], producer, stats)
    dlq = [s for s in producer.sent if s[0] == topics.DLQ_NORMALIZER]
    assert len(dlq) == 1 and stats.skipped == 1 and stats.produced[topics.TELEMETRY] == 1
    assert b"secret-device" not in dlq[0][2] and b"yesterday" not in dlq[0][2]
    assert (topics.RAW_TELEMETRY, 0, 6) in offsets and (topics.RAW_PG_REGISTRY, 0, 10) in offsets


def test_dlq_record_headers_carry_source_position():
    rec = _record(topics.RAW_TELEMETRY, {}, offset=42)
    _key, body, headers = dlq_record(rec, mappers.MappingError("MISSING_FIELD", "msg_id"))
    assert json.loads(body)["source_offset"] == 42
    assert ("cf.source.offset", b"42") in headers
