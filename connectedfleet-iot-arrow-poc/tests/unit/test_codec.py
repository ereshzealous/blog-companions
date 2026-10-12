"""Shared decode/validate behaviour: null vs absent, type and range rejects, UTC timestamps."""

import json

import pytest

from connectedfleet.codec import telemetry as codec

BASE = {
    "event_id": "evt-1",
    "device_id": "sensor-01042",
    "event_time": "2026-10-09T08:30:00.000Z",
    "emitted_at": "2026-10-09T08:30:00.100Z",
    "source_system": "iot-gateway",
    "schema_version": 1,
    "temperature_c": 43.6,
    "battery_pct": 17,
    "signal_dbm": -78,
    "firmware_version": "3.2.1",
    "site_id": "factory-12",
}


def _line(**changes) -> bytes:
    record = {**BASE, **changes}
    for key in [k for k, v in changes.items() if v is ...]:
        del record[key]
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


def _run(*lines: bytes):
    decoded, decode_rejects = codec.decode(list(lines))
    rows, validate_rejects, stats = codec.validate(decoded)
    return rows, decode_rejects + validate_rejects, stats


def test_valid_record_becomes_typed_tuple():
    rows, rejects, _ = _run(_line())
    assert rejects == []
    (row,) = rows
    assert row[0] == "evt-1"
    assert row[2] == 1_791_534_600_000  # 2026-10-09T08:30:00Z in epoch milliseconds
    assert row[3] - row[2] == 100
    assert row[6:9] == (43.6, 17, -78)


def test_absent_and_explicit_null_both_become_null_but_are_counted_separately():
    rows, rejects, stats = _run(_line(temperature_c=...), _line(temperature_c=None))
    assert rejects == []
    assert [r[6] for r in rows] == [None, None]
    assert stats.absent == {"temperature_c": 1}
    assert stats.explicit_null == {"temperature_c": 1}


def test_stats_ignore_rejected_records():
    _, rejects, stats = _run(_line(temperature_c=..., battery_pct=101))
    assert [r.reason for r in rejects] == [codec.OUT_OF_RANGE]
    assert stats.absent == {}


@pytest.mark.parametrize(
    ("changes", "reason", "field"),
    [
        ({"temperature_c": "hot"}, codec.BAD_TYPE, "temperature_c"),
        ({"temperature_c": True}, codec.BAD_TYPE, "temperature_c"),
        ({"temperature_c": 151.0}, codec.OUT_OF_RANGE, "temperature_c"),
        ({"battery_pct": 17.5}, codec.BAD_TYPE, "battery_pct"),
        ({"battery_pct": "17"}, codec.BAD_TYPE, "battery_pct"),
        ({"battery_pct": -1}, codec.OUT_OF_RANGE, "battery_pct"),
        ({"battery_pct": 101}, codec.OUT_OF_RANGE, "battery_pct"),
        ({"signal_dbm": 5}, codec.OUT_OF_RANGE, "signal_dbm"),
        ({"device_id": ...}, codec.MISSING_REQUIRED, "device_id"),
        ({"device_id": ""}, codec.BAD_TYPE, "device_id"),
        ({"device_id": "x" * 65}, codec.TOO_LONG, "device_id"),
        ({"event_time": "not-a-time"}, codec.BAD_TIMESTAMP, "event_time"),
        ({"event_time": "2026-10-09T08:30:00"}, codec.BAD_TIMESTAMP, "event_time"),
        ({"event_time": 1791534600000}, codec.BAD_TIMESTAMP, "event_time"),
        ({"schema_version": 2}, codec.UNSUPPORTED_SCHEMA_VERSION, "schema_version"),
        ({"schema_version": ...}, codec.MISSING_REQUIRED, "schema_version"),
    ],
)
def test_invalid_records_are_rejected_whole(changes, reason, field):
    rows, rejects, _ = _run(_line(**changes))
    assert rows == []
    assert [(r.reason, r.field) for r in rejects] == [(reason, field)]


def test_nan_is_rejected_as_out_of_range():
    line = _line().replace(b"43.6", b"NaN")
    _, rejects, _ = _run(line)
    assert [r.reason for r in rejects] == [codec.OUT_OF_RANGE]


def test_malformed_json_and_non_objects():
    _, rejects, _ = _run(b'{"event_id": "evt-1"', b"[1, 2, 3]")
    assert [r.reason for r in rejects] == [codec.MALFORMED_JSON, codec.NOT_AN_OBJECT]


def test_non_utc_offset_is_converted_to_utc():
    rows, _, _ = _run(_line(event_time="2026-10-09T10:30:00.000+02:00"))
    assert rows[0][2] == 1_791_534_600_000


def test_unknown_fields_are_accepted_and_counted():
    rows, rejects, stats = _run(_line(humidity_pct=41))
    assert len(rows) == 1 and rejects == []
    assert stats.unknown_fields == 1


def test_unicode_identifiers_survive():
    rows, _, _ = _run(_line(device_id="センサー-00089", site_id="工場-東京"))
    assert rows[0][1] == "センサー-00089"
    assert rows[0][10] == "工場-東京"


def test_reject_indices_follow_batch_offset():
    decoded, rejects = codec.decode([b"{", _line()], first_index=500)
    assert rejects[0].index == 500
    assert decoded[0][0] == 501
