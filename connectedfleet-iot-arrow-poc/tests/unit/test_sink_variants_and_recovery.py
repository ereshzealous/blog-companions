"""Lane A connector variants, DLQ recovery and the streaming CSV upload (review feedback, 2026-10-11)."""

import json
from types import SimpleNamespace

import pytest

from connectedfleet.bench import sinkcfg
from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.contracts.canonical import format_timestamp_ms
from connectedfleet import recovery

BASE = "date_time_input_format=best_effort,precise_float_parsing=1"


def _ov(variant):
    return sinkcfg.overrides(variant, "t", "telemetry_fact", "bench_sink", 1000, 250, BASE)


def test_original_sink_buffers_in_the_connector():
    ov = _ov("sink")
    assert ov["bufferCount"] == "1000" and ov["bufferFlushTime"] == "250"
    assert ov["consumer.override.max.poll.records"] == "1000"
    assert "clickhouseSettings" not in ov


def test_tuned_sink_batches_with_kafka_fetch_settings_not_a_connector_buffer():
    """bufferCount=0 hands every poll to ClickHouse at once, so a burst's tail is never held until Connect's
    60 s offset flush; fetch.max.wait.ms bounds how long a poll waits to fill (= the flush timeout)."""
    ov = _ov("sink_tuned")
    assert ov["bufferCount"] == "0" and "bufferFlushTime" not in ov
    assert ov["consumer.override.max.poll.records"] == "1000"
    assert ov["consumer.override.fetch.max.wait.ms"] == "250"
    assert int(ov["consumer.override.fetch.min.bytes"]) > 1


def test_skip_sink_appends_allow_errors_to_existing_clickhouse_settings():
    ov = _ov("sink_skip")
    assert ov["clickhouseSettings"].startswith(BASE + ",")
    assert "input_format_allow_errors_num=" in ov["clickhouseSettings"]


def test_sink_lane_helpers():
    assert all(sinkcfg.is_sink(x) for x in ("sink", "sink_tuned", "sink_skip"))
    assert not sinkcfg.is_sink("row") and sinkcfg.db_for("sink_tuned") == "bench_sink"
    assert sinkcfg.db_for("arrow") == "bench_arrow"
    with pytest.raises(ValueError):
        _ov("sink_magic")


def _event(event_id, battery=17):
    return {"event_id": event_id, "device_id": "sensor-00001", "event_time": format_timestamp_ms(1_791_534_600_000),
            "emitted_at": format_timestamp_ms(1_791_534_600_000), "source_system": "iot-gateway", "schema_version": 1,
            "battery_pct": battery}


def test_recovery_replays_valid_records_and_quarantines_the_rest():
    values = [json.dumps(_event("ok-1")).encode(), json.dumps(_event("bad-1", "seventeen")).encode(), b"{not json",
              json.dumps(_event("ok-2")).encode()]
    keep, quarantine = recovery.classify([(b"k", v) for v in values])
    assert [json.loads(v)["event_id"] for _, v in keep] == ["ok-1", "ok-2"]
    assert [q["reason"].split(":")[0] for q in quarantine] == ["BAD_TYPE", "NOT_JSON"]
    assert quarantine[0]["event_id"] == "bad-1" and "seventeen" not in json.dumps(quarantine)


def test_recovery_replay_reads_from_offset_and_produces_valid_records():
    sent = []
    dlq = [(b"k", json.dumps(_event("ok-1")).encode()), (b"k", json.dumps(_event("bad", "x")).encode())]
    out = recovery.replay(dlq, SimpleNamespace(produce=lambda topic, key, value: sent.append((topic, key, value)),
                                               flush=lambda: None), "recovery-topic")
    assert out == {"read": 2, "replayed": 1, "quarantined": 1, "quarantine": out["quarantine"]}
    assert sent[0][0] == "recovery-topic" and json.loads(sent[0][2])["event_id"] == "ok-1"


def test_insert_file_streams_the_body_instead_of_reading_it(tmp_path, monkeypatch):
    path = tmp_path / "d.csv"
    path.write_bytes(b"a,b\n1,2\n" * 1000)
    seen = {}

    class FakeConn:
        def request(self, method, url, body=None, headers=None):
            seen["body_is_file"] = hasattr(body, "read") and not isinstance(body, (bytes, bytearray))
            seen["length"] = headers["Content-Length"]
            seen["url"] = url

        def getresponse(self):
            return SimpleNamespace(status=200, read=lambda: b"")

    http = ClickHouseHTTP("http://ch:8123", "u", "p")
    monkeypatch.setattr(http, "_connection", lambda: FakeConn())
    http.insert_file({"query": "INSERT INTO t FORMAT CSVWithNames", "log_comment": "case-1"}, path)
    assert seen["body_is_file"] and seen["length"] == str(path.stat().st_size)
    assert "async_insert=0" in seen["url"] and "log_comment=case-1" in seen["url"]
