"""Every custom-lane insert is synchronous (async_insert=0), the same as the official sink.

Observed 2026-10-10 on ClickHouse 26.3 (Lab 03 run lab03-smoke-20261010T165506Z-32b2836): the server
default is async_insert=1 with wait_for_async_insert=1 and an adaptive busy timeout of at least 50 ms.
Lanes B and C inherited it, so every insert waited in the async queue (query_log: ~58 ms per insert),
while the official sink sends async_insert=0 (~3.4 ms per insert). That confounded A vs B/C.
"""

from types import SimpleNamespace

from connectedfleet.bench import csv_backfill
from connectedfleet.clickhouse.http import SYNC_INSERT, ClickHouseHTTP
from connectedfleet.lanes import sink as sink_mod

SETTINGS = SimpleNamespace(clickhouse_http_url="http://ch:8123", clickhouse_user="u", clickhouse_password="p")


def _capture_posts(monkeypatch):
    sent = []

    def fake_post(self, params, body, extra_headers=None):
        sent.append(params)
        return b"{\"count()\": 0}\n"

    monkeypatch.setattr(ClickHouseHTTP, "_post", fake_post)
    return sent


def _capture_native(monkeypatch):
    import clickhouse_connect

    calls = []
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: calls.append(kw) or SimpleNamespace())
    return calls


def test_sync_insert_disables_async_insert():
    assert SYNC_INSERT == {"async_insert": "0"}


def test_arrowstream_insert_is_synchronous(monkeypatch):
    sent = _capture_posts(monkeypatch)
    ClickHouseHTTP("http://ch:8123", "u", "p").insert_arrow_stream("db", "t", ["a"], b"ipc")
    assert sent[0]["async_insert"] == "0"


def test_raw_insert_is_synchronous_and_keeps_caller_settings(monkeypatch):
    sent = _capture_posts(monkeypatch)
    ClickHouseHTTP("http://ch:8123", "u", "p").insert_raw("db", "t", "CSVWithNames", b"a\n1\n",
                                                         settings={"input_format_skip_unknown_fields": "1"})
    assert sent[0]["async_insert"] == "0" and sent[0]["input_format_skip_unknown_fields"] == "1"


def test_row_lane_native_client_is_synchronous(monkeypatch):
    calls = _capture_native(monkeypatch)
    sink_mod.build_sink("row", "bench_row", "none", None, None, SETTINGS)
    assert calls[0]["settings"]["async_insert"] == "0"


def test_csv_backfill_native_client_is_synchronous(monkeypatch, tmp_path):
    calls = _capture_native(monkeypatch)
    _capture_posts(monkeypatch)
    monkeypatch.setattr(csv_backfill, "run_row", lambda native, database, path, sha, batch: {"batches": 0})
    path = tmp_path / "d.csv"
    path.write_text(",".join(csv_backfill.CSV_COLUMNS) + "\n")
    csv_backfill.run("row", path, "db", 10, SETTINGS)
    assert calls[0]["settings"]["async_insert"] == "0"


def test_direct_backfill_streams_the_file_and_tags_its_queries(monkeypatch, tmp_path):
    """Review feedback (2026-10-11): the direct path read the whole file into memory (path.read_bytes()). It now
    streams it, and every backfill query carries the case's log_comment so server CPU/memory can be attributed."""
    calls = []
    monkeypatch.setattr(ClickHouseHTTP, "insert_file", lambda self, params, path, extra_headers=None: calls.append((params, path)))
    path = tmp_path / "d.csv"
    path.write_text(",".join(csv_backfill.CSV_COLUMNS) + "\n")
    monkeypatch.setattr(type(path), "read_bytes", lambda self: (_ for _ in ()).throw(AssertionError("read_bytes used")))
    detail = csv_backfill.run_direct(ClickHouseHTTP("http://ch:8123", "u", "p"), "db", path, "sha", {"log_comment": "case-7"})
    assert calls and calls[0][1] == path and calls[0][0]["log_comment"] == "case-7"
    assert "FROM input(" in calls[0][0]["query"] and detail["body_bytes"] == path.stat().st_size
