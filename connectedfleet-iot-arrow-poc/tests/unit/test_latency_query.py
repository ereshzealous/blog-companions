"""queries.latency_ms reports the maximum and the slow tail, not only p50/p95/p99.

Lab 03 (2026-10-10): the official sink's trailing partial buffer waited for Kafka Connect's 60 s offset
flush; a p99 alone hid how many events waited and for how long.
"""

from connectedfleet.clickhouse import queries


class _FakeCH:
    def __init__(self):
        self.sql = []

    def query_json(self, sql, settings=None):
        self.sql.append(sql)
        return [{"q": [400.0, 900.0, 34000.0], "n": 10000, "max_ms": 35210, "over_5s": 180,
                 "first_insert_ms": 1, "last_insert_ms": 2}]

    def scalar(self, sql):
        return 12


def test_latency_reports_max_and_events_over_5s():
    ch = _FakeCH()
    out = queries.latency_ms(ch, "db", "t")
    assert out["max_ms"] == 35210.0 and out["over_5s"] == 180
    assert "max(" in ch.sql[0] and "countIf(" in ch.sql[0]


def test_server_cost_reads_query_log_by_log_comment():
    class CH:
        def __init__(self):
            self.sql = []

        def command(self, sql, settings=None):
            self.sql.append(sql)
            return ""

        def query_json(self, sql, settings=None):
            self.sql.append(sql)
            return [{"queries": 10, "query_ms": 640, "cpu_us": 1_250_000, "peak_mem": 52_428_800, "read_bytes": 9}]

    ch = CH()
    out = queries.server_cost(ch, "e3-run-1000000-none-direct-r1")
    assert out == {"queries": 10, "query_s": 0.64, "cpu_s": 1.25, "peak_mem_bytes": 52_428_800}
    assert ch.sql[0] == "SYSTEM FLUSH LOGS" and "log_comment = 'e3-run-1000000-none-direct-r1'" in ch.sql[1]


class _SQL:
    def __init__(self):
        self.calls = []

    def query_json(self, sql, settings=None):
        self.calls.append((sql, settings))
        return [{"h": "123", "n": 5}]


def test_content_hash_without_dedupe_has_no_group_by():
    """E3 loads each file once into an emptied table: rows are unique, so the hash needs no GROUP BY (the GROUP BY over
    1M rows x 10 columns hit ClickHouse's memory limit in run lab03-confirm-20261010T194353Z-aa2da9c)."""
    ch = _SQL()
    assert queries.content_hash(ch, "db", "t", ["k", "a", "b"], key="k", dedupe=False) == "123/5"
    sql, _ = ch.calls[0]
    assert "GROUP BY" not in sql and "formatRowNoNewline('TSV', `k`, `a`, `b`)" in sql


def test_content_hash_with_dedupe_may_spill_to_disk():
    ch = _SQL()
    queries.content_hash(ch, "db", "t", ["k", "a"], key="k")
    sql, settings = ch.calls[0]
    assert "GROUP BY `k`" in sql and int(settings["max_bytes_before_external_group_by"]) > 0
