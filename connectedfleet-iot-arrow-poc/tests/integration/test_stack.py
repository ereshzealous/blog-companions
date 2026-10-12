"""Live-stack checks (`pytest tests/integration -m integration`; needs `make up init`).

These verify the integration points the labs depend on, against the real
containers: connector plugins and states, topics, the lane schemas, ArrowStream
and Native inserts (positive and negative), PostgreSQL logical decoding and the
MongoDB replica set. They insert into a scratch database and drop it afterwards.
"""

import json

import pyarrow as pa
import pytest

from connectedfleet import connect, kafka_admin, topics
from connectedfleet.clickhouse import queries
from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.config import POC_ROOT, Settings, load_dotenv
from connectedfleet.contracts import canonical
from connectedfleet.lanes.sink import LaneSink, _ipc

pytestmark = pytest.mark.integration
load_dotenv(POC_ROOT / ".env")
SETTINGS = Settings.from_env()
SCRATCH = "cf_it_scratch"


@pytest.fixture(scope="module")
def ch():
    client = ClickHouseHTTP(SETTINGS.clickhouse_http_url, SETTINGS.clickhouse_user, SETTINGS.clickhouse_password)
    if not client.ping():
        pytest.fail("ClickHouse not reachable: run `make up init`")
    client.command(f"CREATE DATABASE IF NOT EXISTS {SCRATCH}")
    client.command(f"CREATE TABLE IF NOT EXISTS {SCRATCH}.telemetry_fact AS bench_arrow.telemetry_fact")
    client.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    yield client
    client.command(f"DROP DATABASE IF EXISTS {SCRATCH}")


def test_connect_plugins_and_connectors_running():
    client = connect.ConnectClient(SETTINGS.connect_url)
    assert client.missing_plugins() == []
    for name in (*connect.SOURCE_CONNECTORS, connect.SINK_CONNECTOR):
        client.wait_running(name, timeout_s=60)


def test_all_topics_exist():
    from confluent_kafka.admin import AdminClient

    existing = set(AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap}).list_topics(timeout=15).topics)
    assert set(topics.PARTITIONS) <= existing


@pytest.mark.parametrize("database", list(topics.LANE_DATABASES.values()))
def test_lane_tables_match_contracts(ch, database):
    for contract in canonical.CONTRACTS.values():
        observed = queries.describe(ch, database, contract.table)
        assert observed[:-1] == contract.clickhouse_columns and observed[-1][0] == "ingested_at"


def _rows(n=50):
    from connectedfleet.bench.workload import WorkloadSpec, events
    from connectedfleet.contracts.canonical import format_timestamp_ms

    return [json.dumps({**e, "emitted_at": format_timestamp_ms(1_791_534_600_000)}).encode()
            for e in events(WorkloadSpec(n, 5, 0.3))]


def test_arrowstream_insert_roundtrip(ch):
    sink = LaneSink("arrow", "arrow", "arrowstream", SCRATCH, http=ch, native_client=object())
    prepared = sink.prepare(topics.TELEMETRY, _rows())
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    sink.insert(prepared)
    assert queries.counts(ch, SCRATCH, "telemetry_fact")["logical"] == 50
    nulls = queries.null_counts(ch, SCRATCH, "telemetry_fact", ["temperature_c", "battery_pct"])
    table = pa.ipc.open_stream(prepared.payload).read_all()
    assert nulls["temperature_c"] == table.column("temperature_c").null_count


@pytest.mark.parametrize(
    ("values", "stored"),
    [
        (pa.array(["not-a-number"]), None),  # unparsable string -> NULL
        (pa.array(["17"]), 17),  # numeric string -> parsed
        (pa.array([300], pa.int64()), 44),  # out of UInt8 range -> wraps (300 - 256)
        (pa.array([17.5]), 17),  # float -> truncated
    ],
)
def test_arrowstream_type_mismatch_is_coerced_not_rejected(ch, values, stored):
    """Observed on ClickHouse 26.3 (2026-10-10): ArrowStream input is CAST to the column type without error.

    The first-run expectation ("a type mismatch returns an error") was wrong. ClickHouse will not
    protect the table, so lane C must send exactly the contract's Arrow types; validation and range
    checks run before the RecordBatch is built (connectedfleet.codec), never after.
    """
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    bad = pa.RecordBatch.from_arrays([values], names=["battery_pct"])
    ch.insert_arrow_stream(SCRATCH, "telemetry_fact", ["battery_pct"], _ipc(bad))
    result = ch.command(f"SELECT battery_pct FROM {SCRATCH}.telemetry_fact FORMAT JSONEachRow").strip()
    assert json.loads(result)["battery_pct"] == stored


def test_native_insert_matches_arrowstream_content(ch):
    import urllib.parse

    import clickhouse_connect

    url = urllib.parse.urlsplit(SETTINGS.clickhouse_http_url)
    native = clickhouse_connect.get_client(host=url.hostname, port=url.port, username=SETTINGS.clickhouse_user,
                                           password=SETTINGS.clickhouse_password, database=SCRATCH, interface="http",
                                           compress=False)
    values = _rows()
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    LaneSink("row", "row", "native", SCRATCH, native_client=native, http=ch).insert(
        LaneSink("row", "row", "native", SCRATCH, native_client=native, http=ch).prepare(topics.TELEMETRY, values))
    native_hash = queries.content_hash(ch, SCRATCH, "telemetry_fact", canonical.TELEMETRY.names)
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    arrow_sink = LaneSink("arrow", "arrow", "arrowstream", SCRATCH, http=ch, native_client=native)
    arrow_sink.insert(arrow_sink.prepare(topics.TELEMETRY, values))
    assert queries.content_hash(ch, SCRATCH, "telemetry_fact", canonical.TELEMETRY.names) == native_hash


def test_lane_inserts_are_synchronous_on_the_server(ch):
    """Both custom insert paths must bypass the async-insert queue (server default on 26.3 is async_insert=1)."""
    from connectedfleet.lanes.sink import build_sink

    started = ch.scalar("SELECT toString(now64(6))")  # microseconds: a run a second earlier must not match
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    for lane in ("row", "arrow"):
        sink = build_sink(lane, SCRATCH, "none", None, None, SETTINGS)
        sink.insert(sink.prepare(topics.TELEMETRY, _rows(20)))
    ch.command("SYSTEM FLUSH LOGS")
    rows = ch.query_json(
        "SELECT extract(query, 'FORMAT (\\\\w+)') AS fmt, ProfileEvents['AsyncInsertQuery'] AS async_q "
        f"FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(databases, '{SCRATCH}') "
        f"AND event_time_microseconds > '{started}'"
    )
    assert {r["fmt"] for r in rows} >= {"Native", "ArrowStream"}
    assert all(int(r["async_q"]) == 0 for r in rows)


def test_postgres_logical_decoding_and_publications():
    import psycopg

    with psycopg.connect(SETTINGS.postgres_dsn) as conn:
        assert conn.execute("SHOW wal_level").fetchone()[0] == "logical"
        pubs = {r[0] for r in conn.execute("SELECT pubname FROM pg_publication").fetchall()}
        assert {"cf_registry_pub", "cf_outbox_pub"} <= pubs
        slots = {r[0] for r in conn.execute("SELECT slot_name FROM pg_replication_slots").fetchall()}
        assert {"cf_registry_slot", "cf_outbox_slot"} <= slots


def test_mongodb_is_a_replica_set_primary():
    from pymongo import MongoClient

    hello = MongoClient(SETTINGS.mongo_uri, serverSelectionTimeoutMS=5000).admin.command("hello")
    assert hello.get("setName") == "rs0" and hello.get("isWritablePrimary") is True


def test_dlq_topics_are_readable():
    for topic in topics.DLQ_TOPICS:
        assert kafka_admin.message_count(SETTINGS.kafka_bootstrap, topic) >= 0


def test_streamed_file_insert_and_server_cost_by_log_comment(ch, tmp_path):
    """The direct CSV path streams its file and tags its queries; query_log must attribute server cost to the tag."""
    import secrets

    tag = f"it-{secrets.token_hex(4)}"
    path = tmp_path / "rows.jsonl"
    path.write_bytes(b"\n".join(_rows(200)))
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    ch.insert_file({"query": f"INSERT INTO {SCRATCH}.telemetry_fact FORMAT JSONEachRow",
                    "date_time_input_format": "best_effort", "log_comment": tag}, path)
    assert queries.counts(ch, SCRATCH, "telemetry_fact")["logical"] == 200
    cost = queries.server_cost(ch, tag)
    assert cost["queries"] == 1 and cost["cpu_s"] > 0 and cost["peak_mem_bytes"] > 0


def test_content_hash_is_the_same_with_and_without_dedupe_for_unique_rows(ch):
    """E3 skips the de-duplicating GROUP BY when rows are unique; both forms must give the same hash."""
    ch.command(f"TRUNCATE TABLE {SCRATCH}.telemetry_fact")
    sink = LaneSink("arrow", "arrow", "arrowstream", SCRATCH, http=ch, native_client=object())
    sink.insert(sink.prepare(topics.TELEMETRY, _rows(300)))
    cols = canonical.TELEMETRY.names
    assert queries.content_hash(ch, SCRATCH, "telemetry_fact", cols, dedupe=False) == \
        queries.content_hash(ch, SCRATCH, "telemetry_fact", cols)
