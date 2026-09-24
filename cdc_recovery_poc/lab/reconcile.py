#!/usr/bin/env python3
"""Prove convergence: compare PostgreSQL current state with ClickHouse current state, key by key.

Usage: reconcile.py wait-quiet [stable_s] [timeout_s]   wait until Kafka is quiet and the sink has no lag
       reconcile.py compare <label>                     write results/<run-id>/reconcile-<label>.json

Rows are hashed identically on both sides (md5 of '|'-joined text, first 48 bits), bucketed by
device_id % 4096, and mismatched buckets are diffed row by row. Also reports physical versus logical
duplicates, so replay is measured rather than assumed away.

Every mismatched sample key gets its evidence saved: the source row, every physical ClickHouse version and
the key's full change history. The version-ordering check replays change_history twice per key, once ordered
by source position (LSN, or the stream position for incremental snapshot reads) and once by Kafka offset,
and counts the keys whose last event, and final device_registry state, would differ.
"""
import json
import sys
import time

import clickhouse_connect
import psycopg
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common import CONTRACT, KAFKA_BOOTSTRAP, SINK_GROUP, TOPICS, append_jsonl, ch_kwargs, contract_version, control_value, now_ms, pg_dsn, run_dir

BUCKETS = 4096


def columns(table):
    return [c for c, _ in CONTRACT[table][contract_version()]]


def pg_hash(cols):
    return "('x' || substr(md5(concat_ws('|', " + ", ".join(f"coalesce({c}::text, '')" for c in cols) + ")), 1, 12))::bit(48)::bigint"


def ch_hash(cols):
    return "reinterpretAsUInt64(reverse(unhex(substring(hex(MD5(concatWithSeparator('|', " + \
        ", ".join(f"ifNull(toString({c}), '')" for c in cols) + "))), 1, 12))))"


def buckets(pg, ch, table, cols):
    src = {b: (n, int(s)) for b, n, s in pg.execute(
        f"SELECT device_id % {BUCKETS}, count(*), sum(h) FROM (SELECT device_id, {pg_hash(cols)} AS h FROM ops.{table}) x GROUP BY 1")}
    dst = {b: (n, int(s)) for b, n, s in ch.query(
        f"SELECT device_id % {BUCKETS}, count(), sum(h) FROM (SELECT device_id, {ch_hash(cols)} AS h "
        f"FROM cdc.{table}_current FINAL WHERE is_deleted = 0) GROUP BY 1").result_rows}
    return [b for b in range(BUCKETS) if src.get(b, (0, 0)) != dst.get(b, (0, 0))]


def diff_rows(pg, ch, table, cols, bad):
    missing, ghost, content = [], [], []
    for i in range(0, len(bad), 256):
        chunk = bad[i:i + 256]
        in_list = ",".join(map(str, chunk))
        src = dict(pg.execute(f"SELECT device_id, {pg_hash(cols)} FROM ops.{table} WHERE device_id % {BUCKETS} IN ({in_list})").fetchall())
        dst = dict(ch.query(f"SELECT device_id, {ch_hash(cols)} FROM cdc.{table}_current FINAL "
                            f"WHERE is_deleted = 0 AND device_id % {BUCKETS} IN ({in_list})").result_rows)
        missing += [k for k in src if k not in dst]
        ghost += [k for k in dst if k not in src]
        content += [k for k in src if k in dst and src[k] != dst[k]]
    return missing, ghost, content


EVIDENCE_KEYS = 3
SPILL = {"max_bytes_before_external_group_by": 200_000_000, "max_memory_usage": 600_000_000, "max_threads": 2}


def evidence(pg, ch, table, device_id):
    source = pg.execute(f"SELECT to_jsonb(t)::text FROM ops.{table} t WHERE device_id = {device_id}").fetchone()
    versions = ch.query(f"SELECT version, is_deleted, sink_id, toString(applied_at) FROM cdc.{table}_current "
                        f"WHERE device_id = {device_id} ORDER BY version").result_rows
    history = ch.query(f"SELECT op, snapshot, lsn, kafka_partition, kafka_offset, source_ts_ms, sink_id, toString(applied_at), "
                       f"substring(after_json, 1, 400) FROM cdc.change_history WHERE source_table = '{table}' AND device_id = {device_id} "
                       f"ORDER BY kafka_offset, applied_at").result_rows
    return {
        "source_row": json.loads(source[0]) if source else None,
        "clickhouse_versions": [dict(zip(("version", "is_deleted", "sink_id", "applied_at"), r)) for r in versions],
        "change_history": [dict(zip(("op", "snapshot", "position", "partition", "offset", "source_ts_ms", "sink_id", "applied_at", "after"), r))
                           for r in history],
    }


def version_ordering(ch):
    """Would LSN ordering and Kafka-offset ordering disagree about any key?

    One table at a time, single-threaded and spilling early: on a 1.4 GB ClickHouse this check used to
    push the server past its own ceiling and take the whole run's reconciliation down with it. A
    diagnostic is allowed to fail; the reconciliation it sits next to is not.
    """
    settings = {"max_bytes_before_external_group_by": 100_000_000, "max_memory_usage": 400_000_000,
                "max_threads": 1}
    out = {}
    tables = [r[0] for r in ch.query("SELECT DISTINCT source_table FROM cdc.change_history").result_rows]
    for table in sorted(tables):
        try:
            rows = ch.query("""
                SELECT count(),
                       countIf(last_by_position != last_by_offset),
                       countIf(state_by_position != state_by_offset),
                       groupArrayIf(5)(device_id, state_by_position != state_by_offset)
                FROM (
                    SELECT device_id,
                           argMax(cityHash64(event_id), (lsn, kafka_offset)) AS last_by_position,
                           argMax(cityHash64(event_id), kafka_offset) AS last_by_offset,
                           argMax(cityHash64(op = 'd', after_json), (lsn, kafka_offset)) AS state_by_position,
                           argMax(cityHash64(op = 'd', after_json), kafka_offset) AS state_by_offset
                    FROM cdc.change_history WHERE source_table = {table:String}
                    GROUP BY device_id)""",
                parameters={"table": table}, settings=settings).result_rows
            keys, events, states, sample = rows[0]
            out[table] = {"keys": keys, "last_event_differs": events, "final_state_differs": states,
                          "sample_keys": list(sample)}
        except Exception as exc:  # noqa: BLE001 - any failure here is reported, never fatal
            out[table] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    return out


def compare(label):
    pg = psycopg.connect(pg_dsn("admin"), autocommit=True)
    ch = clickhouse_connect.get_client(**ch_kwargs("admin"))
    report = {"label": label, "ts_ms": now_ms(), "contract_version": contract_version(), "tables": {}}
    for table in CONTRACT:
        cols = columns(table)
        started = time.monotonic()
        bad = buckets(pg, ch, table, cols)
        missing, ghost, content = diff_rows(pg, ch, table, cols, bad) if bad else ([], [], [])
        physical, physical_keys = ch.query(f"SELECT count(), uniqExact(device_id) FROM cdc.{table}_current").result_rows[0]
        logical, logical_keys = ch.query(f"SELECT count(), uniqExact(device_id) FROM cdc.{table}_current FINAL WHERE is_deleted = 0").result_rows[0]
        report["tables"][table] = {
            "columns_compared": cols,
            "source_rows": pg.execute(f"SELECT count(*) FROM ops.{table}").fetchone()[0],
            "clickhouse_logical_rows": logical,
            "clickhouse_logical_duplicate_keys": logical - logical_keys,
            "clickhouse_physical_rows": physical,
            "clickhouse_physical_extra_versions": physical - physical_keys,
            "mismatched_buckets": len(bad),
            "missing_in_clickhouse": len(missing),
            "ghost_rows_in_clickhouse": len(ghost),
            "content_mismatches": len(content),
            "samples": {"missing": missing[:10], "ghost": ghost[:10], "content": content[:10]},
            "evidence": {str(k): evidence(pg, ch, table, k) for k in missing[:EVIDENCE_KEYS] + ghost[:EVIDENCE_KEYS] + content[:EVIDENCE_KEYS]},
            "converged": not bad,
            "seconds": round(time.monotonic() - started, 1),
        }
    deleted = control_value("deleted-batch.json")
    if deleted:
        ids = json.loads(deleted)
        in_list = ",".join(map(str, ids))
        report["explicit_delete_batch"] = {
            "keys": len(ids),
            "still_in_source": pg.execute(f"SELECT count(*) FROM ops.device_registry WHERE device_id IN ({in_list})").fetchone()[0],
            "still_visible_in_clickhouse": ch.query(f"SELECT count() FROM cdc.device_registry_current FINAL "
                                                    f"WHERE is_deleted = 0 AND device_id IN ({in_list})").result_rows[0][0],
        }
    # Exact distinct counts via GROUP BY on 128-bit hashes, spilling to disk, so a small ClickHouse can count tens of millions.
    spill = SPILL
    deliveries, deletes, reads = ch.query("SELECT count(), countIf(op = 'd'), countIf(op = 'r') FROM cdc.change_history").result_rows[0]
    distinct_events = ch.query("SELECT count() FROM (SELECT sipHash128(event_id) FROM cdc.change_history GROUP BY 1)", settings=spill).result_rows[0][0]
    distinct_records = ch.query("SELECT count() FROM (SELECT sipHash128(source_table, kafka_partition, kafka_offset) FROM cdc.change_history GROUP BY 1)", settings=spill).result_rows[0][0]
    report["history"] = {
        "deliveries": deliveries, "distinct_change_events": distinct_events, "distinct_kafka_records": distinct_records,
        "sink_replays": deliveries - distinct_records,            # same Kafka record applied more than once
        "capture_replays": distinct_records - distinct_events,    # same source change present at more than one Kafka offset
        "delete_deliveries": deletes, "snapshot_read_deliveries": reads,
    }
    try:
        report["version_ordering"] = version_ordering(ch)
    except Exception as exc:  # noqa: BLE001 - the reconciliation is the result; this check is not
        report["version_ordering"] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    report["converged"] = all(t["converged"] for t in report["tables"].values())
    path = run_dir() / f"reconcile-{label}.json"
    path.write_text(json.dumps(report, indent=1))
    append_jsonl("events.jsonl", {"ts_ms": now_ms(), "event": f"reconcile-{label}", "converged": report["converged"]})
    print(json.dumps(report, indent=1))


def wait_quiet(stable_s=90.0, timeout_s=3600.0):
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "lab-reconcile-probe", "enable.auto.commit": False})
    deadline, last_end, quiet_since = time.monotonic() + timeout_s, None, None
    while time.monotonic() < deadline:
        md = probe.list_topics(timeout=5)
        parts = [TopicPartition(t, p) for t in TOPICS if t in md.topics for p in md.topics[t].partitions]
        ends = {(tp.topic, tp.partition): probe.get_watermark_offsets(tp, timeout=5, cached=False)[1] for tp in parts}
        req = ConsumerGroupTopicPartitions(SINK_GROUP, [TopicPartition(t, p) for t, p in ends])
        committed = {(tp.topic, tp.partition): tp.offset for tp in admin.list_consumer_group_offsets([req])[SINK_GROUP].result().topic_partitions}
        lag = sum(end - max(committed.get(k, 0), 0) for k, end in ends.items())
        total = sum(ends.values())
        if total == last_end and lag == 0:
            quiet_since = quiet_since or time.monotonic()
            if time.monotonic() - quiet_since >= stable_s:
                print(f"quiet: end offsets {total} unchanged for {stable_s:.0f}s, sink lag 0")
                return
        else:
            quiet_since = None
        last_end = total
        print(f"waiting: end_offsets={total} lag={lag}", flush=True)
        time.sleep(5)
    raise SystemExit("timeout waiting for quiescence")


if __name__ == "__main__":
    if sys.argv[1] == "wait-quiet":
        wait_quiet(*(float(a) for a in sys.argv[2:4]))
    elif sys.argv[1] == "compare":
        compare(sys.argv[2])
    else:
        raise SystemExit(__doc__)
