#!/usr/bin/env python3
"""Samples every durable boundary on one clock: source slot, capture task, Kafka, sink progress, ClickHouse.

One JSON line per interval in results/<run-id>/metrics.jsonl. Alert transitions go to alerts.jsonl.
"""
import os
import time

import clickhouse_connect
import psycopg
import requests
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common import CONNECT_WORKERS, CONNECTOR, KAFKA_BOOTSTRAP, SINK_GROUP, SLOT, TOPICS, append_jsonl, ch_kwargs, now_ms, pg_dsn

INTERVAL_S = float(os.environ.get("COLLECT_INTERVAL_S", "2"))
FRESHNESS_SLO_MS = int(os.environ.get("FRESHNESS_SLO_MS", "60000"))

SLOT_SQL = """
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
       s.active, s.wal_status, s.safe_wal_size, s.invalidation_reason,
       pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
       pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint,
       s.restart_lsn::text, s.confirmed_flush_lsn::text
FROM (SELECT 1) AS one LEFT JOIN pg_replication_slots AS s ON s.slot_name = %s"""


def source_metrics(conn):
    row = conn.execute(SLOT_SQL, (SLOT,)).fetchone()
    keys = ["wal_bytes_total", "slot_active", "wal_status", "safe_wal_size", "invalidation_reason",
            "retained_wal_bytes", "unconfirmed_wal_bytes", "restart_lsn", "confirmed_flush_lsn"]
    return dict(zip(keys, row))


def capture_metrics():
    for base in CONNECT_WORKERS:
        try:
            r = requests.get(f"{base}/connectors/{CONNECTOR}/status", timeout=3)
        except requests.RequestException:
            continue
        if r.status_code == 404:
            return {"connector_state": "ABSENT"}
        if r.ok:
            s = r.json()
            task = (s.get("tasks") or [{}])[0]
            return {"connector_state": s["connector"]["state"], "task_state": task.get("state", "UNASSIGNED"),
                    "task_worker": task.get("worker_id"), "task_count": len(s.get("tasks") or []),
                    "task_trace": (task.get("trace") or "").split("\n")[0][:300] or None}
    return {"connector_state": "UNREACHABLE"}


def kafka_metrics(admin, probe):
    md = probe.list_topics(timeout=5)
    parts = [TopicPartition(t, p) for t in TOPICS if t in md.topics for p in md.topics[t].partitions]
    if not parts:
        return {"kafka_end_offsets": 0, "sink_lag": 0, "oldest_unapplied_age_ms": 0}
    ends = {(tp.topic, tp.partition): probe.get_watermark_offsets(tp, timeout=5, cached=False)[1] for tp in parts}
    request = ConsumerGroupTopicPartitions(SINK_GROUP, [TopicPartition(tp.topic, tp.partition) for tp in parts])
    committed = {(tp.topic, tp.partition): tp.offset for tp in admin.list_consumer_group_offsets([request])[SINK_GROUP].result().topic_partitions}
    lagging = []
    lag = 0
    for key, end in ends.items():
        c = committed.get(key, -1)
        start = c if c >= 0 else probe.get_watermark_offsets(TopicPartition(*key), timeout=5)[0]
        if end > start:
            lag += end - start
            lagging.append(TopicPartition(key[0], key[1], start))
    oldest = 0
    if lagging:  # age of the first unapplied record on each lagging partition (Kafka record timestamp)
        probe.assign(lagging)
        seen, deadline = set(), time.monotonic() + 1.5
        while len(seen) < len(lagging) and time.monotonic() < deadline:
            for m in probe.consume(num_messages=len(lagging) * 4, timeout=0.5):
                key = (m.topic(), m.partition())
                if m.error() or key in seen:
                    continue
                seen.add(key)
                oldest = max(oldest, now_ms() - m.timestamp()[1])
        probe.unassign()
    return {"kafka_end_offsets": sum(ends.values()), "sink_lag": lag, "oldest_unapplied_age_ms": oldest,
            "lagging_partitions": len(lagging)}


def sink_metrics(ch):
    rows, applied_max = ch.query("SELECT count(), toUnixTimestamp64Milli(max(applied_at)) FROM cdc.change_history").result_rows[0]
    q = ch.query("""
        SELECT count(), quantilesExact(0.5, 0.95, 0.99)(toUnixTimestamp64Milli(applied_at) - source_ts_ms)
        FROM cdc.change_history
        WHERE applied_at >= now64(3) - INTERVAL 10 SECOND AND snapshot = 'false'""").result_rows[0]
    return {"history_rows": rows, "applied_last_10s": q[0], "freshness_p50_ms": q[1][0] if q[0] else None,
            "freshness_p95_ms": q[1][1] if q[0] else None, "freshness_p99_ms": q[1][2] if q[0] else None,
            "last_applied_ms": applied_max}


def alerts(sample):
    active = set()
    if sample.get("wal_status") in ("unreserved", "lost"):
        active.add(f"source-history-{sample['wal_status']}")
    if sample.get("task_state") == "FAILED" or sample.get("connector_state") == "FAILED":
        active.add("capture-failed")
    if sample.get("task_state") == "UNASSIGNED":
        active.add("capture-unassigned")
    if (sample.get("oldest_unapplied_age_ms") or 0) > FRESHNESS_SLO_MS:
        active.add("freshness-slo-breach")
    return active


def kafka_clients():
    return (AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}),
            Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "lab-collector-probe", "enable.auto.commit": False}))


def main():
    admin, probe = kafka_clients()
    ch = clickhouse_connect.get_client(**ch_kwargs("admin"))
    pg = None
    active_alerts = set()
    while True:
        started = time.monotonic()
        sample = {"ts_ms": now_ms()}
        for name, fn in (("source", lambda: source_metrics(pg)), ("capture", capture_metrics),
                         ("kafka", lambda: kafka_metrics(admin, probe)), ("sink", lambda: sink_metrics(ch))):
            try:
                if name == "source" and (pg is None or pg.closed):
                    pg = psycopg.connect(pg_dsn("monitor"), autocommit=True)
                sample.update(fn())
            except Exception as exc:  # a boundary being down is data, not a collector crash
                sample[f"{name}_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
                if name == "source":
                    pg = None
                if name == "kafka":  # librdkafka clients can stay wedged after a broker outage; start fresh next sample
                    try:
                        probe.close()
                    except Exception:
                        pass
                    admin, probe = kafka_clients()
        now_active = alerts(sample)
        for a in sorted(now_active - active_alerts):
            append_jsonl("alerts.jsonl", {"ts_ms": sample["ts_ms"], "alert": a, "state": "firing"})
            print(f"ALERT firing: {a}", flush=True)
        for a in sorted(active_alerts - now_active):
            append_jsonl("alerts.jsonl", {"ts_ms": sample["ts_ms"], "alert": a, "state": "resolved"})
            print(f"ALERT resolved: {a}", flush=True)
        active_alerts = now_active
        sample["alerts"] = sorted(now_active)
        append_jsonl("metrics.jsonl", sample)
        time.sleep(max(0.0, INTERVAL_S - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
