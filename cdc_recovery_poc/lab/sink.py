#!/usr/bin/env python3
"""Replay-safe ClickHouse sink for Debezium PostgreSQL change events.

Progress rule: Kafka offsets are committed only after ClickHouse has accepted every insert for the batch.
A crash between the insert and the commit replays the batch. The current-state tables absorb replays
through ReplacingMergeTree(version, is_deleted), with version = the record's offset in its Kafka partition.
Every event for a device key lands in one partition in the order Debezium emitted it, so the offset orders
changes, snapshot reads and re-emitted changes for that key. The PostgreSQL LSN is not a safe version here:
incremental snapshot reads carry none (lab run recovery-20260913T201024Z), and a change's LSN records where
it was written in the WAL, not when its transaction committed. The offset version holds only while a key's
partition never changes, so repartitioning a raw topic means rebuilding current state.
The history table keeps every delivery with its source position and offset, so replays stay measurable.

Fault hook: if /control/crash-sink-after-insert exists, one sink claims it (atomic rename) and exits
after its next successful insert and before the offset commit.
"""
import os
import signal
import socket
import time
import uuid

import clickhouse_connect
import orjson
from confluent_kafka import Consumer, KafkaError

from common import CONTRACT, CONTROL, KAFKA_BOOTSTRAP, SINK_GROUP, append_jsonl, ch_kwargs, contract_version, now_ms

SINK_ID = socket.gethostname()
TOPIC_PATTERN = r"^pg-devices\.ops\.(device_registry|device_status)$"
BATCH_MAX = int(os.environ.get("SINK_BATCH_MAX", "10000"))
BATCH_WAIT_S = float(os.environ.get("SINK_BATCH_WAIT_S", "0.5"))
META_COLUMNS = ["version", "is_deleted", "source_ts_ms", "sink_id"]
HISTORY_COLUMNS = ["event_id", "source_table", "op", "snapshot", "device_id", "lsn", "tx_id", "source_ts_ms",
                   "capture_ts_ms", "kafka_partition", "kafka_offset", "after_json", "sink_id", "batch_id"]
VERSION_BASIS = "kafka_offset"

INSERT_ATTEMPTS = int(os.environ.get("SINK_INSERT_ATTEMPTS", "30"))

running = True


def insert_with_retry(client, table, rows, columns, stats):
    """ClickHouse can be busy, over its memory limit or briefly unreachable. Retry the same batch with backoff
    inside a budget; the offsets stay uncommitted until every insert succeeds."""
    for attempt in range(1, INSERT_ATTEMPTS + 1):
        try:
            client.insert(table, rows, column_names=columns)
            return
        except Exception as exc:
            if attempt == INSERT_ATTEMPTS:
                raise
            stats["insert_retries"] += 1
            delay = min(5.0, 0.5 * 2 ** (attempt - 1))
            print(f"[{SINK_ID}] insert into {table} failed (attempt {attempt}): {str(exc)[:160]}; retrying in {delay}s", flush=True)
            time.sleep(delay)


def stop(*_):
    global running
    running = False


def source_position(source):
    """The change's source position, kept in history for identity and diagnostics: its LSN, or for incremental
    snapshot reads, which carry no LSN, the stream position Debezium records in source.sequence."""
    if source.get("lsn"):
        return int(source["lsn"])
    sequence = orjson.loads(source.get("sequence") or "[]")
    return int(sequence[-1]) if sequence and sequence[-1] else 0


def transform(messages, version, batch_id):
    history, current = [], {table: [] for table in CONTRACT}
    tombstones, unmapped = 0, set()
    for m in messages:
        raw = m.value()
        if raw is None:  # Kafka tombstone that follows a delete event; the delete event carries the state change.
            tombstones += 1
            continue
        event = orjson.loads(raw)
        source, op = event["source"], event["op"]
        table = source["table"]
        spec = CONTRACT[table][version]
        lsn = source_position(source)
        if op == "d":
            row = event["before"]
            values = [row["device_id"] if column == "device_id" else default for column, default in spec]
            deleted = 1
        else:
            row = event["after"]
            unmapped.update(f"{table}.{field}" for field in row.keys() - {column for column, _ in spec})
            values = [default if row.get(column) is None else row[column] for column, default in spec]
            deleted = 0
        device_id = row["device_id"]
        current[table].append(values + [m.offset(), deleted, source["ts_ms"], SINK_ID])
        history.append([
            f"{table}:{device_id}:{lsn}:{op}", table, op, str(source.get("snapshot", "false")).lower(), device_id,
            lsn, int(source.get("txId") or 0), source["ts_ms"], int(event.get("ts_ms") or 0), m.partition(),
            m.offset(), orjson.dumps(row).decode() if table == "device_registry" else "", SINK_ID, batch_id,
        ])
    return history, current, tombstones, unmapped


def offset_ranges(messages):
    ranges = {}
    for m in messages:
        key = f"{m.topic()}[{m.partition()}]"
        lo, hi = ranges.get(key, (m.offset(), m.offset()))
        ranges[key] = (min(lo, m.offset()), max(hi, m.offset()))
    return {k: list(v) for k, v in ranges.items()}


def maybe_crash_after_insert(messages, batch_id, rows):
    trigger = CONTROL / "crash-sink-after-insert"
    if not trigger.exists():
        return
    try:
        os.rename(trigger, CONTROL / f"crash-claimed-{SINK_ID}")
    except FileNotFoundError:  # another sink claimed it
        return
    append_jsonl("events.jsonl", {
        "ts_ms": now_ms(), "event": "sink-crash-after-insert-before-commit", "sink_id": SINK_ID,
        "batch_id": batch_id, "rows": rows, "offsets": offset_ranges(messages),
    })
    print(f"[{SINK_ID}] insert acknowledged by ClickHouse; exiting before offset commit", flush=True)
    os._exit(86)


def main():
    signal.signal(signal.SIGTERM, stop)
    client = clickhouse_connect.get_client(**ch_kwargs("sink"))
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": SINK_GROUP,
        "client.id": SINK_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 45000,
        # Regex subscriptions only see topics created after startup on a metadata refresh (default 5 min).
        "topic.metadata.refresh.interval.ms": 10000,
        "fetch.max.bytes": 16777216,
        "max.partition.fetch.bytes": 1048576,
        "queued.min.messages": 10000,
        "queued.max.messages.kbytes": 8192,  # per partition: bounds prefetch memory on a small container
    })
    consumer.subscribe([TOPIC_PATTERN])
    stats = dict(records=0, batches=0, history_rows=0, current_rows=0, tombstones=0, insert_ms=0.0, insert_ms_max=0.0, insert_retries=0)
    unmapped_seen, last_flush = set(), time.monotonic()
    append_jsonl("events.jsonl", {"ts_ms": now_ms(), "event": "sink-started", "sink_id": SINK_ID, "version_basis": VERSION_BASIS})
    print(f"[{SINK_ID}] started; batch_max={BATCH_MAX} wait={BATCH_WAIT_S}s version={VERSION_BASIS}", flush=True)

    while running:
        messages = consumer.consume(num_messages=BATCH_MAX, timeout=BATCH_WAIT_S)
        good = []
        for m in messages:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(m.error())
            else:
                good.append(m)
        if good:
            batch_id = uuid.uuid4().hex[:12]
            version = contract_version()
            history, current, tombstones, unmapped = transform(good, version, batch_id)
            started = time.monotonic()
            for table, rows in current.items():
                if rows:
                    columns = [column for column, _ in CONTRACT[table][version]] + META_COLUMNS
                    insert_with_retry(client, f"cdc.{table}_current", rows, columns, stats)
            if history:
                insert_with_retry(client, "cdc.change_history", history, HISTORY_COLUMNS, stats)
            elapsed_ms = (time.monotonic() - started) * 1000
            maybe_crash_after_insert(good, batch_id, len(history))
            consumer.commit(asynchronous=False)  # progress advances only after the side effect is durable
            stats["records"] += len(good)
            stats["batches"] += 1
            stats["history_rows"] += len(history)
            stats["current_rows"] += sum(len(r) for r in current.values())
            stats["tombstones"] += tombstones
            stats["insert_ms"] += elapsed_ms
            stats["insert_ms_max"] = max(stats["insert_ms_max"], elapsed_ms)
            new_fields = unmapped - unmapped_seen
            if new_fields:
                unmapped_seen |= new_fields
                append_jsonl("events.jsonl", {"ts_ms": now_ms(), "event": "sink-unmapped-fields", "sink_id": SINK_ID,
                                              "fields": sorted(new_fields), "contract_version": version})
        if time.monotonic() - last_flush >= 5:
            batches = stats["batches"] or 1
            append_jsonl(f"sink-{SINK_ID}.jsonl", {
                "ts_ms": now_ms(), "sink_id": SINK_ID, "contract_version": contract_version(),
                **{k: stats[k] for k in ("records", "batches", "history_rows", "current_rows", "tombstones", "insert_retries")},
                "insert_ms_avg": round(stats["insert_ms"] / batches, 1), "insert_ms_max": round(stats["insert_ms_max"], 1),
                "unmapped_fields": sorted(unmapped_seen),
            })
            stats.update(records=0, batches=0, history_rows=0, current_rows=0, tombstones=0, insert_ms=0.0, insert_ms_max=0.0, insert_retries=0)
            last_flush = time.monotonic()

    consumer.close()


if __name__ == "__main__":
    main()
