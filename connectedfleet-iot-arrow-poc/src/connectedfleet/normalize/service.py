"""Normalizer process: raw topics -> canonical topics, with a DLQ.

Commit protocol (at-least-once, no poison loops):
  1. consume a bounded batch from the five raw topics
  2. map + validate each record; produce canonical events keyed by device_id,
     or a DLQ record for rejects (headers: source topic/partition/offset/error)
  3. flush the producer (wait for broker acknowledgement)
  4. only then commit the consumed offsets
A crash between 3 and 4 re-delivers the batch; canonical event_ids are stable,
so the result is a logical duplicate, never a loss. A poison record is
DLQ'd once and committed past.

    python -m connectedfleet.normalize.service [--idle-exit 30]
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from dataclasses import dataclass, field

from connectedfleet import topics
from connectedfleet.config import Settings
from connectedfleet.contracts import canonical
from connectedfleet.kafka_io import KafkaConsumer, KafkaProducer, Record, decode_key
from connectedfleet.normalize.mappers import SKIP, MappingError, map_record

log = logging.getLogger("connectedfleet.normalizer")


@dataclass
class Stats:
    consumed: int = 0
    produced: dict[str, int] = field(default_factory=dict)
    skipped: int = 0
    dlq: dict[str, int] = field(default_factory=dict)
    batches: int = 0


def _decode_value(raw: bytes | None):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        raise MappingError("MALFORMED_JSON") from None


def process(record: Record):
    """Map + validate one raw record. Returns (canonical_topic, key, value_bytes) or None (skip)."""
    value = _decode_value(record.value)
    canonical_topic, event = map_record(
        record.topic, decode_key(record.key), value, record.headers, record.timestamp_ms
    )
    if event is SKIP:
        return None
    contract = canonical.CONTRACTS[canonical_topic]
    try:
        row = canonical.validate(contract, event)
    except canonical.ContractError as err:
        raise MappingError(err.code, err.field) from None
    out = canonical.to_json_value(contract, row)
    # Preserve the absent-vs-null distinction for telemetry measurements: drop keys absent upstream.
    for name in ("temperature_c", "battery_pct", "signal_dbm", "firmware_version", "site_id"):
        if canonical_topic == topics.TELEMETRY and name not in event:
            out.pop(name, None)
    payload = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return canonical_topic, out["device_id"].encode("utf-8"), payload


def dlq_record(record: Record, error: MappingError) -> tuple[bytes | None, bytes, list[tuple[str, bytes]]]:
    """DLQ value names the failing field and source position only; no payload values (PII-safe)."""
    body = {
        "error_code": error.code,
        "field": error.field,
        "source_topic": record.topic,
        "source_partition": record.partition,
        "source_offset": record.offset,
        "stage": "normalizer",
    }
    headers = [
        ("cf.source.topic", record.topic.encode()),
        ("cf.source.partition", str(record.partition).encode()),
        ("cf.source.offset", str(record.offset).encode()),
        ("cf.error.code", error.code.encode()),
    ]
    return record.key, json.dumps(body).encode(), headers


def run_batch(records: list[Record], producer, stats: Stats) -> list[tuple[str, int, int]]:
    """Process one batch; returns offsets to commit (after the caller flushed the producer)."""
    next_offsets: dict[tuple[str, int], int] = {}
    for record in records:
        stats.consumed += 1
        try:
            result = process(record)
        except MappingError as err:
            key, value, headers = dlq_record(record, err)
            producer.produce(topics.DLQ_NORMALIZER, key, value, headers)
            stats.dlq[err.code] = stats.dlq.get(err.code, 0) + 1
            log.warning("dlq topic=%s partition=%s offset=%s code=%s field=%s",
                        record.topic, record.partition, record.offset, err.code, err.field)
        else:
            if result is None:
                stats.skipped += 1
            else:
                canonical_topic, key, payload = result
                producer.produce(canonical_topic, key, payload)
                stats.produced[canonical_topic] = stats.produced.get(canonical_topic, 0) + 1
        position = (record.topic, record.partition)
        next_offsets[position] = max(next_offsets.get(position, -1), record.offset + 1)
    return [(t, p, o) for (t, p), o in sorted(next_offsets.items())]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ConnectedFleet normalizer")
    parser.add_argument("--group", default="cf-normalizer")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--idle-exit", type=float, default=0.0, help="exit after N idle seconds (0 = run forever)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = Settings.from_env()
    consumer = KafkaConsumer(settings.kafka_bootstrap, args.group, list(topics.RAW_TOPICS))
    producer = KafkaProducer(settings.kafka_bootstrap)
    stats = Stats()
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    last_activity = time.monotonic()
    try:
        while not stopping:
            records = consumer.consume(args.batch, args.poll_timeout)
            if not records:
                if args.idle_exit and time.monotonic() - last_activity > args.idle_exit:
                    break
                continue
            last_activity = time.monotonic()
            offsets = run_batch(records, producer, stats)
            producer.flush()  # canonical + DLQ acknowledged by the broker ...
            consumer.commit(offsets)  # ... before the raw offsets move forward
            stats.batches += 1
    finally:
        consumer.close()
        log.info("normalizer stats %s", json.dumps(stats.__dict__))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
