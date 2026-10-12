"""Recover records from a dead-letter topic: re-validate each one, replay the valid ones, quarantine the rest.

The official ClickHouse sink dead-letters a failed batch as a whole, so good records can sit in its DLQ next to
the one that broke the batch (Lab 03 E4: 499 good + 1 bad). They are not lost: this tool reads the DLQ, checks
every record against the canonical contract, re-produces the valid ones to a target topic (from which the sink
inserts them) and reports the invalid ones without their values.

    python -m connectedfleet.recovery --dlq cf.dlq.sink.v1 --to <topic> [--from-offset N] [--contract cf.telemetry.v1]

Replay is at-least-once like every lane: replaying the same DLQ range twice re-produces the same records, and
queries de-duplicate by event_id.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable

from connectedfleet.contracts import canonical


def classify(records: Iterable[tuple[bytes | None, bytes | None]], contract=canonical.TELEMETRY):
    """Split (key, value) pairs into (replayable pairs, quarantine entries). Quarantine entries carry the event id and
    reason only: never the offending values (they may be sensitive or huge)."""
    keep, quarantine = [], []
    for key, value in records:
        try:
            event = json.loads(value or b"")
            if not isinstance(event, dict):
                raise ValueError("not an object")
        except ValueError:
            quarantine.append({"event_id": None, "reason": "NOT_JSON"})
            continue
        try:
            canonical.validate(contract, event)
        except canonical.ContractError as err:
            quarantine.append({"event_id": event.get("event_id"), "reason": f"{err.args[0]}:{err.args[1] if len(err.args) > 1 else ''}"})
            continue
        keep.append((key, value))
    return keep, quarantine


def replay(records: Iterable[tuple[bytes | None, bytes | None]], producer, target_topic: str,
           contract=canonical.TELEMETRY) -> dict:
    records = list(records)
    keep, quarantine = classify(records, contract)
    for key, value in keep:
        producer.produce(target_topic, key, value)
    producer.flush()
    return {"read": len(records), "replayed": len(keep), "quarantined": len(quarantine), "quarantine": quarantine}


def read_topic(bootstrap: str, topic: str, from_offset: int = 0, timeout_s: float = 15.0) -> list[tuple]:
    """All (key, value) pairs of a single-partition-or-more topic from `from_offset` to the current end."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "cf-recovery-reader", "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=timeout_s).topics[topic].partitions
        out = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_s)
            start = max(low, from_offset)
            if start >= high:
                continue
            consumer.assign([TopicPartition(topic, p, start)])
            got = 0
            while got < high - start:
                batch = consumer.consume(num_messages=min(1000, high - start - got), timeout=timeout_s)
                if not batch:
                    raise TimeoutError(f"{topic}[{p}]: read {got} of {high - start} records before timing out")
                for message in batch:
                    if message.error() is None:
                        out.append((message.key(), message.value()))
                    got += 1
        return out
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    from connectedfleet.config import Settings
    from connectedfleet.kafka_io import KafkaProducer

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dlq", required=True)
    parser.add_argument("--to", required=True)
    parser.add_argument("--from-offset", type=int, default=0)
    parser.add_argument("--contract", default="cf.telemetry.v1")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    contract = canonical.CONTRACTS[args.contract]
    result = replay(read_topic(settings.kafka_bootstrap, args.dlq, args.from_offset),
                    KafkaProducer(settings.kafka_bootstrap), args.to, contract)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
