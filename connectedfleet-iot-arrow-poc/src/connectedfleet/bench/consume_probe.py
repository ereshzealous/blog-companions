"""How fast can the workers' Kafka consumer drain a topic when it does no other work? (D-031)

    docker compose run --rm app -m connectedfleet.bench.consume_probe --topic <topic> --count 1000000

Uses the same KafkaConsumer wrapper and consume() call as the lane loop, with no decode and no insert, under
librdkafka's defaults and under the workers' settings. Prints one JSON line per variant.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid

import confluent_kafka

from connectedfleet.config import Settings
from connectedfleet.kafka_io import KafkaConsumer
from connectedfleet.lanes.worker import CONSUMER_CONFIG

VARIANTS = [
    ("librdkafka defaults", {"max.poll.interval.ms": 300_000}),
    ("worker settings", CONSUMER_CONFIG),
    ("librdkafka defaults (repeat)", {"max.poll.interval.ms": 300_000}),
    ("worker settings (repeat)", CONSUMER_CONFIG),
]


def drain(bootstrap: str, topic: str, count: int, batch: int, extra: dict, timeout_s: float = 120.0) -> dict:
    consumer = KafkaConsumer(bootstrap, f"probe-{uuid.uuid4().hex[:8]}", [topic], extra)
    got, first, started = 0, None, time.perf_counter()
    try:
        while got < count and time.perf_counter() - started < timeout_s:
            records = consumer.consume(min(batch, count - got), 0.5)
            if records and first is None:
                first = time.perf_counter()
            got += len(records)
    finally:
        consumer.close()
    seconds = time.perf_counter() - first if first else None
    return {"records": got, "seconds_from_first_record": round(seconds, 3) if seconds else None,
            "records_per_s": round(got / seconds) if seconds else None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=1_000_000)
    parser.add_argument("--batch", type=int, default=1000)
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    for name, extra in VARIANTS:
        result = drain(settings.kafka_bootstrap, args.topic, args.count, args.batch, extra)
        print(json.dumps({"variant": name, "config": extra, "batch": args.batch, "librdkafka": confluent_kafka.libversion()[0],
                          **result}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
