"""Produce seeded raw gateway telemetry to Kafka with a bounded rate.

    python -m connectedfleet.producers.telemetry --count 500 --seed 7 --run-tag lab02x --rate 200

Key = device id (per-device ordering within a partition). Idempotent producer,
acks=all. `sent` is the real send clock in epoch ms.
"""

from __future__ import annotations

import argparse
import json
import time

from connectedfleet import topics
from connectedfleet.config import Settings
from connectedfleet.kafka_io import KafkaProducer
from connectedfleet.producers.gateway import generate_raw


def produce(producer, topic: str, count: int, seed: int, run_tag: str, rate: float, poison_every: int = 0) -> dict:
    started = time.time()
    interval = 1.0 / rate if rate > 0 else 0.0
    next_send = time.monotonic()
    sent = 0
    for record in generate_raw(count, seed, run_tag, poison_every=poison_every):
        if interval:
            delay = next_send - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            next_send += interval
        record["sent"] = int(time.time() * 1000)
        producer.produce(topic, record["dev"].encode(), json.dumps(record, separators=(",", ":")).encode())
        sent += 1
    producer.flush()
    return {"topic": topic, "sent": sent, "seed": seed, "run_tag": run_tag,
            "poison": (count // poison_every) if poison_every else 0,
            "started_at": started, "ended_at": time.time()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--rate", type=float, default=0.0, help="events/second; 0 = as fast as possible")
    parser.add_argument("--poison-every", type=int, default=0)
    parser.add_argument("--topic", default=topics.RAW_TELEMETRY)
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    summary = produce(KafkaProducer(settings.kafka_bootstrap), args.topic, args.count, args.seed, args.run_tag,
                      args.rate, args.poison_every)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
