"""Produce a benchmark workload to a per-run topic (runs inside the `app` container).

    python -m connectedfleet.bench.producer --topic cfbench.x.telemetry --count 100000 --seed 1 [--rate 2000]

`emitted_at` = real send time (epoch ms, the Docker VM clock that ClickHouse
also uses for `ingested_at`), so live-mode latency = ingested_at - emitted_at
needs no cross-machine clock assumption. Prints one JSON summary line.
"""

from __future__ import annotations

import argparse
import json
import time

from connectedfleet.bench.workload import WorkloadSpec, events
from connectedfleet.config import Settings
from connectedfleet.contracts.canonical import format_timestamp_ms
from connectedfleet.kafka_io import KafkaProducer


def poison_positions(count: int, poison: int) -> list[int]:
    """Indices in the sent stream where poison records go: spread evenly, one past each even split.

    Mid-stream, so a poison record shares a batch with good records (a poison sent last sat alone in the
    official sink's trailing buffer and hid its batch-level failure handling; decision D-020).
    """
    return [count * (i + 1) // (poison + 1) + 1 + i for i in range(poison)]


def produce(producer, topic: str, spec: WorkloadSpec, rate: float = 0.0, poison: int = 0) -> dict:
    interval = 1.0 / rate if rate > 0 else 0.0
    next_send = time.monotonic()
    started = time.time()
    sent = 0
    positions = set(poison_positions(spec.count, poison))
    poisoned = 0

    def send_poison() -> None:
        # Contract-violating record (battery_pct is a string) for the E4 poison scenario.
        nonlocal poisoned
        bad = {"event_id": f"poison-{spec.seed}-{poisoned}", "device_id": "sensor-99999", "event_time": format_timestamp_ms(0),
               "emitted_at": format_timestamp_ms(int(time.time() * 1000)), "source_system": "iot-gateway",
               "schema_version": 1, "battery_pct": "seventeen"}
        producer.produce(topic, b"sensor-99999", json.dumps(bad).encode())
        poisoned += 1

    for event in events(spec):
        while sent + poisoned in positions:
            send_poison()
        if interval:
            delay = next_send - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            next_send += interval
        event["emitted_at"] = format_timestamp_ms(int(time.time() * 1000))
        producer.produce(topic, event["device_id"].encode(), json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode())
        sent += 1
    while poisoned < poison:
        send_poison()
    producer.flush()
    return {"topic": topic, "sent": sent, "poison": poison, "started_at": started, "ended_at": time.time(), "spec": spec.describe()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--null-fraction", type=float, default=0.04)
    parser.add_argument("--string-heavy", action="store_true")
    parser.add_argument("--id-prefix", default="bench")
    parser.add_argument("--rate", type=float, default=0.0)
    parser.add_argument("--poison", type=int, default=0)
    args = parser.parse_args(argv)
    spec = WorkloadSpec(args.count, args.seed, args.null_fraction, args.string_heavy, id_prefix=args.id_prefix)
    settings = Settings.from_env()
    print(json.dumps(produce(KafkaProducer(settings.kafka_bootstrap, {"linger.ms": 20}), args.topic, spec, args.rate, args.poison)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
