"""Thin wrappers around confluent-kafka so loops depend on two tiny interfaces.

`Record` is a plain value object. Loops call `consume()`, `commit()` and
`produce()/flush()` only, which lets unit tests substitute in-memory fakes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: dict[str, bytes] = field(default_factory=dict)
    timestamp_ms: int | None = None


class DeliveryFailed(Exception):
    pass


def decode_key(raw: bytes | None):
    """Keys are JSON (Debezium, JsonConverter) or plain UTF-8 strings (our producers)."""
    if raw is None:
        return None
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


class KafkaConsumer:
    def __init__(self, bootstrap: str, group_id: str, topics: list[str], extra: dict | None = None) -> None:
        from confluent_kafka import Consumer

        config = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,  # offsets are committed explicitly, after downstream acknowledgement
            "auto.offset.reset": "earliest",
            "client.id": group_id,
        }
        config.update(extra or {})
        self._consumer = Consumer(config)
        self._consumer.subscribe(list(topics))

    def consume(self, max_records: int, timeout_s: float) -> list[Record]:
        from confluent_kafka import KafkaError, KafkaException

        out: list[Record] = []
        for message in self._consumer.consume(num_messages=max_records, timeout=timeout_s):
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)
            ts_type, ts = message.timestamp()
            out.append(
                Record(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=message.key(),
                    value=message.value(),
                    headers=dict(message.headers() or []),
                    timestamp_ms=ts if ts_type != 0 else None,
                )
            )
        return out

    def commit(self, offsets: list[tuple[str, int, int]]) -> None:
        """Synchronously commit `next offset to read` per (topic, partition)."""
        from confluent_kafka import TopicPartition

        if offsets:
            self._consumer.commit(offsets=[TopicPartition(t, p, o) for t, p, o in offsets], asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


class KafkaProducer:
    def __init__(self, bootstrap: str, extra: dict | None = None) -> None:
        from confluent_kafka import Producer

        config = {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # no broker-side duplicates from producer retries
            "acks": "all",
            "linger.ms": 5,
        }
        config.update(extra or {})
        self._producer = Producer(config)
        self._errors: list[str] = []
        self.delivered = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))
        else:
            self.delivered += 1

    def produce(self, topic: str, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]] | None = None) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key, value=value, headers=headers, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue full: serve delivery callbacks, then retry (bounded memory = backpressure).
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self, timeout_s: float = 60.0) -> None:
        remaining = self._producer.flush(timeout_s)
        if remaining:
            raise DeliveryFailed(f"{remaining} messages not delivered within {timeout_s}s")
        if self._errors:
            errors, self._errors = self._errors, []
            raise DeliveryFailed(f"{len(errors)} delivery errors, first: {errors[0]}")
