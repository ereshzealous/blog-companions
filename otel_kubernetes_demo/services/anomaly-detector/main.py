"""
anomaly-detector — consumes vitals.events, runs rule-based + heuristic ML
to flag AFib / abnormal HR / motion-fall, writes events + alerts to Postgres,
and emits Kafka 'alerts' messages.

Notable: this service exports OTLP to its sidecar collector at localhost:4317
(not the DaemonSet agent). The sidecar applies 100% sampling on alert spans —
demonstrating per-app config in contrast to the cluster-wide DaemonSet agent.
"""

import asyncio
import json
import logging
import os
import random
import signal
import uuid

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.trace import SpanKind, Status, StatusCode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("anomaly-detector")

SERVICE_NAME = "anomaly-detector"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-0.kafka.infra.svc.cluster.local:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "vitals.events")
OUT_TOPIC = os.getenv("OUT_TOPIC", "alerts")
GROUP_ID = os.getenv("KAFKA_GROUP", "anomaly-detector")
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://wearable:wearable@postgres.infra.svc.cluster.local:5432/wearable",
)
# Crucial: this points at the SIDECAR collector, not the node-local agent.
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")


def init_tracer():
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": "0.1.0",
    })
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    AsyncPGInstrumentor().instrument()
    return trace.get_tracer(SERVICE_NAME)


tracer = init_tracer()


def headers_to_carrier(headers):
    return {k: (v.decode() if isinstance(v, (bytes, bytearray)) else v) for k, v in (headers or [])}


def carrier_to_headers(carrier):
    return [(k, v.encode()) for k, v in carrier.items()]


def detect_anomalies(event: dict) -> list[dict]:
    """Rule-based detectors. In a real system this would call ML models."""
    findings = []
    hr_avg = event.get("hr_avg")
    hr_max = event.get("hr_max")
    spo2 = event.get("spo2_avg")
    motion = event.get("motion_class")

    if hr_max and hr_max > 180 and motion in ("rest", None):
        findings.append({"type": "tachycardia_at_rest", "severity": "high"})
    if hr_avg and hr_avg < 40:
        findings.append({"type": "bradycardia", "severity": "high"})
    if spo2 is not None and spo2 < 90:
        findings.append({"type": "hypoxia", "severity": "critical"})
    # Simulated AFib detection — small probability on elevated HR.
    if hr_avg and 100 < hr_avg < 160 and random.random() < 0.02:
        findings.append({"type": "afib_suspected", "severity": "high"})
    return findings


async def handle_event(pool: asyncpg.Pool, producer: AIOKafkaProducer, event: dict):
    with tracer.start_as_current_span(
        "anomaly.evaluate",
        attributes={
            "user.id": event["user_id"],
            "hr.avg": event.get("hr_avg") or 0.0,
            "spo2.avg": event.get("spo2_avg") or 0.0,
        },
    ) as eval_span:
        findings = detect_anomalies(event)
        eval_span.set_attribute("anomaly.count", len(findings))

        if not findings:
            return

        async with pool.acquire() as conn:
            for f in findings:
                event_id = uuid.UUID(event["event_id"]) if _is_uuid(event.get("event_id", "")) else uuid.uuid4()
                alert_id = uuid.uuid4()
                with tracer.start_as_current_span(
                    "alert.create",
                    attributes={
                        "alert.severity": f["severity"],
                        "alert.type": f["type"],
                        "user.id": event["user_id"],
                    },
                ) as alert_span:
                    await conn.execute(
                        """INSERT INTO events (id, user_id, type, severity, payload)
                           VALUES ($1,$2,$3,$4,$5::jsonb)""",
                        event_id, uuid.UUID(event["user_id"]), f["type"], f["severity"],
                        json.dumps(event),
                    )
                    await conn.execute(
                        """INSERT INTO alerts (id, user_id, event_id, status)
                           VALUES ($1,$2,$3,'open')""",
                        alert_id, uuid.UUID(event["user_id"]), event_id,
                    )

                    carrier = {}
                    inject(carrier)
                    alert_msg = {
                        "alert_id": str(alert_id),
                        "user_id": event["user_id"],
                        "type": f["type"],
                        "severity": f["severity"],
                        "event": event,
                    }
                    with tracer.start_as_current_span(
                        "kafka.produce",
                        kind=SpanKind.PRODUCER,
                        attributes={
                            "messaging.system": "kafka",
                            "messaging.destination.name": OUT_TOPIC,
                            "alert.severity": f["severity"],
                        },
                    ):
                        await producer.send_and_wait(
                            OUT_TOPIC,
                            key=event["user_id"].encode(),
                            value=json.dumps(alert_msg).encode(),
                            headers=carrier_to_headers(carrier),
                        )
                    alert_span.set_status(Status(StatusCode.OK))


def _is_uuid(s: str) -> bool:
    try:
        uuid.UUID(s)
        return True
    except Exception:
        return False


async def consume(pool, producer, consumer):
    log.info("Consuming from %s (via sidecar collector)", IN_TOPIC)
    async for msg in consumer:
        carrier = headers_to_carrier(msg.headers)
        ctx = extract(carrier)
        with tracer.start_as_current_span(
            "kafka.consume",
            context=ctx,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": IN_TOPIC,
                "messaging.kafka.partition": msg.partition,
                "messaging.kafka.offset": msg.offset,
            },
        ):
            try:
                event = json.loads(msg.value)
                await handle_event(pool, producer, event)
            except Exception:
                log.exception("evaluating event failed")


async def main():
    log.info("OTLP endpoint (sidecar): %s", OTLP_ENDPOINT)
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await producer.start()
    await consumer.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    task = asyncio.create_task(consume(pool, producer, consumer))
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await consumer.stop()
    await producer.stop()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
