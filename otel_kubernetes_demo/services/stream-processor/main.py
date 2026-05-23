"""
stream-processor — consumes vitals.raw, computes 1-minute rollups,
writes to Postgres, and emits derived events to vitals.events.

Trace context flows in via Kafka headers (traceparent / tracestate)
and is propagated out the same way on emitted events.
"""

import asyncio
import json
import logging
import os
import signal
import statistics
import uuid
from datetime import datetime, timezone

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.trace import SpanKind, Status, StatusCode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("stream-processor")

SERVICE_NAME = "stream-processor"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-0.kafka.infra.svc.cluster.local:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "vitals.raw")
OUT_TOPIC = os.getenv("OUT_TOPIC", "vitals.events")
GROUP_ID = os.getenv("KAFKA_GROUP", "stream-processor")
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://wearable:wearable@postgres.infra.svc.cluster.local:5432/wearable",
)
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")


def init_tracer() -> trace.Tracer:
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


async def process_window(pool: asyncpg.Pool, producer: AIOKafkaProducer, user_id: str, readings: list):
    """Compute aggregates for one user's batch and write rollup row + emit event."""
    with tracer.start_as_current_span(
        "rollup.compute",
        attributes={
            "user.id": user_id,
            "readings.count": len(readings),
        },
    ) as span:
        hrs = [r["heart_rate"] for r in readings if r.get("heart_rate")]
        spo2s = [r["spo2"] for r in readings if r.get("spo2")]
        steps = sum(int(r.get("steps_delta", 0)) for r in readings)
        ts_avg = statistics.mean(r["ts_unix_ms"] for r in readings) / 1000.0
        ts = datetime.fromtimestamp(ts_avg, tz=timezone.utc).replace(second=0, microsecond=0)

        hr_avg = statistics.mean(hrs) if hrs else None
        hr_min = min(hrs) if hrs else None
        hr_max = max(hrs) if hrs else None
        spo2_avg = statistics.mean(spo2s) if spo2s else None

        # Trivial motion classifier from accel magnitude variance.
        mags = [abs(r.get("accel", {}).get("z", 0)) for r in readings]
        motion = "rest"
        if mags:
            var = statistics.pvariance(mags) if len(mags) > 1 else 0
            motion = "active" if var > 0.5 else ("walk" if var > 0.1 else "rest")

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO vitals_rollup_1min
                   (user_id, ts, hr_avg, hr_min, hr_max, spo2_avg, steps, motion_class)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT (user_id, ts) DO UPDATE SET
                     hr_avg=EXCLUDED.hr_avg, hr_min=EXCLUDED.hr_min,
                     hr_max=EXCLUDED.hr_max, spo2_avg=EXCLUDED.spo2_avg,
                     steps=vitals_rollup_1min.steps+EXCLUDED.steps,
                     motion_class=EXCLUDED.motion_class""",
                uuid.UUID(user_id), ts, hr_avg, hr_min, hr_max, spo2_avg, steps, motion,
            )

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "ts_unix_ms": int(ts.timestamp() * 1000),
            "hr_avg": hr_avg,
            "hr_max": hr_max,
            "spo2_avg": spo2_avg,
            "motion_class": motion,
            "readings_count": len(readings),
        }
        carrier = {}
        inject(carrier)
        with tracer.start_as_current_span(
            "kafka.produce",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": OUT_TOPIC,
            },
        ):
            await producer.send_and_wait(
                OUT_TOPIC,
                key=user_id.encode(),
                value=json.dumps(event).encode(),
                headers=carrier_to_headers(carrier),
            )
        span.set_status(Status(StatusCode.OK))


def _producer_span_context(msg):
    """Recover the producer's SpanContext from a message's W3C headers."""
    sc = trace.get_current_span(extract(headers_to_carrier(msg.headers))).get_span_context()
    return sc if sc.is_valid else None


async def consume(pool: asyncpg.Pool, producer: AIOKafkaProducer, consumer: AIOKafkaConsumer):
    """Batch consumption with the OTel messaging pattern (receive + links + process).

    A single poll can return N messages belonging to N DIFFERENT producer traces.
    We do NOT force them into one parent-child tree (that creates giant traces and
    a false parent). Instead:
      * ONE 'receive' span (its own trace) LINKS to every message's producer span
        — the batch boundary, observable but not merged.
      * each message gets a 'process' span that CONTINUES its own producer trace
        (parent = producer ctx) — so every upload keeps a clean end-to-end trace.
    """
    log.info("Consuming from %s (batch getmany + links)", IN_TOPIC)
    while True:
        polled = await consumer.getmany(timeout_ms=1000, max_records=100)
        msgs = [m for records in polled.values() for m in records]
        if not msgs:
            continue

        # ONE receive span over the whole poll, in its OWN trace, linked to all N producers.
        links = [trace.Link(sc) for sc in (_producer_span_context(m) for m in msgs) if sc]
        with tracer.start_as_current_span(
            "vitals.raw receive",
            context=Context(),  # detach → new root: this is the batch-boundary observation
            kind=SpanKind.CONSUMER,
            links=links,
            attributes={
                "messaging.system": "kafka",
                "messaging.operation": "receive",
                "messaging.destination.name": IN_TOPIC,
                "messaging.batch.message_count": len(msgs),
            },
        ):
            pass

        # Each message is processed in ITS OWN end-to-end trace (parent = producer ctx).
        for msg in msgs:
            with tracer.start_as_current_span(
                "vitals.raw process",
                context=extract(headers_to_carrier(msg.headers)),
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.operation": "process",
                    "messaging.destination.name": IN_TOPIC,
                    "messaging.kafka.partition": msg.partition,
                    "messaging.kafka.offset": msg.offset,
                },
            ):
                try:
                    batch = json.loads(msg.value)
                    user_id = batch["user_id"]
                    readings = batch.get("readings", [])
                    if readings:
                        await process_window(pool, producer, user_id, readings)
                except Exception as e:
                    log.exception("processing failed: %s", e)


async def main():
    log.info("OTLP endpoint: %s", OTLP_ENDPOINT)
    log.info("Connecting Postgres: %s", PG_DSN)
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

    consume_task = asyncio.create_task(consume(pool, producer, consumer))
    await stop.wait()
    log.info("shutting down")
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass
    await consumer.stop()
    await producer.stop()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
