"""
insight-worker — batch job. Reads vitals_rollup for the past 24h per user,
computes sleep/recovery/readiness scores, writes insights_daily.

Run as a CronJob (every hour by default).
"""

import asyncio
import logging
import os
import sys
import uuid
from datetime import date, timedelta

import asyncpg

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.trace import Status, StatusCode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("insight-worker")

SERVICE_NAME = "insight-worker"
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://wearable:wearable@postgres.infra.svc.cluster.local:5432/wearable",
)
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")


def init_tracer():
    resource = Resource.create({"service.name": SERVICE_NAME, "service.version": "0.1.0"})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)))
    trace.set_tracer_provider(provider)
    AsyncPGInstrumentor().instrument()
    return trace.get_tracer(SERVICE_NAME)


tracer = init_tracer()


def score(stats: dict) -> dict:
    """Stub: simple deterministic scoring from HR + steps."""
    hr_avg = stats.get("hr_avg") or 70
    steps = stats.get("steps") or 0
    hrv_proxy = max(0, 80 - abs(hr_avg - 60))
    sleep_score = max(0, min(100, 100 - max(0, hr_avg - 60) * 2))
    recovery_score = max(0, min(100, hrv_proxy + (5 if 8000 < steps < 15000 else 0)))
    readiness = int((sleep_score + recovery_score) / 2)
    summary = "rested" if readiness > 75 else ("balanced" if readiness > 50 else "fatigued")
    return {
        "sleep_score": int(sleep_score),
        "recovery_score": int(recovery_score),
        "readiness": readiness,
        "summary": summary,
    }


async def process_user(conn: asyncpg.Connection, user_id: uuid.UUID, day: date):
    with tracer.start_as_current_span(
        "insight.compute",
        attributes={"user.id": str(user_id), "day": str(day)},
    ) as span:
        stats = await conn.fetchrow(
            """SELECT AVG(hr_avg) AS hr_avg,
                      AVG(spo2_avg) AS spo2_avg,
                      SUM(steps) AS steps
               FROM vitals_rollup_1min
               WHERE user_id = $1
                 AND ts >= $2::date
                 AND ts <  $2::date + interval '1 day'""",
            user_id, day,
        )
        if not stats or stats["hr_avg"] is None:
            span.add_event("no data for day")
            return
        s = score({
            "hr_avg": float(stats["hr_avg"]) if stats["hr_avg"] is not None else None,
            "steps": int(stats["steps"]) if stats["steps"] is not None else 0,
        })
        await conn.execute(
            """INSERT INTO insights_daily
                 (user_id, day, sleep_score, recovery_score, readiness, summary)
               VALUES ($1,$2,$3,$4,$5,$6)
               ON CONFLICT (user_id, day) DO UPDATE SET
                 sleep_score=EXCLUDED.sleep_score,
                 recovery_score=EXCLUDED.recovery_score,
                 readiness=EXCLUDED.readiness,
                 summary=EXCLUDED.summary""",
            user_id, day, s["sleep_score"], s["recovery_score"], s["readiness"], s["summary"],
        )
        span.set_status(Status(StatusCode.OK))


async def main():
    target_day = date.today() - timedelta(days=0)
    log.info("Computing insights for %s, OTLP=%s", target_day, OTLP_ENDPOINT)
    with tracer.start_as_current_span("insight.batch", attributes={"day": str(target_day)}):
        conn = await asyncpg.connect(dsn=PG_DSN)
        try:
            users = await conn.fetch("SELECT id FROM users")
            log.info("Processing %d users", len(users))
            for row in users:
                await process_user(conn, row["id"], target_day)
        finally:
            await conn.close()
    log.info("done")
    # Force flush before container exits.
    trace.get_tracer_provider().shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("insight-worker failed")
        sys.exit(1)
