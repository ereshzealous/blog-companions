"""Lane A connector variants for Lab 03 (official ClickHouse Kafka Connect Sink 1.6.0).

  sink         as first benchmarked: the connector buffers records (bufferCount = batch, bufferFlushTime =
               flush timeout). Its last partial buffer is flushed only when new records arrive or on Kafka
               Connect's offset flush (60 s by default), so a burst's tail can wait up to a minute.
  sink_tuned   no connector buffer (bufferCount = 0): every poll is inserted at once. Kafka's own fetch batching
               sets the insert size and wait: max.poll.records = batch, fetch.max.wait.ms = flush timeout,
               fetch.min.bytes ~ one batch of canonical telemetry JSON.
  sink_skip    `sink` plus ClickHouse `input_format_allow_errors_num`: rows ClickHouse cannot parse are skipped by
               the server, so good rows are stored, but a bad row disappears without a DLQ record. Measured only to
               show that trade-off.

The connector sends a failed batch to its DLQ as a whole ("a DLQ will be used for failed batches",
clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink); no variant changes that.
"""

from __future__ import annotations

from connectedfleet import topics

SINK_LANES = ("sink", "sink_tuned", "sink_skip")
APPROX_EVENT_BYTES = 300  # canonical telemetry JSON, measured range 280-330 bytes
ALLOW_ERRORS_NUM = 1000


def is_sink(lane: str) -> bool:
    return lane in SINK_LANES


def db_for(lane: str) -> str:
    return topics.LANE_DATABASES["sink" if is_sink(lane) else lane]


def overrides(variant: str, topic: str, table: str, database: str, batch: int, flush_ms: int,
              base_clickhouse_settings: str) -> dict:
    """Connector config overrides for one benchmark case (all values are strings, as Kafka Connect expects)."""
    if variant not in SINK_LANES:
        raise ValueError(f"unknown sink variant {variant!r}; expected one of {SINK_LANES}")
    common = {
        "topics": topic,
        "topic2TableMap": f"{topic}={table}",
        "database": database,
        "consumer.override.max.poll.records": str(batch),
        "consumer.override.max.partition.fetch.bytes": str(64 * 1024 * 1024),
        "consumer.override.fetch.max.bytes": str(128 * 1024 * 1024),
        "ignorePartitionsWhenBatching": "true",
    }
    if variant == "sink_tuned":
        return {**common, "bufferCount": "0",
                "consumer.override.fetch.max.wait.ms": str(flush_ms),
                "consumer.override.fetch.min.bytes": str(batch * APPROX_EVENT_BYTES)}
    buffered = {**common, "bufferCount": str(batch), "bufferFlushTime": str(flush_ms)}
    if variant == "sink_skip":
        buffered["clickhouseSettings"] = f"{base_clickhouse_settings},input_format_allow_errors_num={ALLOW_ERRORS_NUM}"
    return buffered
