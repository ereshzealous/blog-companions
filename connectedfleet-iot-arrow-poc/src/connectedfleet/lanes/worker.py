"""Lane worker process (B = row, C = arrow).

    python -m connectedfleet.lanes.worker --lane arrow --database bench_arrow \
        --topics cf.telemetry.v1,cf.lifecycle.v1 --group cf-lane-c --batch-size 1000 --flush-ms 1000

Environment: FAULT=after_insert_before_commit and FAULT_AFTER_BATCHES=N inject
the crash window used by Lab 02/03 (exit code 86).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import sys

from connectedfleet import topics
from connectedfleet.config import Settings
from connectedfleet.kafka_io import KafkaConsumer, KafkaProducer
from connectedfleet.lanes.loop import Loop, LoopConfig, jsonl_writer, stats_dict
from connectedfleet.lanes.sink import build_sink



# librdkafka defaults keep 100,000 messages prefetched and then postpone the next fetch by
# fetch.queue.backoff.ms = 1000: a consumer that keeps up is capped near 100K-136K messages/s (measured, D-031).
# Lane A's tuned connector sizes its Kafka fetch settings to the batch; the workers get the same treatment.
CONSUMER_CONFIG = {"max.poll.interval.ms": 300_000, "fetch.queue.backoff.ms": 10}

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ConnectedFleet custom lane worker")
    parser.add_argument("--lane", choices=("row", "arrow"), required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--topics", default=",".join(topics.CANONICAL_TOPICS))
    parser.add_argument("--group", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--flush-ms", type=int, default=1000)
    parser.add_argument("--transform", choices=("none", "risk"), default="none")
    parser.add_argument("--compute", choices=("row", "arrow"), default=None, help="override for the Lab 03 2x2 experiment")
    parser.add_argument("--insert", choices=("native", "arrowstream"), default=None, help="override for the Lab 03 2x2 experiment")
    parser.add_argument("--idle-exit", type=float, default=0.0)
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--metrics-out", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = Settings.from_env()
    sink = build_sink(args.lane, args.database, args.transform, args.compute, args.insert, settings)
    consumer = KafkaConsumer(settings.kafka_bootstrap, args.group, [t for t in args.topics.split(",") if t],
                             CONSUMER_CONFIG)
    dlq = KafkaProducer(settings.kafka_bootstrap)
    config = LoopConfig(
        batch_size=args.batch_size,
        flush_timeout_ms=args.flush_ms,
        idle_exit_s=args.idle_exit,
        max_records=args.max_records,
        fault=os.environ.get("FAULT") or None,
        fault_after_batches=int(os.environ.get("FAULT_AFTER_BATCHES", "1")),
        dlq_topic=topics.DLQ_LANE[args.lane],
    )
    metrics = jsonl_writer(args.metrics_out)
    metrics({"event": "started", "lane": sink.name, "compute": sink.compute, "insert": sink.insert_format,
             "batch_size": args.batch_size, "flush_ms": args.flush_ms, "transform": args.transform, "group": args.group})
    loop = Loop(consumer, sink, dlq, config, metrics)
    try:
        stats = loop.run()
    finally:
        consumer.close()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Process-level scope (this worker only); ru_maxrss is KiB on Linux, where the worker runs.
    metrics({"event": "stopped", "lane": sink.name, "stats": stats_dict(loop.stats),
             "cpu_user_s": usage.ru_utime, "cpu_sys_s": usage.ru_stime,
             "max_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)})
    print(json.dumps(stats_dict(stats)), file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
