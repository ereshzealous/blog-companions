"""The one consumer loop used by lane B and lane C.

Identical for both lanes: bounded poll, batch size, flush timeout, retry and
backoff, DLQ routing and commit point. Only the `Sink` differs, so differences
between lanes come from representation and insert format, not loop mechanics.

Commit protocol, per flush:
  prepare (decode/validate/build/transform/serialize)
  -> insert, retried while the error is retriable
  -> DLQ produce + flush for rejected records
  -> commit offsets (next offset per partition)
A crash after the ClickHouse acknowledgement and before the commit re-delivers
the batch on restart: a physical duplicate with the same event_id. The fault
`after_insert_before_commit` reproduces exactly that window.
"""

from __future__ import annotations

import json
import os
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from typing import Callable, Protocol

from connectedfleet.kafka_io import Record

FAULT_AFTER_INSERT_BEFORE_COMMIT = "after_insert_before_commit"
FAULT_EXIT_CODE = 86


class InsertFailed(Exception):
    def __init__(self, message: str, retriable: bool) -> None:
        super().__init__(message)
        self.retriable = retriable


@dataclass
class Prepared:
    topic: str
    table: str
    columns: list[str]
    payload: object
    rows: int
    rejects: list  # codec Reject objects; .index refers to the values list passed to prepare()
    phase_ns: dict[str, int]
    payload_bytes: int | None = None


class Sink(Protocol):
    name: str

    def prepare(self, topic: str, values: list[bytes]) -> Prepared: ...

    def insert(self, prepared: Prepared) -> int: ...


class Consumer(Protocol):
    def consume(self, max_records: int, timeout_s: float) -> list[Record]: ...

    def commit(self, offsets: list[tuple[str, int, int]]) -> None: ...


class Producer(Protocol):
    def produce(self, topic: str, key, value, headers=None) -> None: ...

    def flush(self, timeout_s: float = 60.0) -> None: ...


@dataclass
class LoopConfig:
    batch_size: int = 1000
    flush_timeout_ms: int = 1000
    poll_timeout_s: float = 0.5
    max_retries: int = 5
    backoff_ms: int = 200
    idle_exit_s: float = 0.0
    max_records: int = 0  # stop after this many records were committed (0 = unbounded)
    fault: str | None = None
    fault_after_batches: int = 1
    dlq_topic: str = ""


@dataclass
class LoopStats:
    consumed: int = 0
    inserted_rows: int = 0
    rejected: int = 0
    tombstones: int = 0
    flushes: int = 0
    retries: int = 0
    committed: dict[str, int] = field(default_factory=dict)


def _group_by_topic(records: list[Record]) -> OrderedDict[str, list[Record]]:
    grouped: OrderedDict[str, list[Record]] = OrderedDict()
    for record in records:
        grouped.setdefault(record.topic, []).append(record)
    return grouped


def next_offsets(records: list[Record]) -> list[tuple[str, int, int]]:
    positions: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.topic, r.partition)
        positions[key] = max(positions.get(key, -1), r.offset + 1)
    return [(t, p, o) for (t, p), o in sorted(positions.items())]


class Loop:
    def __init__(
        self,
        consumer: Consumer,
        sink: Sink,
        dlq: Producer,
        config: LoopConfig,
        metrics: Callable[[dict], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        crash: Callable[[int], None] = os._exit,
    ) -> None:
        self.consumer = consumer
        self.sink = sink
        self.dlq = dlq
        self.config = config
        self.metrics = metrics or (lambda _m: None)
        self.clock = clock
        self.sleep = sleep
        self.crash = crash
        self.stats = LoopStats()

    # ------------------------------------------------------------------ insert with retries
    def _insert(self, prepared: Prepared) -> tuple[int, int]:
        attempt = 0
        while True:
            try:
                return self.sink.insert(prepared), attempt
            except InsertFailed as failure:
                if not failure.retriable or attempt >= self.config.max_retries:
                    raise
                self.stats.retries += 1
                self.sleep(self.config.backoff_ms / 1000 * (2**attempt))
                attempt += 1

    # ------------------------------------------------------------------ one flush
    def flush(self, records: list[Record]) -> None:
        started = time.perf_counter_ns()
        rejected_any = False
        for topic, topic_records in _group_by_topic(records).items():
            live = [r for r in topic_records if r.value is not None]
            self.stats.tombstones += len(topic_records) - len(live)
            prepared = self.sink.prepare(topic, [r.value for r in live])
            for reject in prepared.rejects:
                source = live[reject.index]
                body = {
                    "error_code": reject.reason,
                    "field": reject.field,
                    "source_topic": source.topic,
                    "source_partition": source.partition,
                    "source_offset": source.offset,
                    "stage": f"lane-{self.sink.name}",
                }
                headers = [("cf.error.code", reject.reason.encode()), ("cf.source.offset", str(source.offset).encode())]
                self.dlq.produce(self.config.dlq_topic, source.key, json.dumps(body).encode(), headers)
                rejected_any = True
            self.stats.rejected += len(prepared.rejects)
            insert_ns, attempts = (0, 0)
            if prepared.rows:
                insert_ns, attempts = self._insert(prepared)
                self.stats.inserted_rows += prepared.rows
            self.metrics(
                {
                    "ts": time.time(),
                    "lane": self.sink.name,
                    "topic": topic,
                    "table": prepared.table,
                    "records": len(topic_records),
                    "rows": prepared.rows,
                    "rejects": len(prepared.rejects),
                    **{f"{k}_ns": v for k, v in prepared.phase_ns.items()},
                    "insert_ns": insert_ns,
                    "payload_bytes": prepared.payload_bytes,
                    "attempts": attempts + 1 if prepared.rows else 0,
                }
            )

        self.stats.flushes += 1
        if self.config.fault == FAULT_AFTER_INSERT_BEFORE_COMMIT and self.stats.flushes >= self.config.fault_after_batches:
            # Deliberate crash inside the window: ClickHouse has acknowledged, Kafka offsets are not committed.
            self.metrics({"ts": time.time(), "lane": self.sink.name, "event": "fault_injected", "fault": self.config.fault})
            self.crash(FAULT_EXIT_CODE)
            return

        if rejected_any:
            self.dlq.flush()
        commit_started = time.perf_counter_ns()
        offsets = next_offsets(records)
        self.consumer.commit(offsets)
        for topic, partition, offset in offsets:
            self.stats.committed[f"{topic}[{partition}]"] = offset
        self.stats.consumed += len(records)
        self.metrics(
            {
                "ts": time.time(),
                "lane": self.sink.name,
                "event": "committed",
                "records": len(records),
                "commit_ns": time.perf_counter_ns() - commit_started,
                "flush_total_ns": time.perf_counter_ns() - started,
            }
        )

    # ------------------------------------------------------------------ main loop
    def run(self) -> LoopStats:
        buffer: list[Record] = []
        deadline = 0.0
        last_activity = self.clock()
        cfg = self.config
        flush_s = cfg.flush_timeout_ms / 1000
        while True:
            now = self.clock()
            if buffer:
                wait = max(0.0, min(cfg.poll_timeout_s, deadline - now))
            else:
                # consume(n, timeout) blocks the whole timeout unless n records arrive, so an empty-buffer poll must
                # not outlast the flush timeout, and the deadline counts from the start of the call (Lab 03 finding).
                wait = min(cfg.poll_timeout_s, flush_s)
            call_started = now
            records = self.consumer.consume(cfg.batch_size - len(buffer), wait)
            now = self.clock()
            if records:
                if not buffer:
                    deadline = call_started + flush_s
                buffer.extend(records)
                last_activity = now
            if buffer and (len(buffer) >= cfg.batch_size or now >= deadline):
                self.flush(buffer)
                buffer = []
                if cfg.max_records and self.stats.consumed >= cfg.max_records:
                    break
                continue
            if not records and not buffer and cfg.idle_exit_s and now - last_activity >= cfg.idle_exit_s:
                break
        return self.stats


def jsonl_writer(path: str | None) -> Callable[[dict], None]:
    if not path:
        return lambda _m: None
    handle = open(path, "a", encoding="utf-8", buffering=1)

    def write(metric: dict) -> None:
        handle.write(json.dumps(metric) + "\n")

    return write


def stats_dict(stats: LoopStats) -> dict:
    return asdict(stats)
