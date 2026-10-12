"""The shared lane loop: commit only after ClickHouse acknowledges; DLQ, retries, crash window, flush triggers."""

import pytest

from connectedfleet.codec.telemetry import Reject
from connectedfleet.kafka_io import Record
from connectedfleet.lanes.loop import (
    FAULT_AFTER_INSERT_BEFORE_COMMIT,
    FAULT_EXIT_CODE,
    InsertFailed,
    Loop,
    LoopConfig,
    Prepared,
    next_offsets,
)


class FakeConsumer:
    def __init__(self, batches, log):
        self.batches = list(batches)
        self.log = log
        self.commits = []

    def consume(self, max_records, timeout_s):
        if not self.batches:
            return []
        batch = self.batches.pop(0)
        assert len(batch) <= max_records
        return batch

    def commit(self, offsets):
        self.log.append(("commit", tuple(offsets)))
        self.commits.append(offsets)


class FakeSink:
    name = "fake"

    def __init__(self, log, failures=(), reject_index=None):
        self.log = log
        self.failures = list(failures)
        self.reject_index = reject_index

    def prepare(self, topic, values):
        rejects = [Reject(self.reject_index, "BAD_TYPE", "battery_pct")] if self.reject_index is not None else []
        rows = len(values) - len(rejects)
        return Prepared(topic, "telemetry_fact", ["event_id"], values, rows, rejects, {"decode": 1}, 10)

    def insert(self, prepared):
        if self.failures:
            raise self.failures.pop(0)
        self.log.append(("insert", prepared.rows))
        return 5


class FakeDLQ:
    def __init__(self, log):
        self.log = log

    def produce(self, topic, key, value, headers=None):
        self.log.append(("dlq", topic))

    def flush(self, timeout_s=60.0):
        self.log.append(("dlq_flush",))


def _records(n, start=0, topic="cf.telemetry.v1", partition=0):
    return [Record(topic, partition, start + i, b"k", b"{}") for i in range(n)]


def _loop(batches, sink_kwargs=None, **config):
    log = []
    consumer = FakeConsumer(batches, log)
    crashes = []
    loop = Loop(consumer, FakeSink(log, **(sink_kwargs or {})), FakeDLQ(log),
                LoopConfig(dlq_topic="cf.dlq.lane-b.v1", **config), sleep=lambda s: None, crash=crashes.append)
    return loop, consumer, log, crashes


def test_commit_happens_after_insert_with_next_offsets():
    loop, consumer, log, _ = _loop([_records(3)], batch_size=3, idle_exit_s=0.01, poll_timeout_s=0)
    loop.run()
    assert [e[0] for e in log] == ["insert", "commit"]
    assert consumer.commits == [[("cf.telemetry.v1", 0, 3)]]


def test_next_offsets_per_partition():
    records = _records(2, 10, partition=0) + _records(3, 7, partition=1)
    assert next_offsets(records) == [("cf.telemetry.v1", 0, 12), ("cf.telemetry.v1", 1, 10)]


def test_rejects_go_to_dlq_and_are_flushed_before_commit():
    loop, _, log, _ = _loop([_records(3)], {"reject_index": 1}, batch_size=3, idle_exit_s=0.01, poll_timeout_s=0)
    loop.run()
    kinds = [e[0] for e in log]
    assert kinds == ["dlq", "insert", "dlq_flush", "commit"]
    assert loop.stats.rejected == 1 and loop.stats.inserted_rows == 2


def test_retriable_failure_is_retried_then_committed():
    failures = [InsertFailed("503", retriable=True), InsertFailed("503", retriable=True)]
    loop, consumer, _, _ = _loop([_records(2)], {"failures": failures}, batch_size=2, idle_exit_s=0.01, poll_timeout_s=0)
    loop.run()
    assert loop.stats.retries == 2 and consumer.commits


def test_non_retriable_failure_stops_without_commit():
    loop, consumer, _, _ = _loop([_records(2)], {"failures": [InsertFailed("bad type", retriable=False)]}, batch_size=2,
                                 poll_timeout_s=0)
    with pytest.raises(InsertFailed):
        loop.run()
    assert consumer.commits == []


def test_fault_crashes_inside_the_window_without_commit():
    loop, consumer, log, crashes = _loop([_records(2), _records(2, 2)], batch_size=2, poll_timeout_s=0, idle_exit_s=0.01,
                                         fault=FAULT_AFTER_INSERT_BEFORE_COMMIT, fault_after_batches=1)
    loop.flush(_records(2))
    assert crashes == [FAULT_EXIT_CODE]
    assert ("insert", 2) in log and consumer.commits == []


def test_flush_on_deadline_with_partial_batch():
    now = [0.0]

    def clock():
        now[0] += 0.6
        return now[0]

    log = []
    consumer = FakeConsumer([_records(1), [], []], log)
    loop = Loop(consumer, FakeSink(log), FakeDLQ(log),
                LoopConfig(batch_size=100, flush_timeout_ms=1000, poll_timeout_s=0, idle_exit_s=5, dlq_topic="d"),
                clock=clock, sleep=lambda s: None, crash=lambda c: None)
    loop.run()
    assert ("insert", 1) in log and consumer.commits


def test_max_records_stops_the_loop():
    loop, consumer, _, _ = _loop([_records(2), _records(2, 2), _records(2, 4)], batch_size=2, poll_timeout_s=0, max_records=4)
    stats = loop.run()
    assert stats.consumed == 4 and len(consumer.commits) == 2


def test_tombstones_are_counted_not_inserted():
    records = _records(2) + [Record("cf.telemetry.v1", 0, 2, b"k", None)]
    loop, _, log, _ = _loop([records], batch_size=3, idle_exit_s=0.01, poll_timeout_s=0)
    loop.run()
    assert loop.stats.tombstones == 1 and ("insert", 2) in log


def test_first_records_never_wait_longer_than_the_flush_timeout():
    """librdkafka's consume(n, timeout) blocks the whole timeout unless n records arrive. With an empty buffer the
    loop used to block poll_timeout_s (0.5 s) before starting the flush timer, so a 250 ms flush behaved like
    ~0.75 s (Lab 03, 2026-10-10). The empty-buffer poll is capped at the flush timeout and the deadline counts
    from the start of that call."""
    now = [0.0]
    timeouts, flushed_at = [], []

    class BlockingConsumer(FakeConsumer):
        def consume(self, max_records, timeout_s):
            timeouts.append(timeout_s)
            now[0] += timeout_s  # fewer than max_records arrived: the call blocks for its whole timeout
            return super().consume(max_records, timeout_s)

    class TimedSink(FakeSink):
        def insert(self, prepared):
            flushed_at.append(now[0])
            return super().insert(prepared)

    log = []
    consumer = BlockingConsumer([_records(3), [], [], []], log)
    loop = Loop(consumer, TimedSink(log), FakeDLQ(log),
                LoopConfig(batch_size=1000, flush_timeout_ms=250, poll_timeout_s=0.5, idle_exit_s=2, dlq_topic="d"),
                clock=lambda: now[0], sleep=lambda s: None, crash=lambda c: None)
    loop.run()
    assert timeouts[0] <= 0.25
    assert flushed_at and flushed_at[0] <= 0.25 + 1e-9
    assert ("insert", 3) in log and consumer.commits
