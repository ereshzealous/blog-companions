"""The consumer side of the live lab.

Design notes, because the first version of this file taught the wrong lesson.

**The dependency's useful concurrency is a real semaphore.** `C_STAR` slots, each
held for `SERVICE_MS` of actual PostgreSQL work (`pg_sleep` inside the
transaction). A worker that finds every slot taken blocks, exactly as it would on
a saturated connection pool. That is what makes "more consumers do not add
capacity" a measurement rather than an assertion.

**Prefetch is bounded by `max_poll_records`, and a batch is fully processed
before the next fetch.** Fetch at most N, process those N across the worker pool,
commit, fetch again. Records fetched but not yet written are the "hidden queue"
the article warns about, and `max_poll_records` is exactly its bound.

An earlier version held that bound with a long-lived local queue plus
`pause()`/`resume()`. That is closer to how you would really write this, and it
was unmeasurable here: a blocking poll on paused partitions still costs its
timeout, which starved the worker pool so badly that the *smaller* fleet looked
faster than the larger one — the opposite of the result under test. Pausing
remains the right production advice and the README says so; this file trades that
fidelity for a measurement not dominated by its own instrumentation.

**Offsets are committed after processing, never on a timer.** Auto-commit is off;
a batch is acknowledged only once every row in it is in `fulfilment`.
"""
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import psycopg
from kafka import KafkaConsumer

CRITICAL, DEFERRABLE = "critical", "deferrable"


@dataclass
class Stats:
    processed: int = 0
    deferred: int = 0
    by_class: dict = field(default_factory=lambda: {CRITICAL: 0, DEFERRABLE: 0})
    round_trip_ms: list = field(default_factory=list)   # pick-up -> row written (includes slot wait)
    service_ms: list = field(default_factory=list)      # time inside the DB call
    queue_delay_ms: list = field(default_factory=list)  # produced -> processed, end to end
    peak_unprocessed: int = 0                           # fetched but not yet written
    peak_db_inflight: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


def pct(xs, p):
    if not xs:
        return 0.0
    s = sorted(xs)
    return round(s[min(len(s) - 1, int(len(s) * p))], 1)


class Dependency:
    """PostgreSQL behind a fixed number of useful concurrent slots."""

    def __init__(self, c_star, service_ms):
        self.slots = threading.Semaphore(c_star)
        self.service_s = service_ms / 1000.0
        self.inflight = 0
        self.lock = threading.Lock()

    def write(self, conn, rec, run_id, queue_ms, stats):
        with self.slots:                       # a worker beyond C* waits here
            with self.lock:
                self.inflight += 1
                if self.inflight > stats.peak_db_inflight:
                    stats.peak_db_inflight = self.inflight
            t0 = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    # pg_sleep runs server-side inside the transaction, so the slot
                    # is held for real database time, not a client-side sleep.
                    cur.execute("SELECT pg_sleep(%s)", (self.service_s,))
                    cur.execute(
                        "INSERT INTO fulfilment (order_id, cls, run_id, queue_ms) "
                        "VALUES (%s,%s,%s,%s) ON CONFLICT (order_id) DO NOTHING",
                        (rec["order_id"], rec["cls"], run_id, int(queue_ms)))
                conn.commit()
            finally:
                with self.lock:
                    self.inflight -= 1
            return (time.perf_counter() - t0) * 1000


def run_consumer(*, bootstrap, topic, group, dsn, run_id, workers, c_star, service_ms,
                 prefetch, expect, priority=False, admission_ms=None, deadline_s=120):
    """Consume `expect` records. Returns the measured behaviour.

    `prefetch`     — max records fetched but not yet written (the hidden queue's bound)
    `priority`     — process critical before deferrable within each batch
    `admission_ms` — defer deferrable records already older than this; critical
                     work is never deferred
    """
    stats = Stats()
    dep = Dependency(c_star, service_ms)
    tls = threading.local()

    def conn_for_thread():
        if not hasattr(tls, "conn"):
            tls.conn = psycopg.connect(dsn, autocommit=False)
        return tls.conn

    def handle(rec):
        delay_ms = (time.time() - rec["enqueued_at"]) * 1000
        if admission_ms is not None and rec["cls"] == DEFERRABLE and delay_ms > admission_ms:
            with stats.lock:
                stats.deferred += 1
            return
        t0 = time.perf_counter()
        svc = dep.write(conn_for_thread(), rec, run_id, delay_ms, stats)
        rt = (time.perf_counter() - t0) * 1000
        with stats.lock:
            stats.processed += 1
            stats.by_class[rec["cls"]] = stats.by_class.get(rec["cls"], 0) + 1
            stats.round_trip_ms.append(rt)
            stats.service_ms.append(svc)
            stats.queue_delay_ms.append(delay_ms)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,              # commit only after the rows are written
        auto_offset_reset="earliest",
        max_poll_records=prefetch,             # THE bound on fetched-but-unprocessed work
        fetch_max_wait_ms=100,
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    lag_samples = []
    t_start = time.time()
    t_end = t_start + deadline_s
    last_lag = 0.0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        while time.time() < t_end and stats.processed + stats.deferred < expect:
            batch = consumer.poll(timeout_ms=200, max_records=prefetch)
            recs = [r.value for parts in batch.values() for r in parts]
            if recs:
                if priority:
                    recs.sort(key=lambda v: 0 if v["cls"] == CRITICAL else 1)
                with stats.lock:
                    stats.peak_unprocessed = max(stats.peak_unprocessed, len(recs))
                list(pool.map(handle, recs))   # the whole batch, then commit
                try:
                    consumer.commit()
                except Exception:
                    pass

            now = time.time()
            if now - last_lag > 1.0:           # lag is a broker round trip: sample it
                last_lag = now
                try:
                    assign = consumer.assignment()
                    if assign:
                        ends = consumer.end_offsets(list(assign))
                        lag_samples.append(sum(ends[tp] - (consumer.position(tp) or 0)
                                               for tp in assign))
                except Exception:
                    pass

    elapsed = time.time() - t_start
    try:
        consumer.commit()
    except Exception:
        pass
    consumer.close()

    return {
        "elapsed_s": round(elapsed, 2),
        "throughput_per_s": round(stats.processed / elapsed, 1) if elapsed > 0 else 0.0,
        "processed": stats.processed,
        "deferred": stats.deferred,
        "by_class": stats.by_class,
        "round_trip_p50_ms": pct(stats.round_trip_ms, 0.50),
        "round_trip_p95_ms": pct(stats.round_trip_ms, 0.95),
        "db_service_p50_ms": pct(stats.service_ms, 0.50),
        "queue_delay_p95_ms": pct(stats.queue_delay_ms, 0.95),
        "peak_queue_delay_ms": round(max(stats.queue_delay_ms), 1) if stats.queue_delay_ms else 0.0,
        "peak_unprocessed": stats.peak_unprocessed,
        "peak_db_inflight": stats.peak_db_inflight,
        "peak_lag": max(lag_samples) if lag_samples else 0,
    }
