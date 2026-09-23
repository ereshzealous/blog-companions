#!/usr/bin/env python3
"""Synthetic operational write load for PostgreSQL.

Targets a total change rate across worker processes and records the row changes PostgreSQL actually
committed. Mix by changes: device_status updates 88%, device_registry updates 8%, inserts 3%, deletes 1%.
Live controls: /control/generator-rate overrides the target rate (0 pauses);
/control/generator-v2 makes writes populate device_registry.firmware_channel.
"""
import multiprocessing as mp
import os
import random
import signal
import time

import psycopg

from common import append_jsonl, control_value, now_ms, pg_dsn

WORKERS = int(os.environ.get("GENERATOR_WORKERS", "6"))
DEFAULT_RATE = float(os.environ.get("GENERATOR_RATE", "20000"))
REGISTRY_ROWS = int(os.environ.get("SEED_REGISTRY_ROWS", "2000000"))
STATUS_ROWS = int(os.environ.get("SEED_STATUS_ROWS", "500000"))
OPS = [("status_update", 0.88, 100), ("registry_update", 0.08, 50), ("registry_insert", 0.03, 25), ("registry_delete", 0.01, 10)]
KINDS = [name for name, _, _ in OPS]

UPDATE_STATUS = """
UPDATE ops.device_status AS s
SET battery_pct = v.battery, signal_dbm = v.sig, connectivity = v.conn, last_seen_ms = v.ts, updated_at_ms = v.ts
FROM unnest(%s::bigint[], %s::smallint[], %s::smallint[], %s::text[], %s::bigint[]) AS v(device_id, battery, sig, conn, ts)
WHERE s.device_id = v.device_id"""
UPDATE_REGISTRY = """
UPDATE ops.device_registry AS r
SET status = v.status, firmware_version = v.fw, updated_at_ms = v.ts
FROM unnest(%s::bigint[], %s::text[], %s::text[], %s::bigint[]) AS v(device_id, status, fw, ts)
WHERE r.device_id = v.device_id"""
UPDATE_REGISTRY_V2 = """
UPDATE ops.device_registry AS r
SET status = v.status, firmware_version = v.fw, updated_at_ms = v.ts, firmware_channel = v.channel
FROM unnest(%s::bigint[], %s::text[], %s::text[], %s::bigint[], %s::text[]) AS v(device_id, status, fw, ts, channel)
WHERE r.device_id = v.device_id"""
INSERT_REGISTRY = """
INSERT INTO ops.device_registry (device_id, tenant_id, serial_number, model, gateway_id, status, firmware_version, updated_at_ms)
SELECT * FROM unnest(%s::bigint[], %s::int[], %s::text[], %s::text[], %s::bigint[], %s::text[], %s::text[], %s::bigint[])"""
INSERT_REGISTRY_V2 = """
INSERT INTO ops.device_registry (device_id, tenant_id, serial_number, model, gateway_id, status, firmware_version, updated_at_ms, firmware_channel)
SELECT * FROM unnest(%s::bigint[], %s::int[], %s::text[], %s::text[], %s::bigint[], %s::text[], %s::text[], %s::bigint[], %s::text[])"""
DELETE_REGISTRY = "DELETE FROM ops.device_registry WHERE device_id = ANY(%s::bigint[])"


def target_rate() -> float:
    return float(control_value("generator-rate", DEFAULT_RATE))


def worker(wid: int, stop, counters) -> None:
    rnd = random.Random(wid * 7919 + time.time_ns())
    next_id = 100_000_000 + wid * 10_000_000
    weights = [w for _, w, _ in OPS]
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with psycopg.connect(pg_dsn("app")) as conn:
        cur = conn.cursor()
        pace_at, rate, rate_checked = time.monotonic(), target_rate() / WORKERS, 0.0
        while not stop.is_set():
            now = time.monotonic()
            if now - rate_checked > 1:
                rate, rate_checked = target_rate() / WORKERS, now
            if rate <= 0:
                time.sleep(0.2)
                pace_at = time.monotonic()
                continue
            k = rnd.choices(range(len(OPS)), weights)[0]
            name, _, size = OPS[k]
            v2 = control_value("generator-v2") is not None
            ts = now_ms()
            try:
                if name == "status_update":
                    ids = sorted(rnd.sample(range(1, STATUS_ROWS + 1), size))
                    cur.execute(UPDATE_STATUS, (ids, [rnd.randint(5, 100) for _ in ids], [rnd.randint(-115, -50) for _ in ids],
                                                [rnd.choice(("lte", "wifi", "ethernet", "offline")) for _ in ids], [ts] * size))
                elif name == "registry_update":
                    ids = sorted(rnd.sample(range(1, REGISTRY_ROWS + 1), size))
                    args = [ids, [rnd.choice(("active", "active", "maintenance", "retired")) for _ in ids],
                            [f"fw-5.{rnd.randint(0, 9)}.{rnd.randint(0, 20)}" for _ in ids], [ts] * size]
                    if v2:
                        cur.execute(UPDATE_REGISTRY_V2, args + [[rnd.choice(("stable", "beta", "canary")) for _ in ids]])
                    else:
                        cur.execute(UPDATE_REGISTRY, args)
                elif name == "registry_insert":
                    ids = list(range(next_id, next_id + size))
                    next_id += size
                    args = [ids, [1 + i % 500 for i in ids], [f"SN-{i:010d}" for i in ids], ["gateway-hub-2"] * size,
                            [1000000 + i % 20000 for i in ids], ["active"] * size, ["fw-5.0.0"] * size, [ts] * size]
                    cur.execute(INSERT_REGISTRY_V2 if v2 else INSERT_REGISTRY, args + ([["canary"] * size] if v2 else []))
                else:
                    cur.execute(DELETE_REGISTRY, (sorted(rnd.sample(range(1, REGISTRY_ROWS + 1), size)),))
                changed = cur.rowcount
                conn.commit()
            except psycopg.errors.DeadlockDetected:
                conn.rollback()
                continue
            with counters.get_lock():
                counters[wid * len(OPS) + k] += changed
            pace_at += max(changed, 1) / rate
            delay = pace_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            elif delay < -1:  # cannot keep up; do not accumulate unbounded debt
                pace_at = time.monotonic()


def main() -> None:
    stop = mp.Event()
    counters = mp.Array("q", WORKERS * len(OPS))
    procs = [mp.Process(target=worker, args=(w, stop, counters), daemon=True) for w in range(WORKERS)]
    for p in procs:
        p.start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    previous, last = [0] * len(OPS), time.monotonic()
    print(f"generator: {WORKERS} workers, target {target_rate():.0f} changes/s", flush=True)
    while not stop.is_set():
        time.sleep(5)
        with counters.get_lock():
            totals = [sum(counters[w * len(OPS) + k] for w in range(WORKERS)) for k in range(len(OPS))]
        now = time.monotonic()
        rates = {KINDS[k]: round((totals[k] - previous[k]) / (now - last), 1) for k in range(len(OPS))}
        append_jsonl("generator.jsonl", {"ts_ms": now_ms(), "target_rate": target_rate(), "changes_per_s": round(sum(rates.values()), 1),
                                         "by_kind": rates, "total_changes": sum(totals)})
        previous, last = totals, now
    for p in procs:
        p.join(timeout=10)


if __name__ == "__main__":
    main()
