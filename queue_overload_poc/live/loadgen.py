"""Tier 2 orchestrator: produce a burst into Kafka, consume it, report one JSON line per run.

    python loadgen.py all          every live run
    python loadgen.py L2 L4        just these

Each run gets its own topic so offsets start clean and runs cannot contaminate
one another. Rates are scaled for a laptop — the point is to reproduce the SHAPE
the deterministic tier establishes, not to benchmark Kafka or PostgreSQL.

    C*            8 useful concurrent slots at the dependency
    service       25 ms of real PostgreSQL work per write
    capacity      8 / 0.025  =  320 orders/sec
    normal        267/sec   (below capacity, as in Tier 1's S1)
    spike         640/sec   (2x capacity, as in Tier 1's S2)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import psycopg

from consumer import run_consumer, CRITICAL, DEFERRABLE

BOOTSTRAP = os.environ.get("BOOTSTRAP", "kafka:9092")
DSN = os.environ.get("DSN", "postgresql://postgres:lab@postgres:5432/fulfilment")
C_STAR = int(os.environ.get("C_STAR", "8"))
SERVICE_MS = int(os.environ.get("SERVICE_MS", "25"))

# Nominal capacity from the configured slot time. The *achieved* capacity is
# always a little lower: a real write is pg_sleep + INSERT + COMMIT + a network
# round trip, which measured ~29 ms against a nominal 25 ms. Tier 1 runs the
# healthy case at 83% of capacity because it has no such overhead; doing that
# here would leave ~4% headroom and the "healthy" run would quietly be overloaded.
# So the live lab runs the baseline at a deliberately lower utilisation and says
# so, rather than reporting a healthy run that is not healthy.
CAPACITY = C_STAR / (SERVICE_MS / 1000.0)      # 320/s nominal with the defaults
NORMAL = round(CAPACITY * 0.55)                # 176/s — comfortably inside real capacity
SPIKE = round(CAPACITY * 2)                    # 640/s — unambiguously past it
DEFERRABLE_FRACTION = 0.55
PARTITIONS = 8
# The prefetch bound for every run except the deliberately unbounded one. It is
# a real bound — work above it stays in Kafka, durable and visible in lag —
# without being so small that the poll loop cannot keep the fleet fed.
BOUND = 64                   # bounded prefetch: at most 64 fetched-but-unwritten

# id, what changes, produce rate, seconds, workers, local prefetch bound, priority, admission
RUNS = {
    "L1": dict(name="healthy baseline", rate=NORMAL, secs=12, workers=C_STAR,
               local=BOUND, priority=False, admission=None),
    "L2": dict(name="sustained overload", rate=SPIKE, secs=12, workers=C_STAR,
               local=BOUND, priority=False, admission=None),
    "L3": dict(name="naive consumer scaling", rate=SPIKE, secs=12, workers=C_STAR * 8,
               local=4096, priority=False, admission=None),
    "L4": dict(name="bounded consumption", rate=SPIKE, secs=12, workers=C_STAR * 8,
               local=BOUND, priority=False, admission=None),
    "L5": dict(name="priority only (control)", rate=SPIKE, secs=12, workers=C_STAR * 8,
               local=BOUND, priority=True, admission=None),
    "L6": dict(name="priority + admission", rate=SPIKE, secs=12, workers=C_STAR * 8,
               local=BOUND, priority=True, admission=1500),
}


def make_topic(admin, name):
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=PARTITIONS, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    for _ in range(40):                       # wait for metadata to settle
        if name in admin.list_topics():
            return
        time.sleep(0.25)


def produce(topic, rate, secs, run_id):
    """Emit at `rate`/s for `secs`, carrying an explicit enqueued_at.

    The payload timestamp is deliberate: Kafka's own record timestamp defaults to
    CreateTime (producer clock) and LogAppendTime would measure broker residence.
    Neither is the end-to-end queue delay the SLO cares about, so the lab carries
    its own and says so.
    """
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=5,
                      value_serializer=lambda v: json.dumps(v).encode())
    total = int(rate * secs)
    gap = 1.0 / rate
    start = time.time()
    for i in range(total):
        cls = DEFERRABLE if (i % 100) < int(DEFERRABLE_FRACTION * 100) else CRITICAL
        p.send(topic, {"order_id": int(time.time() * 1000) % 10**9 * 1000 + i,
                       "cls": cls, "run_id": run_id, "enqueued_at": time.time()})
        nxt = start + (i + 1) * gap
        drift = nxt - time.time()
        if drift > 0:
            time.sleep(drift)
    p.flush()
    p.close()
    return total


def db_rows(run_id):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(max(queue_ms),0), coalesce(round(avg(queue_ms)),0) "
                    "FROM fulfilment WHERE run_id = %s", (run_id,))
        return cur.fetchone()


def one(rid):
    cfg = RUNS[rid]
    run_id = f"{rid}-{int(time.time())}"
    topic = f"orders-{run_id}"
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    make_topic(admin, topic)

    # The producer runs as its OWN PROCESS, not a thread. As a thread it shares
    # the GIL with the consumer's worker pool, and a paced producer waking 640
    # times a second starves them badly enough that the run measures Python
    # scheduling instead of the dependency — throughput came out ~9x low.
    prod = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--produce",
                             topic, str(cfg["rate"]), str(cfg["secs"]), run_id])
    time.sleep(1.0)                            # let the first records land

    expect = int(cfg["rate"] * cfg["secs"])
    res = run_consumer(bootstrap=BOOTSTRAP, topic=topic, group=f"g-{run_id}", dsn=DSN,
                       run_id=run_id, workers=cfg["workers"], c_star=C_STAR,
                       service_ms=SERVICE_MS, prefetch=cfg["local"], expect=expect,
                       priority=cfg["priority"], admission_ms=cfg["admission"],
                       deadline_s=cfg["secs"] + 90)
    prod.wait(timeout=cfg["secs"] + 60)
    rows, max_q, avg_q = db_rows(run_id)

    out = {"id": rid, "name": cfg["name"], "produced": int(cfg["rate"] * cfg["secs"]),
           "workers": cfg["workers"], "prefetch_bound": cfg["local"],
           "priority": cfg["priority"], "admission_ms": cfg["admission"],
           "capacity_per_s": round(CAPACITY), "rate_per_s": cfg["rate"],
           "c_star": C_STAR, "service_ms": SERVICE_MS,
           "db_rows": rows, "db_max_queue_ms": int(max_q), "db_avg_queue_ms": int(avg_q),
           **res}
    try:
        admin.delete_topics([topic])
    except Exception:
        pass
    admin.close()
    return out


def main(argv):
    if argv and argv[0] == "--produce":                 # child process: produce and exit
        topic, rate, secs, run_id = argv[1], int(argv[2]), int(argv[3]), argv[4]
        produce(topic, rate, secs, run_id)
        return 0
    ids = [a for a in argv if a in RUNS] or (list(RUNS) if "all" in argv or not argv else [])
    if not ids:
        print(f"usage: loadgen.py all | {' '.join(RUNS)}", file=sys.stderr)
        return 2
    print(f"# capacity {round(CAPACITY)}/s (C*={C_STAR} x {SERVICE_MS}ms) · "
          f"normal {NORMAL}/s · spike {SPIKE}/s")
    results = []
    for rid in ids:
        r = one(rid)
        results.append(r)
        print(json.dumps(r), flush=True)

    print("\nSUMMARY")
    hdr = (f"{'run':4} {'name':24} {'rate/s':>7} {'workers':>8} {'processed':>10} "
           f"{'thr/s':>7} {'rt p95':>8} {'svc p50':>8} {'prefetch':>9} {'age p95':>9} {'deferred':>9}")
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        print(f"{r['id']:4} {r['name']:24} {r['rate_per_s']:>7} {r['workers']:>8} "
              f"{r['processed']:>10} {r['throughput_per_s']:>7} {r['round_trip_p95_ms']:>8} "
              f"{r['db_service_p50_ms']:>8} {r['peak_unprocessed']:>9} "
              f"{r['queue_delay_p95_ms']:>9} {r['deferred']:>9}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
