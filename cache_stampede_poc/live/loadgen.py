"""loadgen: drives the live lab and reads the ground truth.

    python loadgen.py all | L1 | L2 | L3 | L4 | L6

Origin calls are counted twice: by pricing-svc and by PostgreSQL's pg_stat_statements
(calls of the price query). The live lab reproduces the mechanisms on real Redis and PostgreSQL;
it is not a benchmark. Four real app processes host 100 logical pods (coalescing scopes).
"""
import asyncio
import json
import math
import os
import random
import sys
import time

import asyncpg
import redis.asyncio as redis

import minihttp

APPS = os.environ["APPS"].split(",")
PRICING = os.environ["PRICING_URL"]
R = redis.from_url(os.environ["REDIS_URL"])
DB = os.environ["DATABASE_URL"]
LOGICAL = 25


def url(i, sku, mode):
    pod = i % 100
    return f"{APPS[pod // LOGICAL]}/price/{sku}?pod={pod % LOGICAL}&mode={mode}"


async def db_calls(conn):
    v = await conn.fetchval("SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE query LIKE '%FROM prices p, pg_sleep%'")
    return int(v)


async def reset(conn, budget=0):
    await R.flushall()
    await minihttp.post(f"{PRICING}/config?budget={budget}&queue_timeout=0.25")
    await minihttp.post(f"{PRICING}/reset")
    for a in APPS:
        await minihttp.post(f"{a}/reset")
    await conn.execute("UPDATE prices SET price = 69999, version = 10 WHERE sku = 'phone-x'")
    await conn.execute("SELECT pg_stat_statements_reset()")


async def app_stats():
    tot = {}
    for a in APPS:
        for k, v in (await minihttp.get_json(f"{a}/stats"))[1].items():
            tot[k] = tot.get(k, 0) + v
    return tot


def p99(xs):
    xs = sorted(xs)
    return xs[max(0, math.ceil(.99 * len(xs)) - 1)] if xs else 0


async def burst(n, sku, mode):
    lat = []

    async def one(i):
        t = time.perf_counter()
        status, _ = await minihttp.request("GET", url(i, sku, mode))
        lat.append(time.perf_counter() - t)
        return status
    codes = await asyncio.gather(*(one(i) for i in range(n)))
    return codes, lat


async def hot_key(conn, sid, mode, label, seed_stale=False):
    await reset(conn)
    if seed_stale:
        t = time.time()
        await R.set("price:phone-x", json.dumps({"price": 69999, "version": 10, "fresh_until": t - 10, "stale_until": t + 50}), px=50_000)
        await conn.execute("UPDATE prices SET version = 11 WHERE sku = 'phone-x'")
    codes, lat = await burst(1000, "phone-x", mode)
    await asyncio.sleep(1.0)                               # let background refreshes land
    ps = (await minihttp.get_json(f"{PRICING}/stats"))[1]
    st = await app_stats()
    calls = await db_calls(conn)
    after = json.loads(await R.get("price:phone-x"))
    return {"id": sid, "name": label, "requests": 1000, "ok": codes.count(200), "origin_calls_pg": calls,
            "origin_loads_svc": ps["loads"], "max_origin_concurrency": ps["max_in_flight"],
            "coalesced_waiters": st["coalesced_waiters"] + st["lease_waiters"], "stale_served": st["stale_served"],
            "fallback_loads": st["fallback_loads"], "p99_ms": round(p99(lat) * 1000), "cache_version_after": after["version"]}


async def cold(conn, budget, label, seconds=3.0, rate=1000, keys=2000, seed=6):
    await reset(conn, budget=budget)
    rng = random.Random(seed)
    w = [1 / (i + 1) for i in range(keys)]
    skus = [f"sku-{k:05d}" for k in rng.choices(range(keys), w, k=int(seconds * rate))]
    t0 = time.perf_counter()
    codes = []

    async def one(i, sku):
        await asyncio.sleep(max(0, i / rate - (time.perf_counter() - t0)))
        status, _ = await minihttp.request("GET", url(i, sku, "local"))
        codes.append(status)
    await asyncio.gather(*(one(i, s) for i, s in enumerate(skus)))
    ps = (await minihttp.get_json(f"{PRICING}/stats"))[1]
    st = await app_stats()
    return {"id": "L6", "name": label, "requests": len(skus), "ok": codes.count(200), "refused_503": codes.count(503),
            "origin_calls_pg": await db_calls(conn), "origin_loads_svc": ps["loads"], "max_origin_concurrency": ps["max_in_flight"],
            "shed_at_origin": ps["shed"], "fresh_hits": st["fresh_hits"], "budget": budget or "none"}


async def stale_set_race(conn, mode):
    """R2 has already written v11 (₹64,999). A stalled R1 now writes the v10 it read earlier."""
    await reset(conn)
    t = time.time()
    await R.set("price:phone-x", json.dumps({"price": 64999, "version": 11, "fresh_until": t + 300, "stale_until": t + 360}), px=360_000)
    await minihttp.request("POST", f"{APPS[0]}/debug/write?sku=phone-x&price=69999&version=10&mode={mode}")
    after = json.loads(await R.get("price:phone-x"))
    return after["version"], after["price"]


async def l8(conn):
    plain_v, plain_p = await stale_set_race(conn, "plain")
    ver_v, ver_p = await stale_set_race(conn, "versioned")
    return {"id": "L8", "name": "stale-set race (real Redis)", "requests": 2, "ok": 2,
            "origin_calls_pg": 0, "max_origin_concurrency": 0,
            "plain_after": f"v{plain_v} ₹{plain_p:,}", "versioned_after": f"v{ver_v} ₹{ver_p:,}",
            "plain_version": plain_v, "versioned_version": ver_v}


async def main(which):
    conn = await asyncpg.connect(DB)
    if True:
        runs = {
            "L1": lambda: hot_key(conn, "L1", "naive", "naive"),
            "L2": lambda: hot_key(conn, "L2", "local", "local singleflight (100 scopes)"),
            "L3": lambda: hot_key(conn, "L3", "fleet", "fleet refresh lease (Redis SET NX PX)"),
            "L4": lambda: hot_key(conn, "L4", "swr", "stale-while-revalidate", seed_stale=True),
            "L6a": lambda: cold(conn, 0, "cold cache · no origin budget"),
            "L6b": lambda: cold(conn, 20, "cold cache · origin budget N=20"),
            "L8": lambda: l8(conn),
        }
        ids = list(runs) if which == "all" else [k for k in runs if k.startswith(which)]
        out = []
        for k in ids:
            r = await runs[k]()
            out.append(r)
            print(json.dumps(r), flush=True)
    await conn.close()
    print("\nSUMMARY")
    print(f"{'run':<44}{'requests':>9}{'ok':>7}{'pg calls':>10}{'max conc':>10}{'waiters':>9}{'stale':>7}{'p99 ms':>8}")
    for r in out:
        print(f"{r['id'] + ' ' + r['name']:<44}{r['requests']:>9}{r['ok']:>7}{r['origin_calls_pg']:>10}"
              f"{r['max_origin_concurrency']:>10}{r.get('coalesced_waiters', '-'):>9}{r.get('stale_served', '-'):>7}{r.get('p99_ms', '-'):>8}")
    race = next((r for r in out if r["id"] == "L8"), None)
    if race:
        print(f"L8 stale-set race · plain SET -> {race['plain_after']} · version-aware write -> {race['versioned_after']}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "all"))
