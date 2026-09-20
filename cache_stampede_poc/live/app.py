"""app: one real process hosting LOGICAL_PODS logical pods. Each logical pod has its own singleflight
table, so 4 processes x 25 = 100 coalescing scopes. The cache is real Redis: the key's Redis TTL is
stale_until; fresh_until lives in the value. mode = naive | local | fleet | swr."""
import asyncio
import json
import os
import time
import uuid

import redis.asyncio as redis

import minihttp
from fastapi import FastAPI, HTTPException

app = FastAPI()
R = redis.from_url(os.environ["REDIS_URL"], max_connections=2000)
PRICING = os.environ["PRICING_URL"]
PODS = int(os.environ.get("LOGICAL_PODS", 25))
FRESH, STALE = 300.0, 60.0
flights = [dict() for _ in range(PODS)]
S = {}
state = {}
RELEASE = R.register_script("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end")
# Version-aware cache write: refuse only writes that would move the cached source version backwards.
VSET = R.register_script("""
local cur = redis.call('get', KEYS[1])
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] and tonumber(obj['version']) > tonumber(ARGV[2]) then
    return 0
  end
end
redis.call('set', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[3]))
return 1
""")


def entry_json(price, version, t):
    return json.dumps({"price": price, "version": version, "fresh_until": t + FRESH, "stale_until": t + FRESH + STALE})


def reset_stats():
    S.update(requests=0, fresh_hits=0, stale_served=0, misses=0, coalesced_waiters=0, lease_waiters=0,
             fallback_loads=0, errors=0, background_refreshes=0)


reset_stats()


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/reset")
async def reset():
    reset_stats()
    return S


@app.get("/stats")
async def stats():
    return S


async def fetch_and_store(sku):
    status, d = await minihttp.get_json(f"{PRICING}/price/{sku}")
    if status != 200:
        raise HTTPException(503, "origin refused")
    t = time.time()
    entry = {"price": d["price"], "version": d["version"], "fresh_until": t + FRESH, "stale_until": t + FRESH + STALE}
    await R.set(f"price:{sku}", json.dumps(entry), px=int((FRESH + STALE) * 1000))
    return d["price"]


async def fleet_refresh(sku, background=False):
    token, lease = uuid.uuid4().hex, f"refresh:price:{sku}"
    if await R.set(lease, token, nx=True, px=5000):
        try:
            return await fetch_and_store(sku)
        finally:
            await RELEASE(keys=[lease], args=[token])
    if background:
        return None
    S["lease_waiters"] += 1
    deadline = time.time() + 1.0
    while time.time() < deadline:
        await asyncio.sleep(0.025)
        raw = await R.get(f"price:{sku}")
        if raw and json.loads(raw)["fresh_until"] > time.time():
            return json.loads(raw)["price"]
    S["fallback_loads"] += 1
    return await fetch_and_store(sku)


async def coalesced(pod, sku, mode, background=False):
    table = flights[pod]
    if sku in table:
        if not background:
            S["coalesced_waiters"] += 1
        return await asyncio.shield(table[sku])
    fut = asyncio.get_running_loop().create_future()
    table[sku] = fut
    try:
        v = await (fetch_and_store(sku) if mode == "local" else fleet_refresh(sku, background))
        fut.set_result(v)
        return v
    except BaseException as e:
        fut.set_exception(e)
        fut.exception()
        raise
    finally:
        del table[sku]


@app.post("/debug/write")
async def debug_write(sku: str, price: int, version: int, mode: str = "plain"):
    """A refresher writing what it read, possibly late. mode=plain overwrites blindly;
    mode=versioned uses the Lua compare-and-set so the source version cannot go backwards."""
    t, px = time.time(), int((FRESH + STALE) * 1000)
    body = entry_json(price, version, t)
    if mode == "versioned":
        ok = await VSET(keys=[f"price:{sku}"], args=[body, version, px])
    else:
        await R.set(f"price:{sku}", body, px=px)
        ok = 1
    return {"written": bool(ok)}


@app.get("/price/{sku}")
async def price(sku: str, pod: int = 0, mode: str = "naive"):
    S["requests"] += 1
    try:
        raw = await R.get(f"price:{sku}")
        e = json.loads(raw) if raw else None
        if e and time.time() < e["fresh_until"]:
            S["fresh_hits"] += 1
            return {"price": e["price"], "served": "fresh"}
        if e and mode == "swr":
            S["stale_served"] += 1
            if sku not in flights[pod]:
                S["background_refreshes"] += 1
                asyncio.ensure_future(coalesced(pod, sku, "fleet", background=True))
            return {"price": e["price"], "served": "stale"}
        S["misses"] += 1
        if mode == "naive":
            v = await fetch_and_store(sku)
        else:
            v = await coalesced(pod, sku, "local" if mode == "local" else "fleet")
        return {"price": v, "served": "loaded"}
    except HTTPException:
        S["errors"] += 1
        raise
