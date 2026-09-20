"""pricing-svc: the origin. Reads prices from PostgreSQL (each read also runs pg_sleep(0.2) to model
a slow query) behind an admission gate. With one replica, its semaphore IS the aggregate origin budget;
with many replicas it would only be a local limit (see S6)."""
import asyncio
import os

import asyncpg
from fastapi import FastAPI, HTTPException

app = FastAPI()
S = {"loads": 0, "in_flight": 0, "max_in_flight": 0, "shed": 0}
CFG = {"budget": 0, "queue_timeout": 0.25}
state = {"sem": None, "pool": None}
QUERY = "SELECT p.price, p.version FROM prices p, pg_sleep(0.2) WHERE p.sku = $1"


@app.on_event("startup")
async def startup():
    state["pool"] = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=5, max_size=int(os.environ.get("DB_POOL_MAX", 300)))


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/config")
async def config(budget: int = 0, queue_timeout: float = 0.25):
    CFG.update(budget=budget, queue_timeout=queue_timeout)
    state["sem"] = asyncio.Semaphore(budget) if budget else None
    return CFG


@app.post("/reset")
async def reset():
    S.update(loads=0, in_flight=0, max_in_flight=0, shed=0)
    return S


@app.get("/stats")
async def stats():
    return {**S, **CFG}


@app.get("/price/{sku}")
async def price(sku: str):
    sem = state["sem"]
    if sem:
        try:
            await asyncio.wait_for(sem.acquire(), CFG["queue_timeout"])
        except TimeoutError:
            S["shed"] += 1
            raise HTTPException(503, "origin budget exhausted")
    S["loads"] += 1
    S["in_flight"] += 1
    S["max_in_flight"] = max(S["max_in_flight"], S["in_flight"])
    try:
        row = await state["pool"].fetchrow(QUERY, sku)
        if not row:
            raise HTTPException(404)
        return {"sku": sku, "price": row["price"], "version": row["version"]}
    finally:
        S["in_flight"] -= 1
        if sem:
            sem.release()
