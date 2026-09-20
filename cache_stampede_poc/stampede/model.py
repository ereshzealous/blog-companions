"""The moving parts: a shared cache (modelled on Redis), a slow pricing origin, the origin
admission gate, a retry budget, and a pod's read path with the protections switched on or off
by a Policy. Every protection in the article is one field on Policy.
"""
from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field

from .vclock import now


# shared cache
@dataclass
class Entry:
    value: int            # price in rupees
    version: int          # source version from the pricing DB
    fresh_until: float
    stale_until: float    # the key's real TTL: after this it is gone


class SharedCache:
    """A Redis-like shared cache. Each operation costs one network round trip (``rtt``).

    A key disappears at ``stale_until`` (its TTL). Between ``fresh_until`` and ``stale_until``
    it is stale but may still be served if the policy allows it.
    """

    def __init__(self, rtt: float = 0.001):
        self.rtt = rtt
        self.data: dict[str, Entry] = {}
        self.leases: dict[str, tuple[str, float]] = {}
        self.rejected_writes = 0

    async def get(self, key: str) -> Entry | None:
        await asyncio.sleep(self.rtt)
        e = self.data.get(key)
        if e and now() >= e.stale_until:
            del self.data[key]
            return None
        return e

    async def set(self, key: str, entry: Entry, versioned: bool = False) -> bool:
        """Write an entry.

        With ``versioned=True`` the invariant is that the cached **source version never moves
        backwards**. An older version is refused; an equal version is accepted, because that is an
        ordinary refresh of an unchanged value (it renews fresh_until / stale_until).
        """
        await asyncio.sleep(self.rtt)
        cur = self.data.get(key)
        if versioned and cur and cur.version > entry.version:
            self.rejected_writes += 1
            return False
        self.data[key] = entry
        return True

    async def set_nx_px(self, key: str, token: str, px: float) -> bool:
        """SET key token NX PX px: a best-effort, single-instance refresh lease."""
        await asyncio.sleep(self.rtt)
        held = self.leases.get(key)
        if held and now() < held[1]:
            return False
        self.leases[key] = (token, now() + px)
        return True

    async def cas_delete(self, key: str, token: str) -> bool:
        """Release only if the caller still owns it (the usual compare-and-delete Lua script)."""
        await asyncio.sleep(self.rtt)
        held = self.leases.get(key)
        if held and held[0] == token:
            del self.leases[key]
            return True
        return False


# origin
class OriginError(Exception):
    pass


class PricingOrigin:
    """The pricing service + DB. Counts every load and the peak number running at once."""

    def __init__(self, latency: float = 0.2, fail_latency: float = 0.05):
        self.latency = latency
        self.fail_latency = fail_latency
        self.failing = False
        self.calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.price: dict[str, tuple[int, int]] = {}   # key -> (price, version)

    def current(self, key: str) -> tuple[int, int]:
        return self.price.get(key, (69_999, 10))

    async def load(self, key: str) -> tuple[int, int]:
        self.calls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.failing:
                await asyncio.sleep(self.fail_latency)
                raise OriginError("pricing DB unavailable")
            snapshot = self.current(key)          # read happens now...
            await asyncio.sleep(self.latency)     # ...the reply arrives later
            return snapshot
        finally:
            self.in_flight -= 1


class Shed(Exception):
    """The request was refused at the origin boundary (overflow policy: fail fast)."""


class OriginGate:
    """Admission control in front of the origin. ``limit=None`` means no gate at all.

    ``scope`` is only a label, but it is the point of scenario S6: the same Semaphore protects
    very different things depending on where it lives and how many copies of it exist.
    """

    def __init__(self, limit: int | None, queue_timeout: float | None = None, scope: str = "none"):
        self.limit = limit
        self.scope = scope
        self.queue_timeout = queue_timeout
        self.sem = asyncio.Semaphore(limit) if limit else None
        self.shed = 0

    async def run(self, fn):
        if not self.sem:
            return await fn()
        try:
            if self.queue_timeout is None:
                await self.sem.acquire()
            else:
                await asyncio.wait_for(self.sem.acquire(), self.queue_timeout)
        except TimeoutError:
            self.shed += 1
            raise Shed()
        try:
            return await fn()
        finally:
            self.sem.release()


class RetryBudget:
    """Per-client retry budget: retries may not exceed ``ratio`` of the requests this client
    has seen (the shape Google SRE describes; the ratio here is a scenario parameter)."""

    def __init__(self, ratio: float):
        self.ratio = ratio
        self.requests = 0
        self.retries = 0

    def allow_retry(self) -> bool:
        if self.retries < self.ratio * self.requests:
            self.retries += 1
            return True
        return False


# policy + pod
@dataclass
class Policy:
    coalesce: str = "none"           # none | local | fleet
    serve_stale: bool = False        # stale-while-revalidate between fresh_until and stale_until
    fresh_ttl: float = 300.0
    max_stale: float = 60.0
    lease_px: float = 5.0            # fleet refresh lease
    wait_poll: float = 0.025         # how often a lease loser re-reads the cache
    wait_max: float = 1.0            # how long it waits before loading itself
    versioned_writes: bool = False   # refuse cache writes older than the cached version
    attempts: int = 1                # total attempts per origin load (1 = no retries)
    retry_ratio: float | None = None # per-pod retry budget, None = unbounded
    retry_backoff: float = 0.05


@dataclass
class Stats:
    requests: int = 0
    fresh_hits: int = 0
    stale_served: int = 0
    misses: int = 0
    coalesced_waiters: int = 0       # joined another caller's in-flight load (same pod)
    lease_waiters: int = 0           # lost the fleet lease and waited for the owner's value
    lease_fallback_loads: int = 0    # waited wait_max, then loaded anyway
    errors: int = 0
    shed: int = 0
    latencies: list = field(default_factory=list)
    origin_attempts: int = 0


_tokens = itertools.count(1)


class Pod:
    """One application process: a local singleflight table and the read path."""

    def __init__(self, name, cache: SharedCache, origin: PricingOrigin, gate: OriginGate,
                 policy: Policy, stats: Stats, local_gate: OriginGate | None = None):
        self.name, self.cache, self.origin, self.gate = name, cache, origin, gate
        self.local_gate = local_gate or OriginGate(None)
        self.policy, self.stats = policy, stats
        self.flights: dict[str, asyncio.Future] = {}
        self.budget = RetryBudget(policy.retry_ratio) if policy.retry_ratio is not None else None
        self.background: set[asyncio.Task] = set()

    # -- the read path -------------------------------------------------------------------
    async def get_price(self, key: str, trace: list | None = None):
        """Read a price. ``trace`` (optional) receives this call's own outcome."""
        s, p = self.stats, self.policy
        out = "error"
        s.requests += 1
        if self.budget:
            self.budget.requests += 1
        t0 = now()
        try:
            e = await self.cache.get(key)
            if e and now() < e.fresh_until:
                s.fresh_hits += 1
                out = "hit"
                return e.value
            if e and p.serve_stale:                       # stale but servable
                s.stale_served += 1
                out = "stale"
                self._refresh_in_background(key)
                return e.value
            s.misses += 1                                  # absent or too old: controlled sync path
            out = "miss"
            return await self._load(key)
        except Shed:
            s.shed += 1
            out = "shed"
            return None
        except Exception:
            s.errors += 1
            out = "error"
            return None
        finally:
            s.latencies.append(now() - t0)
            if trace is not None:
                trace.append(out)

    def _refresh_in_background(self, key):
        if key in self.flights:
            return
        t = asyncio.ensure_future(self._load(key, background=True))
        self.background.add(t)
        t.add_done_callback(lambda t: (self.background.discard(t), t.exception() if not t.cancelled() else None))

    async def _load(self, key, background=False):
        """Coalescing, per policy. Returns the price."""
        p = self.policy
        if p.coalesce == "none":
            return await self._fetch_and_store(key)
        # local singleflight: one in-flight load per key in this pod
        if key in self.flights:
            if not background:
                self.stats.coalesced_waiters += 1
            return await asyncio.shield(self.flights[key])
        fut = asyncio.get_running_loop().create_future()
        self.flights[key] = fut
        try:
            if p.coalesce == "local":
                value = await self._fetch_and_store(key)
            else:
                value = await self._fleet_refresh(key, background)
            fut.set_result(value)
            return value
        except BaseException as exc:
            fut.set_exception(exc)
            fut.exception()  # mark retrieved
            raise
        finally:
            del self.flights[key]

    async def _fleet_refresh(self, key, background):
        p = self.policy
        token = f"{self.name}-{next(_tokens)}"
        lease = f"refresh:{key}"
        if await self.cache.set_nx_px(lease, token, p.lease_px):
            try:
                return await self._fetch_and_store(key)
            finally:
                await self.cache.cas_delete(lease, token)
        if background:                    # someone else is refreshing; keep serving stale
            return None
        self.stats.lease_waiters += 1
        deadline = now() + p.wait_max
        while now() < deadline:
            await asyncio.sleep(p.wait_poll)
            e = await self.cache.get(key)
            if e and now() < e.fresh_until:
                return e.value
        self.stats.lease_fallback_loads += 1   # bounded wait expired: load ourselves
        return await self._fetch_and_store(key)

    async def _fetch_and_store(self, key):
        price, version = await self._fetch(key)
        t = now()
        await self.cache.set(key, Entry(price, version, t + self.policy.fresh_ttl,
                                        t + self.policy.fresh_ttl + self.policy.max_stale),
                             versioned=self.policy.versioned_writes)
        return price

    async def _fetch(self, key):
        """One origin load, through the local and aggregate gates, with retries per policy."""
        p = self.policy
        attempt = 0
        while True:
            attempt += 1
            self.stats.origin_attempts += 1
            try:
                return await self.local_gate.run(lambda: self.gate.run(lambda: self.origin.load(key)))
            except OriginError:
                if attempt >= p.attempts:
                    raise
                if self.budget and not self.budget.allow_retry():
                    raise
                await asyncio.sleep(p.retry_backoff * attempt)
