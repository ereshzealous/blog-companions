"""The scenarios. Each returns a Result: a metrics row plus the assertions that must hold.

Worked-example parameters (not measurements): 1,000 simultaneous callers, 100 logical pods,
a 200 ms origin load, a 1 ms cache round trip, a 5-minute fresh TTL and a 60-second stale window.
"""
from __future__ import annotations

import asyncio
import math
import random
from dataclasses import dataclass, field

from .model import Entry, OriginGate, Policy, Pod, PricingOrigin, SharedCache, Stats
from .vclock import now, run

KEY = "price:phone-x"
PODS = 100
CALLERS = 1000


@dataclass
class Result:
    id: str
    name: str
    row: dict
    details: list[str]
    checks: list[tuple[str, bool]]
    data: dict = field(default_factory=dict)

    @property
    def ok(self):
        return all(c for _, c in self.checks)


def pct(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, max(0, math.ceil(q * len(v)) - 1))] if v else 0.0


def fleet(policy, gate=None, local_limit=None, pods=PODS, stats=None):
    cache, origin, stats = SharedCache(), PricingOrigin(), stats or Stats()
    gate = gate or OriginGate(None)
    ps = [Pod(f"pod-{i:03d}", cache, origin, gate, policy, stats,
              OriginGate(local_limit, scope="pod") if local_limit else None) for i in range(pods)]
    return cache, origin, stats, ps


def row(stats, origin, cache_hits=None, **extra):
    return {
        "requests": stats.requests,
        "cache_hits": stats.fresh_hits if cache_hits is None else cache_hits,
        "cache_misses": stats.misses,
        "origin_calls": origin.calls,
        "max_origin_concurrency": origin.max_in_flight,
        "coalesced_waiters": stats.coalesced_waiters + stats.lease_waiters,
        "stale_served": stats.stale_served,
        **extra,
    }


async def burst(ps, key=KEY, n=CALLERS):
    """n callers arrive at the same instant, spread round-robin over the pods."""
    return await asyncio.gather(*(ps[i % len(ps)].get_price(key) for i in range(n)))


# S1–S3: one expired hot key
def s1_naive():
    async def main():
        cache, origin, stats, ps = fleet(Policy(coalesce="none"))
        values = await burst(ps)
        return stats, origin, values
    stats, origin, values = run(main())
    return Result("S1", "naive", row(stats, origin), [
        f"{CALLERS} callers missed the same key at the same instant; every one loaded it",
        f"origin_amplification = {origin.calls} origin loads / 1 refresh needed",
    ], [("origin_calls >= 900", origin.calls >= 900),
        ("every caller got the price", all(v == 69_999 for v in values))],
        {"amplification": origin.calls})


def s2_local_singleflight():
    async def main():
        cache, origin, stats, ps = fleet(Policy(coalesce="local"))
        await burst(ps)
        return stats, origin
    stats, origin = run(main())
    active = min(PODS, CALLERS)
    return Result("S2", "local singleflight", row(stats, origin), [
        f"each of {active} pods coalesced its own callers: 1 leader + {CALLERS // active - 1} waiters per pod",
        f"origin loads = one per pod ({origin.calls}), not one per fleet",
    ], [("origin_calls <= active_pods", origin.calls <= active),
        ("origin_calls == active_pods (one per pod)", origin.calls == active)],
        {"active_pods": active})


def s3_fleet_owner():
    async def main():
        cache, origin, stats, ps = fleet(Policy(coalesce="fleet"))
        values = await burst(ps)
        return stats, origin, values
    stats, origin, values = run(main())
    return Result("S3", "fleet refresh owner", row(stats, origin), [
        f"one pod won SET refresh:{KEY} NX PX 5000; {stats.lease_waiters} pod leaders waited on the cache",
        f"{stats.coalesced_waiters} more callers waited inside their own pods; fallback loads: {stats.lease_fallback_loads}",
        f"p99 caller latency {pct(stats.latencies, .99) * 1000:.0f} ms (callers wait for the one refresh)",
    ], [("origin_calls <= 2", origin.calls <= 2),
        ("no fallback loads", stats.lease_fallback_loads == 0),
        ("every caller got the price", all(v == 69_999 for v in values))],
        {"p99_ms": round(pct(stats.latencies, .99) * 1000, 1)})


# S4: stale-while-revalidate
def s4_stale_while_revalidate():
    async def main():
        pol = Policy(coalesce="fleet", serve_stale=True)
        cache, origin, stats, ps = fleet(pol)
        origin.price[KEY] = (69_999, 11)
        t = now()   # entry went stale 10 s ago; still inside its 60 s stale window
        cache.data[KEY] = Entry(69_999, 10, fresh_until=t - 10, stale_until=t + 50)
        values = await burst(ps)
        await asyncio.sleep(1)                                    # let the background refresh land
        after = cache.data[KEY]
        return stats, origin, values, after, t
    stats, origin, values, after, t = run(main())
    p99 = pct(stats.latencies, .99)
    return Result("S4", "stale-while-revalidate", row(stats, origin), [
        f"{stats.stale_served} callers got the stale ₹69,999 at once; p99 latency {p99 * 1000:.0f} ms vs 200 ms origin",
        f"one background refresh; cache now holds version {after.version}, fresh for another {after.fresh_until - t:.0f} s",
    ], [("stale_served == 1000", stats.stale_served == CALLERS),
        ("origin_calls == 1", origin.calls == 1),
        ("p99 caller latency < origin latency", p99 < origin.latency),
        ("refresh landed (version 11)", after.version == 11)],
        {"p99_ms": round(p99 * 1000, 1)})


# S5: TTL jitter
def s5_ttl_jitter(keys=100_000, base=300.0, jitter=60.0, bucket=5.0):
    rng = random.Random(14)

    def hist(ttls):
        h = {}
        for t in ttls:
            b = math.floor(t / bucket) * bucket
            h[b] = h.get(b, 0) + 1
        return dict(sorted(h.items()))
    fixed = [base] * keys
    additive = [base + rng.uniform(0, jitter) for _ in range(keys)]
    envelope = [base - rng.uniform(0, jitter) for _ in range(keys)]   # inside the freshness contract
    hf, ha, he = hist(fixed), hist(additive), hist(envelope)
    mean_e = keys / (jitter / bucket)
    peak_e = max(he.values())
    over = sum(1 for t in additive if t > base)
    return Result("S5", "TTL jitter", {
        "requests": keys, "cache_hits": None, "cache_misses": None, "origin_calls": None,
        "max_origin_concurrency": None, "coalesced_waiters": None, "stale_served": None,
        "peak_expiries_per_5s": f"{max(hf.values())} -> {peak_e}",
    }, [
        f"same TTL: {max(hf.values()):,} keys expire in one 5-second bucket",
        f"ttl = max_ttl - rand(0, {jitter:.0f} s): peak {peak_e:,} per bucket ({max(hf.values()) / peak_e:.1f}x lower), longest TTL {max(envelope):.1f} s",
        f"ttl = base + rand(0, {jitter:.0f} s) spreads just as well but {over:,} of {keys:,} keys outlive the 300 s freshness limit",
    ], [("envelope peak <= 1.3 x mean bucket", peak_e <= 1.3 * mean_e),
        ("envelope jitter never exceeds max_ttl", max(envelope) <= base),
        ("additive jitter breaks the freshness contract", over > 0)],
        {"fixed": hf, "envelope": he, "additive": ha, "peak_fixed": max(hf.values()), "peak_envelope": peak_e,
         "additive_over": over})


# S6: cold cache, limit scope
def _cold_cache(local_limit=None, budget=None, keys=5_000, rate=2_000, seconds=30.0, queue_timeout=0.25, seed=6):
    rng = random.Random(seed)
    weights = [1 / (i + 1) for i in range(keys)]                  # Zipf-like popularity
    n = int(rate * seconds)
    arrivals = [(i / rate, f"price:sku-{k:05d}") for i, k in enumerate(rng.choices(range(keys), weights, k=n))]

    async def main():
        gate = OriginGate(budget, queue_timeout=queue_timeout, scope="origin") if budget else None
        cache, origin, stats, ps = fleet(Policy(coalesce="local"), gate=gate, local_limit=local_limit)
        timeline = {}

        async def one(i, at, key):
            await asyncio.sleep(at - now())
            trace = []
            await ps[i % len(ps)].get_price(key, trace)
            b = math.floor(at)                                     # 1-second buckets
            hit, total = timeline.get(b, (0, 0))
            timeline[b] = (hit + (trace[0] == "hit"), total + 1)
        await asyncio.gather(*(one(i, at, k) for i, (at, k) in enumerate(arrivals)))
        return stats, origin, timeline, len(cache.data), gate
    stats, origin, timeline, warmed, gate = run(main())
    ratio = {b: h / t for b, (h, t) in sorted(timeline.items())}
    t90 = next((b for b, r in ratio.items() if r >= 0.9), None)
    return stats, origin, ratio, t90, warmed, n


def s6_cold_cache(budget=20):
    # A: only per-pod limits (5 each). Every pod is individually "protected".
    sa, oa, ra, t90a, wa, n = _cold_cache(local_limit=5)
    # B: an aggregate origin budget of N = 20 (a bounded DB pool / proxy / shared quota); overflow sheds after 250 ms.
    sb, ob, rb, t90b, wb, _ = _cold_cache(budget=budget)
    # C: the same boundary, sized at N = 50: the recovery-time side of the trade-off.
    sc, oc, rc, t90c, wc, _ = _cold_cache(budget=50)
    shed_pct = 100 * sb.shed / sb.requests
    return Result("S6", "cold cache", row(sb, ob, shed=sb.shed, time_to_90pct_hits=f"{t90b}s"), [
        f"cache starts empty; {n:,} requests over 30 s across 5,000 keys (Zipf), 100 pods with local singleflight",
        f"A · per-pod limit 5 only: peak origin concurrency {oa.max_in_flight} (the per-pod limits sum to 500); 90% hit ratio after {t90a} s",
        f"B · aggregate origin budget N={budget}: peak {ob.max_in_flight}; {ob.calls:,} loads; {sb.shed:,} requests shed "
        f"({shed_pct:.0f}%) after 250 ms; 90% hit ratio after {t90b} s",
        f"C · aggregate origin budget N=50: peak {oc.max_in_flight}; {sc.shed:,} shed ({100 * sc.shed / sc.requests:.0f}%); "
        f"90% hit ratio after {t90c} s",
        "every concurrency limit has a scope: 100 per-pod limits are not an origin limit;",
        "the budget that protects the origin also sets how long recovery takes",
    ], [("B: max_origin_concurrency <= N", ob.max_in_flight <= budget),
        ("A: per-pod limits alone exceed N", oa.max_in_flight > budget),
        ("B and C recover (90% hit ratio reached)", t90b is not None and t90c is not None),
        ("bigger budget recovers faster (C before B)", t90c < t90b)],
        {"a_max": oa.max_in_flight, "b_max": ob.max_in_flight, "c_max": oc.max_in_flight,
         "t90_a": t90a, "t90_b": t90b, "t90_c": t90c, "shed_b": sb.shed, "shed_c": sc.shed,
         "ratio_a": ra, "ratio_b": rb, "ratio_c": rc, "loads_b": ob.calls, "requests": n, "budget": budget})


# S7: retries
def s7_retry_amplification():
    def arm(ratio):
        async def main():
            pol = Policy(coalesce="none", attempts=3, retry_ratio=ratio)
            cache, origin, stats, ps = fleet(pol, gate=OriginGate(50, scope="origin"))
            origin.failing = True
            await asyncio.gather(*(ps[i % PODS].get_price(f"price:sku-{i:05d}") for i in range(CALLERS)))
            return stats, origin
        return run(main())
    su, ou = arm(None)
    sb, ob = arm(0.10)
    return Result("S7", "retry amplification", row(sb, ob, unbounded_calls=ou.calls), [
        f"origin down; {CALLERS} requests for different keys, 3 attempts each, behind the same N=50 origin gate",
        f"unbounded retries: {ou.calls:,} origin calls ({ou.calls / CALLERS:.1f}x the requests)",
        f"10% per-pod retry budget: {ob.calls:,} origin calls ({ob.calls / CALLERS:.2f}x)",
    ], [("unbounded ~3x", ou.calls == 3 * CALLERS),
        ("budget <= 1.1x", ob.calls <= 1.1 * CALLERS)],
        {"unbounded": ou.calls, "budget": ob.calls})


# S8: stale-set race on the lease
def s8_stale_set_race():
    def arm(versioned):
        async def main():
            pol = Policy(coalesce="fleet", lease_px=1.0, versioned_writes=versioned, fresh_ttl=300, max_stale=60)
            cache, origin, stats, ps = fleet(pol, pods=2)
            a, b = ps
            origin.price[KEY] = (69_999, 10)
            orig_store = a._fetch_and_store

            async def slow_store(key):              # R1: reads v10, then stalls 3 s before writing
                price, version = await a._fetch(key)
                await asyncio.sleep(3.0)            # e.g. a GC pause; its 1 s lease expires meanwhile
                t = now()
                await cache.set(key, Entry(price, version, t + 300, t + 360), versioned=versioned)
                return price
            a._fetch_and_store = slow_store
            r1 = asyncio.ensure_future(a.get_price(KEY))
            await asyncio.sleep(1.5)
            origin.price[KEY] = (64_999, 11)        # the sale price changes at t = 1.5 s
            await b.get_price(KEY)                  # R2: lease expired, takes over, writes v11
            await r1
            a._fetch_and_store = orig_store
            final = cache.data[KEY]
            return final, origin.calls, cache.rejected_writes
        return run(main())
    fu, cu, _ = arm(False)
    fv, cv, rej = arm(True)
    same = _same_version_refresh()
    return Result("S8", "stale-set race", {
        "requests": 2, "cache_hits": 0, "cache_misses": 2, "origin_calls": cu, "max_origin_concurrency": 1,
        "coalesced_waiters": 0, "stale_served": 0,
        "final_cache": f"v{fu.version} -> v{fv.version}",
    }, [
        "R1 takes the 1 s lease and reads v10 (₹69,999), then stalls 3 s; the price changes to v11 (₹64,999)",
        f"R2 takes the expired lease and writes v11; R1 wakes and writes v10",
        f"plain SET: cache regresses to v{fu.version} (₹{fu.value:,}) — wasted work became a wrong price",
        f"versioned write (never accept an older source version): R1 refused ({rej} rejected), cache stays v{fv.version} (₹{fv.value:,})",
        f"an unchanged value may still be re-cached: a v{same[0]} refresh over v{same[0]} is accepted and renews the TTL",
    ], [("plain SET regresses the cache", fu.version == 10),
        ("versioned write keeps v11", fv.version == 11 and rej == 1),
        ("versioned write allows an equal-version refresh", same[1])],
        {"plain": fu.version, "versioned": fv.version, "equal_version_refresh": same[1]})


def _same_version_refresh():
    """The version guard must not block a normal refresh of an unchanged value."""
    async def main():
        cache = SharedCache()
        t = now()
        await cache.set(KEY, Entry(69_999, 11, t + 1, t + 2))
        ok = await cache.set(KEY, Entry(69_999, 11, t + 300, t + 360), versioned=True)
        return 11, ok and cache.data[KEY].fresh_until > t + 299
    return run(main())


ALL = {
    "S1": s1_naive, "S2": s2_local_singleflight, "S3": s3_fleet_owner, "S4": s4_stale_while_revalidate,
    "S5": s5_ttl_jitter, "S6": s6_cold_cache, "S7": s7_retry_amplification, "S8": s8_stale_set_race,
}
