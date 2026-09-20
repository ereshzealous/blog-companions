#!/usr/bin/env python3
"""Semantic assertions for the Tier 2 live lab.

    python3 live/verify_results.py reports/06-live-all.txt

Reads the JSON lines the load generator printed and checks the behaviour each live run is supposed
to demonstrate. Real arrival timing varies, so the naive run (L1) is reported but never asserted:
what must hold are the bounds the protections create. Exits non-zero if any assertion fails.
"""
import json
import pathlib
import sys

SCOPES = 100          # 4 app processes x 25 logical pods
BUDGET = 20           # the aggregate origin budget used by the L6 "budget" arm


def main(path):
    runs = [json.loads(line) for line in pathlib.Path(path).read_text().splitlines() if line.startswith("{")]
    by = {}
    for r in runs:
        by[r["id"] + ("-budget" if r.get("budget") == BUDGET else "-none" if r.get("budget") == "none" else "")] = r
    checks = []

    def need(name, cond, detail):
        checks.append((name, bool(cond), detail))

    l1 = by.get("L1")
    if l1:
        print(f"note  L1 naive made {l1['origin_calls_pg']} PostgreSQL calls for {l1['requests']} requests "
              f"(not asserted: real arrival timing varies)")

    if (r := by.get("L2")):
        need("L2 coalescing is bounded by the number of coalescing scopes",
             r["origin_calls_pg"] <= SCOPES, f"{r['origin_calls_pg']} <= {SCOPES}")
    if (r := by.get("L3")):
        need("L3 fleet lease collapses the fleet to ~1 refresh", r["origin_calls_pg"] <= 2, f"{r['origin_calls_pg']} <= 2")
    if (r := by.get("L4")):
        need("L4 stale-while-revalidate makes exactly one origin call", r["origin_calls_pg"] == 1, f"{r['origin_calls_pg']} == 1")
        need("L4 serves every caller from the stale entry", r["stale_served"] == 1000, f"{r['stale_served']} == 1000")
        need("L4 refresh landed (cache holds the new version)", r["cache_version_after"] == 11, f"v{r['cache_version_after']}")
    if (r := by.get("L6-budget")):
        need("L6 aggregate budget bounds origin concurrency", r["max_origin_concurrency"] <= BUDGET,
             f"{r['max_origin_concurrency']} <= {BUDGET}")
        need("L6 overflow is deliberate, not a collapse", r["refused_503"] > 0 and r["ok"] > 0,
             f"{r['ok']} served, {r['refused_503']} shed")
    if (r := by.get("L6-none")):
        need("L6 without a budget, origin concurrency is unbounded by design",
             r["max_origin_concurrency"] > BUDGET, f"{r['max_origin_concurrency']} > {BUDGET}")
    if (r := by.get("L8")):
        need("L8 a plain SET lets a late refresher regress the cache", r["plain_version"] == 10, r["plain_after"])
        need("L8 a version-aware write preserves the newer value", r["versioned_version"] == 11, r["versioned_after"])

    for name, ok, detail in checks:
        print(f"{'  ok  ' if ok else ' FAIL '} {name} · {detail}")
    failed = [n for n, ok, _ in checks if not ok]
    print(f"\nTier 2 semantic assertions: {len(checks) - len(failed)}/{len(checks)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "reports/06-live-all.txt"))
