#!/usr/bin/env python3
"""Cache-stampede POC · Distributed Systems #14 ("Your Cache Is Fast Until It Expires").

    python run.py --scenario all          run S1–S8, print the summary table + details
    python run.py --scenario S3 S4        run some
    python run.py --scenario all --save   also write captured-output/ (summary.txt, summary.json)
    python run.py --scenario all --save --out results/tier1   write them elsewhere

Deterministic: a virtual-time asyncio loop and seeded randomness, so every run prints the same
numbers. It demonstrates the failure model and the mitigations; it is not a benchmark.
"""
import argparse
import json
import pathlib
import sys
import time

from stampede.scenarios import ALL

COLS = [("scenario", 27), ("requests", 9), ("cache_hits", 11), ("cache_misses", 13), ("origin_calls", 13),
        ("max_origin_concurrency", 23), ("coalesced_waiters", 18), ("stale_served", 13), ("result", 7)]


def fmt(v):
    if v is None:
        return "-"
    return f"{v:,}" if isinstance(v, int) else str(v)


def table(results):
    lines = ["".join(h.ljust(w) for h, w in COLS), "".join("-" * (w - 1) + " " for _, w in COLS)]
    for r in results:
        cells = [f"{r.id} {r.name}"] + [fmt(r.row.get(h)) for h, _ in COLS[1:-1]] + ["PASS" if r.ok else "FAIL"]
        lines.append("".join(c.ljust(w) for c, (_, w) in zip(cells, COLS)))
    return "\n".join(lines)


def details(results):
    out = []
    for r in results:
        out.append(f"\n{r.id} · {r.name}")
        out += [f"  {d}" for d in r.details]
        out += [f"  [{'ok' if ok else 'FAILED'}] assert {name}" for name, ok in r.checks]
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", nargs="+", default=["all"])
    ap.add_argument("--save", action="store_true", help="write the captured output")
    ap.add_argument("--out", default="captured-output", help="where --save writes (default: captured-output)")
    a = ap.parse_args(argv)
    ids = list(ALL) if "all" in [s.lower() for s in a.scenario] else [s.upper() for s in a.scenario]
    t = time.perf_counter()
    results = [ALL[i]() for i in ids]
    wall = time.perf_counter() - t
    text = (f"Cache-stampede POC · worked-example parameters · virtual clock · seed-fixed\n"
            f"1,000 callers · 100 logical pods · origin load 200 ms · cache RTT 1 ms\n\n"
            f"{table(results)}\n{details(results)}\n\n"
            f"{sum(r.ok for r in results)}/{len(results)} scenarios passed")
    print(text)
    print(f"(wall time {wall:.1f} s)", file=sys.stderr)
    if a.save:
        out = pathlib.Path(a.out)
        if not out.is_absolute():
            out = pathlib.Path(__file__).parent / out
        out.mkdir(parents=True, exist_ok=True)
        (out / "summary.txt").write_text(text + "\n")
        (out / "summary.json").write_text(json.dumps(
            {r.id: {"name": r.name, "row": r.row, "details": r.details, "checks": r.checks, "ok": r.ok, "data": r.data}
             for r in results}, indent=1, default=str))
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
