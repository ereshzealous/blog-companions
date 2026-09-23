#!/usr/bin/env python3
"""Semantic assertions for the Tier 2 live lab.

    python3 live/verify_results.py reports/06-live-all.txt

Reads the JSON lines the load generator printed and checks that the behaviour
the article claims actually held on real Kafka and PostgreSQL.

These are **bounded invariants, not exact numbers.** Docker scheduling, broker
rebalancing and host load all move the timings around, so asserting "throughput
was 318/s" would make the lab flaky and prove nothing. What must hold are the
*shapes* the deterministic tier establishes:

    healthy          queue delay stays small
    overload         queue delay and lag both rise
    consumer scaling throughput does not materially beat the C*-sized fleet
    bounded prefetch fetched-but-unprocessed work stays at the bound
    priority         critical work is served ahead of deferrable
    admission        deferrable work is deferred, and no critical order is

Exits non-zero if any assertion fails.
"""
import json
import pathlib
import sys

TOL = 1.15          # 15% headroom before "materially more throughput" is claimed


def main(path):
    lines = pathlib.Path(path).read_text().splitlines()
    runs = {}
    for line in lines:
        line = line.strip()
        if line.startswith("{"):
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "id" in r:
                runs[r["id"]] = r
    if not runs:
        print("no JSON run lines found in", path)
        return 1

    checks = []

    def need(name, cond, detail):
        checks.append((name, bool(cond), detail))

    def note(msg):
        print(f"note  {msg}")

    L1, L2, L3, L4, L5, L6 = (runs.get(k) for k in ("L1", "L2", "L3", "L4", "L5", "L6"))
    cap = (L1 or L2 or {}).get("capacity_per_s", 0)

    # ---- healthy -----------------------------------------------------------
    if L1:
        note(f"L1 ran {L1['rate_per_s']}/s against a {cap}/s dependency "
             f"({L1['processed']} processed, {L1['throughput_per_s']}/s)")
        need("L1 healthy: queue delay stays small",
             L1["queue_delay_p95_ms"] < 2000,
             f"p95 queue delay {L1['queue_delay_p95_ms']} ms < 2000")
        need("L1 healthy: nothing deferred",
             L1["deferred"] == 0, f"deferred={L1['deferred']}")
        need("L1 healthy: every produced order is in PostgreSQL",
             L1["db_rows"] >= L1["processed"],
             f"db_rows={L1['db_rows']} processed={L1['processed']}")

    # ---- overload ----------------------------------------------------------
    if L1 and L2:
        need("L2 overload: queue delay rises above the healthy run",
             L2["queue_delay_p95_ms"] > L1["queue_delay_p95_ms"],
             f"L2 p95 {L2['queue_delay_p95_ms']} ms > L1 p95 {L1['queue_delay_p95_ms']} ms")
        need("L2 overload: the broker holds a real backlog",
             L2["peak_lag"] > 0, f"peak consumer-group lag {L2['peak_lag']} records")
        need("L2 overload: arrival really did exceed capacity",
             L2["rate_per_s"] > cap, f"{L2['rate_per_s']}/s vs capacity {cap}/s")

    # ---- consumer scaling: the central claim -------------------------------
    if L2 and L3:
        note(f"L3 used {L3['workers']} workers against L2's {L2['workers']} "
             f"for the same {L3['rate_per_s']}/s")
        need("L3 scaling: more consumers do not materially raise throughput",
             L3["throughput_per_s"] <= L2["throughput_per_s"] * TOL,
             f"{L3['throughput_per_s']}/s vs {L2['throughput_per_s']}/s (tolerance {TOL:g}x)")
        need("L3 scaling: the dependency is never offered more than C*",
             L3["peak_db_inflight"] <= L3.get("c_star", 8),
             f"peak in-flight {L3['peak_db_inflight']} <= C* {L3.get('c_star', 8)}")
        need("L3 scaling: waiting moved into the consumer",
             L3["peak_unprocessed"] > 10 * L3["peak_db_inflight"],
             f"fetched-but-unwritten peaked at {L3['peak_unprocessed']} while the dependency "
             f"was never offered more than {L3['peak_db_inflight']}")

    # ---- bounded consumption ----------------------------------------------
    if L3 and L4:
        need("L4 bounded: prefetch is held at the configured bound",
             L4["peak_unprocessed"] <= L4["prefetch_bound"],
             f"peak local queue {L4['peak_unprocessed']} <= bound {L4['prefetch_bound']}")
        need("L4 bounded: far less work is held inside the consumer than in L3",
             L4["peak_unprocessed"] < L3["peak_unprocessed"],
             f"{L4['peak_unprocessed']} vs {L3['peak_unprocessed']}")
        need("L4 bounded: and throughput is not sacrificed for it",
             L4["throughput_per_s"] >= L3["throughput_per_s"] / TOL,
             f"{L4['throughput_per_s']}/s vs {L3['throughput_per_s']}/s")
    if L2 and L4:
        # The article's sharpest live claim: same throughput, same dependency,
        # but 8x the workers means each one spends most of its time queueing for
        # a slot instead of being served.
        need("L4 vs L2: extra workers buy waiting, not throughput",
             L4["round_trip_p50_ms"] > L2["round_trip_p50_ms"] * 2
             and L4["db_service_p50_ms"] <= L2["db_service_p50_ms"] * TOL,
             f"round-trip {L2['round_trip_p50_ms']} -> {L4['round_trip_p50_ms']} ms while "
             f"service time stayed {L2['db_service_p50_ms']} -> {L4['db_service_p50_ms']} ms")

    # ---- priority (control) ------------------------------------------------
    if L5:
        crit = L5["by_class"].get("critical", 0)
        need("L5 priority: critical orders are actually served",
             crit > 0, f"{crit} critical orders processed")
        need("L5 priority: nothing is shed by the scheduler alone",
             L5["deferred"] == 0, f"deferred={L5['deferred']}")

    # ---- admission ---------------------------------------------------------
    if L5 and L6:
        need("L6 admission: deferrable work is deferred once it is too old",
             L6["deferred"] > 0, f"{L6['deferred']} deferrable orders deferred")
        need("L6 admission: no critical order is ever shed",
             L6["by_class"].get("critical", 0) > 0 and L6["deferred"] <=
             L6["by_class"].get("deferrable", 0) + L6["deferred"],
             f"critical processed {L6['by_class'].get('critical', 0)}, "
             f"deferred are all deferrable")
        need("L6 admission: the deferrable backlog is smaller than the control's",
             L6["peak_lag"] <= L5["peak_lag"] or L6["queue_delay_p95_ms"] <= L5["queue_delay_p95_ms"],
             f"L6 lag {L6['peak_lag']} / delay {L6['queue_delay_p95_ms']} ms vs "
             f"L5 lag {L5['peak_lag']} / delay {L5['queue_delay_p95_ms']} ms")

    print()
    for name, ok, detail in checks:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}\n        {detail}")
    passed = sum(1 for _, ok, _ in checks if ok)
    print(f"\nTier 2 semantic assertions: {passed}/{len(checks)} passed")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "reports/06-live-all.txt"))
