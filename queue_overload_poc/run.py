#!/usr/bin/env python3
"""Tier 1 — the deterministic backlog.

    python3 run.py --scenario all --save                    write captured-output/
    python3 run.py --scenario all --save --out results/tier1  write somewhere else
    python3 run.py --scenario S3
    python3 run.py --sweep

Writes captured-output/summary.txt, summary.json and per-scenario sample CSVs.
Exits non-zero if any pre-registered prediction fails, so a run that contradicts
the article cannot quietly pass.
"""
import argparse
import csv
import json
import pathlib
import sys

from backlog.engine import run_scenario
from backlog.scenarios import build_scenarios, consumer_sweep
from backlog.verify import derive, check

DEFAULT_OUT = pathlib.Path(__file__).parent / "captured-output"

# Which run each scenario's comparative predictions are measured against.
CONTROL_OF = {"S4": "S2", "S5": "S2", "S6": "S2", "S7": "S6", "S4b": "S2"}


def dur(s):
    if s is None:
        return "never"
    return f"{s / 60:.1f} min" if s >= 120 else f"{s:.1f} s"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default="all", help="all, or S1,S3,...")
    ap.add_argument("--save", action="store_true", help="write captured-output/")
    ap.add_argument("--sweep", action="store_true", help="print the consumer sweep only")
    ap.add_argument("--out", default=None, metavar="DIR",
                    help="where --save writes (default: captured-output/)")
    args = ap.parse_args()
    out = pathlib.Path(args.out) if args.out else DEFAULT_OUT

    if args.sweep:
        print_sweep(consumer_sweep())
        return 0

    scenarios = build_scenarios()
    wanted = None if args.scenario == "all" else set(args.scenario.upper().replace("S4B", "S4b").split(","))

    # Every scenario runs first: comparative predictions need their control.
    runs = {sc.id: run_scenario(sc) for sc in scenarios}

    lines, results, total_failed = [], [], 0

    def emit(s=""):
        lines.append(s)
        print(s)

    cfg0 = scenarios[0]
    emit("=" * 80)
    emit("DISTRIBUTED SYSTEMS #15 — QUEUE OVERLOAD · TIER 1 DETERMINISTIC SIMULATION")
    emit("=" * 80)
    emit()
    emit("Worked example: order processing during a flash sale.")
    emit("  normal 5,000/s · capacity 6,000/s · flash sale 12,000/s for 10 min")
    emit(f"  dependency: {cfg0.downstream.describe()}")
    emit(f"  timeliness SLO: oldest message age <= {cfg0.age_slo_s:.0f} s")
    emit()
    emit("  General form:   B_peak = (lambda_s - mu) x D")
    emit("                  W_max  = B_peak / mu")
    emit("                  T_rec  = B_peak / (mu - lambda_n)")
    emit("                  T_rec / D = (lambda_s - mu) / (mu - lambda_n)")
    emit("  Here:           B_peak = 6,000 x 600 = 3,600,000")
    emit("                  W_max  = 3,600,000 / 6,000 = 600 s = 10 min")
    emit("                  T_rec  = 3,600,000 / 1,000 = 3,600 s = 60 min  (6x the spike)")
    emit()

    for sc in scenarios:
        samples, summary = runs[sc.id]
        ctl = CONTROL_OF.get(sc.id)
        controls = {ctl: runs[ctl][1]} if ctl and ctl in runs else {}
        metrics = derive(summary, controls)
        rows, failed = check(summary, metrics, sc.predictions)
        total_failed += failed
        summary["derived"] = metrics
        summary["predictions"] = rows
        summary["passed"] = failed == 0
        summary["control"] = ctl
        results.append(summary)

        if wanted and sc.id not in wanted:
            continue

        emit("-" * 80)
        emit(f"{sc.id} · {sc.name}" + ("   [optional — not part of the main proof]" if sc.optional else ""))
        emit(f"     {sc.question}")
        if ctl:
            emit(f"     compared against {ctl}")
        emit("-" * 80)
        c = summary["config"]
        emit(f"  consumers              {c['workers']}"
             f"{'  (clamped to C*)' if c['downstream_aware'] else '  (unbounded)'}")
        emit(f"  priority scheduler     {'on — critical first' if c['priority_scheduler'] else 'off — arrival order'}")
        emit(f"  admission control      {'age > ' + str(c['admission']['age_threshold_s']) + ' s defers deferrable work' if c['admission']['enabled'] else 'off'}")
        emit()
        emit(f"  peak backlog           {metrics['peak_depth']:>14,}   "
             f"(critical {metrics['peak_depth_critical']:,} · deferrable {metrics['peak_depth_deferrable']:,})")
        emit(f"  peak oldest age        {metrics['peak_oldest_age_s']:>14,.1f} s")
        emit(f"    critical             {metrics['peak_oldest_age_critical_s']:>14,.1f} s")
        emit(f"    deferrable           {metrics['peak_oldest_age_deferrable_s']:>14,.1f} s")
        emit(f"  worst wait             {metrics['max_wait_s']:>14,.1f} s   ({dur(metrics['max_wait_s'])})")
        emit(f"  processed              {metrics['processed']:>14,}")
        emit(f"  deferred               {metrics['deferred']:>14,}"
             f"{'   (none critical)' if metrics['deferred'] else ''}")
        emit(f"  recovery headroom      {metrics['recovery_headroom_per_s']:>14,.0f} /s")
        emit(f"  time to age SLO        {dur(metrics['time_to_age_slo_s']):>14}")
        emit(f"  time to backlog zero   {dur(metrics['time_to_backlog_zero_s']):>14}"
             + (f"   ({metrics['debt_ratio']}x the spike)" if metrics.get("debt_ratio") else ""))
        emit(f"  downstream in-flight   {metrics['peak_downstream_in_flight']:>14,}"
             f"   round-trip {metrics['peak_downstream_response_ms']} ms")
        emit(f"  downstream saturated   {dur(metrics['downstream_saturated_s']):>14}")
        if summary["config"]["downstream"]["deadline_ms"]:
            emit(f"  missed deadline        {metrics['missed_deadline']:>14,}"
                 f"   useful capacity {metrics['useful_capacity_fraction'] * 100:.1f}%  ·  attempts/message {metrics['peak_attempts_per_message']}")
        emit()
        emit("  PRE-REGISTERED PREDICTIONS")
        for r in rows:
            emit(f"    [{'ok' if r['ok'] else 'FAILED'}] {r['metric']} {r['op']} {r['expected']}   actual={r['actual']}")
            emit(f"           {r['claim']}")
        emit()

        if args.save:
            out.mkdir(parents=True, exist_ok=True)
            with open(out / f"{sc.id}-samples.csv", "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=list(samples[0].keys()))
                w.writeheader()
                w.writerows(samples)

    emit("=" * 80)
    emit("CONSUMER SWEEP — what each fleet size actually buys")
    emit("=" * 80)
    sweep = consumer_sweep()
    print_sweep(sweep, emit)

    emit("=" * 80)
    main_runs = [r for r in results if not r["optional"]]
    passed = sum(1 for r in results if r["passed"])
    emit(f"RESULT: {passed}/{len(results)} scenarios passed "
         f"({len(main_runs)} in the main proof, {len(results) - len(main_runs)} optional), "
         f"{sum(len(r['predictions']) for r in results)} predictions checked, {total_failed} failed")
    emit("=" * 80)

    if args.save:
        out.mkdir(parents=True, exist_ok=True)
        (out / "summary.txt").write_text("\n".join(lines) + "\n")
        (out / "summary.json").write_text(json.dumps(
            {"scenarios": results, "consumer_sweep": sweep}, indent=2) + "\n")
        print(f"\nwrote {out}/summary.txt, summary.json and per-scenario CSVs")

    return 1 if total_failed else 0


def print_sweep(rows, emit=print):
    emit(f"  {'workers':>8} {'throughput':>12} {'round-trip':>12} {'= service':>11} {'+ waiting':>11}   N = X x R")
    for r in rows:
        emit(f"  {r['workers']:>8} {r['throughput_per_s']:>11,}/s {r['response_ms']:>9.1f} ms "
             f"{r['service_ms']:>8.1f} ms {r['wait_ms']:>8.1f} ms   {r['little_n']:>8.1f}"
             f"{'   saturated' if r['saturated'] else ''}")
    emit()
    emit("  Throughput is flat from C* onwards. The extra time is waiting for a slot,")
    emit("  not the dependency getting slower: service time never changes.")
    emit()


if __name__ == "__main__":
    sys.exit(main())
