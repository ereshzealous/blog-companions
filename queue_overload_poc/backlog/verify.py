"""Derived metrics and the prediction checker.

Some predictions are about a scenario on its own ("critical age stays under a
second"), and some are comparisons against a named control ("the backlog is a
fraction of S6's"). Both are resolved here so a prediction stays a declarative
row in `scenarios.py` rather than a bespoke assertion.
"""


def derive(summary, controls=None):
    """Adds derived and cross-scenario metrics.

    `controls` maps a name to another run's summary — S4/S5/S6 are judged against
    S2, and S7 against S6, so that each mechanism is credited only with what it
    actually changed.
    """
    m = dict(summary["metrics"])
    cfg = summary["config"]

    # Everything enqueued is either processed or still waiting; deferred work
    # never entered the queue at all.
    m["conservation_ok"] = m["enqueued"] == m["processed"] + m["final_depth"]
    m["accounted"] = m["processed"] + m["final_depth"] + m["deferred"]

    # How long repayment took relative to the spike that caused it.
    spike = cfg.get("spike_duration_s") or 0
    if spike and m["time_to_backlog_zero_s"] is not None:
        m["debt_ratio"] = round(m["time_to_backlog_zero_s"] / spike, 2)
    else:
        m["debt_ratio"] = None

    # The general prediction for this workload, so the run can be checked
    # against arithmetic rather than against itself.
    peak = max(ph["rate"] for ph in cfg["profile"])
    normal = cfg["profile"][-1]["rate"]
    mu = cfg["downstream"]["plateau_per_s"]
    m["predicted_b_peak"] = round((peak - mu) * spike) if spike else None
    m["predicted_w_max_s"] = round((peak - mu) * spike / mu, 1) if spike else None
    m["predicted_recovery_s"] = (round((peak - mu) * spike / (mu - normal), 1)
                                 if spike and mu > normal else None)

    # Fraction of downstream capacity that advanced a distinct message, rather
    # than redoing one whose write had already landed.
    if m["completions_downstream"]:
        m["useful_capacity_fraction"] = round(
            1.0 - m["wasted_downstream_work"] / m["completions_downstream"], 4)
    else:
        m["useful_capacity_fraction"] = 1.0

    for name, ctl in (controls or {}).items():
        c = ctl["metrics"]
        key = name.lower()
        if c["processed"]:
            m[f"throughput_vs_{key}"] = round(m["processed"] / c["processed"], 4)
        if c["peak_depth"]:
            m[f"peak_depth_vs_{key}"] = round(m["peak_depth"] / c["peak_depth"], 4)
        if c["peak_oldest_age_s"]:
            m[f"age_vs_{key}"] = round(m["peak_oldest_age_s"] / c["peak_oldest_age_s"], 4)
        if c["peak_downstream_response_ms"]:
            m[f"response_vs_{key}"] = round(
                m["peak_downstream_response_ms"] / c["peak_downstream_response_ms"], 4)
    return m


OPS = {
    "<=": lambda a, b: a <= b,
    ">=": lambda a, b: a >= b,
    "==": lambda a, b: a == b,
    "<": lambda a, b: a < b,
    ">": lambda a, b: a > b,
}


def check(summary, metrics, predictions):
    """Evaluates every prediction. Returns (rows, n_failed)."""
    rows, failed = [], 0
    for pred in predictions:
        name, op, want = pred["metric"], pred["op"], pred["value"]

        # Conservation is written as an identity rather than a number, because
        # the identity is the point.
        if want == "processed+deferred+final_depth":
            got = metrics["enqueued"] + metrics["deferred"]
            want_val = metrics["processed"] + metrics["final_depth"] + metrics["deferred"]
            ok = got == want_val
            rows.append({"metric": name, "op": op,
                         "expected": f"{want_val:,} (processed+deferred+depth)",
                         "actual": f"{got:,}", "ok": ok, "claim": pred["claim"]})
            failed += not ok
            continue

        got = metrics.get(name)
        if got is None:
            rows.append({"metric": name, "op": op, "expected": want, "actual": None,
                         "ok": False, "claim": pred["claim"] + "  [metric missing]"})
            failed += 1
            continue
        ok = bool(OPS[op](got, want))
        rows.append({"metric": name, "op": op, "expected": want, "actual": got,
                     "ok": ok, "claim": pred["claim"]})
        failed += not ok
    return rows, failed
