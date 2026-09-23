"""The scenarios, with their predictions registered in advance.

Every scenario declares what it expects *before* it runs. A prediction that does
not hold fails the run, so these are experiments rather than demonstrations.

THE WORKED EXAMPLE
------------------
Order processing during a flash sale.

    normal arrival     5,000 orders/sec   (lambda_n)
    service capacity   6,000 orders/sec   (mu = C* 30 concurrent / 5 ms)
    flash-sale arrival 12,000 orders/sec  (lambda_s)
    spike duration     10 minutes         (D)

The general results, which the full edition states in this form:

    B_peak     = (lambda_s - mu) * D                    = 3,600,000
    W_max      = B_peak / mu                            = 600 s   = 10 min
    T_recovery = B_peak / (mu - lambda_n)               = 3,600 s = 60 min
    T_recovery / D = (lambda_s - mu) / (mu - lambda_n)  = 6

The special case lambda_s = 2*mu is worth naming because it is tidy — there
W_max = D exactly, and T_recovery/D reduces to mu / (mu - lambda_n) — but it is a
special case, not the general rule.

THE EXPERIMENT ORDER
--------------------
S1-S5 carry class metadata but ignore it. S6 turns on the priority scheduler and
nothing else. S7 turns on admission and nothing else. That puts the control
*before* the treatment, so each run differs from its predecessor by one variable
and the causal attribution is clean.
"""
from .engine import (Scenario, TrafficProfile, AdmissionPolicy,
                     ConsumptionPolicy, Scheduler)
from .downstream import Downstream

ARRIVAL_NORMAL = 5_000
ARRIVAL_SPIKE = 12_000
SPIKE_S = 10 * 60          # 600 s
WARMUP_S = 2 * 60
RECOVERY_TAIL_S = 75 * 60  # long enough to drain 3.6M at 1,000/s
AGE_SLO_S = 60.0           # the stated timeliness SLO
DEFERRABLE_FRACTION = 0.55


def fulfilment_db(**kw):
    """30 useful concurrent slots at 5 ms each -> a 6,000/sec plateau."""
    return Downstream(useful_concurrency=30, service_time_s=0.005, **kw)


def p(metric, op, value, claim):
    return {"metric": metric, "op": op, "value": value, "claim": claim}


def build_scenarios():
    steady = [(600, ARRIVAL_NORMAL)]
    spike = [(WARMUP_S, ARRIVAL_NORMAL), (SPIKE_S, ARRIVAL_SPIKE)]
    spike_then_normal = spike + [(RECOVERY_TAIL_S, ARRIVAL_NORMAL)]
    common = dict(age_slo_s=AGE_SLO_S, deferrable_fraction=DEFERRABLE_FRACTION)

    s = []

    # ---------------------------------------------------------------- S1
    s.append(Scenario(
        "S1", "Healthy baseline",
        "Does the model behave when arrival is below capacity?",
        TrafficProfile(steady), fulfilment_db(),
        ConsumptionPolicy(workers=30), **common,
        predictions=[
            p("peak_depth", "<=", 1_000, "below capacity the backlog stays near zero"),
            p("peak_oldest_age_s", "<=", 1.0, "nothing waits: oldest age stays under a second"),
            p("peak_downstream_response_ms", "<=", 5.01, "the dependency runs at its service time"),
            p("max_wait_s", "<=", 1.0, "no order waits measurably"),
        ]))

    # ---------------------------------------------------------------- S2
    s.append(Scenario(
        "S2", "Sustained overload",
        "What does a durable queue do when arrival exceeds service?",
        TrafficProfile(spike), fulfilment_db(),
        ConsumptionPolicy(workers=30), **common,
        predictions=[
            p("peak_depth", ">=", 3_500_000, "backlog grows at the rate deficit to ~3.6M"),
            p("peak_oldest_age_s", ">=", 290, "oldest message age grows to ~300 s"),
            p("enqueued", "==", "processed+deferred+final_depth",
              "the modelled queue conserves every message"),
            p("peak_downstream_response_ms", "<=", 5.01,
              "the dependency is at capacity but not degraded"),
        ]))

    # ---------------------------------------------------------------- S3
    s.append(Scenario(
        "S3", "Latency debt after traffic normalises",
        "The spike is over. Is the system?",
        TrafficProfile(spike_then_normal), fulfilment_db(),
        ConsumptionPolicy(workers=30), **common,
        predictions=[
            p("max_wait_s", ">=", 580, "the worst wait is ~600 s — B_peak / mu"),
            p("time_to_backlog_zero_s", ">=", 3_400,
              "the backlog needs ~3,600 s to drain at 1,000/s of headroom"),
            p("debt_ratio", ">=", 5.5, "repayment takes ~6x the length of the spike"),
            p("recovery_headroom_per_s", "==", 1_000, "headroom is mu - lambda_n"),
        ]))

    # ---------------------------------------------------------------- S4
    s.append(Scenario(
        "S4", "Naive consumer scaling",
        "Does adding consumers create downstream capacity?",
        TrafficProfile(spike), fulfilment_db(),
        ConsumptionPolicy(workers=480), **common,
        predictions=[
            p("peak_downstream_in_flight", ">=", 480,
              "16x the consumers means 16x the in-flight downstream work"),
            p("peak_downstream_response_ms", ">=", 79,
              "downstream round-trip rises 16x, from 5 ms to 80 ms"),
            p("throughput_vs_s2", "<=", 1.001, "throughput does not improve at all over S2"),
            p("peak_depth_vs_s2", ">=", 0.999, "the backlog is no smaller than with 30 consumers"),
            p("downstream_saturated_s", ">=", 500, "the dependency is saturated throughout"),
        ]))

    # ---------------------------------------------------------------- S5
    s.append(Scenario(
        "S5", "Downstream-aware consumption",
        "Does bounding concurrency protect the dependency?",
        TrafficProfile(spike), fulfilment_db(),
        ConsumptionPolicy(workers=480, downstream_aware=True), **common,
        predictions=[
            p("peak_downstream_in_flight", "<=", 30,
              "the same 480-worker fleet offers at most C* = 30"),
            p("peak_downstream_response_ms", "<=", 5.01,
              "the dependency never leaves its service time"),
            p("downstream_saturated_s", "==", 0, "the dependency is never saturated"),
            p("peak_depth_vs_s2", ">=", 0.999, "and the backlog still grows exactly as before"),
        ]))

    # ---------------------------------------------------------------- S6 · CONTROL
    # Only the scheduler changes from S5. This runs BEFORE admission so the
    # effect of priority is measured on its own and cannot be credited to
    # shedding afterwards.
    s.append(Scenario(
        "S6", "Priority scheduling only (control)",
        "What does prioritising critical work change on its own?",
        TrafficProfile(spike), fulfilment_db(),
        ConsumptionPolicy(workers=480, downstream_aware=True),
        scheduler=Scheduler(priority=True), **common,
        predictions=[
            p("peak_oldest_age_critical_s", "<=", 1.0,
              "critical orders stay timely: priority alone keeps them at zero wait"),
            p("peak_oldest_age_deferrable_s", ">=", 290,
              "deferrable work absorbs the entire delay instead"),
            p("peak_depth_vs_s2", ">=", 0.999,
              "and the TOTAL backlog is unchanged: priority reorders debt, it does not bound it"),
            p("deferred", "==", 0, "nothing is shed, by construction"),
        ]))

    # ---------------------------------------------------------------- S7 · TREATMENT
    # Only admission changes from S6.
    s.append(Scenario(
        "S7", "Priority + admission control",
        "Can backpressure bound the debt that priority only rearranges?",
        TrafficProfile(spike), fulfilment_db(),
        ConsumptionPolicy(workers=480, downstream_aware=True),
        scheduler=Scheduler(priority=True),
        admission=AdmissionPolicy(enabled=True, age_threshold_s=30.0), **common,
        predictions=[
            p("peak_depth_vs_s6", "<=", 0.10,
              "admission holds the backlog to a fraction of the priority-only control"),
            p("deferred", ">=", 1_000_000, "the bound is paid for by deferring deferrable work"),
            p("deferred_critical", "==", 0, "no customer order is ever shed"),
            p("peak_oldest_age_critical_s", "<=", 1.0, "critical orders remain timely"),
            p("peak_downstream_response_ms", "<=", 5.01,
              "and the dependency stays at its service time throughout"),
        ]))

    # ---------------------------------------------------------------- S4b · OPTIONAL
    # Deliberately kept out of the main proof. A goodput-loss result is only
    # meaningful if the semantics are stated in full, so they are:
    #   deadline            100 ms, measured at the caller
    #   cancellation        the caller stops waiting; the slot work COMPLETES
    #                       (the write lands — this is the realistic case)
    #   retry               up to 3 attempts, idempotent by order id
    #   goodput             completions that returned inside the deadline
    #   service variance    cv = 0.30, seeded, so misses appear gradually
    s.append(Scenario(
        "S4b", "Deadline misses and retry amplification (optional)",
        "What happens once the downstream wait crosses a caller deadline?",
        TrafficProfile(spike),
        fulfilment_db(timeout_s=0.100, max_attempts=3, cv=0.30),
        ConsumptionPolicy(workers=600), **common, optional=True,
        predictions=[
            p("peak_downstream_response_ms", ">=", 99,
              "at 600 workers the round-trip reaches the 100 ms deadline"),
            p("peak_attempts_per_message", ">", 1.2,
              "callers give up and retry, so each message costs more than one attempt"),
            p("useful_capacity_fraction", "<", 0.9,
              "a measurable share of downstream capacity redoes writes that already landed"),
            p("throughput_vs_s2", "<", 1.0,
              "the queue drains MORE SLOWLY than with 30 workers, despite 20x the fleet"),
        ]))

    return s


def consumer_sweep(workers_list=(15, 30, 60, 120, 240, 480, 600)):
    """What each fleet size actually buys.

    Everything is held constant except fleet size. Throughput plateaus at C*/s;
    the round-trip grows because workers queue for slots. This is the table
    behind "more consumers move the queue, they do not remove it".
    """
    down = fulfilment_db()
    rows = []
    for n in workers_list:
        r = down.offer(n, 1.0)
        rows.append({
            "workers": n,
            "response_ms": round(r["response_s"] * 1000, 2),
            "service_ms": round(down.service_time * 1000, 2),
            "wait_ms": max(0.0, round(r["wait_s"] * 1000, 2)),
            "throughput_per_s": round(r["throughput"]),
            "saturated": r["saturated"],
            # Little's Law, reported so the figure can show that it holds.
            "little_n": round(r["throughput"] * r["response_s"], 2),
        })
    return rows
