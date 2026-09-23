"""Tier 1 test suite.

Four kinds of test:
  * model properties  — the queue and the capacity pool behave as claimed
  * emergent laws     — Little's Law holds on the pool; it is checked, not assumed
  * queue arithmetic  — the general formulas, swept over parameters
  * pre-registration  — every scenario's declared predictions hold

The sweeps matter most. If `B_peak = (lambda_s - mu) * D` only held for one set
of constants it would be a coincidence; the tests vary the rates to show it is a
property of the system.
"""
import pytest

from backlog.engine import (run_scenario, TrafficProfile, ConsumptionPolicy,
                               Scenario, Scheduler, AdmissionPolicy)
from backlog.downstream import Downstream, CapacityPool
from backlog.queue import BrokerQueue
from backlog.scenarios import build_scenarios, consumer_sweep, fulfilment_db
from backlog.verify import derive, check

CONTROL_OF = {"S4": "S2", "S5": "S2", "S6": "S2", "S7": "S6", "S4b": "S2"}


@pytest.fixture(scope="module")
def runs():
    return {sc.id: (sc,) + run_scenario(sc) for sc in build_scenarios()}


@pytest.fixture(scope="module")
def metrics(runs):
    out = {}
    for sid, (_, _, summary) in runs.items():
        ctl = CONTROL_OF.get(sid)
        controls = {ctl: runs[ctl][2]} if ctl else {}
        out[sid] = derive(summary, controls)
    return out


# ---------------------------------------------------------------- queue
def test_queue_is_fifo_and_lossless():
    q = BrokerQueue(10)
    q.enqueue(0, 100)
    q.enqueue(5, 100)
    assert q.dequeue(10, 150) == [(10, 100), (5, 50)]
    assert q.depth == 50
    assert q.total_enqueued == 200
    assert q.total_dequeued == 150


def test_oldest_age_reads_the_head_not_an_average():
    q = BrokerQueue(10)
    q.enqueue(0, 10)
    q.enqueue(100, 1_000_000)
    # a million recent messages must not drag the reported age down
    assert q.oldest_age_s(100) == 10.0
    q.dequeue(100, 10)
    assert q.oldest_age_s(100) == 0.0


# ---------------------------------------------------------------- capacity pool
def test_throughput_plateaus_at_slots_over_service_time():
    d = Downstream(30, 0.005)
    assert d.offer(30, 1.0)["throughput"] == pytest.approx(6000, rel=1e-3)
    assert d.offer(480, 1.0)["throughput"] == pytest.approx(6000, rel=1e-3)


def test_service_time_never_changes_only_waiting_does():
    """The dependency does not get slower. Callers wait longer for a slot."""
    d = Downstream(30, 0.005)
    for n in (30, 60, 480):
        r = d.offer(n, 1.0)
        assert r["response_s"] - r["wait_s"] == pytest.approx(0.005, rel=1e-6)


@pytest.mark.parametrize("n", [30, 60, 120, 240, 480, 600])
def test_littles_law_holds_on_the_pool(n):
    """N = X * R, measured rather than assumed.

    Response time is not computed by a formula anywhere in the model — it comes
    out of simulated slot contention — so this is a real check on the mechanism.
    """
    r = Downstream(30, 0.005).offer(n, 1.0)
    assert r["throughput"] * r["response_s"] == pytest.approx(n, rel=0.01)


@pytest.mark.parametrize("slots,service", [(30, 0.005), (8, 0.004), (64, 0.02)])
def test_plateau_is_slots_over_service_time(slots, service):
    d = Downstream(slots, service)
    assert d.offer(slots * 20, 1.0)["throughput"] == pytest.approx(slots / service, rel=1e-3)


def test_consumer_sweep_never_shows_throughput_gain():
    rows = consumer_sweep()
    saturated = [r for r in rows if r["saturated"] or r["workers"] >= 30]
    plateau = saturated[0]["throughput_per_s"]
    for r in saturated:
        assert r["throughput_per_s"] == plateau
    assert [r["response_ms"] for r in rows] == sorted(r["response_ms"] for r in rows)


# ---------------------------------------------------------------- queue arithmetic
@pytest.mark.parametrize("lam_s,mu,D", [
    (12_000, 6_000, 600), (9_000, 6_000, 600), (12_000, 6_000, 300), (20_000, 5_000, 120),
])
def test_peak_backlog_is_the_rate_deficit_times_duration(lam_s, mu, D):
    slots = 30
    down = Downstream(slots, slots / mu)
    sc = Scenario("X", "arith", "", TrafficProfile([(60, mu // 2), (D, lam_s)]),
                  down, ConsumptionPolicy(workers=slots))
    _, summary = run_scenario(sc)
    assert summary["metrics"]["peak_depth"] == pytest.approx((lam_s - mu) * D, rel=0.02)


@pytest.mark.parametrize("lam_s,mu,lam_n,D", [
    (12_000, 6_000, 5_000, 600), (9_000, 6_000, 5_000, 600),
    (12_000, 6_000, 3_000, 300), (20_000, 5_000, 4_000, 120),
])
def test_recovery_follows_the_general_formula(lam_s, mu, lam_n, D):
    """T_recovery = B_peak / (mu - lambda_n), for any rates — not just 2x.

    An earlier version of the article used `mu / (mu - lambda_n)`, which is only
    correct when lambda_s == 2 * mu. These cases include ratios where the two
    differ, so the wrong formula cannot pass.
    """
    slots = 30
    down = Downstream(slots, slots / mu)
    b_peak = (lam_s - mu) * D
    expected = b_peak / (mu - lam_n)
    tail = expected * 1.4 + 600
    sc = Scenario("X", "arith", "", TrafficProfile([(60, lam_n), (D, lam_s), (tail, lam_n)]),
                  down, ConsumptionPolicy(workers=slots))
    _, summary = run_scenario(sc)
    assert summary["metrics"]["time_to_backlog_zero_s"] == pytest.approx(expected, rel=0.03)


def test_the_two_by_capacity_case_is_a_special_case_not_the_rule():
    """At lambda_s = 2*mu the worst wait equals the spike; elsewhere it does not."""
    mu, D = 6_000, 600
    for lam_s, equals_D in ((12_000, True), (9_000, False)):
        down = Downstream(30, 30 / mu)
        sc = Scenario("X", "arith", "", TrafficProfile([(60, 3_000), (D, lam_s), (6_000, 3_000)]),
                      down, ConsumptionPolicy(workers=30))
        _, summary = run_scenario(sc)
        w_max = summary["metrics"]["max_wait_s"]
        assert (w_max == pytest.approx(D, rel=0.03)) is equals_D


# ---------------------------------------------------------------- determinism
def test_runs_are_byte_identical():
    a = run_scenario(build_scenarios()[1])[1]
    b = run_scenario(build_scenarios()[1])[1]
    assert a == b


# ---------------------------------------------------------------- conservation
@pytest.mark.parametrize("sid", ["S1", "S2", "S3", "S4", "S5", "S6", "S7"])
def test_no_message_is_ever_lost(sid, metrics):
    m = metrics[sid]
    assert m["enqueued"] == m["processed"] + m["final_depth"]
    assert m["conservation_ok"]


# ---------------------------------------------------------------- pre-registration
@pytest.mark.parametrize("sid", ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S4b"])
def test_every_pre_registered_prediction_holds(sid, runs, metrics):
    sc, _, summary = runs[sid]
    rows, _ = check(summary, metrics[sid], sc.predictions)
    bad = [f"{r['metric']} {r['op']} {r['expected']} but got {r['actual']}" for r in rows if not r["ok"]]
    assert not bad, f"{sid}: " + "; ".join(bad)


# ---------------------------------------------------------------- the article's claims
def test_s4_buys_no_throughput_and_costs_round_trip(metrics):
    s2, s4 = metrics["S2"], metrics["S4"]
    assert s4["processed"] == s2["processed"]
    assert s4["peak_downstream_in_flight"] == 16 * s2["peak_downstream_in_flight"]
    assert s4["peak_downstream_response_ms"] == pytest.approx(
        16 * s2["peak_downstream_response_ms"], rel=0.02)


def test_s5_protects_the_dependency_without_helping_the_backlog(metrics):
    s2, s5 = metrics["S2"], metrics["S5"]
    assert s5["peak_downstream_response_ms"] == s2["peak_downstream_response_ms"]
    assert s5["downstream_saturated_s"] == 0
    assert s5["peak_depth"] == s2["peak_depth"]


def test_priority_reorders_debt_but_does_not_bound_it(metrics):
    """S6 is the control: it changes who waits, not how much waiting exists."""
    s2, s6 = metrics["S2"], metrics["S6"]
    assert s6["peak_oldest_age_critical_s"] == 0.0
    assert s6["peak_oldest_age_deferrable_s"] > 290
    assert s6["peak_depth"] == s2["peak_depth"]
    assert s6["deferred"] == 0


def test_admission_bounds_the_debt_priority_only_rearranged(metrics):
    """S7 changes exactly one thing from S6, so the difference is admission's."""
    s6, s7 = metrics["S6"], metrics["S7"]
    assert s7["peak_oldest_age_critical_s"] == s6["peak_oldest_age_critical_s"] == 0.0
    assert s7["peak_depth"] < 0.10 * s6["peak_depth"]
    assert s7["deferred"] > 0 and s6["deferred"] == 0
    assert s7["deferred_critical"] == 0


def test_s4b_wastes_capacity_rather_than_producing_zero_goodput(metrics):
    """Past the deadline the cost is redone work, not lost work.

    The write still lands; the caller stops waiting and retries. So progress
    slows but never stops, and the queue still drains — more slowly than with
    16x fewer workers.
    """
    s2, s4b = metrics["S2"], metrics["S4b"]
    assert s4b["peak_attempts_per_message"] > 1.2
    assert 0 < s4b["useful_capacity_fraction"] < 0.9
    assert s4b["processed"] > 0                       # never zero goodput
    assert s4b["processed"] < s2["processed"]         # but worse than 30 workers


# ---------------------------------------------------------------- class-aware metrics
@pytest.mark.parametrize("sid", ["S1", "S2", "S3", "S4", "S5", "S6", "S7"])
def test_age_is_tracked_per_class_in_every_scenario(sid, metrics):
    """Class metadata exists everywhere; only the scheduler acts on it."""
    m = metrics[sid]
    for k in ("peak_oldest_age_critical_s", "peak_oldest_age_deferrable_s",
              "peak_depth_critical", "peak_depth_deferrable"):
        assert k in m


def test_a_single_global_age_would_hide_the_priority_result(metrics):
    """The reason class-aware age is necessary rather than merely nice.

    In S6 the global oldest age is terrible while critical work is perfectly
    healthy. One global number cannot express that.
    """
    s6 = metrics["S6"]
    assert s6["peak_oldest_age_s"] > 290
    assert s6["peak_oldest_age_critical_s"] == 0.0
