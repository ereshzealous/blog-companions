"""The backlog engine.

Fixed-timestep loop, no randomness. Given a scenario the run produces identical
output every time, which is what lets the article quote exact numbers and lets a
test assert them.

TWO CLASSES, ALWAYS
-------------------
Every message carries a class — `critical` (the customer's order and the
fulfilment state that goes with it) or `deferrable` (enrichment, analytics,
recommendations, bulk reconciliation). The class is present in *every* scenario
but ignored until the scheduler is switched on, so S6 changes exactly one
variable relative to S5, and S7 changes exactly one relative to S6.

THREE DIFFERENT "RECOVERIES"
----------------------------
Measured separately, because they are separate facts, and calling all three
"recovery" is how an article ends up contradicting itself:

    peak_oldest_age        how late the system ever got
    time_to_age_slo        when lateness came back inside the stated SLO
    time_to_backlog_zero   when the backlog finally reached zero
"""
from .queue import BrokerQueue

TICK_HZ = 10          # 100 ms resolution
SAMPLE_EVERY_S = 1    # one metrics row per simulated second

CRITICAL, DEFERRABLE = "critical", "deferrable"


class TrafficProfile:
    """Piecewise-constant arrival rate: [(duration_s, rate_per_s), ...]."""

    def __init__(self, phases):
        self.phases = phases
        self.total_s = sum(d for d, _ in phases)

    def rate_at(self, t):
        elapsed = 0.0
        for duration, rate in self.phases:
            if t < elapsed + duration:
                return rate
            elapsed += duration
        return self.phases[-1][1]

    def spike_end_s(self):
        """End of the highest-rate phase — where repayment begins."""
        peak = max(r for _, r in self.phases)
        elapsed = 0.0
        for duration, rate in self.phases:
            elapsed += duration
            if rate == peak:
                return elapsed
        return 0.0

    def spike_duration_s(self):
        peak = max(r for _, r in self.phases)
        return sum(d for d, r in self.phases if r == peak)

    def normal_rate(self):
        """The baseline rate, which sets recovery headroom.

        The minimum rate in the profile, not the last one: a scenario that stops
        while still in the spike (S2) has no recovery phase, and taking its last
        phase would report the spike rate as the baseline.
        """
        return min(r for _, r in self.phases)


class AdmissionPolicy:
    """Backpressure at a policy boundary.

    Critical work is never shed. For an order platform, refusing a paying
    customer because fulfilment is behind is a business decision this
    architecture does not make on its own. What admission controls is
    *deferrable* work — enrichment, analytics, reconciliation — which can wait
    or be dropped without a customer noticing.
    """

    def __init__(self, enabled=False, age_threshold_s=30.0):
        self.enabled = enabled
        self.age_threshold_s = age_threshold_s

    def admit(self, critical, deferrable, oldest_age_s):
        """Returns (critical_admitted, deferrable_admitted, deferred)."""
        if not self.enabled or oldest_age_s <= self.age_threshold_s:
            return critical, deferrable, 0.0
        return critical, 0.0, deferrable


class Scheduler:
    """Which class the consumer fleet serves first.

    Off (the default) is strict arrival order across both classes — the ordinary
    behaviour of a single topic, where class metadata exists but nothing acts on
    it. On is critical-first.
    """

    def __init__(self, priority=False):
        self.priority = priority

    def order(self, q_crit, q_def, tick):
        if self.priority:
            return (q_crit, q_def)
        if q_crit.oldest_age_ticks(tick) >= q_def.oldest_age_ticks(tick):
            return (q_crit, q_def)
        return (q_def, q_crit)


class ConsumptionPolicy:
    """How much concurrency the consumer fleet offers the downstream.

    `workers` is the fleet's raw parallelism; `downstream_aware` clamps it to the
    dependency's useful concurrency. That clamp is the only difference between
    S4 and S5.
    """

    def __init__(self, workers=30, downstream_aware=False):
        self.workers = workers
        self.downstream_aware = downstream_aware

    def offered_concurrency(self, downstream, pending):
        if pending <= 0:
            return 0
        limit = self.workers
        if self.downstream_aware:
            limit = min(limit, downstream.c_star)
        return limit


class Scenario:
    def __init__(self, sid, name, question, profile, downstream, consumption,
                 admission=None, scheduler=None, deferrable_fraction=0.0,
                 age_slo_s=60.0, predictions=None, optional=False):
        self.id = sid
        self.name = name
        self.question = question
        self.profile = profile
        self.downstream = downstream
        self.consumption = consumption
        self.admission = admission or AdmissionPolicy()
        self.scheduler = scheduler or Scheduler()
        self.deferrable_fraction = deferrable_fraction
        self.age_slo_s = age_slo_s
        self.predictions = predictions or []
        self.optional = optional


class WaitHistogram:
    """Wait times bucketed at 100 ms: exact percentiles without storing one
    sample per message."""

    def __init__(self):
        self.buckets = {}
        self.count = 0.0

    def add(self, wait_s, n):
        if n <= 0:
            return
        key = int(round(wait_s * 10))
        self.buckets[key] = self.buckets.get(key, 0.0) + n
        self.count += n

    def percentile(self, p):
        if not self.count:
            return 0.0
        target, seen = self.count * p, 0.0
        for key in sorted(self.buckets):
            seen += self.buckets[key]
            if seen >= target:
                return key / 10
        return max(self.buckets) / 10

    def max(self):
        return (max(self.buckets) / 10) if self.buckets else 0.0

    def mean(self):
        if not self.count:
            return 0.0
        return sum(k * n for k, n in self.buckets.items()) / 10 / self.count


def run_scenario(scenario):
    """Execute one scenario. Returns (samples, summary)."""
    dt = 1.0 / TICK_HZ
    total_ticks = int(round(scenario.profile.total_s * TICK_HZ))
    spike_end_s = scenario.profile.spike_end_s()

    q = {CRITICAL: BrokerQueue(TICK_HZ), DEFERRABLE: BrokerQueue(TICK_HZ)}
    down = scenario.downstream
    waits = {CRITICAL: WaitHistogram(), DEFERRABLE: WaitHistogram()}

    deferred_total = 0.0
    completions_total = 0.0
    missed_deadline_total = 0.0
    wasted_total = 0.0
    peak_attempts = 1.0
    saturated_ticks = 0
    deadline_ticks = 0
    peak_response_s = 0.0
    peak_age = {CRITICAL: 0.0, DEFERRABLE: 0.0}
    age_slo_at_s = None
    backlog_zero_at_s = None

    samples = []
    for tick in range(total_ticks):
        t = tick * dt

        # 1 · arrivals, split by class
        rate = scenario.profile.rate_at(t)
        arrivals = rate * dt
        defer_in = arrivals * scenario.deferrable_fraction
        crit_in = arrivals - defer_in

        # 2 · admission sees the lateness the system already carries
        age_now = max(q[CRITICAL].oldest_age_s(tick), q[DEFERRABLE].oldest_age_s(tick))
        crit_ok, defer_ok, deferred = scenario.admission.admit(crit_in, defer_in, age_now)
        deferred_total += deferred

        # 3 · durable append
        q[CRITICAL].enqueue(tick, crit_ok)
        q[DEFERRABLE].enqueue(tick, defer_ok)

        # 4 · concurrency offered to the dependency
        pending = q[CRITICAL].depth + q[DEFERRABLE].depth
        conc = scenario.consumption.offered_concurrency(down, pending)

        # 5 · what the dependency actually retires
        result = down.offer(conc, dt)
        budget = min(result["progress"], pending)
        completions_total += result["completions"]
        missed_deadline_total += result["missed_deadline"]
        wasted_total += result["wasted"]
        peak_attempts = max(peak_attempts, result["attempts_per_message"])
        if result["saturated"]:
            saturated_ticks += 1
        if result["deadline_exceeded"]:
            deadline_ticks += 1
        peak_response_s = max(peak_response_s, result["response_s"])

        # 6 · drain, in the scheduler's order
        served = 0.0
        for queue in scenario.scheduler.order(q[CRITICAL], q[DEFERRABLE], tick):
            if budget - served <= 0:
                break
            cls = CRITICAL if queue is q[CRITICAL] else DEFERRABLE
            for wait_ticks, n in queue.dequeue(tick, budget - served):
                waits[cls].add(wait_ticks * dt, n)
                served += n

        age = {c: q[c].oldest_age_s(tick) for c in (CRITICAL, DEFERRABLE)}
        for c in (CRITICAL, DEFERRABLE):
            peak_age[c] = max(peak_age[c], age[c])
        oldest = max(age.values())
        depth = q[CRITICAL].depth + q[DEFERRABLE].depth

        # the three recoveries, each on its own terms
        if t > spike_end_s:
            if age_slo_at_s is None and oldest <= scenario.age_slo_s:
                age_slo_at_s = t
            if backlog_zero_at_s is None and depth < 1.0:
                backlog_zero_at_s = t

        if tick % int(SAMPLE_EVERY_S * TICK_HZ) == 0:
            samples.append({
                "t_s": round(t, 1),
                "arrival_rate": round(rate, 1),
                "admitted_rate": round((crit_ok + defer_ok) / dt, 1),
                "deferred_rate": round(deferred / dt, 1),
                "service_rate": round(served / dt, 1),
                "depth": round(depth, 1),
                "depth_critical": round(q[CRITICAL].depth, 1),
                "depth_deferrable": round(q[DEFERRABLE].depth, 1),
                "oldest_age_s": round(oldest, 2),
                "oldest_age_critical_s": round(age[CRITICAL], 2),
                "oldest_age_deferrable_s": round(age[DEFERRABLE], 2),
                "downstream_in_flight": conc,
                "downstream_response_ms": round(result["response_s"] * 1000, 2),
                "downstream_saturated": result["saturated"],
            })

    final_depth = q[CRITICAL].depth + q[DEFERRABLE].depth
    peak_depth = max(max(s["depth"] for s in samples), final_depth)
    headroom = down.max_throughput - scenario.profile.normal_rate()

    summary = {
        "id": scenario.id,
        "name": scenario.name,
        "question": scenario.question,
        "optional": scenario.optional,
        "config": {
            "profile": [{"duration_s": d, "rate": r} for d, r in scenario.profile.phases],
            "spike_duration_s": scenario.profile.spike_duration_s(),
            "workers": scenario.consumption.workers,
            "downstream_aware": scenario.consumption.downstream_aware,
            "priority_scheduler": scenario.scheduler.priority,
            "downstream": {
                "useful_concurrency": down.c_star,
                "service_time_ms": down.service_time * 1000,
                "plateau_per_s": down.max_throughput,
                "deadline_ms": (down.timeout * 1000) if down.timeout else None,
            },
            "admission": {
                "enabled": scenario.admission.enabled,
                "age_threshold_s": scenario.admission.age_threshold_s,
            },
            "deferrable_fraction": scenario.deferrable_fraction,
            "age_slo_s": scenario.age_slo_s,
        },
        "metrics": {
            "duration_s": round(scenario.profile.total_s, 1),
            "spike_end_s": round(spike_end_s, 1),
            "recovery_headroom_per_s": round(headroom, 1),

            "peak_depth": round(peak_depth),
            "peak_depth_critical": round(max(s["depth_critical"] for s in samples)),
            "peak_depth_deferrable": round(max(s["depth_deferrable"] for s in samples)),
            "final_depth": round(final_depth),

            "peak_oldest_age_s": round(max(peak_age.values()), 2),
            "peak_oldest_age_critical_s": round(peak_age[CRITICAL], 2),
            "peak_oldest_age_deferrable_s": round(peak_age[DEFERRABLE], 2),

            "max_wait_s": round(max(waits[CRITICAL].max(), waits[DEFERRABLE].max()), 2),
            "max_wait_critical_s": round(waits[CRITICAL].max(), 2),
            "max_wait_deferrable_s": round(waits[DEFERRABLE].max(), 2),
            "p95_wait_s": round(max(waits[CRITICAL].percentile(0.95),
                                    waits[DEFERRABLE].percentile(0.95)), 2),

            "processed": round(sum(q[c].total_dequeued for c in q)),
            "enqueued": round(sum(q[c].total_enqueued for c in q)),
            "deferred": round(deferred_total),
            "deferred_critical": 0,
            "completions_downstream": round(completions_total),
            "missed_deadline": round(missed_deadline_total),
            "wasted_downstream_work": round(wasted_total),
            "peak_attempts_per_message": round(peak_attempts, 3),

            "time_to_age_slo_s": (round(age_slo_at_s - spike_end_s, 1)
                                  if age_slo_at_s is not None else None),
            "time_to_backlog_zero_s": (round(backlog_zero_at_s - spike_end_s, 1)
                                       if backlog_zero_at_s is not None else None),

            "peak_downstream_response_ms": round(peak_response_s * 1000, 2),
            "peak_downstream_in_flight": max(s["downstream_in_flight"] for s in samples),
            "downstream_saturated_s": round(saturated_ticks * dt, 1),
            "downstream_deadline_exceeded_s": round(deadline_ticks * dt, 1),
        },
    }
    return samples, summary
