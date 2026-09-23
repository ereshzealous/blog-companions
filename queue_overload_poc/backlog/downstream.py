"""The downstream dependency — a bounded-capacity subsystem.

This is the component that actually has capacity. The broker has storage; this
has a service rate. Everything the article argues follows from keeping those two
things apart.

WHAT IS MODELLED
----------------
`C*` service slots, each taking `service_time` to complete one request. A worker
that finds every slot busy *waits*. That is the whole model — there is no latency
formula anywhere in this file.

The response time a caller experiences is therefore an *emergent* property of
slot contention, measured by simulating it:

    response time = time spent waiting for a slot + time being served

An earlier version of this file computed `latency = base * n / C*` directly.
That is the right answer, but asserting it assumes the conclusion the article is
trying to demonstrate. Now the slots are simulated and the relationship is
something the tests *check* (via Little's Law, N = X * R) rather than something
the model is told.

WHY NOT A RETROGRADE (USL) CURVE
--------------------------------
The Universal Scalability Law adds a coherency term so throughput peaks and then
declines, which would make "add consumers" look dramatic. Calibrating it to this
story turns out to be infeasible: requiring near-linear speedup up to n* and a
peak at the same point forces a negative contention coefficient. Rather than fit
a curve to a desired conclusion, the model keeps plain slot contention, where
throughput simply plateaus — which is the honest and more useful result.
"""
import heapq


class CapacityPool:
    """`slots` servers, each taking `service_time` per request.

    `measure(n)` runs a closed loop of `n` workers against the pool and reports
    what they actually experience. Deterministic: with `cv == 0` the service time
    is constant; with `cv > 0` a seeded generator spreads it, which S4b needs so
    that deadline misses appear gradually instead of all at once.
    """

    def __init__(self, slots=30, service_time=0.005, cv=0.0, seed=12345):
        self.slots = slots
        self.service_time = service_time
        self.cv = cv
        self.seed = seed
        self.max_throughput = slots / service_time
        self._cache = {}

    def _service_times(self, count):
        """Deterministic service times. cv == 0 gives a constant."""
        if self.cv == 0:
            return [self.service_time] * count
        # mulberry32-style PRNG, so a run is reproducible without importing random
        state = self.seed
        out = []
        for _ in range(count):
            state = (state + 0x6D2B79F5) & 0xFFFFFFFF
            t = state
            t = (t ^ (t >> 15)) * (1 | t) & 0xFFFFFFFF
            t = (t + ((t ^ (t >> 7)) * (61 | t) & 0xFFFFFFFF)) ^ t & 0xFFFFFFFF
            u = ((t ^ (t >> 14)) & 0xFFFFFFFF) / 4294967296
            # two-point spread with the requested coefficient of variation:
            # mean stays `service_time`, so throughput is unchanged
            out.append(self.service_time * (1 + self.cv if u < 0.5 else 1 - self.cv))
        return out

    def measure(self, n, completions=4000):
        """Closed loop of `n` workers. Returns measured pool behaviour.

        Every worker submits a request, waits for a free slot, is served, and
        immediately submits again — the behaviour of a consumer fleet with a
        backlog to work through.
        """
        n = int(n)
        if n <= 0:
            return {"workers": 0, "throughput": 0.0, "response_s": 0.0,
                    "wait_s": 0.0, "service_s": self.service_time, "responses": []}
        if n in self._cache:
            return self._cache[n]

        warmup = max(n * 4, 400)
        total = warmup + completions
        times = self._service_times(total)

        free = [0.0] * self.slots          # when each slot next becomes free
        ready = [0.0] * n                  # when each worker next submits
        heapq.heapify(free)
        heapq.heapify(ready)

        samples, done = [], 0          # (finish_time, response_time) after warmup
        for i in range(total):
            arrival = heapq.heappop(ready)
            slot_free = heapq.heappop(free)
            start = arrival if arrival > slot_free else slot_free
            finish = start + times[i]
            heapq.heappush(free, finish)
            heapq.heappush(ready, finish)      # closed loop: straight back in
            done += 1
            if done > warmup:
                samples.append((finish, finish - arrival))

        # With deterministic service the slots complete in synchronised bursts,
        # so the loop usually stops part-way through one. Measuring up to the
        # last *whole* burst keeps the rate exact instead of counting a partial
        # burst against a full interval.
        t_first = samples[0][0]
        t_last = max(t for t, _ in samples)
        whole = [(t, r) for t, r in samples if t < t_last] or samples
        t_end = max(t for t, _ in whole)
        counted = [(t, r) for t, r in whole if t > t_first]
        window = t_end - t_first
        throughput = len(counted) / window if window > 0 else 0.0
        responses = [r for _, r in samples]
        mean_response = sum(responses) / len(responses)
        result = {
            "workers": n,
            "throughput": throughput,
            "response_s": mean_response,
            "wait_s": mean_response - self.service_time,
            "service_s": self.service_time,
            "responses": responses,
        }
        self._cache[n] = result
        return result


class Downstream:
    """The fulfilment database, as seen by the consumer fleet."""

    def __init__(self, useful_concurrency=30, service_time_s=0.005,
                 timeout_s=None, max_attempts=1, cv=0.0):
        self.c_star = useful_concurrency
        self.service_time = service_time_s
        self.timeout = timeout_s
        self.max_attempts = max_attempts
        self.pool = CapacityPool(useful_concurrency, service_time_s, cv=cv)
        self.max_throughput = self.pool.max_throughput

    def offer(self, concurrency, seconds):
        """Run at `concurrency` in-flight requests for `seconds`.

        `completions` is what the pool retires. `progress` is how many distinct
        MESSAGES advance, which is the number the queue drains by.

        With no deadline the two are equal. With a deadline they diverge: the
        caller stops waiting, but the slot work still completes and the write
        still lands (writes are idempotent by order id). The message is retried,
        the retry redoes work that was already done, and the dependency spends
        capacity making no new progress. That is retry amplification — the cost
        is wasted capacity, NOT lost work, and never a claim of zero goodput
        merely because a caller gave up.
        """
        m = self.pool.measure(concurrency)
        completions = m["throughput"] * seconds

        if self.timeout is None:
            return {
                "concurrency": concurrency,
                "response_s": m["response_s"],
                "wait_s": m["wait_s"],
                "throughput": m["throughput"],
                "completions": completions,
                "progress": completions,
                "attempts_per_message": 1.0,
                "wasted": 0.0,
                "missed_deadline": 0.0,
                "saturated": concurrency > self.c_star,
                "deadline_exceeded": False,
                "in_time_fraction": 1.0,
            }

        # Fraction of requests that came back inside the caller's deadline.
        resp = m["responses"]
        in_time = sum(1 for r in resp if r <= self.timeout) / len(resp) if resp else 1.0
        # A message needs 1/in_time attempts on average before a caller sees it
        # succeed, capped at the retry budget. Every extra attempt is capacity
        # spent redoing a write that already landed.
        attempts = min(1.0 / in_time, float(self.max_attempts)) if in_time > 0 else float(self.max_attempts)
        progress = completions / attempts
        return {
            "concurrency": concurrency,
            "response_s": m["response_s"],
            "wait_s": m["wait_s"],
            "throughput": m["throughput"],
            "completions": completions,
            "progress": progress,
            "attempts_per_message": attempts,
            "wasted": completions - progress,
            "missed_deadline": completions * (1 - in_time),
            "saturated": concurrency > self.c_star,
            "deadline_exceeded": in_time < 1.0,
            "in_time_fraction": in_time,
        }

    def describe(self):
        return (f"C*={self.c_star} service={self.service_time * 1000:.0f}ms "
                f"plateau={self.max_throughput:,.0f}/s"
                + (f" deadline={self.timeout * 1000:.0f}ms" if self.timeout else ""))
