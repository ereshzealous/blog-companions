"""The broker queue.

A durable FIFO log. It never drops a message and it never fails — that is the
whole point of the article, so the model must not cheat by quietly losing work.

Messages are held as (enqueue_tick, count) batches rather than one object per
message. A run spans two hours of simulated time at 12,000 messages/sec, so
individual objects would mean ~50M allocations for no extra fidelity: every
message enqueued on the same tick has, by construction, the same age and the
same fate. Batching makes the run exact and fast at the same time.

Because the log is strictly FIFO, the head batch *is* the oldest message. Age is
therefore read directly rather than estimated:

    oldest_age = now - head.enqueue_tick
"""
from collections import deque


class BrokerQueue:
    """A durable, unbounded, strictly-FIFO queue with exact age accounting."""

    def __init__(self, tick_hz):
        self.tick_hz = tick_hz
        self._batches = deque()      # (enqueue_tick, remaining_count)
        self.depth = 0               # messages waiting
        self.total_enqueued = 0
        self.total_dequeued = 0

    def enqueue(self, tick, count):
        """Append `count` messages produced during `tick`."""
        if count <= 0:
            return
        # Same-tick arrivals coalesce into the tail batch: identical age, identical fate.
        if self._batches and self._batches[-1][0] == tick:
            head_tick, n = self._batches[-1]
            self._batches[-1] = (head_tick, n + count)
        else:
            self._batches.append((tick, count))
        self.depth += count
        self.total_enqueued += count

    def dequeue(self, tick, budget):
        """Remove up to `budget` messages from the head.

        Returns [(wait_ticks, count), ...] so the caller can fold the waits into
        a histogram. Splitting a partially-consumed batch keeps FIFO exact: the
        remainder stays at the head and keeps its original enqueue tick.
        """
        taken = []
        while budget > 0 and self._batches:
            enq_tick, n = self._batches[0]
            take = min(n, budget)
            if take == n:
                self._batches.popleft()
            else:
                self._batches[0] = (enq_tick, n - take)
            taken.append((tick - enq_tick, take))
            self.depth -= take
            self.total_dequeued += take
            budget -= take
        return taken

    def oldest_age_ticks(self, tick):
        """Age of the oldest waiting message. 0 when the queue is empty.

        This is the signal the article argues for, and it is the one a broker
        does not hand you: depth is a counter, age needs the head's timestamp.
        """
        if not self._batches:
            return 0
        return tick - self._batches[0][0]

    def oldest_age_s(self, tick):
        return self.oldest_age_ticks(tick) / self.tick_hz
