"""A deterministic virtual clock for asyncio.

Code under test uses ordinary ``asyncio.sleep`` and ``loop.time()``. When the loop has no
ready callbacks, the clock jumps straight to the next scheduled timer instead of waiting,
so a 10-second scenario runs in milliseconds and produces the same numbers on every run.

This relies on two private attributes of ``asyncio.BaseEventLoop`` (``_ready`` and
``_scheduled``). That is acceptable for a teaching POC; it is not production code.
"""
import asyncio
import heapq


class VirtualTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__()
        self._virtual_now = 0.0

    def time(self):
        return self._virtual_now

    def _run_once(self):
        if not self._ready:
            # drop cancelled timers at the head, exactly as BaseEventLoop does
            while self._scheduled and self._scheduled[0]._cancelled:
                handle = heapq.heappop(self._scheduled)
                handle._scheduled = False
                self._timer_cancelled_count -= 1
            if self._scheduled:
                self._virtual_now = max(self._virtual_now, self._scheduled[0]._when)
        super()._run_once()


def run(coro):
    """Run a coroutine on a fresh virtual-time loop."""
    return asyncio.run(coro, loop_factory=VirtualTimeLoop)


def now():
    return asyncio.get_running_loop().time()
