"""Host-load guard for timing cases on a shared machine (B-007, B-008).

The benchmark runs inside Docker Desktop's VM, but the VM shares the host's cores with every native process. A Rust
build or another benchmark on the same laptop slows every lane, including the official sink. Native processes above
`threshold_pct` CPU count as busy. Before each timing case the harness waits until their summed CPU has stayed within
`case_limit_pct` for `quiet_s` seconds (one background core on a 15-core host is tolerated; a multi-core build is not);
while the case runs it samples native load; if the sum exceeded `case_limit_pct` the case is discarded and run again
(up to `max_attempts`). The last attempt is kept either way and labelled, and the validator rejects it if the
host was still busy. Docker's own VM, Docker Desktop and this session's tools are not "foreign" load.

Load is whole-host CPU time minus Docker's VM, Docker Desktop and the harness (HostCpu), between samples. `ps %cpu`
is a decaying average that misses jobs starting inside a short case, and per-process sampling (CpuDelta) misses
processes that start and exit between samples, such as a test suite's binaries.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time

# Docker Desktop and its VM run our own stack; macOS security and indexing daemons (syspolicyd, XProtect, Spotlight)
# wake up when the harness launches docker CLI processes. None of them is foreign load.
OWN = ("Virtualization.VirtualMachine", "com.docker", "Docker.app", "claude", "syspolicyd", "Xprotect", "mds_stores",
       "mdworker", "WindowServer")


class HostBusy(RuntimeError):
    """The host never became quiet within max_wait_s."""


def native_busy(threshold_pct: float = 50.0, exclude_pids: set[int] | frozenset[int] = frozenset(),
                runner=subprocess.run) -> list[tuple[float, str]]:
    """Native processes above threshold_pct CPU, busiest first, excluding Docker's VM and our own processes."""
    out = runner(["ps", "-Ao", "pid=,pcpu=,comm="], capture_output=True, text=True, timeout=10).stdout
    busy = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid, pcpu = int(parts[0]), float(parts[1])
        except ValueError:
            continue
        comm = parts[2].strip()
        if pid in exclude_pids or pcpu < threshold_pct or any(x in comm for x in OWN) or comm.endswith("/ps"):
            continue
        busy.append((pcpu, comm))
    return sorted(busy, reverse=True)


def parse_cputime(text: str) -> float:
    """ps `time` ([dd-][hh:]mm:ss.ss, minutes may exceed 59 on macOS) -> seconds."""
    days = 0
    if "-" in text:
        d, text = text.split("-", 1)
        days = int(d)
    parts = text.split(":")
    seconds = float(parts[-1])
    minutes = int(parts[-2]) if len(parts) > 1 else 0
    hours = int(parts[-3]) if len(parts) > 2 else 0
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


class CpuDelta:
    """Probe: native processes above threshold_pct CPU *since the previous call*, from cumulative CPU time.

    `ps %cpu` is a decaying average over up to a minute, so a job that starts during a short case barely shows;
    differences of cumulative CPU time between two samples measure what each process used in that interval. A process
    that appeared within the interval is charged all of its CPU time. The first call only takes a snapshot."""

    def __init__(self, threshold_pct: float = 50.0, exclude_pids: set[int] | frozenset[int] = frozenset(),
                 runner=subprocess.run, clock=time.monotonic):
        self.threshold_pct, self.exclude_pids, self.runner, self.clock = threshold_pct, set(exclude_pids), runner, clock
        self.prev: tuple[float, dict[int, tuple[float, str]]] | None = None

    def snapshot(self) -> dict[int, tuple[float, str]]:
        out = self.runner(["ps", "-Ao", "pid=,time=,comm="], capture_output=True, text=True, timeout=10).stdout
        snap = {}
        for line in out.splitlines():
            parts = line.split(None, 2)
            if len(parts) < 3:
                continue
            try:
                pid, cpu = int(parts[0]), parse_cputime(parts[1])
            except ValueError:
                continue
            comm = parts[2].strip()
            if pid in self.exclude_pids or any(x in comm for x in OWN) or comm.endswith("/ps") or comm == "ps":
                continue
            snap[pid] = (cpu, comm)
        return snap

    def __call__(self) -> list[tuple[float, str]]:
        now, snap = self.clock(), self.snapshot()
        prev, self.prev = self.prev, (now, snap)
        if prev is None or now <= prev[0]:
            return []
        dt = now - prev[0]
        busy = []
        for pid, (cpu, comm) in snap.items():
            pct = (cpu - prev[1].get(pid, (0.0, comm))[0]) / dt * 100
            if pct > self.threshold_pct:
                busy.append((pct, comm))
        return sorted(busy, reverse=True)


def _psutil_busy_cpu_s() -> float:
    """Whole-host *user* CPU-seconds. System time is left out on purpose: the benchmark's own disk and network
    traffic through Docker's VM is kernel time that belongs to no readable process (kernel_task), and it reached
    several cores during the fastest lanes. Other people's heavy jobs (builds, tests, soak runs) are user time."""
    import psutil

    t = psutil.cpu_times()
    return t.user + getattr(t, "nice", 0.0)


class _OwnCpu:
    """User CPU-seconds used so far by Docker's VM, Docker Desktop and the harness: live processes (psutil) plus, when
    `children` is set, the harness's finished child processes (getrusage RUSAGE_CHILDREN: the many short docker CLI
    calls it makes exit between samples). A process that exits keeps its last reading so the total never drops."""

    def __init__(self, root_pids: set[int], children: bool = True):
        self.root_pids, self.children = root_pids, children
        self.last: dict[int, float] = {}

    def live_cpu_s(self) -> float:
        import psutil

        mine: set[int] = set()
        for pid in self.root_pids:
            try:
                proc = psutil.Process(pid)
                mine.add(pid)
                mine.update(c.pid for c in proc.children(recursive=True))
            except psutil.Error:
                continue
        live_mine = 0.0
        for proc in psutil.process_iter(["name", "exe", "cpu_times"]):
            info = proc.info
            times = info.get("cpu_times")
            if times is None:
                continue
            if proc.pid in mine:
                # the harness's own tree: counted while alive; once reaped it moves into RUSAGE_CHILDREN
                live_mine += times.user
            elif any(x in f"{info.get('exe') or ''} {info.get('name') or ''}" for x in OWN[:3]):
                self.last[proc.pid] = times.user  # Docker VM, Docker Desktop, com.docker.*
        return sum(self.last.values()) + live_mine

    def __call__(self) -> float:
        total = self.live_cpu_s()
        if self.children:
            import resource

            total += resource.getrusage(resource.RUSAGE_CHILDREN).ru_utime
        return total


class HostCpu:
    """Probe: CPU used by everything on the host except Docker's VM (our stack), Docker Desktop and the harness, since
    the previous call. Whole-host counters include processes that started and exited between samples (a test suite's
    short-lived binaries), which per-process sampling misses. Returns [(percent of one core, label)] above
    threshold_pct, else []. The first call only takes a snapshot."""

    def __init__(self, threshold_pct: float = 50.0, busy_cpu_s=None, own_cpu_s=None, clock=time.monotonic,
                 root_pids: set[int] | None = None):
        self.threshold_pct, self.clock = threshold_pct, clock
        self.busy_cpu_s = busy_cpu_s or _psutil_busy_cpu_s
        self.own_cpu_s = own_cpu_s or _OwnCpu(root_pids or {os.getpid()})
        self.prev: tuple[float, float, float] | None = None

    def __call__(self) -> list[tuple[float, str]]:
        now, busy, own = self.clock(), self.busy_cpu_s(), self.own_cpu_s()
        prev, self.prev = self.prev, (now, busy, own)
        if prev is None or now <= prev[0]:
            return []
        other = ((busy - prev[1]) - (own - prev[2])) / (now - prev[0]) * 100
        return [(other, "other host work (all processes except Docker's VM, Docker Desktop and the harness)")] \
            if other > self.threshold_pct else []


class HostGuard:
    def __init__(self, threshold_pct: float = 50.0, quiet_s: float = 20.0, poll_s: float = 5.0,
                 max_wait_s: float = 7200.0, case_limit_pct: float = 100.0, max_attempts: int = 4,
                 sample_s: float = 2.0, probe=None, clock=time.monotonic, sleep=time.sleep, log=None):
        self.threshold_pct, self.quiet_s, self.poll_s = threshold_pct, quiet_s, poll_s
        self.max_wait_s, self.case_limit_pct, self.max_attempts, self.sample_s = max_wait_s, case_limit_pct, max_attempts, sample_s
        own = {os.getpid(), os.getppid()}
        self.probe = probe or HostCpu(threshold_pct, root_pids=own)
        self.clock, self.sleep, self.log = clock, sleep, log or (lambda message: None)
        self.discarded: list[dict] = []
        self.waited_s = 0.0
        self.quiet_since: float | None = None  # carried across cases: every sample, between or during cases, updates it

    def _observe(self, busy: list[tuple[float, str]], now: float) -> float:
        total = sum(p for p, _ in busy)
        if total > self.case_limit_pct:  # same limit that decides a re-run
            self.quiet_since = None
        elif self.quiet_since is None:
            self.quiet_since = now
        return total

    def wait_quiet(self) -> float:
        """Block until the host has been quiet for quiet_s; return the seconds waited. Quiet time observed during
        the previous case counts, so on a host that stays quiet only the first case waits the full window."""
        start = self.clock()
        announced = False
        while True:
            now = self.clock()
            busy = self.probe()
            self._observe(busy, now)
            if self.quiet_since is None and not announced:
                self.log(f"host busy ({busy[0][0]:.0f}% CPU: {busy[0][1][-60:]}); waiting for it to settle")
                announced = True
            if self.quiet_since is not None and now - self.quiet_since >= self.quiet_s:
                waited = now - start
                self.waited_s += waited
                return waited
            if now - start > self.max_wait_s:
                raise HostBusy(f"host still busy after {self.max_wait_s:.0f} s: {busy[:3]}")
            self.sleep(self.poll_s)

    def sample_during(self, fn):
        """Run fn while sampling native load; return (result, {max_native_cpu_pct, top})."""
        stop = threading.Event()
        peak = {"max_native_cpu_pct": 0.0, "top": None}

        def sampler():
            while not stop.is_set():
                try:
                    busy = self.probe()
                except Exception:  # noqa: BLE001 - sampling must never break a case
                    busy = []
                total = self._observe(busy, self.clock())
                if total > peak["max_native_cpu_pct"]:
                    peak["max_native_cpu_pct"], peak["top"] = total, busy[0][1][-60:] if busy else None
                stop.wait(self.sample_s)

        thread = threading.Thread(target=sampler, daemon=True)
        thread.start()
        try:
            result = fn()
        finally:
            stop.set()
            thread.join(10)
        return result, peak

    def run_case(self, label: str, fn) -> dict:
        """Run a timing case on a quiet host; re-run it if other work overlapped it. Returns fn's measurement dict
        with a `host` entry: attempt, seconds waited, peak native CPU, and busy=True if the last attempt still overlapped."""
        for attempt in range(1, self.max_attempts + 1):
            waited = self.wait_quiet()
            result, peak = self.sample_during(fn)
            busy = peak["max_native_cpu_pct"] > self.case_limit_pct
            if busy and attempt < self.max_attempts:
                self.discarded.append({"label": label, "attempt": attempt, **peak})
                self.log(f"{label}: discarded, host busy during the case ({peak['max_native_cpu_pct']:.0f}% native CPU, "
                         f"{peak['top']}); running it again")
                continue
            result = dict(result or {})
            result["host"] = {"attempt": attempt, "waited_s": round(waited, 1), "busy": busy, **peak}
            return result
        raise AssertionError("unreachable")
