"""Background samplers used during a benchmark lane run.

* ContainerSampler: `docker stats --no-stream` once per interval for one container
  (CPU %, memory usage). CPU seconds are integrated from the sampled CPU % over
  the sampling intervals; the scope is the whole container cgroup.
* cgroup_cpu_usage_s: exact cgroup v2 CPU time (`cpu.stat usage_usec`) read via
  `docker exec`, used for the long-lived Kafka Connect container (lane A).
* LagSampler: Kafka consumer-group lag on one topic, once per interval.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time


def _parse_bytes(text: str) -> float:
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "kB": 1000, "MB": 1000**2, "GB": 1000**3}
    text = text.strip()
    for unit in sorted(units, key=len, reverse=True):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * units[unit]
    return float(text)


def parse_stats_line(line: str) -> dict:
    """One `docker stats --format '{{json .}}'` line -> {cpu_pct, mem_bytes}."""
    data = json.loads(line)
    cpu = float(str(data.get("CPUPerc", "0")).rstrip("%") or 0)
    mem = _parse_bytes(str(data.get("MemUsage", "0B")).split("/")[0])
    return {"cpu_pct": cpu, "mem_bytes": mem}


class ContainerSampler(threading.Thread):
    def __init__(self, container: str, interval_s: float = 1.0) -> None:
        super().__init__(daemon=True)
        self.container = container
        self.interval = interval_s
        self.samples: list[dict] = []
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            started = time.monotonic()
            proc = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", self.container],
                                  capture_output=True, text=True)
            if proc.returncode == 0 and proc.stdout.strip():
                sample = parse_stats_line(proc.stdout.strip().splitlines()[0])
                sample["t"] = time.time()
                self.samples.append(sample)
            self._stop_event.wait(max(0.0, self.interval - (time.monotonic() - started)))

    def stop(self) -> dict:
        self._stop_event.set()
        self.join(timeout=10)
        return summarize(self.samples)


def summarize(samples: list[dict]) -> dict:
    if not samples:
        return {"samples": 0, "cpu_s_integrated": None, "peak_mem_bytes": None, "mean_cpu_pct": None}
    cpu_s = 0.0
    for previous, current in zip(samples, samples[1:]):
        cpu_s += previous["cpu_pct"] / 100.0 * (current["t"] - previous["t"])
    return {
        "samples": len(samples),
        "cpu_s_integrated": cpu_s,
        "peak_mem_bytes": max(s["mem_bytes"] for s in samples),
        "mean_cpu_pct": sum(s["cpu_pct"] for s in samples) / len(samples),
    }


def cgroup_cpu_usage_s(container: str) -> float | None:
    proc = subprocess.run(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"], capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6
    return None


def window_lag(series: list[dict], start: float, end: float) -> dict:
    """Consumer lag over [start, end] only. None samples (no committed offsets yet) are not zeros: if none were
    observed in the window, the lag is unmeasured (max_lag None), never reported as 0."""
    inside = [s for s in series if start <= s["t"] <= end]
    values = [s["lag"] for s in inside if s["lag"] is not None]
    return {"samples": len(inside), "observed": len(values), "max_lag": max(values) if values else None,
            "mean_lag": sum(values) / len(values) if values else None}


class LagSampler(threading.Thread):
    def __init__(self, bootstrap: str, group: str, topic: str, interval_s: float = 1.0) -> None:
        super().__init__(daemon=True)
        self.args = (bootstrap, group, topic)
        self.interval = interval_s
        self.samples: list[dict] = []
        self._stop_event = threading.Event()

    def run(self) -> None:
        from connectedfleet.kafka_admin import group_lag

        while not self._stop_event.is_set():
            try:
                lag = group_lag(*self.args)
            except Exception:  # noqa: BLE001 - group may not exist yet
                lag = None
            self.samples.append({"t": time.time(), "lag": lag})
            self._stop_event.wait(self.interval)

    def stop(self) -> dict:
        self._stop_event.set()
        self.join(timeout=10)
        values = [s["lag"] for s in self.samples if s["lag"] is not None]
        return {"samples": len(self.samples), "max_lag": max(values) if values else None,
                "mean_lag": sum(values) / len(values) if values else None, "series": self.samples}
