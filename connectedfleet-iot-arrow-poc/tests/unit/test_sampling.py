"""docker-stats parsing and CPU integration (pure functions; no Docker)."""

import pytest

from connectedfleet.bench import sampling


@pytest.mark.parametrize(
    ("mem", "expected"),
    [("512MiB / 1GiB", 512 * 1024**2), ("1.5GiB / 2GiB", 1.5 * 1024**3), ("900kB / 1GB", 900_000), ("12B / 1GiB", 12)],
)
def test_parse_stats_line_memory_units(mem, expected):
    sample = sampling.parse_stats_line('{"CPUPerc": "150.25%", "MemUsage": "' + mem + '"}')
    assert sample["cpu_pct"] == 150.25
    assert sample["mem_bytes"] == pytest.approx(expected)


def test_summarize_integrates_cpu_over_time():
    samples = [{"t": 0.0, "cpu_pct": 200.0, "mem_bytes": 10}, {"t": 1.0, "cpu_pct": 100.0, "mem_bytes": 30},
               {"t": 3.0, "cpu_pct": 0.0, "mem_bytes": 20}]
    summary = sampling.summarize(samples)
    assert summary["cpu_s_integrated"] == pytest.approx(2.0 * 1.0 + 1.0 * 2.0)
    assert summary["peak_mem_bytes"] == 30


def test_summarize_empty():
    assert sampling.summarize([])["cpu_s_integrated"] is None


def test_samplers_start_and_stop_cleanly(monkeypatch):
    """Regression (Lab 03 first run, 2026-10-10): `self._stop = Event()` shadowed Thread._stop(),
    so stop() -> join() raised "TypeError: 'Event' object is not callable" and every case errored."""
    import subprocess as _subprocess

    from connectedfleet import kafka_admin

    class _Done:
        returncode = 0
        stdout = '{"CPUPerc":"12.5%","MemUsage":"100MiB / 1GiB"}\n'

    monkeypatch.setattr(_subprocess, "run", lambda *a, **k: _Done())
    monkeypatch.setattr(kafka_admin, "group_lag", lambda *a: 3)
    container = sampling.ContainerSampler("x", interval_s=0.01)
    lag = sampling.LagSampler("b", "g", "t", interval_s=0.01)
    container.start()
    lag.start()
    summary = container.stop()
    lag_summary = lag.stop()
    assert not container.is_alive() and not lag.is_alive()
    assert summary["samples"] >= 1 and lag_summary["max_lag"] == 3


def test_window_lag_ignores_samples_outside_the_produce_window():
    series = [{"t": 1.0, "lag": 900}, {"t": 10.0, "lag": 300}, {"t": 11.0, "lag": 120}, {"t": 30.0, "lag": 0}]
    out = sampling.window_lag(series, 9.0, 12.0)
    assert out == {"samples": 2, "observed": 2, "max_lag": 300, "mean_lag": 210.0}


def test_window_lag_is_unmeasured_when_no_offsets_were_committed_in_the_window():
    """Kafka Connect commits sink offsets every offset.flush.interval.ms (60 s default): during a 10 s burst
    the group has no committed offsets, and the later 0 must not be reported as the burst's lag (2026-10-10)."""
    series = [{"t": 10.0, "lag": None}, {"t": 15.0, "lag": None}, {"t": 70.0, "lag": 0}]
    out = sampling.window_lag(series, 9.0, 21.0)
    assert out["max_lag"] is None and out["observed"] == 0 and out["samples"] == 2
