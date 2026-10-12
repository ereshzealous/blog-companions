"""Host-load guard: timing cases run only on a quiet host, and a case that overlapped other work is re-run."""

import subprocess

import pytest

from connectedfleet.bench import hostload
from connectedfleet.bench.validate import validate_run

PS = """\
  101  12.0 /usr/sbin/syslogd
  202 930.5 target/release/t14
  303 140.0 /System/Library/Frameworks/Virtualization.framework/Versions/A/XPCServices/com.apple.Virtualization.VirtualMachine.xpc/Contents/MacOS/com.apple.Virtualization.VirtualMachine
  404  88.0 /opt/homebrew/bin/python3.12
  505  60.0 /Applications/Docker.app/Contents/MacOS/com.docker.backend
  606  75.0 claude
"""


def fake_ps(output):
    def run(cmd, **kwargs):
        assert cmd[0] == "ps"
        return subprocess.CompletedProcess(cmd, 0, stdout=output)
    return run


def test_native_busy_ignores_docker_vm_own_tools_and_own_pids():
    busy = hostload.native_busy(threshold_pct=50, exclude_pids={404}, runner=fake_ps(PS))
    assert busy == [(930.5, "target/release/t14")]


def test_native_busy_is_empty_on_a_quiet_host():
    assert hostload.native_busy(threshold_pct=50, runner=fake_ps("  1 3.0 launchd\n")) == []


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_wait_quiet_waits_until_the_host_has_been_quiet_long_enough():
    clock = FakeClock()
    readings = iter([[(900.0, "t14")]] * 3 + [[]] * 100)
    guard = hostload.HostGuard(quiet_s=20, poll_s=5, probe=lambda: next(readings), clock=clock, sleep=clock.sleep)
    waited = guard.wait_quiet()
    assert waited == pytest.approx(35)  # 3 busy polls (15 s), then 20 s quiet


def test_wait_quiet_gives_up_after_max_wait():
    clock = FakeClock()
    guard = hostload.HostGuard(quiet_s=20, poll_s=5, max_wait_s=60, probe=lambda: [(900.0, "t14")], clock=clock,
                               sleep=clock.sleep)
    with pytest.raises(hostload.HostBusy):
        guard.wait_quiet()


def test_run_case_reruns_a_case_that_overlapped_host_load():
    clock = FakeClock()
    during = iter([[(700.0, "rustc")], [], []])  # busy during attempt 1, quiet during attempt 2
    guard = hostload.HostGuard(quiet_s=0, poll_s=1, case_limit_pct=100, max_attempts=3, probe=lambda: [],
                               clock=clock, sleep=clock.sleep)
    guard.sample_during = lambda fn: (fn(), {"max_native_cpu_pct": sum(p for p, _ in next(during)), "top": None})
    calls = []
    m = guard.run_case("E1 row rep 1", lambda: calls.append(1) or {"wall_s": 1.0})
    assert len(calls) == 2
    assert m["host"]["attempt"] == 2 and m["host"]["max_native_cpu_pct"] == 0
    assert guard.discarded[0]["label"] == "E1 row rep 1" and guard.discarded[0]["max_native_cpu_pct"] == 700.0


def test_run_case_marks_the_last_attempt_busy_when_the_host_never_settles():
    clock = FakeClock()
    guard = hostload.HostGuard(quiet_s=0, poll_s=1, case_limit_pct=100, max_attempts=2, probe=lambda: [],
                               clock=clock, sleep=clock.sleep)
    guard.sample_during = lambda fn: (fn(), {"max_native_cpu_pct": 500.0, "top": "t14"})
    m = guard.run_case("x", lambda: {})
    assert m["host"]["busy"] is True and m["host"]["attempt"] == 2


def _manifest(host):
    case = {"case_id": "E3-0000", "experiment": "E3", "lane": "direct", "status": "COMPLETED", "warmup": False,
            "repetition": 1, "workload_key": "E3-10-none", "config": {"rows": 10},
            "measurements": {"rows_loaded": 10, "counts": {"logical": 10, "physical": 10}, "host": host}}
    return {"profile": "smoke", "status": "COMPLETED", "code": {"git_sha": "a" * 40, "git_dirty": False},
            "versions": {"python": "3.12", "pyarrow": "25", "stack": {"x": 1}}, "workloads": {"E3-10-none": {"file": {"rows": 10}}},
            "cases": [case]}


def test_validator_rejects_a_case_that_ran_under_host_load():
    result = validate_run(_manifest({"busy": True, "max_native_cpu_pct": 500.0, "attempt": 2}))
    reasons = result["cases"][0]["reasons"]
    assert any(r.startswith("HOST:") for r in reasons)


def test_validator_accepts_a_quiet_case():
    result = validate_run(_manifest({"busy": False, "max_native_cpu_pct": 12.0, "attempt": 1}))
    assert not any(r.startswith("HOST:") for r in result["cases"][0]["reasons"])


def test_one_busy_core_counts_as_quiet_enough():
    # a single process near one core on a 15-core host does not hold a case back; summed native CPU must stay
    # within case_limit_pct, the same limit that decides whether a finished case is re-run
    clock = FakeClock()
    guard = hostload.HostGuard(quiet_s=10, poll_s=5, case_limit_pct=150, probe=lambda: [(101.0, "f10_join_memory")],
                               clock=clock, sleep=clock.sleep)
    assert guard.wait_quiet() == pytest.approx(10)


def _e3_case(i, rep, host=None, warmup=False):
    m = {"rows_loaded": 10, "counts": {"logical": 10, "physical": 10}}
    if host is not None:
        m["host"] = host
    return {"case_id": f"E3-{i:04d}", "experiment": "E3", "lane": "direct", "variant": {}, "mode": "file",
            "status": "COMPLETED", "warmup": warmup, "repetition": rep, "workload_key": "E3-10-none",
            "config": {"rows": 10, "compression": "none", "method": "direct"}, "measurements": m}


def _confirm(cases, status="COMPLETED"):
    return {"profile": "confirm", "status": status, "code": {"git_sha": "a" * 40, "git_dirty": False},
            "versions": {"python": "3.12", "pyarrow": "25", "stack": {"x": 1}},
            "workload_spec": {"profile": "confirm", "repetitions": 3},
            "workloads": {"E3-10-none": {"file": {"rows": 10}}}, "cases": cases}


BUSY = {"busy": True, "max_native_cpu_pct": 400.0, "attempt": 4, "top": "t14"}


def test_a_busy_case_is_excluded_but_the_run_stays_valid_with_enough_clean_repetitions():
    cases = [_e3_case(0, 0, BUSY, warmup=True)] + [_e3_case(i, i, BUSY if i == 2 else None) for i in range(1, 6)]
    result = validate_run(_confirm(cases))
    assert result["run"]["valid"] is True
    assert [c["valid"] for c in result["cases"]] == [False, True, False, True, True, True]
    assert any("HOST" in w for w in result["run"]["warnings"])


def test_the_run_is_invalid_when_host_exclusions_leave_too_few_repetitions():
    cases = [_e3_case(i, i, BUSY if i <= 3 else None) for i in range(1, 6)]
    result = validate_run(_confirm(cases))
    assert result["run"]["valid"] is False
    assert any(r.startswith("REPS:") and "E3" in r for r in result["run"]["reasons"])


def test_an_interrupted_run_is_invalid():
    result = validate_run(_confirm([_e3_case(i, i) for i in range(1, 4)], status="RUNNING"))
    assert result["run"]["valid"] is False
    assert any(r.startswith("STATUS:") for r in result["run"]["reasons"])


def test_parse_cputime_formats():
    assert hostload.parse_cputime("0:01.50") == pytest.approx(1.5)
    assert hostload.parse_cputime("753:32.05") == pytest.approx(753 * 60 + 32.05)
    assert hostload.parse_cputime("1:02:03.5") == pytest.approx(3723.5)
    assert hostload.parse_cputime("2-01:00:00") == pytest.approx(2 * 86400 + 3600)


def test_cpu_delta_measures_current_use_not_the_decaying_average():
    snaps = iter([
        "  202 10:00.00 target/release/t14\n  303 500:00.00 com.apple.Virtualization.VirtualMachine\n  77 0:01.00 bash\n",
        "  202 10:08.00 target/release/t14\n  303 500:09.00 com.apple.Virtualization.VirtualMachine\n  77 0:01.10 bash\n"
        "  88 0:03.00 rustc\n",
    ])
    clock = FakeClock()

    def runner(cmd, **kwargs):
        assert cmd[:2] == ["ps", "-Ao"]
        return subprocess.CompletedProcess(cmd, 0, stdout=next(snaps))

    probe = hostload.CpuDelta(threshold_pct=50, runner=runner, clock=clock)
    assert probe() == []          # first call only takes a snapshot
    clock.t += 2.0
    # t14 used 8 s of CPU in 2 s (400%); rustc started inside the interval (3 s -> 150%); the VM and bash are ignored
    assert probe() == [(pytest.approx(400.0), "target/release/t14"), (pytest.approx(150.0), "rustc")]


def test_host_cpu_counts_all_other_work_including_short_lived_processes():
    # host busy CPU-seconds since boot, and CPU-seconds of Docker's VM + Docker Desktop + this harness
    busy = iter([1000.0, 1016.0])      # 16 CPU-s in 2 s = 800% of the whole host
    own = iter([500.0, 509.0])          # of which 9 CPU-s (450%) were our own stack
    clock = FakeClock()
    probe = hostload.HostCpu(busy_cpu_s=lambda: next(busy), own_cpu_s=lambda: next(own), clock=clock)
    assert probe() == []
    clock.t += 2.0
    [(pct, label)] = probe()
    assert pct == pytest.approx(350.0)  # 800% - 450%: other work, whether or not its processes still exist
    assert "other" in label


def test_host_cpu_reports_nothing_when_only_our_stack_is_busy():
    busy, own, clock = iter([0.0, 10.0]), iter([0.0, 9.9]), FakeClock()
    probe = hostload.HostCpu(busy_cpu_s=lambda: next(busy), own_cpu_s=lambda: next(own), clock=clock)
    probe()
    clock.t += 2.0
    assert probe() == []  # 5% is noise, below the threshold


def test_quiet_time_carries_over_between_cases():
    # the first case waits for the full quiet window; on a host that stays quiet the next case starts at once
    clock = FakeClock()
    guard = hostload.HostGuard(quiet_s=60, poll_s=5, probe=lambda: [], clock=clock, sleep=clock.sleep)
    assert guard.wait_quiet() == pytest.approx(60)
    clock.t += 30
    assert guard.wait_quiet() == pytest.approx(0)


def test_other_work_restarts_the_quiet_window():
    clock = FakeClock()
    readings = iter([[]] * 13 + [[(400.0, "build")]] + [[]] * 100)
    guard = hostload.HostGuard(quiet_s=60, poll_s=5, probe=lambda: next(readings), clock=clock, sleep=clock.sleep)
    assert guard.wait_quiet() == pytest.approx(60)   # 13 quiet polls, t = 0..60
    clock.t += 5
    assert guard.wait_quiet() == pytest.approx(65)   # busy at t = 65, quiet from t = 70: returns at t = 130


def test_own_cpu_includes_finished_child_processes(monkeypatch):
    # the harness starts many short docker CLI processes; once reaped they are gone from the process list,
    # but their CPU time is in getrusage(RUSAGE_CHILDREN) and must not count as other work
    import resource

    class Usage:
        ru_utime, ru_stime = 3.0, 1.5

    monkeypatch.setattr(resource, "getrusage", lambda who: Usage())
    own = hostload._OwnCpu(root_pids=set(), children=True)
    monkeypatch.setattr(own, "live_cpu_s", lambda: 10.0)
    assert own() == pytest.approx(13.0)  # user time only: 10 live + 3.0 reaped
