"""Every scenario's assertions must hold, and the run must be deterministic."""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from stampede.model import Entry, SharedCache  # noqa: E402
from stampede.scenarios import ALL  # noqa: E402
from stampede.vclock import run  # noqa: E402


@pytest.mark.parametrize("sid", list(ALL))
def test_scenario_assertions(sid):
    r = ALL[sid]()
    failed = [name for name, ok in r.checks if not ok]
    assert not failed, f"{sid} failed: {failed}"


@pytest.mark.parametrize("sid", ["S1", "S3", "S4", "S6", "S8"])
def test_deterministic(sid):
    assert ALL[sid]().row == ALL[sid]().row


@pytest.mark.parametrize("cached,incoming,accepted", [(11, 10, False), (11, 11, True), (11, 12, True)])
def test_version_guard_never_moves_backwards(cached, incoming, accepted):
    """Reject only writes that would move the cached source version backwards."""
    async def main():
        c = SharedCache()
        await c.set("k", Entry(1, cached, 1, 2))
        ok = await c.set("k", Entry(2, incoming, 10, 20), versioned=True)
        return ok, c.data["k"].version
    ok, version = run(main())
    assert ok is accepted
    assert version == (incoming if accepted else cached)
