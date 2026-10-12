"""Row path and Arrow path must produce identical results for identical input."""

import math

import pytest

from connectedfleet.fixtures import telemetry as fixtures
from connectedfleet.lab01.runner import iter_batches, run_once

FIXTURE = fixtures.generate(4_800)


@pytest.mark.parametrize("batch_size", [100, 1_000, 10_000])
def test_row_and_arrow_outputs_match(batch_size):
    row = run_once("row", FIXTURE.lines, batch_size)
    arrow = run_once("arrow", FIXTURE.lines, batch_size)

    assert row.valid_rows == arrow.valid_rows > 0
    assert row.rejects == arrow.rejects == FIXTURE.expected_rejects
    assert row.field_stats == arrow.field_stats
    assert row.risk_counts == arrow.risk_counts
    assert row.risk_digest == arrow.risk_digest
    assert row.alerts == arrow.alerts
    assert len(row.site_summary) == len(arrow.site_summary)
    for r, a in zip(row.site_summary, arrow.site_summary):
        assert r["site_id"] == a["site_id"]
        for key in ("events", "risk_true", "risk_unknown", "temperature_count", "battery_min"):
            assert r[key] == a[key], (r["site_id"], key)
        if r["temperature_mean"] is None:
            assert a["temperature_mean"] is None
        else:
            assert math.isclose(r["temperature_mean"], a["temperature_mean"], rel_tol=1e-9)


def test_results_do_not_depend_on_batch_size():
    small = run_once("arrow", FIXTURE.lines, 100)
    large = run_once("arrow", FIXTURE.lines, 10_000)
    assert small.risk_digest == large.risk_digest
    assert small.alerts == large.alerts
    assert small.batches == 48 and large.batches == 1


def test_every_risk_state_occurs():
    result = run_once("row", FIXTURE.lines, 1_000)
    assert all(result.risk_counts[state] > 0 for state in ("true", "false", "unknown"))


def test_unknown_site_is_grouped_identically():
    row = run_once("row", FIXTURE.lines, 1_000)
    arrow = run_once("arrow", FIXTURE.lines, 1_000)
    assert row.site_summary[0]["site_id"] is None
    assert arrow.site_summary[0]["site_id"] is None


def test_batches_are_bounded():
    sizes = [len(chunk) for _, chunk in iter_batches(FIXTURE.lines, 1_000)]
    assert max(sizes) <= 1_000
    assert sum(sizes) == len(FIXTURE.lines)


def test_build_phase_exists_only_on_arrow_path():
    row = run_once("row", FIXTURE.lines, 1_000)
    arrow = run_once("arrow", FIXTURE.lines, 1_000)
    assert row.phase_ns["build"] == 0
    assert arrow.phase_ns["build"] > 0
