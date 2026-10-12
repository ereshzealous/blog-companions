"""Fixture determinism and coverage of every injected edge case."""

from collections import Counter

from connectedfleet.codec import telemetry as codec
from connectedfleet.fixtures import telemetry as fixtures

# One full cycle of malformed kinds: 12 kinds x period 200.
FULL_CYCLE = fixtures.MALFORMED_PERIOD * len(fixtures.MALFORMED_KINDS)


def test_same_seed_same_bytes():
    a = fixtures.generate(5_000, seed=7)
    b = fixtures.generate(5_000, seed=7)
    assert a.sha256 == b.sha256
    assert a.lines == b.lines


def test_different_seed_different_bytes():
    assert fixtures.generate(1_000, seed=7).sha256 != fixtures.generate(1_000, seed=8).sha256


def test_prefix_stability():
    """A smaller fixture is a prefix of a larger one with the same seed."""
    small = fixtures.generate(1_000)
    large = fixtures.generate(3_000)
    assert large.lines[:1_000] == small.lines


def test_full_cycle_contains_every_edge_case():
    fx = fixtures.generate(FULL_CYCLE)
    for kind, _ in fixtures.MALFORMED_KINDS:
        assert fx.injected.get(kind) == 1, kind
    for key in (
        "battery_at_20",
        "temperature_at_40",
        "just_risky",
        "both_at_boundary",
        "temperature_null",
        "temperature_absent",
        "battery_null",
        "battery_absent",
        "late_event",
    ):
        assert fx.injected.get(key, 0) > 0, key


def test_observed_rejects_equal_injected_rejects():
    fx = fixtures.generate(FULL_CYCLE * 2)
    decoded, decode_rejects = codec.decode(fx.lines)
    _, validate_rejects, stats = codec.validate(decoded)
    observed = Counter(r.reason for r in decode_rejects + validate_rejects)
    assert dict(observed) == fx.expected_rejects
    assert stats.unknown_fields == fx.injected["unknown_extra_field"]


def test_unicode_ids_present():
    fx = fixtures.generate(2_000)
    text = b"\n".join(fx.lines).decode("utf-8")
    assert "capteur-é" in text or "センサー-" in text
    assert any(site in text for site in ("werk-münchen", "工場-東京", "planta-são-paulo", "zavod-новосибирск"))


def test_event_times_are_fixed_not_wall_clock():
    fx = fixtures.generate(10)
    assert b'"event_id":"evt-00000000"' in fx.lines[0]
    assert b'"event_time":"2026-10-09T08:00:00.000Z"' in fx.lines[0] or b"2026-10-09T07:" in fx.lines[0]
