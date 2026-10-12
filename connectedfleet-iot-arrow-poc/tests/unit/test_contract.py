"""The frozen telemetry contract: Kleene logic, strict boundaries, schema alignment."""

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from connectedfleet.contracts import telemetry as contract

KLEENE_TRUTH_TABLE = [
    (True, True, True),
    (True, False, False),
    (False, True, False),
    (False, False, False),
    (None, False, False),
    (False, None, False),
    (None, True, None),
    (True, None, None),
    (None, None, None),
]


@pytest.mark.parametrize(("left", "right", "expected"), KLEENE_TRUTH_TABLE)
def test_kleene_and_matches_truth_table(left, right, expected):
    assert contract.kleene_and(left, right) is expected


@pytest.mark.parametrize(("left", "right", "expected"), KLEENE_TRUTH_TABLE)
def test_python_kleene_matches_pyarrow_and_kleene(left, right, expected):
    arrow = pc.and_kleene(pa.array([left], pa.bool_()), pa.array([right], pa.bool_()))[0].as_py()
    assert arrow is expected


@pytest.mark.parametrize(
    ("battery", "temperature", "expected"),
    [
        (19, 40.1, True),  # just inside both thresholds
        (20, 40.1, False),  # battery boundary is strict
        (19, 40.0, False),  # temperature boundary is strict
        (20, 40.0, False),
        (5, 80.0, True),
        (None, 30.0, False),  # unknown AND false -> false
        (None, 45.0, None),  # unknown AND true -> unknown
        (10, None, None),
        (50, None, False),
        (None, None, None),
    ],
)
def test_risk_flag_boundaries_and_nulls(battery, temperature, expected):
    assert contract.risk_flag(battery, temperature) is expected


def test_typed_fields_align_with_arrow_schema():
    renamed = [name.removesuffix("_ms") for name in contract.TYPED_FIELDS]
    assert renamed == contract.ARROW_SCHEMA.names


def test_enriched_schema_matches_enriched_fields():
    assert list(contract.ENRICHED_FIELDS) == contract.ENRICHED_ARROW_SCHEMA.names


def test_timestamps_are_utc_milliseconds():
    for name in ("event_time", "emitted_at"):
        field = contract.ARROW_SCHEMA.field(name)
        assert field.type == pa.timestamp("ms", tz="UTC")
