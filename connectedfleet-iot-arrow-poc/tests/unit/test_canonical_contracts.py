"""Executable contracts agree with the JSON Schemas, the examples and the Lab 01 telemetry types."""

import json
from pathlib import Path

import jsonschema
import pytest

from connectedfleet import topics
from connectedfleet.contracts import canonical
from connectedfleet.contracts import telemetry as tel

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"


def test_every_canonical_topic_has_a_contract_and_table():
    assert set(canonical.CONTRACTS) == set(topics.CANONICAL_TOPICS)
    for topic, contract in canonical.CONTRACTS.items():
        assert contract.table == topics.TOPIC_TABLE[topic]


def test_telemetry_contract_matches_lab01_arrow_schema():
    assert canonical.TELEMETRY.arrow_schema == tel.ARROW_SCHEMA


@pytest.mark.parametrize("topic", topics.CANONICAL_TOPICS)
def test_examples_validate_against_json_schema_and_contract(topic):
    schema = json.loads((CONTRACTS / f"{topic}.schema.json").read_text())
    example = json.loads((CONTRACTS / "examples" / f"{topic}.json").read_text())
    jsonschema.validate(example, schema)
    row = canonical.validate(canonical.CONTRACTS[topic], example)
    assert len(row) == len(canonical.CONTRACTS[topic].fields)


@pytest.mark.parametrize("topic", topics.CANONICAL_TOPICS)
def test_json_schema_required_fields_are_non_nullable_in_contract(topic):
    schema = json.loads((CONTRACTS / f"{topic}.schema.json").read_text())
    contract = canonical.CONTRACTS[topic]
    non_nullable = {f.name for f in contract.fields if not f.nullable}
    assert set(schema["required"]) <= set(contract.names)
    assert set(schema["required"]) <= non_nullable


@pytest.mark.parametrize("topic", topics.CANONICAL_TOPICS)
def test_validate_then_to_json_roundtrips(topic):
    contract = canonical.CONTRACTS[topic]
    example = json.loads((CONTRACTS / "examples" / f"{topic}.json").read_text())
    again = canonical.validate(contract, canonical.to_json_value(contract, canonical.validate(contract, example)))
    assert again == canonical.validate(contract, example)


def test_enum_bool_and_required_rules():
    example = json.loads((CONTRACTS / "examples" / "cf.device-registry.v1.json").read_text())
    contract = canonical.DEVICE_REGISTRY
    with pytest.raises(canonical.ContractError) as err:
        canonical.validate(contract, {**example, "operation": "upsert"})
    assert err.value.code == "BAD_ENUM"
    with pytest.raises(canonical.ContractError) as err:
        canonical.validate(contract, {**example, "is_deleted": 1})
    assert err.value.code == "BAD_TYPE"
    with pytest.raises(canonical.ContractError) as err:
        canonical.validate(contract, {k: v for k, v in example.items() if k != "version"})
    assert err.value.code == "MISSING_REQUIRED"
    with pytest.raises(canonical.ContractError) as err:
        canonical.validate(contract, {**example, "schema_version": 2})
    assert err.value.code == "UNSUPPORTED_SCHEMA_VERSION"
    row = canonical.validate(contract, {**example, "is_deleted": True})
    assert row[contract.names.index("is_deleted")] == 1


def test_timestamps_are_utc_milliseconds_and_reformat_exactly():
    ms = canonical.parse_timestamp_ms("2026-10-09T10:30:00.123+02:00", "t")
    assert canonical.format_timestamp_ms(ms) == "2026-10-09T08:30:00.123Z"
    with pytest.raises(canonical.ContractError):
        canonical.parse_timestamp_ms("2026-10-09T08:30:00", "t")
