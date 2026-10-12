"""Connector JSON files render with environment placeholders and keep Debezium's own `${...}` syntax."""

import json
from pathlib import Path

import pytest

from connectedfleet import connect, topics

ROOT = Path(__file__).resolve().parents[2]
FAKE_ENV = {
    "DEBEZIUM_PG_USER": "u", "DEBEZIUM_PG_PASSWORD": "p", "POSTGRES_DB": "db",
    "MONGO_DEBEZIUM_USER": "mu", "MONGO_DEBEZIUM_PASSWORD": "mp",
    "CLICKHOUSE_USER": "cu", "CLICKHOUSE_PASSWORD": "cp",
}


@pytest.mark.parametrize("name", [*connect.SOURCE_CONNECTORS, connect.SINK_CONNECTOR])
def test_connector_renders_without_doc_keys_or_placeholders(name):
    spec = connect.load(name)
    assert spec["name"] == name
    rendered = connect.render(spec["config"], FAKE_ENV)
    assert not any(k.startswith("_") for k in rendered)
    assert "{{ENV:" not in json.dumps(rendered)
    assert all(isinstance(v, str) for v in rendered.values()), "Connect REST expects string values"


def test_missing_environment_variable_is_an_error():
    with pytest.raises(connect.ConnectError):
        connect.render(connect.load("cf-pg-registry")["config"], {})


def test_outbox_router_keeps_routed_by_value_placeholder():
    rendered = connect.render(connect.load("cf-pg-outbox")["config"], FAKE_ENV)
    assert rendered["transforms.outbox.route.topic.replacement"] == "cfraw.outbox.${routedByValue}"
    assert "cfraw.outbox.device" == rendered["transforms.outbox.route.topic.replacement"].replace("${routedByValue}", "device")
    assert topics.RAW_OUTBOX == "cfraw.outbox.device"


def test_sink_maps_every_canonical_topic_to_its_table():
    rendered = connect.render(connect.load("cf-clickhouse-sink")["config"], FAKE_ENV)
    mapping = dict(pair.split("=") for pair in rendered["topic2TableMap"].split(","))
    assert mapping == topics.TOPIC_TABLE
    assert rendered["topics"].split(",") == list(topics.CANONICAL_TOPICS)
    assert rendered["exactlyOnce"] == "false"
    assert rendered["errors.deadletterqueue.topic.name"] == topics.DLQ_SINK


def test_source_topic_prefixes_match_raw_topic_names():
    reg = connect.render(connect.load("cf-pg-registry")["config"], FAKE_ENV)
    assert f"{reg['topic.prefix']}.{reg['table.include.list']}" == topics.RAW_PG_REGISTRY
    mongo = connect.render(connect.load("cf-mongo-state")["config"], FAKE_ENV)
    assert f"{mongo['topic.prefix']}.{mongo['collection.include.list']}" == topics.RAW_MONGO_STATE
