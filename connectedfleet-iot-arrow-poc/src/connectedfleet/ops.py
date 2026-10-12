"""Operational commands behind `make init`, `make seed`, `make status` (run on the host via uv).

    python -m connectedfleet.ops init        # topics, ClickHouse DDL, Mongo users + seed, connectors
    python -m connectedfleet.ops seed        # deterministic source data (idempotent upserts)
    python -m connectedfleet.ops versions    # write versions.json from the running stack
    python -m connectedfleet.ops status      # connectors, topic sizes, lane table counts

Every step is idempotent and prints what it did.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone

from connectedfleet import connect as connect_mod
from connectedfleet import kafka_admin, topics
from connectedfleet.clickhouse import schema
from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.config import POC_ROOT, Settings, load_dotenv
from connectedfleet.sources import mongo_state


def _clickhouse(settings: Settings) -> ClickHouseHTTP:
    return ClickHouseHTTP(settings.clickhouse_http_url, settings.clickhouse_user, settings.clickhouse_password)


def _mongo_root_uri() -> str:
    user = os.environ["MONGO_ROOT_USER"]
    password = os.environ["MONGO_ROOT_PASSWORD"]
    host = os.environ.get("MONGO_HOST", "localhost:27017")
    return f"mongodb://{user}:{password}@{host}/?directConnection=true&authSource=admin"


def init_topics(settings: Settings) -> dict:
    return kafka_admin.create_topics(settings.kafka_bootstrap, kafka_admin.all_topic_specs())


def init_clickhouse(settings: Settings) -> int:
    return schema.apply(_clickhouse(settings))


def init_mongo_users() -> list[str]:
    """Application user (readWrite) and Debezium user. Debezium roles are a POC simplification:
    readAnyDatabase + clusterMonitor. VERIFY least-privilege set against Debezium 3.6 MongoDB docs."""
    from pymongo import MongoClient

    client = MongoClient(_mongo_root_uri(), serverSelectionTimeoutMS=15_000)
    admin = client.admin
    wanted = {
        os.environ.get("MONGO_APP_USER", "cf_app"): (
            os.environ["MONGO_APP_PASSWORD"], [{"role": "readWrite", "db": mongo_state.DATABASE}]),
        os.environ.get("MONGO_DEBEZIUM_USER", "cf_debezium"): (
            os.environ["MONGO_DEBEZIUM_PASSWORD"],
            [{"role": "readAnyDatabase", "db": "admin"}, {"role": "clusterMonitor", "db": "admin"}]),
    }
    done = []
    for user, (password, roles) in wanted.items():
        exists = admin.command("usersInfo", user)["users"]
        if exists:
            admin.command("updateUser", user, pwd=password, roles=roles)
            done.append(f"{user}: updated")
        else:
            admin.command("createUser", user, pwd=password, roles=roles)
            done.append(f"{user}: created")
    return done


def seed(settings: Settings) -> dict:
    coll = mongo_state.collection(settings.mongo_uri)
    return {"mongo_device_state_docs": mongo_state.seed(coll), "postgres": "seeded by infra/postgres/01-schema.sql on first start"}


def init_connectors(settings: Settings) -> dict:
    client = connect_mod.ConnectClient(settings.connect_url)
    missing = client.missing_plugins()
    if missing:
        raise SystemExit(f"Kafka Connect is missing plugins: {missing}")
    result = {}
    for name in (*connect_mod.SOURCE_CONNECTORS, connect_mod.SINK_CONNECTOR):
        status = connect_mod.register(client, name)
        result[name] = status["connector"]["state"]
    return result


def versions(settings: Settings) -> dict:
    snapshot: dict = {"captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    try:
        images = subprocess.run(["docker", "compose", "images", "--format", "json"], cwd=POC_ROOT,
                                capture_output=True, text=True, check=True, timeout=60).stdout
        snapshot["compose_images"] = json.loads(images) if images.strip().startswith("[") else [
            json.loads(line) for line in images.splitlines() if line.strip()]
    except (OSError, subprocess.SubprocessError, ValueError) as err:
        snapshot["compose_images"] = f"unavailable: {err}"
    ch = _clickhouse(settings)
    snapshot["clickhouse"] = ch.scalar("SELECT version()")
    client = connect_mod.ConnectClient(settings.connect_url)
    snapshot["kafka_connect"] = client.version()
    snapshot["connector_plugins"] = client._request("GET", "/connector-plugins")
    try:
        import psycopg

        with psycopg.connect(settings.postgres_dsn) as conn:
            snapshot["postgres"] = conn.execute("SHOW server_version").fetchone()[0]
    except Exception as err:  # noqa: BLE001
        snapshot["postgres"] = f"unavailable: {err}"
    try:
        from pymongo import MongoClient

        snapshot["mongodb"] = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000).server_info()["version"]
    except Exception as err:  # noqa: BLE001
        snapshot["mongodb"] = f"unavailable: {err}"
    import confluent_kafka
    import pyarrow

    snapshot["python"] = platform.python_version()
    snapshot["pyarrow"] = pyarrow.__version__
    snapshot["confluent_kafka"] = confluent_kafka.version()[0]
    snapshot["librdkafka"] = confluent_kafka.libversion()[0]
    try:
        import clickhouse_connect

        snapshot["clickhouse_connect"] = clickhouse_connect.__version__
    except Exception:  # noqa: BLE001
        pass
    (POC_ROOT / "versions.json").write_text(json.dumps(snapshot, indent=2, default=str) + "\n")
    return snapshot


def status(settings: Settings) -> dict:
    client = connect_mod.ConnectClient(settings.connect_url)
    out: dict = {"connectors": {}}
    for name in client.list():
        st = client.status(name) or {}
        out["connectors"][name] = [st.get("connector", {}).get("state"), *[t["state"] for t in st.get("tasks", [])]]
    out["topics"] = {}
    for topic in (*topics.RAW_TOPICS, *topics.CANONICAL_TOPICS, *topics.DLQ_TOPICS):
        try:
            out["topics"][topic] = kafka_admin.message_count(settings.kafka_bootstrap, topic)
        except Exception as err:  # noqa: BLE001
            out["topics"][topic] = f"error: {err}"
    ch = _clickhouse(settings)
    out["clickhouse"] = {
        db: {t: int(ch.scalar(f"SELECT count() FROM `{db}`.`{t}`") or 0) for t in topics.TOPIC_TABLE.values()}
        for db in topics.LANE_DATABASES.values()
    }
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("init", "topics", "clickhouse", "mongo-users", "connectors", "seed", "versions", "status"))
    args = parser.parse_args(argv)
    load_dotenv(POC_ROOT / ".env")
    settings = Settings.from_env()

    steps = {
        "topics": lambda: init_topics(settings),
        "clickhouse": lambda: {"statements_applied": init_clickhouse(settings)},
        "mongo-users": init_mongo_users,
        "seed": lambda: seed(settings),
        "connectors": lambda: init_connectors(settings),
        "versions": lambda: versions(settings),
        "status": lambda: status(settings),
    }
    # Seed Mongo BEFORE registering connectors so the initial snapshot (op "r") is observable.
    order = ("topics", "clickhouse", "mongo-users", "seed", "connectors", "versions") if args.command == "init" else (args.command,)
    for step in order:
        print(f"== {step}", flush=True)
        print(json.dumps(steps[step](), indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
