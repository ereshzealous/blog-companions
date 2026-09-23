"""Shared configuration for the lab tools. Every tool writes JSON Lines into results/<run-id>/."""
import os
import pathlib
import time

import orjson

CONTROL = pathlib.Path(os.environ.get("CONTROL_DIR", "/control"))
RESULTS = pathlib.Path(os.environ.get("RESULTS_DIR", "/results"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CONNECT_WORKERS = ["http://connect-1:8083", "http://connect-2:8083"]
CONNECTOR = os.environ.get("CONNECTOR_NAME", "pg-devices")
SLOT = "cdc_devices_clickhouse"
SINK_GROUP = "clickhouse-sink"
TOPICS = ["pg-devices.ops.device_registry", "pg-devices.ops.device_status"]

# The sink's data contract: columns per table and version, with the value used for delete rows.
CONTRACT = {
    "device_registry": {
        1: [("device_id", 0), ("tenant_id", 0), ("serial_number", ""), ("model", ""), ("gateway_id", 0),
            ("status", ""), ("firmware_version", ""), ("updated_at_ms", 0)],
    },
    "device_status": {
        1: [("device_id", 0), ("tenant_id", 0), ("battery_pct", 0), ("signal_dbm", 0), ("connectivity", ""),
            ("last_seen_ms", 0), ("updated_at_ms", 0)],
    },
}
CONTRACT["device_registry"][2] = CONTRACT["device_registry"][1] + [("firmware_channel", None)]
CONTRACT["device_status"][2] = CONTRACT["device_status"][1]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def control_value(name: str, default=None):
    path = CONTROL / name
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return default


def contract_version() -> int:
    return int(control_value("contract-version", "1"))


def run_dir() -> pathlib.Path:
    path = RESULTS / control_value("run-id", "adhoc")
    path.mkdir(parents=True, exist_ok=True)
    return path


def append_jsonl(name: str, record: dict) -> None:
    with open(run_dir() / name, "ab") as f:
        f.write(orjson.dumps(record) + b"\n")


def pg_dsn(role: str) -> str:
    user, secret = {
        "app": ("device_app", "APP_PASSWORD"),
        "admin": ("lab_admin", "PG_ADMIN_PASSWORD"),
        "monitor": ("cdc_monitor", "MONITOR_PASSWORD"),
    }[role]
    return f"host=postgres port=5432 dbname=devices user={user} password={os.environ[secret]}"


def ch_kwargs(role: str = "sink") -> dict:
    user, secret = {"sink": ("cdc_sink", "CLICKHOUSE_SINK_PASSWORD"), "admin": ("lab", "CLICKHOUSE_PASSWORD")}[role]
    return dict(host="clickhouse", port=8123, username=user, password=os.environ[secret], database="cdc")
