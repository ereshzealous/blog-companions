"""Kafka Connect REST helpers: idempotent registration and real readiness checks.

Registration uses `PUT /connectors/<name>/config` (create-or-update). Placeholders
`{{ENV:NAME}}` in connectors/*.json are filled from the environment, and keys
starting with `_` (inline documentation) are stripped before sending.
https://kafka.apache.org/documentation/#connect_rest
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

from connectedfleet.config import POC_ROOT

CONNECTORS_DIR = POC_ROOT / "connectors"
SOURCE_CONNECTORS = ("cf-pg-registry", "cf-pg-outbox", "cf-mongo-state")
SINK_CONNECTOR = "cf-clickhouse-sink"
REQUIRED_PLUGINS = (
    "io.debezium.connector.postgresql.PostgresConnector",
    "io.debezium.connector.mongodb.MongoDbConnector",
    "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
)
_PLACEHOLDER = re.compile(r"\{\{ENV:([A-Z0-9_]+)\}\}")


class ConnectError(Exception):
    pass


def render(config: dict, env: dict | None = None) -> dict:
    env = os.environ if env is None else env
    out = {}
    for key, value in config.items():
        if key.startswith("_"):
            continue
        if isinstance(value, str):
            missing = [name for name in _PLACEHOLDER.findall(value) if name not in env]
            if missing:
                raise ConnectError(f"environment variable(s) {missing} required by '{key}'")
            value = _PLACEHOLDER.sub(lambda m: env[m.group(1)], value)
        out[key] = value
    return out


def load(name: str, directory: Path = CONNECTORS_DIR) -> dict:
    return json.loads((directory / f"{name}.json").read_text())


class ConnectClient:
    def __init__(self, base_url: str, timeout_s: float = 30.0) -> None:
        self.base = base_url.rstrip("/")
        self.timeout = timeout_s

    def _request(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"{self.base}{path}", data=data, method=method, headers={"Content-Type": "application/json", "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as err:
            if err.code == 404 and method in ("GET", "DELETE"):
                return None
            raise ConnectError(f"{method} {path} -> HTTP {err.code}: {err.read().decode(errors='replace')[:500]}") from err
        return json.loads(raw) if raw else None

    def plugins(self) -> list[str]:
        return [p["class"] for p in self._request("GET", "/connector-plugins") or []]

    def missing_plugins(self) -> list[str]:
        loaded = set(self.plugins())
        return [p for p in REQUIRED_PLUGINS if p not in loaded]

    def put_config(self, name: str, config: dict) -> dict:
        return self._request("PUT", f"/connectors/{name}/config", config)

    def status(self, name: str) -> dict | None:
        return self._request("GET", f"/connectors/{name}/status")

    def delete(self, name: str) -> None:
        self._request("DELETE", f"/connectors/{name}")

    def pause(self, name: str) -> None:
        self._request("PUT", f"/connectors/{name}/pause")

    def resume(self, name: str) -> None:
        self._request("PUT", f"/connectors/{name}/resume")

    def restart(self, name: str) -> None:
        self._request("POST", f"/connectors/{name}/restart?includeTasks=true")

    def list(self) -> list[str]:
        return self._request("GET", "/connectors") or []

    def version(self) -> dict:
        return self._request("GET", "/") or {}

    def wait_running(self, name: str, timeout_s: float = 120.0) -> dict:
        """RUNNING means the connector and every task report RUNNING (not merely 'registered')."""
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            last = self.status(name)
            if last:
                states = [last["connector"]["state"], *[t["state"] for t in last.get("tasks", [])]]
                if "FAILED" in states:
                    trace = next((t.get("trace", "") for t in last.get("tasks", []) if t["state"] == "FAILED"), "")
                    raise ConnectError(f"{name} FAILED: {trace[:800]}")
                if last.get("tasks") and all(s == "RUNNING" for s in states):
                    return last
            time.sleep(2)
        raise ConnectError(f"{name} not RUNNING within {timeout_s}s; last status: {json.dumps(last)[:500]}")


def register(client: ConnectClient, name: str, overrides: dict | None = None, rename: str | None = None) -> dict:
    """Create-or-update a connector from connectors/<name>.json; returns the final RUNNING status."""
    spec = load(name)
    config = render({**spec["config"], **(overrides or {})})
    target = rename or spec["name"]
    client.put_config(target, config)
    return client.wait_running(target)
