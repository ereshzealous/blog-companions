"""Apply infra/clickhouse/schema.sql to each lane database (idempotent: CREATE ... IF NOT EXISTS)."""

from __future__ import annotations

import re
from pathlib import Path

from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.config import POC_ROOT
from connectedfleet.topics import LANE_DATABASES

SCHEMA_FILE = POC_ROOT / "infra" / "clickhouse" / "schema.sql"

# Tables truncated between benchmark/lab runs (the *_current tables are fed by materialized views).
DATA_TABLES = (
    "telemetry_fact",
    "telemetry_enriched",
    "lifecycle_events",
    "diagnostic_events",
    "device_registry_versions",
    "device_registry_current",
    "device_state_versions",
    "device_state_current",
    "diagnostic_backfill",
)


def statements(database: str, path: Path = SCHEMA_FILE) -> list[str]:
    text = path.read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    return [s.strip().replace("{db}", database) for s in text.split(";") if s.strip()]


def table_columns(path: Path = SCHEMA_FILE) -> dict[str, list[tuple[str, str]]]:
    """Parse CREATE TABLE column lists -> {table: [(name, type), ...]} (used by tests and validation)."""
    tables: dict[str, list[tuple[str, str]]] = {}
    for stmt in statements("db", path):
        match = re.match(r"CREATE TABLE IF NOT EXISTS db\.(\w+)\s*\((.*)\)\s*ENGINE", stmt, re.S)
        if not match:
            continue
        cols = []
        for line in match.group(2).splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            name, _, rest = line.partition(" ")
            col_type = rest.strip().split(" DEFAULT ")[0].strip()
            cols.append((name, col_type))
        tables[match.group(1)] = cols
    return tables


def apply(client: ClickHouseHTTP, databases: tuple[str, ...] = tuple(LANE_DATABASES.values())) -> int:
    count = 0
    for database in databases:
        for stmt in statements(database):
            client.command(stmt)
            count += 1
    return count


def truncate(client: ClickHouseHTTP, database: str, tables: tuple[str, ...] = DATA_TABLES) -> None:
    for table in tables:
        client.command(f"TRUNCATE TABLE IF EXISTS `{database}`.`{table}`")
