"""infra/clickhouse/schema.sql must match the executable contracts column-for-column."""

import pytest

from connectedfleet.bench import csv_backfill
from connectedfleet.clickhouse import schema
from connectedfleet.contracts import canonical

INGESTED = ("ingested_at", "DateTime64(3, 'UTC')")
TABLES = schema.table_columns()


@pytest.mark.parametrize("contract", list(canonical.CONTRACTS.values()), ids=lambda c: c.table)
def test_contract_tables_match_ddl(contract):
    assert TABLES[contract.table] == [*contract.clickhouse_columns, INGESTED]


def test_enriched_table_is_telemetry_plus_risk_flag():
    expected = [*canonical.TELEMETRY.clickhouse_columns, ("risk_flag", "Nullable(Bool)"), INGESTED]
    assert TABLES["telemetry_enriched"] == expected


def test_current_state_tables_are_subsets_of_history():
    for current, history in (("device_registry_current", "device_registry_versions"), ("device_state_current", "device_state_versions")):
        history_cols = dict(TABLES[history])
        for name, col_type in TABLES[current]:
            assert history_cols[name] == col_type, (current, name)


def test_backfill_table_matches_csv_backfill_columns():
    names = [n for n, _ in TABLES["diagnostic_backfill"] if n != "ingested_at"]
    assert names == list(csv_backfill.INSERT_COLUMNS)
    types = [t for n, t in TABLES["diagnostic_backfill"] if n != "ingested_at"]
    assert types == list(csv_backfill.CH_TYPES)


def test_statements_are_templated_per_database():
    stmts = schema.statements("bench_row")
    assert stmts and all("{db}" not in s for s in stmts)
    assert stmts[0] == "CREATE DATABASE IF NOT EXISTS bench_row"
    assert all("IF NOT EXISTS" in s for s in stmts), "every statement must be idempotent"


def test_truncate_list_covers_every_table():
    assert set(schema.DATA_TABLES) == set(TABLES)
