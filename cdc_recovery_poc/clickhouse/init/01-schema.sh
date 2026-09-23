#!/bin/bash
# Change history (append-only) and current state (version-aware) for each captured table.
set -euo pipefail

clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS cdc;

-- What changed: one row per delivered change event. Replays are kept, so duplicates are measurable.
CREATE TABLE IF NOT EXISTS cdc.change_history
(
    event_id        String,
    source_table    LowCardinality(String),
    op              LowCardinality(String),
    snapshot        LowCardinality(String),
    device_id       Int64,
    lsn             UInt64,
    tx_id           UInt64,
    source_ts_ms    Int64,
    capture_ts_ms   Int64,
    kafka_partition UInt16,
    kafka_offset    UInt64,
    after_json      String CODEC(ZSTD(3)),
    sink_id         LowCardinality(String),
    batch_id        String,
    applied_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (toStartOfMinute(applied_at), source_table, device_id);

-- What is true now: the highest source position wins; deletes are rows with is_deleted = 1.
CREATE TABLE IF NOT EXISTS cdc.device_registry_current
(
    device_id        Int64,
    tenant_id        Int32,
    serial_number    String,
    model            LowCardinality(String),
    gateway_id       Int64,
    status           LowCardinality(String),
    firmware_version String,
    updated_at_ms    Int64,
    version          UInt64,
    is_deleted       UInt8,
    source_ts_ms     Int64,
    sink_id          LowCardinality(String),
    applied_at       DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY device_id;

CREATE TABLE IF NOT EXISTS cdc.device_status_current
(
    device_id     Int64,
    tenant_id     Int32,
    battery_pct   Int16,
    signal_dbm    Int16,
    connectivity  LowCardinality(String),
    last_seen_ms  Int64,
    updated_at_ms Int64,
    version       UInt64,
    is_deleted    UInt8,
    source_ts_ms  Int64,
    sink_id       LowCardinality(String),
    applied_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY device_id;

CREATE USER IF NOT EXISTS cdc_sink IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_SINK_PASSWORD}';
GRANT SELECT, INSERT ON cdc.* TO cdc_sink;
SQL
