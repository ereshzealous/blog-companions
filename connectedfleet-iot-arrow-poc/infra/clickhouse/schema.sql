-- ConnectedFleet ClickHouse model, applied identically to bench_sink, bench_row and bench_arrow.
-- `{db}` is substituted by connectedfleet.clickhouse.schema; every statement is idempotent.
-- Column names/types must match src/connectedfleet/contracts/canonical.py (checked by tests/unit/test_clickhouse_schema.py).
-- `ingested_at` is filled by the server at insert time; no lane sends it.

CREATE DATABASE IF NOT EXISTS {db};

-- Append-only telemetry. Duplicates from at-least-once delivery stay physically present;
-- logical reads use uniqExact(event_id) / LIMIT 1 BY event_id.
CREATE TABLE IF NOT EXISTS {db}.telemetry_fact
(
    event_id         String,
    device_id        String,
    event_time       DateTime64(3, 'UTC'),
    emitted_at       DateTime64(3, 'UTC'),
    source_system    LowCardinality(String),
    schema_version   UInt8,
    temperature_c    Nullable(Float64),
    battery_pct      Nullable(UInt8),
    signal_dbm       Nullable(Int16),
    firmware_version LowCardinality(Nullable(String)),
    site_id          LowCardinality(Nullable(String)),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time, event_id);

-- Lab 03 E2 only: lanes B and C insert telemetry plus the risk flag they computed.
CREATE TABLE IF NOT EXISTS {db}.telemetry_enriched
(
    event_id         String,
    device_id        String,
    event_time       DateTime64(3, 'UTC'),
    emitted_at       DateTime64(3, 'UTC'),
    source_system    LowCardinality(String),
    schema_version   UInt8,
    temperature_c    Nullable(Float64),
    battery_pct      Nullable(UInt8),
    signal_dbm       Nullable(Int16),
    firmware_version LowCardinality(Nullable(String)),
    site_id          LowCardinality(Nullable(String)),
    risk_flag        Nullable(Bool),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS {db}.lifecycle_events
(
    event_id         String,
    device_id        String,
    event_time       DateTime64(3, 'UTC'),
    emitted_at       DateTime64(3, 'UTC'),
    source_system    LowCardinality(String),
    schema_version   UInt8,
    event_type       LowCardinality(String),
    aggregate_type   LowCardinality(String),
    firmware_version Nullable(String),
    site_id          Nullable(String),
    customer_id      Nullable(String),
    reason           Nullable(String),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time, event_id);

-- file_sha256 + row_number separate a logical duplicate (same event_id) from a physical re-import.
CREATE TABLE IF NOT EXISTS {db}.diagnostic_events
(
    event_id         String,
    device_id        String,
    event_time       DateTime64(3, 'UTC'),
    emitted_at       DateTime64(3, 'UTC'),
    source_system    LowCardinality(String),
    schema_version   UInt8,
    file_sha256      String,
    row_number       UInt32,
    error_code       LowCardinality(String),
    severity         LowCardinality(String),
    battery_pct      Nullable(UInt8),
    temperature_c    Nullable(Float64),
    firmware_version Nullable(String),
    note             Nullable(String),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (device_id, event_time, event_id);

-- Full CDC history (every version, including delete markers).
CREATE TABLE IF NOT EXISTS {db}.device_registry_versions
(
    event_id         String,
    device_id        String,
    event_time       DateTime64(3, 'UTC'),
    emitted_at       DateTime64(3, 'UTC'),
    source_system    LowCardinality(String),
    schema_version   UInt8,
    operation        LowCardinality(String),
    is_deleted       UInt8,
    version          UInt64,
    customer_id      Nullable(String),
    site_id          Nullable(String),
    firmware_version Nullable(String),
    state            LowCardinality(Nullable(String)),
    updated_at       Nullable(DateTime64(3, 'UTC')),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (device_id, version, event_id);

-- Current state: background merges keep the highest version per device and drop is_deleted rows
-- eventually. Correct reads need FINAL (or argMax by version); merges are not immediate.
-- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
CREATE TABLE IF NOT EXISTS {db}.device_registry_current
(
    device_id        String,
    version          UInt64,
    is_deleted       UInt8,
    customer_id      Nullable(String),
    site_id          Nullable(String),
    firmware_version Nullable(String),
    state            LowCardinality(Nullable(String)),
    updated_at       Nullable(DateTime64(3, 'UTC')),
    event_time       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY device_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.device_registry_current_mv TO {db}.device_registry_current AS
SELECT device_id, version, is_deleted, customer_id, site_id, firmware_version, state, updated_at, event_time
FROM {db}.device_registry_versions;

CREATE TABLE IF NOT EXISTS {db}.device_state_versions
(
    event_id            String,
    device_id           String,
    event_time          DateTime64(3, 'UTC'),
    emitted_at          DateTime64(3, 'UTC'),
    source_system       LowCardinality(String),
    schema_version      UInt8,
    operation           LowCardinality(String),
    is_deleted          UInt8,
    version             UInt64,
    config_profile      Nullable(String),
    sampling_interval_s Nullable(UInt32),
    reporting_mode      LowCardinality(Nullable(String)),
    desired_firmware    Nullable(String),
    full_document       UInt8,
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (device_id, version, event_id);

CREATE TABLE IF NOT EXISTS {db}.device_state_current
(
    device_id           String,
    version             UInt64,
    is_deleted          UInt8,
    config_profile      Nullable(String),
    sampling_interval_s Nullable(UInt32),
    reporting_mode      LowCardinality(Nullable(String)),
    desired_firmware    Nullable(String),
    full_document       UInt8,
    event_time          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY device_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.device_state_current_mv TO {db}.device_state_current AS
SELECT device_id, version, is_deleted, config_profile, sampling_interval_s, reporting_mode, desired_firmware, full_document, event_time
FROM {db}.device_state_versions;

-- Lab 03 E3: direct CSV backfill target (never fed from Kafka). Same table in every bench DB.
CREATE TABLE IF NOT EXISTS {db}.diagnostic_backfill
(
    row_number       UInt32,
    device_id        String,
    diagnosed_at     DateTime64(3, 'UTC'),
    error_code       LowCardinality(String),
    severity         LowCardinality(String),
    battery_pct      Nullable(UInt8),
    temperature_c    Nullable(Float64),
    firmware_version Nullable(String),
    note             Nullable(String),
    file_sha256      String,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (device_id, diagnosed_at, row_number);
