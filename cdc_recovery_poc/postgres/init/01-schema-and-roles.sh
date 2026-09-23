#!/bin/bash
# Operational schema, least-privilege roles, publication and synthetic seed data.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v cdc_password="$CDC_CAPTURE_PASSWORD" \
  -v app_password="$APP_PASSWORD" \
  -v monitor_password="$MONITOR_PASSWORD" \
  -v registry_rows="$SEED_REGISTRY_ROWS" \
  -v status_rows="$SEED_STATUS_ROWS" <<'SQL'
CREATE SCHEMA ops;

CREATE TABLE ops.device_registry (
  device_id        bigint PRIMARY KEY,
  tenant_id        integer NOT NULL,
  serial_number    text    NOT NULL,
  model            text    NOT NULL,
  gateway_id       bigint  NOT NULL,
  status           text    NOT NULL,
  firmware_version text    NOT NULL,
  updated_at_ms    bigint  NOT NULL
);

CREATE TABLE ops.device_status (
  device_id     bigint   PRIMARY KEY,
  tenant_id     integer  NOT NULL,
  battery_pct   smallint NOT NULL,
  signal_dbm    smallint NOT NULL,
  connectivity  text     NOT NULL,
  last_seen_ms  bigint   NOT NULL,
  updated_at_ms bigint   NOT NULL
) WITH (fillfactor = 70);

-- Debezium source signaling channel (incremental snapshots write watermarks here).
CREATE TABLE ops.debezium_signal (
  id   varchar(42) PRIMARY KEY,
  type varchar(32) NOT NULL,
  data varchar(2048)
);

-- Application identity: DML only.
CREATE ROLE device_app LOGIN PASSWORD :'app_password';
GRANT USAGE ON SCHEMA ops TO device_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ops.device_registry, ops.device_status TO device_app;
GRANT INSERT ON ops.debezium_signal TO device_app;

-- Capture identity: replication + read, plus INSERT on the signal table for snapshot watermarks. Not a superuser.
CREATE ROLE cdc_capture LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT USAGE ON SCHEMA ops TO cdc_capture;
GRANT SELECT ON ops.device_registry, ops.device_status TO cdc_capture;
GRANT SELECT, INSERT, DELETE ON ops.debezium_signal TO cdc_capture;

-- Monitoring identity: slot and WAL visibility only.
CREATE ROLE cdc_monitor LOGIN PASSWORD :'monitor_password';
GRANT pg_monitor TO cdc_monitor;

-- The publication is owned by the database team, not auto-created by the connector.
CREATE PUBLICATION cdc_devices FOR TABLE ops.device_registry, ops.device_status, ops.debezium_signal;

INSERT INTO ops.device_registry
SELECT g,
       1 + (g % 500),
       'SN-' || lpad(g::text, 10, '0'),
       (ARRAY['bp-cuff-2', 'pulse-ox-1', 'ecg-patch-3', 'glucose-4', 'gateway-hub-2'])[1 + (g % 5)],
       1000000 + (g % 20000),
       'active',
       'fw-4.' || (g % 7) || '.' || (g % 13),
       1757750400000 + g
FROM generate_series(1, :registry_rows) AS g;

INSERT INTO ops.device_status
SELECT g,
       1 + (g % 500),
       (20 + g % 80)::smallint,
       (-110 + g % 60)::smallint,
       (ARRAY['lte', 'wifi', 'ethernet'])[1 + (g % 3)],
       1757750400000 + g,
       1757750400000 + g
FROM generate_series(1, :status_rows) AS g;

ANALYZE ops.device_registry;
ANALYZE ops.device_status;
SQL
