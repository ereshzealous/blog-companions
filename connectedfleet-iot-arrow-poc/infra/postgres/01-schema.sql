-- ConnectedFleet PostgreSQL sources. Runs once, on first container start (docker-entrypoint-initdb.d).
-- wal_level=logical is set on the server command line in compose.yaml.

CREATE TABLE device_registry (
    device_id        text PRIMARY KEY,
    customer_id      text,
    site_id          text,
    firmware_version text,
    state            text NOT NULL CHECK (state IN ('registered', 'active', 'maintenance', 'retired')),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
-- REPLICA IDENTITY DEFAULT (primary key only): delete events carry just device_id.
-- This is intentional; the canonical delete marker therefore has null descriptive fields.

-- Transactional outbox. Written in the same transaction as the business change; insert-only during tests.
CREATE TABLE outbox_event (
    id            uuid PRIMARY KEY,
    aggregatetype text NOT NULL,
    aggregateid   text NOT NULL,
    type          text NOT NULL,
    payload       jsonb NOT NULL,
    occurred_at   timestamptz NOT NULL DEFAULT now()
);

-- Three pre-existing devices, so the initial Debezium snapshot (op = "r") is observable.
INSERT INTO device_registry (device_id, customer_id, site_id, firmware_version, state, updated_at) VALUES
    ('sensor-00001', 'acme-logistics', 'factory-01', '3.2.0', 'active',     '2026-10-01T00:00:00Z'),
    ('sensor-00002', 'acme-logistics', 'factory-02', '3.2.0', 'active',     '2026-10-01T00:00:00Z'),
    ('sensor-00003', 'polar-foods',    'werk-münchen', '3.1.9', 'maintenance', '2026-10-01T00:00:00Z');

-- One publication per connector, each with its own replication slot (configured in connectors/*.json).
-- The connectors run with publication.autocreate.mode=disabled.
CREATE PUBLICATION cf_registry_pub FOR TABLE device_registry;
CREATE PUBLICATION cf_outbox_pub FOR TABLE outbox_event;
