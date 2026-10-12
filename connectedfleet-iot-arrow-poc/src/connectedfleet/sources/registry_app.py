"""A tiny registry application that writes business rows and outbox events atomically.

Every business change and its outbox row are written in ONE PostgreSQL
transaction (`with conn.transaction()`): either both commit or neither does.
Debezium then captures the committed outbox row. This guarantees a committed
change has a committed event; it does not make the downstream pipeline
exactly-once. Consumers still dedupe by the stable outbox id.

Outbox ids are deterministic (uuid5 of run tag + step), so Lab 02 knows
exactly which events must, and must not, appear downstream.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

NAMESPACE = uuid.UUID("6f1c6a52-2a0b-4f1e-9d55-3c1f0b0c0a11")


def outbox_id(run_tag: str, step: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{run_tag}:{step}"))


class SimulatedFailure(Exception):
    """Raised inside a transaction to force a rollback."""


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    event_type: str
    device_id: str
    payload: dict


def connect(dsn: str):
    import psycopg

    return psycopg.connect(dsn, autocommit=False)


def _insert_outbox(cur, event: OutboxEvent) -> None:
    from psycopg.types.json import Jsonb

    cur.execute(
        "INSERT INTO outbox_event (id, aggregatetype, aggregateid, type, payload, occurred_at) "
        "VALUES (%s, 'device', %s, %s, %s, now())",
        (event.event_id, event.device_id, event.event_type, Jsonb({"device_id": event.device_id, **event.payload})),
    )


def activate_device(conn, device_id: str, customer_id: str, site_id: str, firmware: str, event_id: str) -> OutboxEvent:
    event = OutboxEvent(event_id, "DeviceActivated", device_id,
                        {"customer_id": customer_id, "site_id": site_id, "firmware_version": firmware})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO device_registry (device_id, customer_id, site_id, firmware_version, state, updated_at) "
            "VALUES (%s, %s, %s, %s, 'active', now())",
            (device_id, customer_id, site_id, firmware),
        )
        _insert_outbox(cur, event)
    return event


def update_firmware(conn, device_id: str, firmware: str, event_id: str) -> OutboxEvent:
    event = OutboxEvent(event_id, "FirmwareUpdated", device_id, {"firmware_version": firmware})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE device_registry SET firmware_version = %s, updated_at = now() WHERE device_id = %s",
            (firmware, device_id),
        )
        if cur.rowcount != 1:
            raise LookupError(device_id)
        _insert_outbox(cur, event)
    return event


def assign_device(conn, device_id: str, site_id: str, customer_id: str, event_id: str) -> OutboxEvent:
    event = OutboxEvent(event_id, "DeviceAssigned", device_id, {"site_id": site_id, "customer_id": customer_id})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE device_registry SET site_id = %s, customer_id = %s, updated_at = now() WHERE device_id = %s",
            (site_id, customer_id, device_id),
        )
        if cur.rowcount != 1:
            raise LookupError(device_id)
        _insert_outbox(cur, event)
    return event


def retire_and_delete(conn, device_id: str, event_id: str, reason: str) -> OutboxEvent:
    event = OutboxEvent(event_id, "DeviceDeactivated", device_id, {"reason": reason})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM device_registry WHERE device_id = %s", (device_id,))
        if cur.rowcount != 1:
            raise LookupError(device_id)
        _insert_outbox(cur, event)
    return event


def set_state(conn, device_id: str, state: str) -> None:
    """A registry change with no business event: plain CDC only."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("UPDATE device_registry SET state = %s, updated_at = now() WHERE device_id = %s", (state, device_id))


def rolled_back_firmware_update(conn, device_id: str, firmware: str, event_id: str) -> OutboxEvent:
    """Write the business change AND the outbox row, then fail before commit: neither may become visible."""
    event = OutboxEvent(event_id, "FirmwareUpdated", device_id, {"firmware_version": firmware})
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE device_registry SET firmware_version = %s, updated_at = now() WHERE device_id = %s",
                (firmware, device_id),
            )
            _insert_outbox(cur, event)
            raise SimulatedFailure("forced rollback after writing both rows")
    except SimulatedFailure:
        pass
    return event


def registry_row(conn, device_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("SELECT device_id, customer_id, site_id, firmware_version, state FROM device_registry WHERE device_id = %s",
                    (device_id,))
        row = cur.fetchone()
    conn.rollback()  # end the implicit read transaction
    if row is None:
        return None
    return dict(zip(("device_id", "customer_id", "site_id", "firmware_version", "state"), row))


def outbox_exists(conn, event_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM outbox_event WHERE id = %s", (event_id,))
        found = cur.fetchone() is not None
    conn.rollback()
    return found
