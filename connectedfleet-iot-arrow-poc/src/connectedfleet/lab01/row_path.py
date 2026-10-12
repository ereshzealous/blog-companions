"""Row path: typed Python tuples, one record at a time.

This is the control. It applies exactly the same risk rule, projection and
per-site aggregation as the Arrow path, without building columns.
"""

from __future__ import annotations

import json

from connectedfleet.contracts import telemetry as contract
from connectedfleet.lab01 import aggregate

NAME = "row"
OUTPUT_FORMAT = "JSONEachRow (one JSON object per line; event_time as epoch milliseconds)"


def transform(rows: list[tuple], state: dict) -> tuple[list[tuple], list[str]]:
    """Return (enriched rows, alert event ids) and update the per-site state."""
    risk_of = contract.risk_flag
    enriched: list[tuple] = []
    alerts: list[str] = []
    for event_id, device_id, event_ms, _emitted, _source, _version, temp, battery, _signal, _fw, site in rows:
        risk = risk_of(battery, temp)
        enriched.append((event_id, device_id, event_ms, site, temp, battery, risk))
        if risk is True:
            alerts.append(event_id)
        agg = state.get(site)
        if agg is None:
            agg = state[site] = aggregate.new_site()
        agg[aggregate.EVENTS] += 1
        if risk is True:
            agg[aggregate.RISK_TRUE] += 1
        elif risk is None:
            agg[aggregate.RISK_UNKNOWN] += 1
        if temp is not None:
            agg[aggregate.TEMP_COUNT] += 1
            agg[aggregate.TEMP_SUM] += temp
        if battery is not None:
            current = agg[aggregate.BATTERY_MIN]
            if current is None or battery < current:
                agg[aggregate.BATTERY_MIN] = battery
    return enriched, alerts


def serialize(enriched: list[tuple]) -> bytes:
    """Encode the enriched batch as JSONEachRow, the row-oriented counterpart of an IPC stream."""
    fields = contract.ENRICHED_FIELDS
    dumps = json.dumps
    return b"\n".join(
        dumps(dict(zip(fields, row)), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for row in enriched
    )


def risk_vector(enriched: list[tuple]) -> list[tuple[str, bool | None]]:
    """(event_id, risk_flag) pairs, used only by the untimed parity check."""
    return [(row[0], row[6]) for row in enriched]
