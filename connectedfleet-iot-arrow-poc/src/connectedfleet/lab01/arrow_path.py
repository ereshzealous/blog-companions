"""Arrow path: build typed columns, then apply vectorized compute kernels.

Building the RecordBatch is a real, separately timed cost: validated rows
are transposed into per-column Python lists and copied into Arrow buffers.
Only after that do the column kernels (`pyarrow.compute`) run.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from connectedfleet.contracts import telemetry as contract
from connectedfleet.lab01 import aggregate

NAME = "arrow"
OUTPUT_FORMAT = "Arrow IPC streaming format (one stream per batch; the body of an ArrowStream insert)"

_SCHEMA = contract.ARROW_SCHEMA
_ENRICHED_SCHEMA = contract.ENRICHED_ARROW_SCHEMA
_TYPES = [f.type for f in _SCHEMA]

_AGGREGATIONS = [
    ("risk_true", "count"),
    ("risk_true", "sum"),
    ("risk_unknown", "sum"),
    ("temperature_c", "count"),
    ("temperature_c", "sum"),
    ("battery_pct", "min"),
]


def build(rows: list[tuple]) -> pa.RecordBatch:
    """Transpose validated row tuples into typed Arrow arrays."""
    if not rows:
        return pa.RecordBatch.from_pylist([], schema=_SCHEMA)
    columns = list(zip(*rows))
    arrays = [pa.array(values, type=arrow_type) for values, arrow_type in zip(columns, _TYPES)]
    return pa.RecordBatch.from_arrays(arrays, schema=_SCHEMA)


def transform(batch: pa.RecordBatch, state: dict) -> tuple[pa.RecordBatch, pa.Array]:
    """Return (enriched batch, alert event ids) and update the per-site state."""
    battery = batch.column("battery_pct")
    temperature = batch.column("temperature_c")
    site = batch.column("site_id")

    risk = pc.and_kleene(
        pc.less(battery, contract.BATTERY_RISK_BELOW),
        pc.greater(temperature, contract.TEMPERATURE_RISK_ABOVE),
    )
    enriched = pa.RecordBatch.from_arrays(
        [
            batch.column("event_id"),
            batch.column("device_id"),
            batch.column("event_time"),
            site,
            temperature,
            battery,
            risk,
        ],
        schema=_ENRICHED_SCHEMA,
    )
    # filter() drops rows whose mask is null, so only definite `true` risks become alerts.
    alerts = pc.filter(batch.column("event_id"), risk)

    partial = pa.table(
        {
            "site_id": site,
            "risk_true": pc.cast(pc.fill_null(risk, False), pa.int64()),
            "risk_unknown": pc.cast(pc.is_null(risk), pa.int64()),
            "temperature_c": temperature,
            "battery_pct": battery,
        }
    ).group_by("site_id").aggregate(_AGGREGATIONS)
    for row in partial.to_pylist():
        aggregate.merge_partial(
            state,
            row["site_id"],
            events=row["risk_true_count"],
            risk_true=row["risk_true_sum"] or 0,
            risk_unknown=row["risk_unknown_sum"] or 0,
            temp_count=row["temperature_c_count"],
            temp_sum=row["temperature_c_sum"],
            battery_min=row["battery_pct_min"],
        )
    return enriched, alerts


def serialize(enriched: pa.RecordBatch) -> pa.Buffer:
    """Write one Arrow IPC stream (schema message + record batch + end-of-stream) into memory."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, enriched.schema) as writer:
        writer.write_batch(enriched)
    return sink.getvalue()


def risk_vector(enriched: pa.RecordBatch) -> list[tuple[str, bool | None]]:
    """(event_id, risk_flag) pairs, used only by the untimed parity check."""
    return list(zip(enriched.column("event_id").to_pylist(), enriched.column("risk_flag").to_pylist()))
