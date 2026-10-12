"""Read back the physical buffers of real Arrow arrays.

Nothing here is drawn by hand: every value comes from `Array.buffers()` on
arrays built by pyarrow. The four-row toy batch is the same data drawn in
figure 01-F03, so the figure's layout can be checked against real buffers.
"""

from __future__ import annotations

import struct

import pyarrow as pa

from connectedfleet.fixtures.telemetry import TOY_OBSERVATIONS

_ROLES = {
    "string": ("validity", "offsets", "data"),
    "large_string": ("validity", "offsets", "data"),
}


def _roles(arr: pa.Array) -> tuple[str, ...]:
    return _ROLES.get(str(arr.type), ("validity", "data"))


def validity_bits(arr: pa.Array) -> str | None:
    """Validity bitmap as a '1'/'0' string, one char per slot, least-significant bit first."""
    buf = arr.buffers()[0]
    if buf is None:
        return None
    raw = buf.to_pybytes()
    start = arr.offset
    return "".join("1" if raw[(start + i) // 8] >> ((start + i) % 8) & 1 else "0" for i in range(len(arr)))


def describe(arr: pa.Array) -> dict:
    buffers = []
    for role, buf in zip(_roles(arr), arr.buffers()):
        buffers.append({"role": role, "present": buf is not None, "size_bytes": None if buf is None else buf.size})
    return {
        "type": str(arr.type),
        "length": len(arr),
        "null_count": arr.null_count,
        "buffers": buffers,
        "nbytes": arr.nbytes,
    }


def toy_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(
        list(TOY_OBSERVATIONS),
        schema=pa.schema(
            [
                pa.field("device_id", pa.string()),
                pa.field("temperature_c", pa.float64()),
                pa.field("battery_pct", pa.uint8()),
            ]
        ),
    )


def illustrative_layout() -> dict:
    """Exact buffer contents of the four-row toy batch used in figure 01-F03."""
    batch = toy_batch()
    device, temperature, battery = batch.columns
    n = batch.num_rows
    offsets_raw = device.buffers()[1].to_pybytes()
    temp_raw = temperature.buffers()[1].to_pybytes()
    return {
        "label": "illustrative (4 toy rows; real pyarrow buffers, not a benchmark)",
        "rows": list(TOY_OBSERVATIONS),
        "device_id": {
            **describe(device),
            "validity_bits_lsb_first": validity_bits(device),
            "offsets_int32": list(struct.unpack(f"<{n + 1}i", offsets_raw[: (n + 1) * 4])),
            "data_utf8": device.buffers()[2].to_pybytes().decode("utf-8"),
        },
        "temperature_c": {
            **describe(temperature),
            "validity_bits_lsb_first": validity_bits(temperature),
            "values_float64": [
                None if not temperature[i].is_valid else v
                for i, v in enumerate(struct.unpack(f"<{n}d", temp_raw[: n * 8]))
            ],
            "note": "the null slot still occupies 8 bytes in the data buffer; its value is undefined",
        },
        "battery_pct": {
            **describe(battery),
            "validity_bits_lsb_first": validity_bits(battery),
            "values_uint8": list(battery.buffers()[1].to_pybytes()[:n]),
            "note": "no nulls, so pyarrow allocates no validity bitmap",
        },
    }


def observed_layout(batch: pa.RecordBatch) -> dict:
    """Buffer presence and sizes for a real validated telemetry batch."""
    return {
        "label": "observed (real validated telemetry batch from the lab fixture)",
        "num_rows": batch.num_rows,
        "batch_nbytes": batch.nbytes,
        "columns": {name: describe(col) for name, col in zip(batch.schema.names, batch.columns)},
    }
