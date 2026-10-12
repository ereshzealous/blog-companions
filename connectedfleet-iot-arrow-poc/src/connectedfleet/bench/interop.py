"""Lab 03 E5 (optional): hand one Arrow table to other Arrow-native libraries and time each step.

Runs in-process on the host (`uv sync --group interop`). It times wall-clock
steps only; it does NOT assert whether a library copied buffers (that needs
allocator-level instrumentation this POC does not have). Results are labelled
accordingly.
"""

from __future__ import annotations

import statistics
import time

import duckdb  # ImportError here marks E5 as NOT_RUN (optional `interop` dependency group)
import polars as pl

import pyarrow as pa

from connectedfleet.bench.workload import WorkloadSpec, events
from connectedfleet.codec import telemetry as codec
from connectedfleet.lab01 import arrow_path


def _time(fn, repeat: int = 5) -> dict:
    samples = []
    result = None
    for _ in range(repeat):
        started = time.perf_counter()
        result = fn()
        samples.append((time.perf_counter() - started) * 1000)
    return {"median_ms": statistics.median(samples), "min_ms": min(samples), "max_ms": max(samples), "repeat": repeat,
            "result_rows": getattr(result, "num_rows", None) or (len(result) if hasattr(result, "__len__") else None)}


def measure(count: int, seed: int) -> dict:
    import json

    spec = WorkloadSpec(count, seed)
    lines = [json.dumps({**e, "emitted_at": e["event_time"]}).encode() for e in events(spec)]
    decoded, _ = codec.decode(lines)
    rows, _, _ = codec.validate(decoded)
    table = pa.Table.from_batches([arrow_path.build(rows)])

    con = duckdb.connect()
    con.register("telemetry", table)
    sql = "SELECT site_id, count(*) AS n, avg(temperature_c) AS t FROM telemetry GROUP BY site_id"
    return {
        "event_count": count,
        "table_nbytes": table.nbytes,
        "versions": {"pyarrow": pa.__version__, "polars": pl.__version__, "duckdb": duckdb.__version__},
        "pyarrow_group_by": _time(lambda: table.group_by("site_id").aggregate([("event_id", "count"), ("temperature_c", "mean")])),
        "polars_from_arrow": _time(lambda: pl.from_arrow(table)),
        "polars_group_by": _time(lambda: pl.from_arrow(table).group_by("site_id").agg(pl.len(), pl.col("temperature_c").mean())),
        "duckdb_scan_group_by": _time(lambda: con.execute(sql).fetch_arrow_table()),
        "copy_behaviour": "not asserted: wall-clock timings only",
        "scope": "host process (not the Docker VM)",
    }
