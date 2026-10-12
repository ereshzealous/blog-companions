"""One timed pass of a processing path over a fixture, in bounded batches.

Phase boundaries (each measured with `time.perf_counter_ns`):
  decode     JSON bytes -> Python dicts                   (shared code, both paths)
  validate   dicts -> typed row tuples + rejects           (shared code, both paths)
  build      row tuples -> Arrow arrays/RecordBatch        (Arrow path only)
  transform  risk rule + projection + per-site aggregate
  serialize  enriched batch -> bytes ready to send
              (row: JSONEachRow; Arrow: IPC stream in memory)

`total` is the sum of the five phases. Untimed bookkeeping (parity digests,
reject counting) happens between batches and is excluded from every phase.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from time import perf_counter_ns
from types import ModuleType

from connectedfleet.codec import telemetry as codec
from connectedfleet.lab01 import aggregate, arrow_path, row_path

PHASES = ("decode", "validate", "build", "transform", "serialize")
PATHS: dict[str, ModuleType] = {row_path.NAME: row_path, arrow_path.NAME: arrow_path}


@dataclass
class RunOutput:
    path: str
    batch_size: int
    batches: int
    phase_ns: dict[str, int]
    valid_rows: int
    rejects: dict[str, int]
    field_stats: codec.FieldStats
    payload_bytes: int
    alerts: list[str]
    risk_counts: dict[str, int]
    risk_digest: str
    site_summary: list[dict] = field(default_factory=list)

    @property
    def total_ns(self) -> int:
        return sum(self.phase_ns.values())


def iter_batches(lines: list[bytes], batch_size: int):
    """Yield (first_index, lines) slices; at most `batch_size` records are in flight."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    for start in range(0, len(lines), batch_size):
        yield start, lines[start : start + batch_size]


def run_once(path_name: str, lines: list[bytes], batch_size: int) -> RunOutput:
    path = PATHS[path_name]
    builds_columns = path is arrow_path
    phase_ns = dict.fromkeys(PHASES, 0)
    state: dict = {}
    rejects: Counter = Counter()
    field_stats = codec.FieldStats()
    risk_hash = hashlib.sha256()
    risk_counts = {"true": 0, "false": 0, "unknown": 0}
    alerts: list[str] = []
    valid_rows = 0
    payload_bytes = 0
    batches = 0

    for start, chunk in iter_batches(lines, batch_size):
        t0 = perf_counter_ns()
        decoded, decode_rejects = codec.decode(chunk, start)
        t1 = perf_counter_ns()
        rows, validate_rejects, stats = codec.validate(decoded)
        t2 = perf_counter_ns()
        if builds_columns:
            built = path.build(rows)
            t3 = perf_counter_ns()
        else:
            built = rows
            t3 = t2
        enriched, batch_alerts = path.transform(built, state)
        t4 = perf_counter_ns()
        payload = path.serialize(enriched)
        t5 = perf_counter_ns()

        phase_ns["decode"] += t1 - t0
        phase_ns["validate"] += t2 - t1
        phase_ns["build"] += t3 - t2
        phase_ns["transform"] += t4 - t3
        phase_ns["serialize"] += t5 - t4

        # ---- untimed bookkeeping for the parity check ----
        batches += 1
        valid_rows += len(rows)
        payload_bytes += payload.size if hasattr(payload, "size") else len(payload)
        rejects.update(r.reason for r in decode_rejects)
        rejects.update(r.reason for r in validate_rejects)
        field_stats.merge(stats)
        alerts.extend(batch_alerts if isinstance(batch_alerts, list) else batch_alerts.to_pylist())
        for event_id, risk in path.risk_vector(enriched):
            risk_hash.update(f"{event_id}:{risk}\n".encode())
            risk_counts["true" if risk is True else "false" if risk is False else "unknown"] += 1

    return RunOutput(
        path=path_name,
        batch_size=batch_size,
        batches=batches,
        phase_ns=phase_ns,
        valid_rows=valid_rows,
        rejects=dict(sorted(rejects.items())),
        field_stats=field_stats,
        payload_bytes=payload_bytes,
        alerts=alerts,
        risk_counts=risk_counts,
        risk_digest=risk_hash.hexdigest(),
        site_summary=aggregate.finalize(state),
    )
