"""Subprocess entry point: measure one (path, fixture, batch size) combination.

Run in a fresh interpreter so the process memory high-water mark belongs to
this combination only. The fixture is read from a JSON-lines file written by
the orchestrator and its SHA-256 is re-checked before anything is timed.

    python -m connectedfleet.lab01.worker --path arrow --fixture f.jsonl \
        --sha256 <hex> --batch 1000 --warmups 1 --reps 5 --out result.json
"""

from __future__ import annotations

import argparse
import dataclasses
import gc
import hashlib
import json
import sys
from pathlib import Path

import pyarrow as pa

from connectedfleet.instrumentation.env import peak_rss_bytes
from connectedfleet.lab01.runner import PATHS, PHASES, run_once


def load_lines(path: Path) -> tuple[list[bytes], str]:
    data = path.read_bytes()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line)
        digest.update(b"\n")
    return lines, digest.hexdigest()


def measure(path: str, lines: list[bytes], sha256: str, batch: int, warmups: int, reps: int) -> dict:
    """Warm up, then time `reps` passes. Memory figures are meaningful only in a fresh process."""
    gc.collect()
    rss_baseline = peak_rss_bytes()

    for _ in range(warmups):
        run_once(path, lines, batch)

    repetitions = []
    last = None
    for rep in range(reps):
        gc.collect()
        last = run_once(path, lines, batch)
        repetitions.append({"rep": rep, **last.phase_ns, "total": last.total_ns})

    pool = pa.default_memory_pool()
    return {
        "path": path,
        "event_count": len(lines),
        "batch_size": batch,
        "fixture_sha256": sha256,
        "warmups": warmups,
        "repetitions": reps,
        "phases": list(PHASES),
        "phase_ns": repetitions,
        "batches": last.batches,
        "valid_rows": last.valid_rows,
        "rejects": last.rejects,
        "field_stats": dataclasses.asdict(last.field_stats),
        "payload_bytes": last.payload_bytes,
        "alert_count": len(last.alerts),
        "alert_digest": hashlib.sha256("\n".join(last.alerts).encode()).hexdigest(),
        "risk_counts": last.risk_counts,
        "risk_digest": last.risk_digest,
        "site_summary": last.site_summary,
        "memory": {
            "rss_baseline_bytes": rss_baseline,
            "rss_peak_bytes": peak_rss_bytes(),
            "arrow_pool_peak_bytes": pool.max_memory(),
            "arrow_pool_backend": pool.backend_name,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", choices=sorted(PATHS), required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    lines, sha = load_lines(args.fixture)
    if sha != args.sha256:
        print(f"fixture hash mismatch: expected {args.sha256}, got {sha}", file=sys.stderr)
        return 2
    result = measure(args.path, lines, sha, args.batch, args.warmups, args.reps)
    args.out.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
