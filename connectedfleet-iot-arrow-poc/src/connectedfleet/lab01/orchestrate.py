"""Lab 01 orchestrator: fixtures -> isolated measurements -> parity -> evidence files.

Configuration precedence: command-line flags > LAB01_* environment variables > profile.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from connectedfleet.codec import telemetry as codec
from connectedfleet.fixtures import telemetry as fixtures
from connectedfleet.instrumentation.env import git_state, host_info, versions
from connectedfleet.lab01 import arrow_path, buffers, parity, report, worker
from connectedfleet.lab01.runner import PHASES

POC_ROOT = Path(__file__).resolve().parents[3]
RESULTS_ROOT = POC_ROOT / "benchmarks" / "results" / "lab-01"

PROFILES = {
    "quick": {"sizes": [10_000], "batches": [1_000], "warmups": 1, "repetitions": 3},
    "default": {"sizes": [10_000, 100_000], "batches": [100, 1_000, 10_000], "warmups": 1, "repetitions": 5},
    "full": {"sizes": [10_000, 100_000, 1_000_000], "batches": [100, 1_000, 10_000], "warmups": 1, "repetitions": 3},
}

DEFINITIONS = {
    "phase_timer": "time.perf_counter_ns around each phase, summed over all batches of one pass",
    "total": "sum of decode + validate + build + transform + serialize; parity bookkeeping excluded",
    "statistic": "median over measured repetitions (min and max also recorded)",
    "memory": report.MEMORY_METHOD,
    "gc": "Python garbage collector left enabled (realistic); gc.collect() before each repetition",
}


def _ints(value: str) -> list[int]:
    return [int(part) for part in value.split(",") if part.strip()]


def resolve_config(args: argparse.Namespace) -> dict:
    config = dict(PROFILES[args.profile])
    env = os.environ
    for key, env_name, parse in (
        ("sizes", "LAB01_SIZES", _ints),
        ("batches", "LAB01_BATCHES", _ints),
        ("warmups", "LAB01_WARMUPS", int),
        ("repetitions", "LAB01_REPETITIONS", int),
    ):
        if env.get(env_name):
            config[key] = parse(env[env_name])
    config["seed"] = int(env.get("LAB01_SEED", fixtures.DEFAULT_SEED))
    for key in ("sizes", "batches", "warmups", "repetitions", "seed"):
        value = getattr(args, key, None)
        if value is not None:
            config[key] = value
    if config["repetitions"] < 1 or config["warmups"] < 0:
        raise SystemExit("repetitions must be >= 1 and warmups >= 0")
    config["profile"] = args.profile
    return config


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _measure_subprocess(path: str, fixture_file: Path, sha: str, batch: int, config: dict, out: Path) -> tuple[dict | None, str | None]:
    cmd = [
        sys.executable,
        "-m",
        "connectedfleet.lab01.worker",
        "--path", path,
        "--fixture", str(fixture_file),
        "--sha256", sha,
        "--batch", str(batch),
        "--warmups", str(config["warmups"]),
        "--reps", str(config["repetitions"]),
        "--out", str(out),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "PYTHONHASHSEED": "0"})
    if proc.returncode != 0:
        return None, f"{path} worker exit {proc.returncode}: {proc.stderr.strip()[-500:]}"
    return json.loads(out.read_text()), None


def sample_batch(seed: int, size: int):
    """A real validated, Arrow-built batch from the lab fixture for the observed buffer report."""
    fx = fixtures.generate(size, seed)
    decoded, _ = codec.decode(fx.lines)
    rows, _, _ = codec.validate(decoded)
    return arrow_path.build(rows)


def run_lab(config: dict, results_root: Path = RESULTS_ROOT, in_process: bool = False) -> tuple[int, Path, str]:
    started = _utc()
    code = git_state(POC_ROOT)
    run_id = f"lab01-{started.replace('-', '').replace(':', '')}-{(code['git_sha'] or 'nogit')[:7]}"
    run_dir = results_root / "runs" / run_id

    fixture_meta: dict[int, dict] = {}
    combos: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="cf-lab01-") as tmp_name:
        tmp = Path(tmp_name)
        fixture_lines: dict[int, list[bytes]] = {}
        for size in config["sizes"]:
            fx = fixtures.generate(size, config["seed"])
            fixture_file = tmp / f"telemetry-{size}.jsonl"
            fixture_file.write_bytes(b"\n".join(fx.lines) + b"\n")
            fixture_meta[size] = {
                "file": fixture_file,
                "sha256": fx.sha256,
                "bytes": fx.byte_size,
                "injected": fx.injected,
                "expected_rejects": fx.expected_rejects,
            }
            if in_process:
                fixture_lines[size] = fx.lines

        order_flip = False
        for size in config["sizes"]:
            meta = fixture_meta[size]
            for batch in config["batches"]:
                run_order = ["arrow", "row"] if order_flip else ["row", "arrow"]
                order_flip = not order_flip
                outputs: dict[str, dict] = {}
                errors: list[str] = []
                for path in run_order:
                    if in_process:
                        outputs[path] = json.loads(
                            json.dumps(
                                worker.measure(
                                    path, fixture_lines[size], meta["sha256"], batch, config["warmups"], config["repetitions"]
                                )
                            )
                        )
                        continue
                    result, error = _measure_subprocess(
                        path, meta["file"], meta["sha256"], batch, config, tmp / f"{path}-{size}-{batch}.json"
                    )
                    if error:
                        errors.append(error)
                    else:
                        outputs[path] = result
                checks = (
                    parity.compare(outputs["row"], outputs["arrow"], meta["expected_rejects"])
                    if len(outputs) == 2
                    else []
                )
                combos.append(
                    {
                        "event_count": size,
                        "batch_size": batch,
                        "run_order": run_order,
                        "row": outputs.get("row"),
                        "arrow": outputs.get("arrow"),
                        "parity": checks,
                        "errors": errors,
                        "ok": not errors and bool(checks) and all(c["ok"] for c in checks),
                    }
                )

    status = "PASS" if combos and all(c["ok"] for c in combos) else "FAIL"
    sample_size = min(config["sizes"])
    buffer_report = {
        "illustrative": buffers.illustrative_layout(),
        "observed": buffers.observed_layout(sample_batch(config["seed"], min(sample_size, 1_000))),
    }
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "lab": "lab-01",
        "status": status,
        "started_at_utc": started,
        "ended_at_utc": _utc(),
        "code": code,
        "host": host_info(),
        "versions": versions(),
        "config": {**config, "in_process": in_process},
        "workload": {
            str(size): {k: v for k, v in meta.items() if k != "file"} for size, meta in fixture_meta.items()
        },
        "phases": list(PHASES),
        "measurement_definitions": DEFINITIONS,
        "sections": {
            "measured": ["phases.csv", "summary.csv", "memory.csv", "parity.json"],
            "illustrative": ["buffers.json#illustrative"],
            "observed_structure": ["buffers.json#observed"],
        },
        "limitations": list(report.GUARDRAILS)
        + (["in_process=true: memory figures are not isolated per combination"] if in_process else []),
    }
    text = report.write_all(run_dir, manifest, combos, buffer_report)
    latest = {"run_id": run_id, "status": status, "path": str(run_dir.relative_to(results_root)), "ended_at_utc": manifest["ended_at_utc"]}
    (results_root / "latest.json").write_text(json.dumps(latest, indent=2))
    return (0 if status == "PASS" else 1), run_dir, text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lab 01: row vs Arrow at the JSON-to-column boundary")
    parser.add_argument("--profile", choices=sorted(PROFILES), default=os.environ.get("LAB01_PROFILE", "default"))
    parser.add_argument("--sizes", type=_ints, default=None, help="comma-separated event counts")
    parser.add_argument("--batches", type=_ints, default=None, help="comma-separated batch sizes")
    parser.add_argument("--warmups", type=int, default=None)
    parser.add_argument("--repetitions", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)
    config = resolve_config(args)
    exit_code, run_dir, text = run_lab(config)
    print(text)
    print(f"evidence: {run_dir.relative_to(POC_ROOT)}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
