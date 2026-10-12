"""Write Lab 01 technical evidence: machine-readable files plus a plain-text summary.

Everything written here is technical evidence for the POC (JSON, CSV, text).
Article graphics and HTML are produced elsewhere, from these files.
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path

from connectedfleet.lab01.runner import PHASES

GUARDRAILS = (
    "Same-process microbenchmark on one machine: it shows where time goes inside one worker, "
    "not pipeline, Kafka or ClickHouse throughput.",
    "decode and validate run identical shared code in both paths.",
    "build is the Arrow path's extra conversion step (rows -> typed arrays); the row path has no build phase.",
    "serialize emits different formats: JSONEachRow for the row path, an in-memory Arrow IPC stream for the Arrow path. "
    "No network, Kafka or ClickHouse is involved in Lab 01.",
    "Memory is the process high-water RSS (ru_maxrss) of a fresh process per combination, minus the RSS after the "
    "fixture was loaded; the Arrow pool peak covers Arrow-allocated buffers only.",
)

MEMORY_METHOD = (
    "fresh subprocess per (size, batch, path); rss_baseline = ru_maxrss after fixture load + gc; "
    "rss_peak = ru_maxrss after warmups and repetitions; arrow_pool_peak = default_memory_pool().max_memory()"
)


def _ms(ns: float) -> float:
    return ns / 1_000_000


def phase_stats(result: dict) -> dict[str, dict[str, float]]:
    """Median/min/max milliseconds per phase (and total) across measured repetitions."""
    stats = {}
    for phase in (*PHASES, "total"):
        values = [rep[phase] for rep in result["phase_ns"]]
        stats[phase] = {
            "median_ms": _ms(statistics.median(values)),
            "min_ms": _ms(min(values)),
            "max_ms": _ms(max(values)),
        }
    return stats


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def write_all(run_dir: Path, manifest: dict, combos: list[dict], buffers: dict) -> str:
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_rows, summary_rows, memory_rows = [], [], []
    for combo in combos:
        for path in ("row", "arrow"):
            result = combo.get(path)
            if not result:
                continue
            for rep in result["phase_ns"]:
                for phase in (*PHASES, "total"):
                    if path == "row" and phase == "build":
                        continue
                    raw_rows.append([combo["event_count"], combo["batch_size"], path, rep["rep"], phase, rep[phase]])
            for phase, s in phase_stats(result).items():
                if path == "row" and phase == "build":
                    continue
                summary_rows.append(
                    [
                        combo["event_count"],
                        combo["batch_size"],
                        path,
                        phase,
                        round(s["median_ms"], 4),
                        round(s["min_ms"], 4),
                        round(s["max_ms"], 4),
                        result["repetitions"],
                        round(s["median_ms"] * 1000 / combo["event_count"], 4),
                    ]
                )
            mem = result["memory"]
            memory_rows.append(
                [
                    combo["event_count"],
                    combo["batch_size"],
                    path,
                    mem["rss_baseline_bytes"],
                    mem["rss_peak_bytes"],
                    max(0, mem["rss_peak_bytes"] - mem["rss_baseline_bytes"]),
                    mem["arrow_pool_peak_bytes"],
                    result["payload_bytes"],
                ]
            )

    _write_csv(run_dir / "phases.csv", ["event_count", "batch_size", "path", "rep", "phase", "ns"], raw_rows)
    _write_csv(
        run_dir / "summary.csv",
        ["event_count", "batch_size", "path", "phase", "median_ms", "min_ms", "max_ms", "reps", "us_per_event_median"],
        summary_rows,
    )
    _write_csv(
        run_dir / "memory.csv",
        [
            "event_count",
            "batch_size",
            "path",
            "rss_baseline_bytes",
            "rss_peak_bytes",
            "rss_delta_bytes",
            "arrow_pool_peak_bytes",
            "payload_bytes",
        ],
        memory_rows,
    )
    parity = [
        {
            "event_count": c["event_count"],
            "batch_size": c["batch_size"],
            "run_order": c["run_order"],
            "errors": c["errors"],
            "checks": c["parity"],
            "ok": c["ok"],
        }
        for c in combos
    ]
    (run_dir / "parity.json").write_text(json.dumps(parity, indent=2, ensure_ascii=False))
    (run_dir / "buffers.json").write_text(json.dumps(buffers, indent=2, ensure_ascii=False))
    (run_dir / "run.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    text = render_text(manifest, combos)
    (run_dir / "summary.txt").write_text(text, encoding="utf-8")
    return text


def render_text(manifest: dict, combos: list[dict]) -> str:
    lines = [
        "ConnectedFleet Lab 01 — row vs Arrow at the JSON-to-column boundary",
        f"run_id      {manifest['run_id']}",
        f"status      {manifest['status']}",
        f"git         {manifest['code']['git_sha'] or 'uncommitted'}{' (dirty tree)' if manifest['code']['git_dirty'] else ''}",
        f"host        {manifest['host']['architecture']} · {manifest['host']['cpu_count']} CPUs · {manifest['host']['os']}",
        f"versions    Python {manifest['versions']['python']} · pyarrow {manifest['versions']['pyarrow']}",
        f"protocol    {manifest['config']['warmups']} warm-up + {manifest['config']['repetitions']} measured repetitions; "
        "medians reported",
        "",
        "MEASURED — median milliseconds per phase (n/a = phase does not exist on that path)",
    ]
    header = f"{'events':>8} {'batch':>6} {'path':<6} " + " ".join(f"{p:>10}" for p in (*PHASES, "total")) + f" {'µs/event':>9}"
    lines.append(header)
    lines.append("-" * len(header))
    for combo in combos:
        for path in ("row", "arrow"):
            result = combo.get(path)
            if not result:
                lines.append(f"{combo['event_count']:>8} {combo['batch_size']:>6} {path:<6} FAILED: {combo['errors']}")
                continue
            stats = phase_stats(result)
            cells = []
            for phase in (*PHASES, "total"):
                cells.append(f"{'n/a':>10}" if path == "row" and phase == "build" else f"{stats[phase]['median_ms']:>10.2f}")
            per_event = stats["total"]["median_ms"] * 1000 / combo["event_count"]
            lines.append(f"{combo['event_count']:>8} {combo['batch_size']:>6} {path:<6} " + " ".join(cells) + f" {per_event:>9.3f}")
    lines += ["", "MEASURED — memory (MiB)"]
    lines.append(f"{'events':>8} {'batch':>6} {'path':<6} {'rss_delta':>10} {'arrow_pool':>11} {'payload':>9}")
    for combo in combos:
        for path in ("row", "arrow"):
            result = combo.get(path)
            if not result:
                continue
            mem = result["memory"]
            delta = max(0, mem["rss_peak_bytes"] - mem["rss_baseline_bytes"]) / 2**20
            pool = mem["arrow_pool_peak_bytes"] / 2**20
            payload = result["payload_bytes"] / 2**20
            lines.append(f"{combo['event_count']:>8} {combo['batch_size']:>6} {path:<6} {delta:>10.1f} {pool:>11.1f} {payload:>9.1f}")
    lines += ["", "PARITY (row vs Arrow, same fixture and batch size)"]
    for combo in combos:
        failed = [c["check"] for c in combo["parity"] if not c["ok"]]
        verdict = "PASS" if combo["ok"] else f"FAIL {failed or combo['errors']}"
        lines.append(f"{combo['event_count']:>8} {combo['batch_size']:>6}  {verdict}")
    lines += ["", "HOW TO READ THIS"]
    lines += [f"- {g}" for g in GUARDRAILS]
    lines.append("")
    return "\n".join(lines)
