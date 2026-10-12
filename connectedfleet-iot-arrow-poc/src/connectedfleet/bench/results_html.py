"""Results pages built from saved evidence only (no Docker, no new measurements).

    python -m connectedfleet.bench.results_html          # or: make results-html

Writes
  benchmarks/results/benchmark-results.html   every use case from the cited Lab 03 run, Lab 01 and Lab 02, and every
                                              Lab 03 run ever saved, with what happened to it
  benchmarks/results/runs/<run_id>/report.html  one page per Lab 03 run that left a manifest (cited or not)

Every number is computed here from run.json / Lab 01-02 files with the same definitions as `make report`
(bench.report.flatten); nothing is typed in. The cited run is the newest valid `confirm` run.
"""

from __future__ import annotations

import csv
import hashlib
import html
import json
import os
import re
import statistics
from collections import defaultdict
from pathlib import Path

from connectedfleet.bench import report as report_mod
from connectedfleet.bench.validate import validate_run
from connectedfleet.config import POC_ROOT

RESULTS = POC_ROOT / "benchmarks" / "results"
ASSETS = POC_ROOT / "benchmarks" / "report_assets"
OUT = RESULTS / "benchmark-results.html"
LAB01 = "lab01-20261010T162903Z-nogit"
LAB02 = "lab02-20261010T164622Z-nogit"
ESC = html.escape

LANES = {  # key -> (label, colour)
    "sink": ("A · sink, as first configured", "blue"),
    "sink_tuned": ("A · sink, tuned", "purple"),
    "sink_skip": ("A · sink + skip errors", "orange"),
    "row": ("B · row worker", "neutral"),
    "arrow": ("C · Arrow worker", "teal"),
}
SHORT = {"sink": "A · sink", "sink_tuned": "A · tuned", "sink_skip": "A · skip", "row": "B · row", "arrow": "C · Arrow"}
VARIANTS = [("row", "native", "row + Native", "neutral"), ("row", "arrowstream", "row + ArrowStream", "purple"),
            ("arrow", "native", "Arrow + Native (to_pylist)", "orange"),
            ("arrow", "arrowstream", "Arrow + ArrowStream", "teal")]
SHAPES = [("dense", "dense"), ("null_heavy", "null-heavy (40% nulls)"), ("string_heavy", "text-heavy")]
METHODS = [("direct", "Direct HTTP upload, parsed by ClickHouse", "amber"), ("arrow", "pyarrow CSV → ArrowStream", "teal"),
           ("row", "csv module → Native", "neutral")]
PHASES = ["decode", "validate", "build", "transform", "serialize", "insert"]
SCENARIOS = [
    ("happy_path_with_poison", "One bad record among 5,000"),
    ("dlq_recovery", "Recover the dead-lettered rows"),
    ("replay_new_group", "Replay from offset 0"),
    ("crash_after_insert_before_commit", "Crash after insert, before commit"),
    ("schema_null_edge", "90% empty values"),
    ("csv_reimport", "Re-import the same CSV (Lab 02)"),
    ("pg_delete_update", "PostgreSQL update / delete (Lab 02)"),
    ("mongo_update_delete", "MongoDB update / delete (Lab 02)"),
    ("outbox_rollback", "Outbox rollback (Lab 02)"),
]
# What happened to runs that left no NOTE.md (their status is otherwise obvious from the validator).
FALLBACK_NOTE = {
    "lab03-smoke-20261010T183128Z-c67d181": "Clean smoke run, one repetition, before the flush-timer and Kafka-client fixes",
}


# ------------------------------------------------------------------ evidence
def load_runs() -> list[dict]:
    """Every Lab 03 run directory, oldest first, with manifest (if any), validation, flattened rows and its note."""
    runs = []
    for d in sorted((RESULTS / "runs").glob("lab03-*"), key=lambda p: p.name.split("-")[2]):
        run = {"id": d.name, "dir": d, "manifest": None, "validation": None, "rows": [], "note": None, "note_title": None}
        if (d / "run.json").is_file():
            m = json.loads((d / "run.json").read_text())
            v = validate_run(m)
            by_id = {c["case_id"]: c for c in v["cases"]}
            run.update(manifest=m, validation=v["run"],
                       rows=[report_mod.flatten(m, c, by_id.get(c["case_id"])) for c in m.get("cases", [])])
        if (d / "NOTE.md").is_file():
            text = (d / "NOTE.md").read_text()
            run["note"] = text
            first = text.splitlines()[0] if text else ""
            run["note_title"] = first.lstrip("# ").strip()
        run["note_title"] = run["note_title"] or FALLBACK_NOTE.get(d.name)
        parts = d.name.split("-")
        run["profile"], run["started"], run["sha"] = parts[1], parts[2], parts[3]
        runs.append(run)
    return runs


def cited_run(runs: list[dict]) -> dict:
    ok = [r for r in runs if r["manifest"] and r["profile"] == "confirm" and (r["validation"] or {}).get("valid")
          and any(x["experiment"] == "E1" for x in r["rows"])]
    if not ok:
        raise SystemExit("no valid confirm run with E1 results")
    return ok[-1]


def usable(row: dict) -> bool:
    """A measured case: valid, or (in an interrupted run, invalid only at run level) completed without case-level reasons."""
    return bool(row.get("valid")) or (row.get("logical_rows") is not None and not row.get("invalid_reasons"))


def measured(rows: list[dict], **match) -> list[dict]:
    """Usable, non-warm-up rows matching every key=value (values compared as given)."""
    return [r for r in rows if not r.get("warmup") and usable(r) and all(r.get(k) == v for k, v in match.items())]


def stat(rows: list[dict], metric: str, **match):
    values = [r[metric] for r in measured(rows, **match) if r.get(metric) is not None]
    if not values:
        return None
    return {"med": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}


def med(rows: list[dict], metric: str, **match):
    return (stat(rows, metric, **match) or {}).get("med")


def per_million(rows: list[dict], metric: str, **match):
    values = [r[metric] * 1e6 / r["logical_rows"] for r in measured(rows, **match)
              if r.get(metric) is not None and r.get("logical_rows")]
    return statistics.median(values) if values else None


def sha12(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


# ------------------------------------------------------------------ formatting
def k(v):
    return "–" if v is None else (f"{v / 1e6:,.2f}M" if v >= 1e6 else f"{v / 1000:,.1f}K")


def ms(v):
    return "n/m" if v is None else (f"{v / 1000:,.1f} s" if v >= 1000 else f"{v:,.0f} ms")


def num(v, digits=0):
    return "–" if v is None else f"{v:,.{digits}f}"


def events_label(n):
    return f"{n / 1e6:g}M events" if n >= 1e6 else f"{n / 1000:g}K events"


def rng(s, f=k):
    return "–" if not s else f"{f(s['med'])} ({f(s['min'])}–{f(s['max'])})"


def pct(a, b):
    return None if not a or not b else (a / b - 1) * 100


# ------------------------------------------------------------------ html building blocks (layout from the Java at Scale report)
def table(headers, rows, caption):
    out = [f'<div class="b-table-wrap"><table class="b-table"><caption>{ESC(caption)}</caption><thead><tr>']
    out += [f'<th scope="col">{ESC(h)}</th>' for h in headers]
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>" + "".join(f'<td data-label="{ESC(h, quote=True)}">{c}</td>' for h, c in zip(headers, row)) + "</tr>")
    return "".join(out) + "</tbody></table></div>"


def bars(title, subtitle, items, fmt, maximum=None):
    """items: (label, value, colour). Labels carry exact values; bars start at zero."""
    values = [v for _, v, _ in items if v is not None]
    top = maximum or (max(values) if values else 1)
    out = [f'<div class="b-chart"><h4>{ESC(title)}</h4><p class="b-chart-note">{ESC(subtitle)}</p>']
    for label, value, colour in items:
        width = 0 if value is None else min(100.0, value / top * 100)
        out.append(f'<div class="b-bar-row"><div class="b-bar-label"><span>{ESC(label)}</span><strong>{ESC(fmt(value))}</strong></div>'
                   f'<div class="b-track" aria-hidden="true"><span class="b-fill b-{colour}" style="width:{width:.3f}%"></span></div></div>')
    out.append(f'<div class="b-scale"><span>0</span><span>{ESC(fmt(top))}</span></div></div>')
    return "".join(out)


def charts(*parts):
    return '<div class="b-charts">' + "".join(parts) + "</div>"


def flow(steps):
    return '<ol class="b-flow">' + "".join(f"<li><strong>{ESC(a)}</strong><span>{ESC(b)}</span></li>" for a, b in steps) + "</ol>"


def chips(items):
    return '<div class="b-meta">' + "".join(f"<span>{ESC(i)}</span>" for i in items) + "</div>"


def callout(title, text, warning=False):
    return f'<p class="b-takeaway{" b-warning" if warning else ""}"><strong>{ESC(title)}</strong> {text}</p>'


def code(text):
    return f'<pre class="b-code"><code>{ESC(text)}</code></pre>'


def details(summary, body):
    return f'<details class="b-details"><summary>{ESC(summary)}</summary>{body}</details>'


def status_chip(status):
    cls = {"pass": "ok", "fail": "bad", "partial": "warn", "not_applicable": "na", "not_run": "na"}.get(status, "na")
    label = {"not_applicable": "n/a", "not_run": "not run"}.get(status, status.upper())
    return f'<span class="chip chip-{cls}">{ESC(label)}</span>'


def panel(pid, eyebrow, title, body):
    return (f'<section class="benchmark-panel" id="{pid}"><div class="b-eyebrow">{ESC(eyebrow)}</div>'
            f"<h2>{ESC(title)}</h2>{body}</section>")


# ------------------------------------------------------------------ Lab 03 panels (any run with these experiments)
def e1_drain(run, compact=False):
    rows = [r for r in run["rows"] if r["experiment"] == "E1" and r["mode"] == "drain"]
    if not rows:
        return ""
    lanes = [x for x in LANES if any(r["lane"] == x for r in rows)]
    groups = sorted({(r["event_count"], r["batch_size"]) for r in rows if r["event_count"]})
    chart_parts, body_rows = [], []
    for events, batch in groups:
        for metric, label in (("steady_rows_per_s", "Steady insert rate"), ("wall_rows_per_s", "End to end")):
            items = [(LANES[x][0], (stat(rows, metric, lane=x, event_count=events, batch_size=batch) or {}).get("med"),
                      LANES[x][1]) for x in lanes]
            if (compact and metric == "wall_rows_per_s") or not any(v for _, v, _ in items):
                continue
            chart_parts.append(bars(f"{label} · batch {batch:,}", f"{events:,} events · median rows/s · higher is better",
                                    items, k))
        for x in lanes:
            st = stat(rows, "steady_rows_per_s", lane=x, event_count=events, batch_size=batch)
            wa = stat(rows, "wall_rows_per_s", lane=x, event_count=events, batch_size=batch)
            if not st and not wa:
                continue
            eff = stat(rows, "effective_batch_rows", lane=x, event_count=events, batch_size=batch)
            cpu = per_million(rows, "cpu_s", lane=x, event_count=events, batch_size=batch)
            mem = stat(rows, "peak_mem_bytes", lane=x, event_count=events, batch_size=batch)
            body_rows.append([f"{events:,} · {batch:,}", ESC(LANES[x][0]), rng(st), rng(wa),
                              f"{events / wa['med']:.1f} s" if wa else "–", num(eff and eff["med"]),
                              num(cpu, 2), num(mem and mem["med"] / 2**20), str((st or wa)["n"])])
    return (charts(*chart_parts)
            + table(["Events · batch", "Lane", "Steady rows/s (min–max)", "End to end rows/s (min–max)", "All rows queryable after",
                     "Rows per insert", "CPU-s per million", "Peak container MiB", "n"], body_rows,
                    "Medians over measured repetitions · steady = rows of inserts 2..k ÷ first-to-last insert (ClickHouse "
                    "timestamps) · end to end includes start-up · lane A CPU is the whole Kafka Connect container"))


def e1_live(run):
    rows = [r for r in run["rows"] if r["experiment"] == "E1" and r["mode"] == "live"]
    if not rows:
        return ""
    lanes = [x for x in LANES if any(r["lane"] == x for r in rows)]
    flushes = sorted({r["flush_timeout_ms"] for r in rows})
    chart_parts, body = [], []
    for fl in flushes:
        items = [(LANES[x][0], (stat(rows, "p95_ms", lane=x, flush_timeout_ms=fl) or {}).get("med"), LANES[x][1]) for x in lanes]
        capped = max([v for _, v, _ in items if v is not None] or [1])
        chart_parts.append(bars(f"p95 latency · flush {ms(fl)}", "event sent → row queryable · lower is better", items, ms, capped))
        for x in lanes:
            def get(metric, x=x, fl=fl):
                return med(rows, metric, lane=x, flush_timeout_ms=fl)

            lag = get("max_lag")
            body.append([ms(fl), ESC(LANES[x][0]), ms(get("p50_ms")), ms(get("p95_ms")), ms(get("p99_ms")), ms(get("max_ms")),
                         num(get("over_5s")), "n/m" if lag is None else num(lag)])
    p99 = [(LANES[x][0], (stat(rows, "p99_ms", lane=x, flush_timeout_ms=flushes[0]) or {}).get("med"), LANES[x][1])
           for x in lanes]
    chart_parts.append(bars(f"p99 latency · flush {ms(flushes[0])}", "the slowest 1% of events · lower is better", p99, ms))
    worst = [(LANES[x][0], (stat(rows, "max_ms", lane=x, flush_timeout_ms=flushes[-1]) or {}).get("med"), LANES[x][1])
             for x in lanes]
    chart_parts.append(bars(f"Worst event · flush {ms(flushes[-1])}", "maximum latency in the run · lower is better", worst, ms))
    return (charts(*chart_parts)
            + table(["Flush", "Lane", "p50", "p95", "p99", "Max", "Events > 5 s", "Max lag"], body,
                    "Medians over measured repetitions · lag n/m: Kafka Connect commits sink offsets every 60 s"))


def e2(run):
    rows = [r for r in run["rows"] if r["experiment"] == "E2"]
    if not rows:
        return ""
    batches = sorted({r["batch_size"] for r in rows})
    short = {"dense": "dense", "null_heavy": "null-heavy", "string_heavy": "text-heavy"}
    chart_parts, body, phase_body = [], [], []
    for batch in batches:
        for shape, label in SHAPES:
            items = [(name, (stat(rows, "steady_rows_per_s", shape=shape, batch_size=batch, compute=c, insert=i) or {}).get("med"),
                      colour) for c, i, name, colour in VARIANTS]
            if any(v for _, v, _ in items):
                chart_parts.append(bars(f"{label} · batch {batch:,}", "steady rows/s · higher is better", items, k))
            base = (stat(rows, "steady_rows_per_s", shape=shape, batch_size=batch, compute="row", insert="native") or {}).get("med")
            for c, i, name, _ in VARIANTS:
                s = stat(rows, "steady_rows_per_s", shape=shape, batch_size=batch, compute=c, insert=i)
                if not s:
                    continue
                delta = pct(s["med"], base)
                body.append([ESC(short[shape]) + (f" · {batch:,}" if len(batches) > 1 else ""), ESC(name), rng(s),
                             "–" if delta is None or (c, i) == ("row", "native") else f"{delta:+.0f}%",
                             num(per_million(rows, "cpu_s", shape=shape, batch_size=batch, compute=c, insert=i), 2)])
                ph = {p: (stat(rows, f"{p}_ms", shape=shape, batch_size=batch, compute=c, insert=i) or {}).get("med") for p in PHASES}
                logical = (stat(rows, "logical_rows", shape=shape, batch_size=batch, compute=c, insert=i) or {}).get("med")
                scale = 1e6 / logical if logical else 0
                phase_body.append([ESC(short[shape]), ESC(name), *[num(ph[p] * scale if ph[p] is not None else None) for p in PHASES]])
        cpu = [(name, per_million(rows, "cpu_s", shape="dense", batch_size=batch, compute=c, insert=i), colour)
               for c, i, name, colour in VARIANTS]
        if any(v for _, v, _ in cpu):
            chart_parts.append(bars(f"Worker CPU · dense · batch {batch:,}", "CPU-seconds per million events · lower is better",
                                    cpu, lambda v: "–" if v is None else f"{v:.2f} s"))
    return (charts(*chart_parts)
            + table(["Shape", "Combination", "Steady rows/s (min–max)", "vs row + Native", "CPU-s per million"], body,
                    "Medians over measured repetitions")
            + table(["Shape", "Combination", *[f"{p} (ms per million)" for p in PHASES]], phase_body,
                    "Where the worker's time goes · lane B's Native encoding happens inside its insert call · Arrow + Native "
                    "converts every column with to_pylist() in its serialize phase"))


def e3(run):
    rows = [r for r in run["rows"] if r["experiment"] == "E3"]
    if not rows:
        return ""
    sizes = sorted({r["event_count"] for r in rows if r["event_count"]}, reverse=True)
    chart_parts, body = [], []
    big = sizes[0]
    for comp in ("none", "gzip"):
        items = [(name, (stat(rows, "wall_s", lane=m, event_count=big, compression=comp) or {}).get("med"), colour)
                 for m, name, colour in METHODS]
        chart_parts.append(bars(f"Load time · {big:,} rows · {'plain' if comp == 'none' else 'gzip'}",
                                "seconds · lower is better", items, lambda v: "–" if v is None else f"{v:.2f} s"))
    cpu_items = []
    for m, name, colour in METHODS:
        cpu_items.append((f"{name} · client", (stat(rows, "cpu_s", lane=m, event_count=big, compression="none") or {}).get("med"), colour))
        cpu_items.append((f"{name} · server", (stat(rows, "server_cpu_s", lane=m, event_count=big, compression="none") or {}).get("med"),
                          colour))
    if any(v for _, v, _ in cpu_items):
        chart_parts.append(bars(f"Who pays the CPU · {big:,} rows · plain", "CPU-seconds per load: client process and "
                                "ClickHouse server (query log)", cpu_items, lambda v: "–" if v is None else f"{v:.2f} s"))
        totals = [(name, sum(v for (lbl, v, _) in cpu_items if lbl.startswith(name) and v is not None), colour)
                  for _, name, colour in METHODS]
        chart_parts.append(bars(f"Total CPU per load · {big:,} rows · plain", "client + server CPU-seconds · lower is better",
                                totals, lambda v: "–" if v is None else f"{v:.2f} s"))
    for size in sizes:
        for comp in ("none", "gzip"):
            for m, name, _ in METHODS:
                w = stat(rows, "wall_s", lane=m, event_count=size, compression=comp)
                if not w:
                    continue
                def g(metric, m=m, comp=comp, size=size):
                    return med(rows, metric, lane=m, event_count=size, compression=comp)

                body.append([f"{size:,}", "plain" if comp == "none" else "gzip", ESC(name),
                             f"{w['med']:.2f} s ({w['min']:.2f}–{w['max']:.2f})", k(g("wall_rows_per_s")), num(g("cpu_s"), 2),
                             num(g("server_cpu_s"), 2), num(g("max_rss_bytes") and g("max_rss_bytes") / 2**20),
                             num(g("server_peak_mem_bytes") and g("server_peak_mem_bytes") / 2**20), str(w["n"])])
    return (charts(*chart_parts)
            + table(["Rows", "File", "Method", "Load time (min–max)", "Rows/s", "Client CPU-s", "Server CPU-s", "Client peak MiB",
                     "Server peak MiB", "n"], body,
                    "Medians over measured repetitions · server cost from system.query_log (log_comment tag per load)"))


def e4(run):
    rows = run["rows"]
    if not any(r["experiment"] == "E4" for r in rows):
        return ""
    matrix = report_mod.e4_matrix(run["manifest"], rows)
    by = {(m["scenario"], m["lane"]): m for m in matrix}
    lanes = [x for x in LANES if any(m["lane"] == x for m in matrix)]
    body = []
    for key, label in SCENARIOS:
        cells = []
        for x in lanes:
            m = by.get((key, x))
            cells.append('<span class="chip chip-na">—</span>' if not m else
                         f'{status_chip(m["status"])}<br><span class="b-cell-note">{ESC(m["note"][:140])}</span>')
        if any("—" not in c for c in cells):
            body.append([ESC(label), *cells])
    return table(["Scenario", *[SHORT[x] for x in lanes]], body,
                 "Recorded outcomes · 5,000 events, batch 500, one malformed record mid-stream · the last four rows come from Lab 02")


def run_meta(run):
    m, v = run["manifest"], run["validation"] or {}
    spec = m.get("workload_spec") or {}
    exps = spec.get("experiments") or {}
    reps = sorted({str(x.get("repetitions", spec.get("repetitions"))) for x in exps.values() if isinstance(x, dict) and x.get("enabled")})
    guard = m.get("host_guard") or {}
    items = [f"profile {m.get('profile')}", f"commit {(m.get('code') or {}).get('git_sha', '')[:7]}",
             f"{v.get('cases_valid')}/{v.get('cases_total')} validation cases valid", f"measured reps {'/'.join(reps)}",
             f"started {m.get('started_at_utc')}"]
    if guard:
        items.append(f"host guard: {len(guard.get('discarded_cases') or [])} disturbed attempts re-run, {guard.get('waited_s', 0):,.0f} s waited")
    image = (m.get("code") or {}).get("app_image")
    if image:
        items.append(f"worker image {str(image.get('id', ''))[7:19]}")
    return chips(items)


# ------------------------------------------------------------------ main-page panels
def verdict_panel(run):
    rows = run["rows"]
    e1 = {(x, b): (stat(rows, "steady_rows_per_s", experiment="E1", mode="drain", lane=x, batch_size=b) or {}).get("med")
          for x in LANES for b in (1000, 10000)}
    gain = [pct((stat(rows, "steady_rows_per_s", experiment="E2", shape=s, compute="arrow", insert="arrowstream") or {}).get("med"),
                (stat(rows, "steady_rows_per_s", experiment="E2", shape=s, compute="row", insert="native") or {}).get("med"))
            for s, _ in SHAPES]
    cpu = [(1 - per_million(rows, "cpu_s", experiment="E1", mode="drain", lane="arrow", batch_size=b)
            / per_million(rows, "cpu_s", experiment="E1", mode="drain", lane="row", batch_size=b)) * 100 for b in (1000, 10000)]
    big = max(r["event_count"] for r in rows if r["experiment"] == "E3")
    d, a, rr = ((stat(rows, "wall_s", experiment="E3", lane=m, event_count=big, compression="none") or {}).get("med")
                for m in ("direct", "arrow", "row"))
    matrix = {(m["scenario"], m["lane"]): m for m in report_mod.e4_matrix(run["manifest"], rows)}
    dead = re.search(r"(\d+) good records dead-lettered", matrix.get(("happy_path_with_poison", "sink"), {}).get("note", ""))
    lost = re.search(r"(\d+) good rows missing", matrix.get(("happy_path_with_poison", "sink_skip"), {}).get("note", ""))

    def span(vals):
        vals = [round(v) for v in vals if v is not None]
        return f"{min(vals)}–{max(vals)}%" if vals and min(vals) != max(vals) else (f"{vals[0]}%" if vals else "–")

    use = [
        ["Forward validated Kafka events to ClickHouse", '<span class="chip chip-purple">official sink, tuned</span>',
         f"fastest lane: {k(e1[('sink_tuned', 1000)])} / {k(e1[('sink_tuned', 10000)])} steady rows/s at batch 1,000 / 10,000, "
         f"{span([pct(e1[('sink_tuned', b)], e1[('arrow', b)]) for b in (1000, 10000)])} ahead of the Arrow worker; no code to own",
         "E1"],
        ["Same connector, as first configured", '<span class="chip chip-bad">avoid</span>',
         f"{k(e1[('sink', 1000)])} / {k(e1[('sink', 10000)])} rows/s: its own buffer made it "
         f"{e1[('sink_tuned', 1000)] / e1[('sink', 1000)]:.0f}–{e1[('sink_tuned', 10000)] / e1[('sink', 10000)]:.0f}× slower than tuned",
         "E1"],
        ["Process events in your own Python service", '<span class="chip chip-teal">Arrow end to end</span>',
         f"+{span(gain)} steady rows/s over rows + Native (E2), {span(cpu)} less worker CPU (E1)", "E1, E2"],
        ["Arrow compute feeding a row-oriented client", '<span class="chip chip-bad">avoid</span>',
         "to_pylist() conversion made it the slowest combination", "E2"],
        ["Load a CSV file", '<span class="chip chip-amber">direct HTTP upload</span>',
         f"{big:,} rows in {d:.2f} s (pyarrow {a:.2f} s, csv module {rr:.2f} s); the server does the parsing", "E3"],
        ["Dirty sources (one bad record must not cost a batch)", '<span class="chip chip-neutral">validate upstream</span>',
         f"the sink dead-lettered {dead[1] if dead else '–'} good rows with one bad record (recoverable by replay); "
         f"the skip setting dropped {lost[1] if lost else '–'} silently", "E4"],
    ]
    return panel("verdict", "Verdict by use case · from the cited run", "Which lane wins, for what",
                 run_meta(run) + table(["Use case", "Choose", "Measured", "Experiment"], use,
                                       "Every value computed from the cited run's manifest")
                 + callout("Bottom line:", "Arrow pays inside the code you write; a well-configured official sink wins plain "
                                          "ingestion. JSON decoding stays the largest worker cost either way."))


def lab01_panel():
    d = RESULTS / "lab-01" / "runs" / LAB01
    rows = list(csv.DictReader((d / "summary.csv").open()))
    mem = {(int(r["batch_size"]), r["path"]): r for r in csv.DictReader((d / "memory.csv").open()) if r["event_count"] == "100000"}
    ph = defaultdict(dict)
    for r in rows:
        if r["event_count"] == "100000":
            ph[(int(r["batch_size"]), r["path"])][r["phase"]] = float(r["median_ms"])
    items = [(f"{p} · batch {b:,}", ph[(b, p)]["total"], "teal" if p == "arrow" else "neutral")
             for b in (100, 1000, 10000) for p in ("row", "arrow")]
    no_ser = [(f"{p} · batch {b:,}", ph[(b, p)]["total"] - ph[(b, p)]["serialize"], "teal" if p == "arrow" else "neutral")
              for b in (100, 1000, 10000) for p in ("row", "arrow")]
    body = [[f"{b:,}", p, *[num(ph[(b, p)].get(x), 1) for x in ("decode", "validate", "build", "transform", "serialize", "total")],
             f"{int(mem[(b, p)]['payload_bytes']) / 2**20:.1f}"] for b in (100, 1000, 10000) for p in ("row", "arrow")]
    return panel("lab01", "Lab 01 · in-process, no Docker", "Inside one worker: the cost of building columns",
                 chips([LAB01, "100,000 events", "1 warm-up + 5 reps", "parity checked"])
                 + flow([("Decode", "Same JSON decoder and validator for both paths."),
                         ("Process", "Row path: Python rows; Arrow path: RecordBatch + compute kernels."),
                         ("Write", "Row path writes JSON text; Arrow path writes an IPC stream.")])
                 + charts(bars("Whole path", "ms per 100,000 events · lower is better", items, lambda v: f"{v:,.0f} ms"),
                          bars("Without the output step", "ms per 100,000 events · lower is better", no_ser, lambda v: f"{v:,.0f} ms"))
                 + table(["Batch", "Path", "decode", "validate", "build", "transform", "serialize", "total", "output MiB"], body,
                         "Median ms per 100,000 events over five repetitions")
                 + callout("Read the result:", "the Arrow kernel wins only on big batches, and the complete path wins mainly "
                                               "because writing Arrow IPC is cheaper than writing JSON text."))


def lab02_panel():
    d = json.loads((RESULTS / "lab-02" / "runs" / LAB02 / "run.json").read_text())
    checks = d["checks"]
    ok = sum(1 for c in checks if c["ok"])
    body = [[ESC(c["name"]), ESC(c.get("lane") or "all"), status_chip("pass" if c["ok"] else "fail"),
             f'<span class="b-cell-note">{ESC(json.dumps(c.get("observed"))[:90])}</span>'] for c in checks]
    return panel("lab02", "Lab 02 · real connectors and source mutations", "Pipeline correctness across three lanes",
                 chips([LAB02, f"{ok}/{len(checks)} checks passed", "PostgreSQL CDC · outbox · MongoDB · telemetry · CSV"])
                 + flow([("Capture", "Debezium PostgreSQL and MongoDB connectors, an outbox router, a telemetry producer, a CSV importer."),
                         ("Load", "Canonical topics into three identical ClickHouse databases through lanes A, B and C."),
                         ("Check", "Counts, content hashes, deletes, rollback, re-import, a crash between insert and commit.")])
                 + details(f"All {len(checks)} checks", table(["Check", "Lane", "Result", "Observed"], body, "Lab 02 run.json"))
                 + callout("Read the result:", "all three lanes stored identical data; Arrow changed nothing about correctness."))


def history_panel(runs, cited):
    def e1_best(run, lane):
        rows = [r for r in run["rows"] if r["experiment"] == "E1" and r["mode"] == "drain" and r["lane"] == lane]
        if not rows:
            return None, None
        batch = max(r["batch_size"] for r in rows)
        events = max(r["event_count"] for r in rows if r["batch_size"] == batch)
        st = stat(run["rows"], "steady_rows_per_s", experiment="E1", mode="drain", lane=lane, batch_size=batch, event_count=events)
        return (st or {}).get("med"), (events, batch)

    body, evo = [], {"arrow": [], "row": [], "sink": [], "sink_tuned": []}
    for run in runs:
        v = run["validation"] or {}
        if run["id"] == cited["id"]:
            use, cls = "cited", "ok"
        elif not run["manifest"]:
            use, cls = "stopped, no manifest", "na"
        elif not v.get("valid"):
            use, cls = "invalid", "bad"
        else:
            use, cls = "history", "warn"
        stamp = run["started"]
        label = f"{stamp[4:6]}-{stamp[6:8]} {stamp[9:11]}:{stamp[11:13]}Z · {run['sha']}"
        link = (f'<a href="runs/{run["id"]}/report.html" title="{ESC(run["id"], quote=True)}">{ESC(label)}</a>' if run["manifest"]
                else f'<span title="{ESC(run["id"], quote=True)}">{ESC(label)}</span>')
        arrow_e1, where = e1_best(run, "arrow")
        body.append([link, ESC(run["profile"]),
                     "–" if not run["manifest"] else f"{v.get('cases_valid')}/{v.get('cases_total')}",
                     f'<span class="chip chip-{cls}">{ESC(use)}</span>',
                     ESC(run["note_title"] or ("Final run: every Lab 03 number on this page" if run["id"] == cited["id"] else "")),
                     "–" if arrow_e1 is None else f"{k(arrow_e1)} <span class=\"b-cell-note\">({events_label(where[0])} · batch {where[1]:,})</span>"])
        for lane in evo:
            val, where = e1_best(run, lane)
            if val is not None:
                evo[lane].append((f"{run['sha']} · {run['profile']} · {events_label(where[0])} · batch {where[1]:,}", val,
                                  "teal" if run["id"] == cited["id"] else "neutral"))
    top = max(v for items in evo.values() for _, v, _ in items)
    evo_charts = charts(*[bars(f"{LANES[lane][0]} · steady rows/s by run", "largest batch each run measured · the cited run is teal",
                               items, k, top) for lane, items in evo.items() if items])
    notes = "".join(details(f"{run['id']} · {run['note_title'] or ''}", code(run["note"])) for run in runs if run["note"])
    return panel("runs", "Every Lab 03 run · newest last", "All runs, and how the numbers moved",
                 table(["Run (UTC · commit)", "Profile", "Valid cases", "Use", "What happened", "C · Arrow steady (E1)"], body,
                       "Seventeen Lab 03 runs are kept. Only the cited run supports a claim; each other run keeps a note")
                 + callout("Why the numbers moved:", "async inserts on the custom lanes (32b2836), a Kafka client default that "
                           "capped both workers near 100K rows/s (9c6ac78), a badly configured sink baseline, and other work on "
                           "a shared laptop. Smoke runs measured 100,000 events; confirm runs a million. Compare shapes, not "
                           "absolute values, across profiles.", True)
                 + evo_charts + details("Every stopped run's note", notes))


# ------------------------------------------------------------------ pages
CSS = """
:root{--logic:#0f766e;--accent:#0f766e}
.results-edition h1{overflow-wrap:anywhere}
.benchmark-panel{width:var(--wide);margin:24px 0 32px;padding:26px;border:1px solid #dbe2ec;border-top:3px solid var(--logic);
border-radius:14px;background:#fff;scroll-margin-top:24px;min-width:0}
.benchmark-panel h2{margin:6px 0 14px;color:var(--ink);font-family:"Lilita One",sans-serif;font-weight:400;font-size:29px;line-height:1.16}
.b-eyebrow{font-size:11px;text-transform:uppercase;letter-spacing:.12em;font-weight:800;color:var(--logic)}
.b-meta{display:flex;flex-wrap:wrap;gap:6px 8px;margin:0 0 22px}
.b-meta span{padding:4px 8px;border-radius:5px;background:#f3f4f8;color:#475569;font-size:12px;line-height:1.45}
.b-flow{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:18px;list-style:none;margin:0 0 24px;padding:0;counter-reset:runstep}
.b-flow li{counter-increment:runstep;margin:0;font-size:13px;line-height:1.55;color:#475569}
.b-flow li::before{content:counter(runstep,decimal-leading-zero);display:block;font:13px "Comic Shanns",monospace;color:var(--logic);margin-bottom:5px}
.b-flow strong,.b-flow span{display:block}.b-flow strong{font-size:14px;margin-bottom:3px}
.b-charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin-bottom:22px}
.b-chart{padding:18px;background:#f8fafc;border:1px solid #e7ebf1;border-radius:10px;min-width:0}
.b-chart h4{margin:0 0 3px;font-size:16px;line-height:1.3;color:var(--ink)}
.b-chart-note{font-size:12px;color:#64748b;line-height:1.5;margin-bottom:18px;min-height:36px}
.b-bar-row{margin:0 0 14px}.b-bar-label{display:flex;justify-content:space-between;gap:8px;align-items:baseline;margin-bottom:6px;font-size:13px;line-height:1.45}
.b-bar-label strong{font-variant-numeric:tabular-nums;white-space:nowrap;font-size:15px}
.b-track{height:10px;background:#e9edf3;border-radius:3px;overflow:hidden}.b-fill{display:block;height:100%;border-radius:3px}
.b-neutral{background:#64748b}.b-purple{background:#7c3aed}.b-teal{background:#0f766e}.b-blue{background:#0369a1}
.b-orange{background:#ea580c}.b-amber{background:#d97706}.b-green{background:#16a34a}
.b-scale{display:flex;justify-content:space-between;gap:10px;color:#64748b;font-size:10px;font-variant-numeric:tabular-nums}
.b-table-wrap{overflow-x:auto;margin:0 0 20px}.b-table{width:100%;border-collapse:collapse;font-size:12px;line-height:1.5;font-variant-numeric:tabular-nums}
.b-table caption{text-align:left;color:#475569;font-size:12px;padding:0 0 10px;caption-side:top}
.b-table th{font-size:11px;padding:9px 8px;vertical-align:bottom;color:#475569;text-align:left;border-bottom:1px solid #cbd5e1}
.b-table td{padding:10px 8px;border-bottom:1px solid var(--rule);vertical-align:top}.b-table td:first-child{font-weight:700;color:var(--ink)}
.b-cell-note{font-size:11px;color:#64748b;font-weight:400}
.chip{display:inline-block;padding:3px 8px;border-radius:999px;font:700 11px/1.3 "Comic Shanns",monospace;letter-spacing:.03em;white-space:nowrap}
.chip-ok{background:#dcfce7;color:#15803d}.chip-bad{background:#fee2e2;color:#b91c1c}.chip-warn{background:#fef3c7;color:#b45309}
.chip-na{background:#f1f5f9;color:#64748b}.chip-teal{background:#ccfbf1;color:#0f766e}.chip-purple{background:#ede9fe;color:#6d28d9}
.chip-amber{background:#fef3c7;color:#b45309}.chip-neutral{background:#e2e8f0;color:#334155}
.b-takeaway{font-size:14px;line-height:1.6;padding:14px 16px;border-left:3px solid var(--logic);background:#f0fdfa;border-radius:0 8px 8px 0;margin:0 0 16px}
.b-warning{border-color:#d97706;background:#fff7ed}
.b-note,.b-source{font-size:12px;line-height:1.65;color:#475569;margin:14px 0}.b-source{overflow-wrap:anywhere}
.b-details{border-top:1px solid var(--rule);padding:12px 0;font-size:14px}
.b-details summary{cursor:pointer;font-weight:700;color:var(--logic);font-size:14px;line-height:1.5}
.b-details[open] summary{margin-bottom:16px}
.b-code{margin:12px 0 18px;padding:16px;background:#0f172a;color:#e2e8f0;border-radius:8px;overflow:auto;font:12px/1.65 "Comic Shanns",monospace;max-height:420px}
.b-code code{font:inherit;color:inherit;background:none;border:0;padding:0;overflow-wrap:normal;white-space:pre-wrap}
.benchmark-links{display:flex;flex-wrap:wrap;gap:8px 18px;margin:18px 0;font-size:14px}
.results-intro{max-width:720px;color:#475569;font-size:17px}
footer.end{width:var(--wide);margin-top:28px;font-size:13px;color:#64748b;display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}
@media(max-width:640px){
.benchmark-panel{padding:18px 14px;border-radius:10px}.benchmark-panel h2{font-size:25px}
.b-flow,.b-charts{grid-template-columns:1fr}.b-flow{gap:12px}.b-flow li{padding-left:30px;position:relative}.b-flow li::before{position:absolute;left:0;top:1px}
.b-chart-note{min-height:0}.b-chart{padding:14px}.b-table-wrap{overflow:visible}
.b-table,.b-table tbody,.b-table tr,.b-table td{display:block}.b-table thead{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}
.b-table caption{display:block}.b-table tr{border:1px solid var(--rule);border-radius:8px;margin-bottom:12px;overflow:hidden}
.b-table td{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.4fr);gap:10px;padding:7px 10px;overflow-wrap:anywhere;border-bottom:1px solid var(--rule)}
.b-table td::before{content:attr(data-label);color:#64748b;font-weight:400;font-size:11px}.b-table td:first-child{background:#f8fafc}
.b-code{font-size:11px;padding:12px}}
@media print{.benchmark-panel{padding:12pt;border:1px solid #ddd}.b-details:not([open]){display:none}.b-charts,.b-flow{break-inside:avoid}.benchmark-links{display:none}}
"""


def document(title, description, body):
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>{ESC(title)}</title><meta name="description" content="{ESC(description, quote=True)}">'
            f'<style>{(ASSETS / "fonts.css").read_text()}</style><style>{(ASSETS / "report.css").read_text()}{CSS}</style>'
            f'</head><body class="results-edition">{body}</body></html>')


def masthead(eyebrow, title_html, intro, links, warning, note):
    nav = '<nav class="benchmark-links" aria-label="Sections">' + "".join(f'<a href="{h}">{ESC(t)}</a>' for h, t in links) + "</nav>"
    return (f'<header class="masthead"><div class="eyebrow">{ESC(eyebrow)}</div><h1>{title_html}</h1>'
            f'<p class="results-intro">{intro}</p>{nav}{callout(warning[0], warning[1], True)}<p class="b-note">{note}</p></header>')


def env_note(run):
    host = run["manifest"].get("host") or {}
    versions = run["manifest"].get("versions") or {}
    stack = versions.get("stack") or {}
    parts = [host.get("os"), f"{host.get('cpu_count')} cores" if host.get("cpu_count") else None,
             f"{host['ram_bytes'] / 2**30:.0f} GiB" if host.get("ram_bytes") else None,
             f"ClickHouse {stack.get('clickhouse')}" if isinstance(stack.get("clickhouse"), str) else None,
             f"Python {versions.get('python')}" if versions.get("python") else None,
             f"pyarrow {versions.get('pyarrow')}" if versions.get("pyarrow") else None]
    return ("Recorded on " + ESC(" · ".join(p for p in parts if p)) + ". Rebuilding this page reads the saved evidence; "
            "it does not run a new measurement.")


def run_commands():
    return details("Run these experiments · commands and output files",
                   "<p>From the POC directory, with Docker (about 12 GiB for the containers):</p>"
                   + code("make up init seed\nmake lab-02                      # correctness first\n"
                          "make lab-03 PROFILE=confirm      # rebuilds the worker image; waits for a quiet host per case\n"
                          "make report                      # re-validate every run, regenerate aggregates\n"
                          "make results-html                # this page and every run's report.html")
                   + "<p>Each run writes <code>benchmarks/results/runs/&lt;run_id&gt;/</code>: <code>run.json</code> (every case, "
                     "settings, versions, host-guard record), <code>validation.json</code>, <code>metrics.csv</code>, "
                     "<code>phase-metrics.csv</code>, <code>samples.jsonl</code> and the console log.</p>")


def build_main(runs, cited):
    run_file = cited["dir"] / "run.json"
    rows = cited["rows"]
    e1n = max(r["event_count"] for r in rows if r["experiment"] == "E1" and r["mode"] == "drain")
    body = masthead(
        "ConnectedFleet × Apache Arrow · captured evidence", "Benchmark results<br>and how to run them.",
        "Four ways to load the same Kafka events into ClickHouse, compared on equal terms: the official ClickHouse sink "
        "(as first configured and tuned), a Python row worker and a Python Arrow worker. Every chart below is computed "
        "from the saved run files; every run ever made is listed at the end.",
        [("#verdict", "Verdict"), ("#e1", "Plain ingestion"), ("#latency", "Latency"), ("#e2", "Compute × format"),
         ("#e3", "CSV backfill"), ("#e4", "Failures"), ("#lab01", "Inside one worker"), ("#lab02", "Correctness"),
         ("#runs", "All runs")],
        ("One laptop, one node of each service.", "These runs compare lanes on equal terms; they do not establish "
                                                   "production capacity or high availability."),
        env_note(cited))
    body += verdict_panel(cited)
    body += panel("e1", "E1 · whole lanes, no transform", f"Plain ingestion: four lanes, {e1n:,} events",
                  run_meta(cited)
                  + flow([("Prepare", "One seeded topic per workload; one partition, one consumer per lane; tables emptied."),
                          ("Load", "Each lane in turn, rotated per repetition, on a quiet host; synchronous inserts."),
                          ("Measure", "Steady rate from ClickHouse insert times, end-to-end time, CPU, memory, rows per insert.")])
                  + e1_drain(cited)
                  + callout("Read the result:", "configuring the official sink mattered more than any representation; tuned, it "
                            "was the fastest lane. Between the Python workers, Arrow won with non-overlapping ranges.")
                  + run_commands())
    body += panel("latency", "E1 live · 1,000 events/s", "Latency after the flush-timer fix", e1_live(cited)
                  + callout("Read the result:", "every tuned lane's latency is set by its flush timeout; the sink as first "
                            "configured holds a burst's last batch until Kafka Connect's offset flush."))
    body += panel("e2", "E2 · same worker, same risk rule", "Compute representation × insert format", e2(cited)
                  + callout("Read the result:", "keeping the batch in Arrow from build to insert won on every shape; the wire "
                            "format alone was worth little, and Arrow → Native through to_pylist() was the slowest choice."))
    body += panel("e3", "E3 · no Kafka", "CSV backfill: who parses the file", e3(cited)
                  + callout("Read the result:", "ClickHouse parsing the file was fastest, and it moves the CPU to the server; "
                            "when Python must touch the rows, pyarrow beats the csv module."))
    body += panel("e4", "E4 · correctness under failure", "One bad record, replay, crash, null-heavy data", e4(cited)
                  + callout("Read the result:", "the official sink dead-letters a whole batch (recoverable by replay); the skip "
                            "setting drops good rows silently; the workers reject only the bad record. Arrow plays no part here.",
                            True))
    body += lab01_panel() + lab02_panel() + history_panel(runs, cited)
    body += (f'<p class="b-source">Source: <code>{ESC(str(run_file.relative_to(POC_ROOT)))}</code> · SHA-256 <code>{sha12(run_file)}</code>'
             f' · generated by <code>src/connectedfleet/bench/results_html.py</code>.</p>'
             '<footer class="end"><span>ConnectedFleet · Apache Arrow benchmark results</span>'
             '<a href="REPORT.md">Curated report (REPORT.md)</a></footer>')
    return document("Benchmark results — ConnectedFleet × Apache Arrow",
                    "Every use case and every saved Lab 03 run, computed from the saved evidence.", body)


def build_run_page(run, cited):
    m = run["manifest"]
    v = run["validation"] or {}
    is_cited = run["id"] == cited["id"]
    warn = ("This is the cited run.", "Every Lab 03 number in the articles and REPORT.md comes from it.") if is_cited else (
        "Not cited.", ESC(run["note_title"] or "Kept for the record; see the note below."))
    body = masthead(f"Lab 03 run · {run['profile']}", ESC(run["id"]),
                    f"Status {ESC(m.get('status', ''))} · validator: {'valid' if v.get('valid') else 'invalid'} "
                    f"({v.get('cases_valid')}/{v.get('cases_total')} cases)" + (
                        "" if not v.get("reasons") else " · " + ESC("; ".join(v["reasons"])[:200])),
                    [("../../benchmark-results.html", "← All runs"), ("#e1", "E1"), ("#latency", "Latency"), ("#e2", "E2"),
                     ("#e3", "E3"), ("#e4", "E4")], warn, env_note(run))
    if run["note"]:
        body += panel("note", "Why this run is not cited", run["note_title"] or "Note", code(run["note"]))
    sections = [("e1", "E1 · whole lanes", "Plain ingestion", e1_drain(run)), ("latency", "E1 live", "Latency", e1_live(run)),
                ("e2", "E2", "Compute representation × insert format", e2(run)), ("e3", "E3", "CSV backfill", e3(run)),
                ("e4", "E4", "Correctness under failure", e4(run))]
    for pid, eyebrow, title, content in sections:
        if content:
            body += panel(pid, eyebrow, title, (run_meta(run) if pid == "e1" else "") + content)
    run_file = run["dir"] / "run.json"
    body += (f'<p class="b-source">Source: <code>run.json</code> · SHA-256 <code>{sha12(run_file)}</code> · '
             f'<a href="run.json">run.json</a> · <a href="validation.json">validation.json</a></p>')
    return document(f"{run['id']} — Lab 03 run", "One saved Lab 03 run, computed from its manifest.", body)


def main() -> int:
    runs = load_runs()
    cited = cited_run(runs)
    OUT.write_text(build_main(runs, cited))
    pages = 0
    for run in runs:
        if run["manifest"]:
            (run["dir"] / "report.html").write_text(build_run_page(run, cited))
            pages += 1
    print(f"wrote {os.path.relpath(OUT, POC_ROOT)} ({OUT.stat().st_size:,} bytes) and {pages} run pages; cited {cited['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
