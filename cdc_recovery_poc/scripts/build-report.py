#!/usr/bin/env python3
"""Turn recorded runs into pages you can look at.

For each run it writes results/<run-id>/report.html: the verdict and its checks, the fault
timeline, four charts drawn from the collector's two-second samples, the reconciliation
numbers and the alerts. It also writes results/index.html, one card per run.

Everything is inline SVG and inline CSS, so a report opens from disk with no network and no
dependencies, and can be attached to a ticket as a single file.

Usage:
  scripts/build-report.py                      every run under results/
  scripts/build-report.py results/<run-id> ... only these runs
  scripts/build-report.py --pdf [run ...]      also print each page to report.pdf (needs Chrome)
"""
import html
import importlib.util
import json
import pathlib
import sys
from datetime import datetime, timezone

LAB = pathlib.Path(__file__).resolve().parent.parent
RESULTS = LAB / "results"
spec = importlib.util.spec_from_file_location("verify_run", LAB / "scripts/verify-run.py")
verify_run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_run)

INK, MUTED, LINE = "#1e293b", "#868e96", "#dee2e6"
BLUE, VIOLET, GREEN, RED, ORANGE = "#1971c2", "#7048e8", "#2f9e44", "#c92a2a", "#e8590c"
# Events worth a marker on the charts, in the order they can occur.
FAULTS = {
    # recovery test
    "source-ddl": "schema change", "contract-v2": "contract v2",
    "incremental-snapshot-signal": "incremental snapshot", "clickhouse-throttled": "ClickHouse throttled",
    "connect-worker-killed": "Connect worker killed",
    "sink-crash-after-insert-before-commit": "sink crash after write",
    "delete-batch": "deletes committed", "capture-task-running": "capture resumed",
    "clickhouse-restored": "ClickHouse restored", "backlog-drained": "backlog drained",
    # capture pressure
    "kafka-paused": "Kafka paused", "kafka-unpaused": "Kafka back", "caught-up": "caught up",
    # history loss
    "slot-retention-bounded": "retention bounded", "capture-stopped": "capture stopped",
    "source-history-lost": "history lost", "normal-resume-requested": "resume attempted",
    "normal-resume-refused": "resume refused", "normal-resume-stalled": "resume stalled",
    "resnapshot-start": "resnapshot", "resnapshot-sweep": "sweep for missed deletes",
    # capacity
    "phase-fill-tasks1-start": "fill, tasks.max=1", "phase-catchup-tasks1-start": "drain, tasks.max=1",
    "phase-fill-tasks4-start": "fill, tasks.max=4", "phase-catchup-tasks4-start": "drain, tasks.max=4",
    "phase-c1-start": "sink capacity, 1 process", "phase-c2-start": "sink capacity, 2 processes",
    # every scenario
    "generator-stopped": "load stopped", "reconcile-final": "reconciled",
    "reconcile-after-resnapshot": "reconciled after resnapshot", "reconcile-after-sweep": "reconciled after sweep",
    "reconcile-capacity": "reconciled",
}
CHARTS = [
    ("sink_lag", "Kafka lag", "records waiting for the sink", ORANGE, "count"),
    ("freshness_p99_ms", "p99 freshness", "source commit to queryable", RED, "ms"),
    ("retained_wal_bytes", "WAL retained by the slot", "the source's reservoir", VIOLET, "bytes"),
    ("applied_last_10s", "Sink throughput", "changes applied per 10 s window", GREEN, "count"),
]
esc = html.escape


def human(value, unit):
    if value is None:
        return "—"
    if unit == "bytes":
        for suffix, size in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
            if abs(value) >= size:
                return f"{value / size:.2f} {suffix}"
        return f"{value:.0f} B"
    if unit == "ms":
        if value >= 60000:
            return f"{value / 60000:.1f} min"
        return f"{value / 1000:.1f} s" if value >= 1000 else f"{value:.0f} ms"
    if abs(value) >= 1e6:
        return f"{value / 1e6:.2f} M"
    if abs(value) >= 1e3:
        return f"{value / 1e3:.1f} K"
    return f"{value:,.0f}"


def chart(samples, key, title, subtitle, color, unit, markers, width=1120, height=210):
    points = [(t, v) for t, v in samples if v is not None]
    if not points:
        return f'<div class="chart empty"><h3>{esc(title)}</h3><p>no samples</p></div>'
    xs = [t for t, _ in points]
    ys = [v for _, v in points]
    x0, x1 = min(xs), max(xs) or 1
    top = max(ys) or 1
    pad_l, pad_r, pad_t, pad_b = 8, 8, 16, 26
    plot_w, plot_h = width - pad_l - pad_r, height - pad_t - pad_b

    def px(t):
        return pad_l + (t - x0) / (x1 - x0 or 1) * plot_w

    def py(v):
        return pad_t + plot_h - (v / top) * plot_h

    line = " ".join(f"{px(t):.1f},{py(v):.1f}" for t, v in points)
    area = f"{px(x0):.1f},{pad_t + plot_h:.1f} {line} {px(x1):.1f},{pad_t + plot_h:.1f}"
    grid = "".join(
        f'<line x1="{pad_l}" y1="{py(top * f):.1f}" x2="{width - pad_r}" y2="{py(top * f):.1f}" '
        f'stroke="{LINE}" stroke-width="1" stroke-dasharray="2 4"/>'
        f'<text x="{pad_l + 2}" y="{py(top * f) - 4:.1f}" font-size="11" fill="{MUTED}">{human(top * f, unit)}</text>'
        for f in (0.5, 1.0))
    peak_t, peak_v = max(points, key=lambda p: p[1])
    flags = ""
    for t, label in markers:
        if not x0 <= t <= x1:
            continue
        x = px(t)
        flags += (f'<line x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{pad_t + plot_h}" stroke="{INK}" '
                  f'stroke-width="1" stroke-dasharray="3 3" opacity=".45"/>')
    ticks = "".join(
        f'<text x="{px(x0 + (x1 - x0) * f):.1f}" y="{height - 8}" font-size="11" fill="{MUTED}" '
        f'text-anchor="middle">{(x0 + (x1 - x0) * f) / 60:.0f} min</text>' for f in (0, 0.25, 0.5, 0.75, 1))
    return (f'<div class="chart"><h3>{esc(title)} <span>{esc(subtitle)}</span>'
            f'<b style="color:{color}">peak {human(peak_v, unit)}</b></h3>'
            f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}: {esc(subtitle)}, '
            f'peak {esc(human(peak_v, unit))}">{grid}{flags}'
            f'<polygon points="{area}" fill="{color}" opacity=".13"/>'
            f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="2" '
            f'stroke-linejoin="round"/>'
            f'<circle cx="{px(peak_t):.1f}" cy="{py(peak_v):.1f}" r="3.5" fill="{color}"/>{ticks}</svg></div>')


def timeline(events, faults):
    if not faults:
        return ""
    items = "".join(
        f'<li><span class="dot">{i + 1}</span><b>{esc(label)}</b>'
        f'<span class="t">{t / 60:.1f} min</span></li>' for i, (t, label) in enumerate(faults))
    return f'<section><h2>What happened</h2><ol class="timeline">{items}</ol></section>'


def reconciliation(summary):
    cards = ""
    for label, r in summary.get("reconcile", {}).items():
        state = "ok" if r["converged"] else "bad"
        rows = ""
        for table, t in r["tables"].items():
            stats = [("source rows", t["source_rows"]), ("missing", t["missing_in_clickhouse"]),
                     ("ghost", t["ghost_rows_in_clickhouse"]), ("different", t["content_mismatches"]),
                     ("duplicate keys", t["clickhouse_logical_duplicate_keys"])]
            rows += (f'<div class="rec-table"><h4>{esc(table)}</h4><div class="stats">' + "".join(
                f'<div class="{"stat bad" if name != "source rows" and value else "stat"}">'
                f'<b>{value:,}</b><span>{name}</span></div>' for name, value in stats) + "</div></div>")
        batch = r.get("explicit_delete_batch")
        if batch:
            rows += ('<div class="rec-table"><h4>explicit delete batch</h4><div class="stats">' + "".join(
                f'<div class="{"stat bad" if "visible" in k and v else "stat"}"><b>{v:,}</b>'
                f'<span>{k.replace("_", " ")}</span></div>' for k, v in batch.items()) + "</div></div>")
        history = r.get("history") or {}
        if history:
            rows += ('<div class="rec-table"><h4>replays seen at the sink</h4><div class="stats">' + "".join(
                f'<div class="stat"><b>{v:,}</b><span>{k.replace("_", " ")}</span></div>'
                for k, v in history.items()) + "</div></div>")
        cards += (f'<div class="rec {state}"><h3>reconcile · {esc(label)}'
                  f'<span>{"converged" if r["converged"] else "did not converge"}</span></h3>{rows}</div>')
    return f"<section><h2>Reconciliation</h2>{cards}</section>" if cards else ""


def page(run):
    summary = run.summary
    checks = run.checks
    failed = [c for c in checks if not c[0]]
    verdict = "PASS" if not failed else "FAIL"
    events = [(e["t_s"], e["event"], e.get("detail")) for e in summary.get("timeline", [])]
    faults, seen = [], set()
    for t_s, name, _ in events:
        if name in FAULTS and name not in seen:
            seen.add(name)
            faults.append((t_s, FAULTS[name]))
    start = min((e["ts_ms"] for e in run.events), default=0)
    end = max((e["ts_ms"] for e in run.events), default=0)
    samples = []
    metrics_file = run.path / "metrics.jsonl"
    if metrics_file.exists():
        for line in metrics_file.read_text().splitlines():
            if line.strip():
                m = json.loads(line)
                samples.append(m)
    charts = "".join(
        chart([((m["ts_ms"] - start) / 1000, m.get(key)) for m in samples], key, title, sub, color, unit, faults)
        for key, title, sub, color, unit in CHARTS)
    check_rows = "".join(
        f'<li class="{"ok" if ok else "bad"}"><b>{"PASS" if ok else "FAIL"}</b>'
        f'<span>{esc(label)}</span><i>{esc(detail)}</i></li>' for ok, label, detail in checks)
    alerts = "".join(
        f'<li><b style="color:{RED if a["state"] == "firing" else GREEN}">{esc(a["state"])}</b>'
        f'<span>{esc(a["alert"])}</span><i>{a["t_s"] / 60:.1f} min</i></li>'
        for a in summary.get("alerts", []))
    versions = summary.get("versions", {})
    meta = " · ".join(f"{k} {v}" for k, v in versions.items() if isinstance(v, str))
    when = datetime.fromtimestamp(start / 1000, timezone.utc).strftime("%d %B %Y, %H:%M UTC") if start else ""
    duration = f"{(end - start) / 60000:.0f} min" if end > start else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(run.scenario)} · {esc(run.path.name)}</title><style>
:root{{color-scheme:light}}
*{{box-sizing:border-box}}
body{{margin:0;background:#f7f9fc;color:{INK};font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif}}
.wrap{{max-width:1180px;margin:0 auto;padding:32px 24px 72px}}
header{{background:#fff;border:1px solid {LINE};border-radius:14px;padding:22px 26px;margin-bottom:26px}}
h1{{margin:0 0 6px;font-size:26px;letter-spacing:-.02em}}
h1 span{{color:{MUTED};font-weight:500}}
.verdict{{display:inline-block;padding:4px 12px;border-radius:999px;font:700 13px/1.5 system-ui;letter-spacing:.08em}}
.verdict.PASS{{background:#ebfbee;color:{GREEN};border:1px solid #b2f2bb}}
.verdict.FAIL{{background:#fff5f5;color:{RED};border:1px solid #ffc9c9}}
.meta{{color:{MUTED};font-size:13px;margin-top:10px;word-break:break-word}}
h2{{font-size:15px;letter-spacing:.14em;text-transform:uppercase;color:{BLUE};margin:34px 0 14px}}
section{{margin-bottom:8px}}
ul,ol{{list-style:none;margin:0;padding:0}}
.checks li,.alerts li{{display:flex;gap:12px;align-items:baseline;background:#fff;border:1px solid {LINE};
border-radius:10px;padding:10px 14px;margin-bottom:7px}}
.checks li b{{font:700 12px/1.5 system-ui;letter-spacing:.06em;min-width:42px}}
.checks li.ok b{{color:{GREEN}}}.checks li.bad b{{color:{RED}}}
.checks li.bad{{border-color:#ffc9c9;background:#fff8f8}}
.checks li i,.alerts li i{{margin-left:auto;color:{MUTED};font-style:normal;font-size:13px;text-align:right}}
.timeline{{display:flex;flex-wrap:wrap;gap:8px}}
.timeline li{{display:flex;align-items:center;gap:8px;background:#fff;border:1px solid {LINE};
border-radius:999px;padding:6px 14px 6px 6px;font-size:14px}}
.timeline .dot{{display:inline-flex;width:22px;height:22px;border-radius:50%;background:{INK};color:#fff;
font:700 12px/22px system-ui;justify-content:center}}
.timeline .t{{color:{MUTED};font-size:12px}}
.chart{{background:#fff;border:1px solid {LINE};border-radius:12px;padding:14px 16px 6px;margin-bottom:14px}}
.chart h3{{margin:0 0 4px;font-size:15px;display:flex;gap:10px;align-items:baseline}}
.chart h3 span{{color:{MUTED};font-weight:400;font-size:13px}}
.chart h3 b{{margin-left:auto;font-size:13px}}
.chart svg{{width:100%;height:auto;display:block}}
.rec{{background:#fff;border:1px solid {LINE};border-left:4px solid {GREEN};border-radius:12px;
padding:16px 18px;margin-bottom:14px}}
.rec.bad{{border-left-color:{RED}}}
.rec h3{{margin:0 0 12px;font-size:15px;display:flex;gap:10px;align-items:baseline}}
.rec h3 span{{color:{MUTED};font-weight:400;font-size:13px}}
.rec-table h4{{margin:12px 0 8px;font:700 12px/1.5 system-ui;letter-spacing:.1em;text-transform:uppercase;color:{MUTED}}}
.stats{{display:flex;flex-wrap:wrap;gap:10px}}
.stat{{border:1px solid {LINE};border-radius:10px;padding:8px 14px;min-width:120px;background:#fbfdff}}
.stat b{{display:block;font-size:19px;letter-spacing:-.01em}}
.stat span{{color:{MUTED};font-size:12px}}
.stat.bad{{border-color:#ffc9c9;background:#fff5f5}}.stat.bad b{{color:{RED}}}
footer{{margin-top:34px;color:{MUTED};font-size:13px;border-top:1px solid {LINE};padding-top:16px}}
a{{color:{BLUE}}}
@media(max-width:640px){{.wrap{{padding:18px 14px 48px}}h1{{font-size:21px}}}}
</style></head><body><div class="wrap">
<header><h1>{esc(run.scenario)} <span>· {esc(run.path.name)}</span></h1>
<span class="verdict {verdict}">{verdict}</span>
<span style="margin-left:10px;color:{MUTED};font-size:13px">{len(checks) - len(failed)}/{len(checks)} checks ·
{esc(when)} · {esc(duration)}</span>
<p class="meta">{esc(meta)}</p></header>
<section><h2>Checks</h2><ul class="checks">{check_rows}</ul></section>
{timeline(run.events, faults)}
<section><h2>Every boundary, sampled every two seconds</h2>{charts}</section>
{reconciliation(summary)}
{f'<section><h2>Alerts</h2><ul class="alerts">{alerts}</ul></section>' if alerts else ''}
<footer>Synthetic operational device data; no personal or clinical data. Every number on this page is read
from <code>summary.json</code>, <code>metrics.jsonl</code> and <code>events.jsonl</code> in this run folder —
nothing is typed in by hand. Evidence for this implementation at these versions, not a general proof of CDC.
<br>Generated by <code>scripts/build-report.py</code>. See <code>report.md</code> for the same run in text.</footer>
</div></body></html>"""


def index(entries):
    cards = ""
    for run, verdict, passed, total, headline in entries:
        cls = "ok" if verdict == "PASS" else "bad"
        numbers = "".join(f'<div class="stat"><b>{esc(v)}</b><span>{esc(k)}</span></div>' for k, v in headline)
        cards += (f'<a class="card {cls}" href="{esc(run.path.name)}/report.html">'
                  f'<h3>{esc(run.scenario)}<span>{esc(run.path.name)}</span></h3>'
                  f'<span class="verdict {verdict}">{verdict} · {passed}/{total}</span>'
                  f'<div class="stats">{numbers}</div></a>')
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>CDC failure-recovery lab · runs</title><style>
body{{margin:0;background:#f7f9fc;color:{INK};font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif}}
.wrap{{max-width:1180px;margin:0 auto;padding:36px 24px 72px}}
h1{{font-size:28px;letter-spacing:-.02em;margin:0 0 6px}}
p.lead{{color:{MUTED};margin:0 0 26px}}
.card{{display:block;background:#fff;border:1px solid {LINE};border-left:4px solid {GREEN};border-radius:12px;
padding:16px 18px;margin-bottom:12px;text-decoration:none;color:inherit}}
.card.bad{{border-left-color:{RED}}}
.card:hover{{box-shadow:0 2px 12px rgba(30,41,59,.08)}}
.card h3{{margin:0 0 8px;font-size:17px;display:flex;gap:10px;align-items:baseline}}
.card h3 span{{color:{MUTED};font-weight:400;font-size:13px}}
.verdict{{display:inline-block;padding:3px 10px;border-radius:999px;font:700 12px/1.5 system-ui;letter-spacing:.06em}}
.verdict.PASS{{background:#ebfbee;color:{GREEN}}}.verdict.FAIL{{background:#fff5f5;color:{RED}}}
.stats{{display:flex;flex-wrap:wrap;gap:10px;margin-top:12px}}
.stat{{border:1px solid {LINE};border-radius:10px;padding:7px 13px;min-width:120px;background:#fbfdff}}
.stat b{{display:block;font-size:17px}}.stat span{{color:{MUTED};font-size:12px}}
footer{{margin-top:30px;color:{MUTED};font-size:13px;border-top:1px solid {LINE};padding-top:16px}}
</style></head><body><div class="wrap"><h1>CDC failure-recovery lab</h1>
<p class="lead">Every recorded run, newest first. A card opens that run's full report: checks, fault timeline,
the four boundary charts and the reconciliation.</p>{cards}
<footer>Runs that fail are kept on purpose: a checker that passes everything proves nothing.</footer>
</div></body></html>"""


def headline(run):
    f = run.summary.get("findings", {})
    out = []
    final = run.summary.get("reconcile", {}).get("final") or run.summary.get("reconcile", {}).get("after-sweep")
    if final:
        rows = sum(t["source_rows"] for t in final["tables"].values())
        wrong = sum(t["missing_in_clickhouse"] + t["ghost_rows_in_clickhouse"] + t["content_mismatches"]
                    for t in final["tables"].values())
        out.append(("rows reconciled", f"{rows:,}"))
        out.append(("missing, ghost or different", f"{wrong:,}"))
    if "drain" in f:
        out.append(("backlog drained", f"{f['drain']['backlog']:,}"))
    if "sink_mu_2_processes" in f:
        out.append(("sink capacity, 2 processes", f"{f['sink_mu_2_processes']:,.0f}/s"))
    if "retained_wal_bytes_at_unpause" in f:
        out.append(("WAL retained during the outage", human(f["retained_wal_bytes_at_unpause"], "bytes")))
    if "source-history-lost" in f:
        out.append(("slot state", "lost, detected"))
    return out[:4]


CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def to_pdf(html_path):
    html_path = html_path.resolve()
    """Print a report page to PDF beside it. Chrome sometimes writes the file and then hangs, so it
    runs under a timeout and the file itself is the check."""
    import shutil
    import subprocess
    import tempfile
    chrome = CHROME if pathlib.Path(CHROME).exists() else shutil.which("chromium") or shutil.which("google-chrome")
    if not chrome:
        print("  no Chrome found, skipped PDF")
        return None
    pdf = html_path.with_suffix(".pdf")
    profile = tempfile.mkdtemp()
    try:
        subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                        "--run-all-compositor-stages-before-draw", "--virtual-time-budget=15000",
                        f"--user-data-dir={profile}", f"--print-to-pdf={pdf}", html_path.as_uri()],
                       capture_output=True, timeout=180)
    except subprocess.TimeoutExpired:
        pass
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    return pdf if pdf.exists() and pdf.stat().st_size else None


def main(argv):
    want_pdf = "--pdf" in argv
    argv = [a for a in argv if a != "--pdf"]
    paths = [pathlib.Path(a) for a in argv] or sorted(
        (p for p in RESULTS.iterdir() if (p / "summary.json").exists()), reverse=True)
    entries = []
    for path in paths:
        if not (path / "summary.json").exists():
            print(f"{path}: no summary.json, skipped")
            continue
        run = verify_run.Run(path)
        verify_run.common(run)
        scenario = verify_run.SCENARIOS.get(run.scenario)
        if scenario:
            scenario(run)
        (path / "report.html").write_text(page(run))
        if want_pdf:
            pdf = to_pdf(path / "report.html")
            if pdf:
                print(f"{path.name}: report.pdf written ({pdf.stat().st_size // 1024} KB)")
        failed = sum(1 for ok, _, _ in run.checks if not ok)
        entries.append((run, "PASS" if not failed else "FAIL", len(run.checks) - failed, len(run.checks),
                        headline(run)))
        print(f"{path.name}: report.html written ({len(run.checks) - failed}/{len(run.checks)} checks)")
    if entries:
        entries.sort(key=lambda e: e[0].path.name.split("-")[-1], reverse=True)
        (RESULTS / "index.html").write_text(index(entries))
        print(f"results/index.html written ({len(entries)} runs)")
        if want_pdf:
            pdf = to_pdf(RESULTS / "index.html")
            if pdf:
                print(f"results/index.pdf written ({pdf.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
