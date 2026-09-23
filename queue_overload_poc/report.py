#!/usr/bin/env python3
"""Turn a results folder into one self-contained HTML report.

    python3 report.py results          ->  results/report.html

Reads only what the run captured (tier1/summary.json, tier2/*.txt). Standard
library only: no network, no external assets, no build step. The charts are
inline SVG built from the captured numbers, and the only JavaScript is the
browser's own <details> element — that is, none.
"""
from __future__ import annotations

import html
import json
import pathlib
import re
import sys
from datetime import datetime, timezone

C = {"green": "#0f8a4d", "red": "#c81e3a", "amber": "#b45309", "blue": "#1d4ed8",
     "violet": "#6d28d9", "ink": "#0b1220", "muted": "#8a96ab", "line": "#dde3ec",
     "zone": "#f7f9fc"}
n = lambda v: f"{v:,}" if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)
e = html.escape


# ---------------------------------------------------------------- loading
def load(out: pathlib.Path):
    d = {"sim": {}, "live": [], "verify": "", "pytest": "", "prereq": "", "result": "", "summary": ""}
    sj = out / "tier1" / "summary.json"
    if sj.exists():
        d["sim"] = json.loads(sj.read_text())
    for rel, key in ((("tier1", "pytest.txt"), "pytest"),
                     (("tier1", "prerequisites.txt"), "prereq"),
                     (("tier1", "summary.txt"), "summary"),
                     (("tier2", "06b-live-verify.txt"), "verify")):
        p = out.joinpath(*rel)
        if p.exists():
            d[key] = p.read_text()
    la = out / "tier2" / "06-live-all.txt"
    if la.exists():
        for line in la.read_text().splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "id" in r:
                    d["live"].append(r)
    rm = out / "RESULT.md"
    if rm.exists():
        d["result"] = rm.read_text()
    return d


def metrics(sim):
    return {s["id"]: s.get("derived", s.get("metrics", {})) for s in sim.get("scenarios", [])}


def names(sim):
    return {s["id"]: s.get("name", s["id"]) for s in sim.get("scenarios", [])}


# ---------------------------------------------------------------- svg helpers
def bars(rows, width=800, rh=44, gap=12, fmt=n):
    """Horizontal bars: [(label, value, colour, note)]."""
    if not rows:
        return ""
    top = max(r[1] for r in rows) or 1
    lw, vw = 250, 130
    bw = width - lw - vw
    h = len(rows) * (rh + gap)
    out = [f'<svg viewBox="0 0 {width} {h}" width="100%" role="img">']
    for i, (label, val, col, note) in enumerate(rows):
        y = i * (rh + gap)
        w = max(3, (val / top) * bw)
        out.append(f'<text x="0" y="{y + rh * .62}" font-size="15" fill="{C["ink"]}">{e(label)}</text>')
        out.append(f'<rect x="{lw}" y="{y + 6}" width="{bw}" height="{rh - 12}" fill="{C["zone"]}" rx="3"/>')
        out.append(f'<rect x="{lw}" y="{y + 6}" width="{w:.1f}" height="{rh - 12}" fill="{col}" rx="3"/>')
        out.append(f'<text x="{lw + bw + 12}" y="{y + rh * .62}" font-size="15" font-weight="600" fill="{col}">{e(fmt(val))}</text>')
        if note:
            out.append(f'<text x="{lw + bw + 12}" y="{y + rh * .62 + 17}" font-size="12" fill="{C["muted"]}">{e(note)}</text>')
    out.append("</svg>")
    return "".join(out)


def series(points, width=800, height=230, colour=C["blue"], marks=()):
    """One time series: points = [(x, y)] in data units."""
    if len(points) < 2:
        return ""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    x0, x1 = min(xs), max(xs) or 1
    y1 = max(ys) or 1
    pad = 34
    px = lambda x: pad + (x - x0) / (x1 - x0 or 1) * (width - pad * 2)
    py = lambda y: height - pad - (y / y1) * (height - pad * 2)
    path = " ".join(f"{'M' if i == 0 else 'L'}{px(x):.1f},{py(y):.1f}" for i, (x, y) in enumerate(points))
    area = f"M{px(x0):.1f},{height - pad} " + path[1:] + f" L{px(x1):.1f},{height - pad} Z"
    out = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">',
           f'<path d="{area}" fill="{colour}" opacity=".10"/>',
           f'<line x1="{pad}" y1="{height - pad}" x2="{width - pad}" y2="{height - pad}" stroke="{C["line"]}" stroke-width="1.5"/>',
           f'<path d="{path}" fill="none" stroke="{colour}" stroke-width="2.5"/>']
    for mx, label, col in marks:
        out.append(f'<line x1="{px(mx):.1f}" y1="{pad - 14}" x2="{px(mx):.1f}" y2="{height - pad}" '
                   f'stroke="{col}" stroke-width="1.5" stroke-dasharray="4 4"/>')
        out.append(f'<text x="{px(mx) + 6:.1f}" y="{pad - 4}" font-size="12" fill="{col}">{e(label)}</text>')
    out.append(f'<text x="{pad}" y="{pad - 16}" font-size="12" fill="{C["muted"]}">peak {n(round(y1))}</text>')
    out.append("</svg>")
    return "".join(out)


def stack(rows, width=800, rh=46, gap=12):
    """Stacked service / waiting bars: [(label, service, waiting)]."""
    if not rows:
        return ""
    top = max(s + w for _, s, w in rows) or 1
    lw, vw = 150, 140
    bw = width - lw - vw
    h = len(rows) * (rh + gap)
    out = [f'<svg viewBox="0 0 {width} {h}" width="100%" role="img">']
    for i, (label, svc, wait) in enumerate(rows):
        y = i * (rh + gap)
        ws = max(2, svc / top * bw)
        ww = wait / top * bw
        out.append(f'<text x="0" y="{y + rh * .62}" font-size="15" fill="{C["ink"]}">{e(label)}</text>')
        out.append(f'<rect x="{lw}" y="{y + 8}" width="{ws:.1f}" height="{rh - 16}" fill="{C["blue"]}" rx="2"/>')
        if ww > 0.5:
            out.append(f'<rect x="{lw + ws:.1f}" y="{y + 8}" width="{ww:.1f}" height="{rh - 16}" '
                       f'fill="{C["amber"]}" opacity=".35" stroke="{C["amber"]}" rx="2"/>')
        out.append(f'<text x="{lw + bw + 12}" y="{y + rh * .62}" font-size="15" font-weight="600" '
                   f'fill="{C["ink"]}">{svc + wait:.0f} ms</text>')
    out.append("</svg>")
    return "".join(out)


# ---------------------------------------------------------------- page
CSS = """
:root{--ink:#0b1220;--muted:#8a96ab;--line:#dde3ec;--zone:#f7f9fc;--ok:#0f8a4d;--bad:#c81e3a;
--sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
--mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
*{box-sizing:border-box}
body{margin:0;background:#fff;color:var(--ink);font:16px/1.6 var(--sans);padding:0 20px 80px}
.wrap{max-width:980px;margin:0 auto}
header{padding:48px 0 24px;border-bottom:1px solid var(--line)}
h1{font-size:34px;line-height:1.2;margin:8px 0 6px;letter-spacing:-.02em}
.sub{color:var(--muted);margin:0}
.verdict{display:inline-block;margin-top:18px;padding:8px 18px;border-radius:999px;
font-weight:700;letter-spacing:.06em;font-size:14px}
.verdict.pass{background:#e8f8ef;color:var(--ok)}.verdict.fail{background:#ffeef1;color:var(--bad)}
h2{font-size:22px;margin:44px 0 6px;letter-spacing:-.01em}
h2 .note{font-weight:400;font-size:15px;color:var(--muted);margin-left:10px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin:22px 0 0}
.tile{border:1px solid var(--line);border-radius:8px;padding:14px 16px}
.tile .k{font:600 11px/1 var(--mono);letter-spacing:.09em;text-transform:uppercase;color:var(--muted)}
.tile .v{font-size:27px;font-weight:700;margin-top:8px;letter-spacing:-.02em}
.tile .n{font-size:13px;color:var(--muted);margin-top:2px}
table{border-collapse:collapse;width:100%;margin:16px 0;font-size:14.5px}
th,td{border-bottom:1px solid var(--line);padding:9px 10px;text-align:left;vertical-align:top}
th{font:600 11px/1.4 var(--mono);letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}
td.num{font-family:var(--mono);white-space:nowrap}
.ok{color:var(--ok);font-weight:600}.bad{color:var(--bad);font-weight:600}
pre{background:var(--zone);border:1px solid var(--line);border-radius:6px;padding:14px 16px;
overflow-x:auto;font:13px/1.5 var(--mono)}
details{border:1px solid var(--line);border-radius:6px;padding:10px 14px;margin:12px 0}
summary{cursor:pointer;font-weight:600;font-size:15px}
.legend{font-size:13px;color:var(--muted);margin-top:6px}
.sw{display:inline-block;width:11px;height:11px;border-radius:2px;vertical-align:-1px;margin-right:5px}
footer{margin-top:56px;padding-top:18px;border-top:1px solid var(--line);color:var(--muted);font-size:13px}
@media print{body{padding:0}details{break-inside:avoid}}
"""


def build(out: pathlib.Path) -> str:
    d = load(out)
    M, NM = metrics(d["sim"]), names(d["sim"])
    sweep = d["sim"].get("consumer_sweep", [])
    passed = "**PASS**" in d["result"]
    tier1_only = not d["live"]

    def m(sid, key, default=0):
        return M.get(sid, {}).get(key, default)

    # headline tiles
    tiles = []
    if M:
        tiles = [
            ("peak backlog", f'{m("S2","peak_depth")/1e6:.2f}M', "S2 · sustained overload"),
            ("worst wait", f'{round(m("S3","max_wait_s")/60)} min', "S3 · the customer's delay"),
            ("backlog clears", f'+{round((m("S3","time_to_backlog_zero_s") or 0)/60)} min',
             f'{m("S3","debt_ratio")}x the spike'),
            ("16x consumers buy", f'{m("S4","throughput_vs_s2")}x', "S4 · throughput vs S2"),
            ("admission bounds it", f'{m("S7","peak_depth")/1e6:.2f}M',
             f'{round((m("S7","peak_depth_vs_s6") or 0)*100,1)}% of the S6 control'),
        ]
    tile_html = "".join(
        f'<div class="tile"><div class="k">{e(k)}</div><div class="v">{e(v)}</div>'
        f'<div class="n">{e(note)}</div></div>' for k, v, note in tiles)

    # backlog comparison
    order = ["S2", "S4", "S5", "S6", "S7"]
    backlog_rows = [(f'{sid} · {NM.get(sid, sid)}', m(sid, "peak_depth"),
                     C["green"] if sid == "S7" else (C["amber"] if sid in ("S5", "S6") else C["red"]),
                     "") for sid in order if sid in M]

    # the age curve, straight from the S3 samples if they were captured
    s3 = out / "tier1" / "S3-samples.csv"
    curve = ""
    if s3.exists():
        rows = s3.read_text().splitlines()
        hdr = rows[0].split(",")
        ti, ai = hdr.index("t_s"), hdr.index("oldest_age_s")
        pts = []
        for r in rows[1::10]:
            f = r.split(",")
            pts.append((float(f[ti]), float(f[ai])))
        slo = 60
        marks = [(720, "traffic normal", C["green"])]
        z = m("S3", "time_to_backlog_zero_s")
        if z:
            marks.append((720 + z, "backlog zero", C["ink"]))
        curve = series(pts, colour=C["amber"], marks=marks)

    # the consumer sweep, service vs waiting
    sweep_rows = [(f'{r["workers"]} workers', r["service_ms"], r["wait_ms"]) for r in sweep]

    # live table
    live_html = ""
    if d["live"]:
        head = ("<tr><th>run</th><th>what changed</th><th>rate/s</th><th>workers</th>"
                "<th>throughput/s</th><th>rt p95</th><th>svc p50</th><th>prefetch held</th>"
                "<th>queue delay p95</th><th>deferred</th></tr>")
        body = "".join(
            f'<tr><td class="num">{e(r["id"])}</td><td>{e(r["name"])}</td>'
            f'<td class="num">{n(r["rate_per_s"])}</td><td class="num">{n(r["workers"])}</td>'
            f'<td class="num">{n(r["throughput_per_s"])}</td>'
            f'<td class="num">{n(r["round_trip_p95_ms"])} ms</td>'
            f'<td class="num">{n(r["db_service_p50_ms"])} ms</td>'
            f'<td class="num">{n(r["peak_unprocessed"])}</td>'
            f'<td class="num">{n(r["queue_delay_p95_ms"])} ms</td>'
            f'<td class="num">{n(r["deferred"])}</td></tr>' for r in d["live"])
        live_html = f"<table>{head}{body}</table>"

    # scenario detail
    scen_html = ""
    for s in d["sim"].get("scenarios", []):
        rows = "".join(
            f'<tr><td class="{"ok" if p["ok"] else "bad"}">{"ok" if p["ok"] else "FAIL"}</td>'
            f'<td class="num">{e(str(p["metric"]))} {e(str(p["op"]))} {e(str(p["expected"]))}</td>'
            f'<td class="num">{e(str(p["actual"]))}</td><td>{e(p["claim"])}</td></tr>'
            for p in s.get("predictions", []))
        ok = all(p["ok"] for p in s.get("predictions", []))
        scen_html += (f'<details><summary>{e(s["id"])} · {e(s["name"])} '
                      f'<span class="{"ok" if ok else "bad"}">{"all held" if ok else "FAILED"}</span>'
                      f'</summary><p style="color:var(--muted);margin:8px 0 0">{e(s.get("question",""))}</p>'
                      f'<table><tr><th></th><th>prediction</th><th>actual</th><th>claim</th></tr>'
                      f'{rows}</table></details>')

    verify = d["verify"]
    vpass = re.search(r"Tier 2 semantic assertions: (\d+)/(\d+) passed", verify)

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Queue overload POC · run report</title><style>{CSS}</style></head><body><div class="wrap">
<header>
  <p class="sub" style="font:600 12px/1 var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--muted)">
    Distributed Systems · #15 · POC run report</p>
  <h1>Your queue is durable. Your system is still overloaded.</h1>
  <p class="sub">Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from
     <code>{e(out.name)}/</code>{' · Tier 1 only' if tier1_only else ' · Tier 1 + Tier 2'}</p>
  <span class="verdict {'pass' if passed else 'fail'}">{'PASS' if passed else 'FAIL'}</span>
</header>

<h2>Headline<span class="note">every number read from the captured run</span></h2>
<div class="tiles">{tile_html}</div>

<h2>Peak backlog by scenario<span class="note">same spike, different controls</span></h2>
{bars(backlog_rows)}
<p class="legend">S2 through S6 all peak at the same backlog. Only S7 — admission control — moves it.</p>

{'<h2>Oldest message age · S3<span class="note">the spike ends at minute 12; the debt does not</span></h2>' + curve if curve else ''}

<h2>Consumer sweep<span class="note">where the time actually goes</span></h2>
{stack(sweep_rows)}
<p class="legend"><span class="sw" style="background:{C['blue']}"></span>service time — constant, the
dependency never slows down &nbsp;&nbsp;
<span class="sw" style="background:{C['amber']};opacity:.45"></span>waiting for a slot — this is the
queue, relocated</p>

<h2>Scenarios<span class="note">predictions registered before the run</span></h2>
{scen_html}

{'<h2>Tier 2 · live Kafka + PostgreSQL</h2>' + live_html if live_html else ''}
{'<pre>' + e(verify.strip()) + '</pre>' if verify else ''}
{'<p class="legend">Semantic assertions: <span class="' + ('ok' if vpass and vpass.group(1)==vpass.group(2) else 'bad') + '">' + e(vpass.group(0)) + '</span></p>' if vpass else ''}

<details><summary>Tier 1 summary (raw)</summary><pre>{e(d['summary'][:6000])}</pre></details>
<details><summary>pytest</summary><pre>{e(d['pytest'].strip()[-3000:])}</pre></details>
<details><summary>prerequisites</summary><pre>{e(d['prereq'].strip())}</pre></details>

<footer>Worked-example parameters, not a benchmark. The dependency is a bounded-capacity model in
Tier 1 and a real PostgreSQL write path behind a fixed number of slots in Tier 2. This report is one
self-contained file: no network, no external assets, no JavaScript.</footer>
</div></body></html>"""


def main(argv):
    if not argv:
        print(__doc__.strip())
        return 2
    out = pathlib.Path(argv[0])
    if not out.is_dir():
        print(f"no such results folder: {out}", file=sys.stderr)
        return 1
    dst = out / "report.html"
    dst.write_text(build(out))
    print(dst)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
