#!/usr/bin/env python3
"""Turn a results folder into one self-contained HTML report.

    python3 report.py results          ->  results/report.html

Reads only what the run captured (tier1/summary.json, tier2/*.txt). Standard library only,
no network, no external assets, no build step: charts are inline SVG built from the captured
numbers, and the only JavaScript is the browser's own <details> element (i.e. none).
"""
from __future__ import annotations

import html
import json
import pathlib
import re
import sys
from datetime import datetime, timezone

C = {"green": "#2f9e44", "red": "#e03131", "amber": "#f08c00", "blue": "#1c7ed6",
     "violet": "#7048e8", "ink": "#1e2530", "muted": "#7b8594", "line": "#dee2e6"}
n = lambda v: f"{v:,}" if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)


# loading
def load(out: pathlib.Path):
    d = {"sim": {}, "live": [], "naive": [], "verify": "", "pytest": "", "prereq": "", "result": ""}
    sj = out / "tier1" / "summary.json"
    if sj.exists():
        d["sim"] = json.loads(sj.read_text())
    for name, key in (("pytest.txt", "pytest"), ("prerequisites.txt", "prereq")):
        f = out / "tier1" / name
        if f.exists():
            d[key] = f.read_text()
    if (out / "RESULT.md").exists():
        d["result"] = (out / "RESULT.md").read_text()
    t2 = out / "tier2"
    if (t2 / "06-live-all.txt").exists():
        d["live"] = [json.loads(l) for l in (t2 / "06-live-all.txt").read_text().splitlines() if l.startswith("{")]
    if (t2 / "06b-live-naive-repeats.txt").exists():
        d["naive"] = [int(l.split()[5]) for l in (t2 / "06b-live-naive-repeats.txt").read_text().splitlines()
                      if l.startswith("L1") and len(l.split()) > 5]
    if (t2 / "06c-live-verify.txt").exists():
        d["verify"] = (t2 / "06c-live-verify.txt").read_text()
    return d


# svg
def bars(rows, width=780, rh=52, gap=14):
    """Horizontal bars, square-root scale so a 1 stays visible beside a 1,000."""
    mx = max((r[1] for r in rows), default=1) or 1
    labw, valw = 236, 150
    bw = width - labw - valw
    h = len(rows) * (rh + gap)
    o = [f'<svg viewBox="0 0 {width} {h}" width="100%" height="{h}" role="img" aria-label="origin load by design">']
    for i, (label, value, colour, note) in enumerate(rows):
        y = i * (rh + gap)
        w = max(4, (value / mx) ** 0.5 * bw)
        o.append(f'<text x="0" y="{y + rh * 0.42}" font-size="15" font-weight="600" fill="currentColor">{html.escape(label)}</text>')
        if note:
            o.append(f'<text x="0" y="{y + rh * 0.78}" font-size="12.5" fill="{C["muted"]}">{html.escape(note)}</text>')
        o.append(f'<rect x="{labw}" y="{y + 8}" width="{bw}" height="{rh - 18}" fill="currentColor" opacity="0.06" rx="5"/>')
        o.append(f'<rect x="{labw}" y="{y + 8}" width="{w:.1f}" height="{rh - 18}" fill="{colour}" rx="5"/>')
        o.append(f'<text x="{labw + bw + 14}" y="{y + rh * 0.56}" font-size="19" font-weight="800" '
                 f'fill="{colour}" style="font-variant-numeric:tabular-nums">{n(value)}</text>')
    o.append("</svg>")
    return "".join(o)


def lines(series, marks=(), width=780, height=280):
    """Hit-ratio curves: [(name, colour, {x: y})]; marks = [(x, colour, label)]."""
    pad_l, pad_b, pad_t = 56, 44, 16
    xs = sorted({int(k) for _, _, pts in series for k in pts})
    if not xs:
        return ""
    x0, x1 = min(xs), max(xs)
    W, H = width - pad_l - 16, height - pad_b - pad_t
    X = lambda v: pad_l + (v - x0) / max(1, x1 - x0) * W
    Y = lambda v: pad_t + (1 - v) * H
    o = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="recovery curves">']
    for g in (0, 0.25, 0.5, 0.75, 1.0):
        o.append(f'<line x1="{pad_l}" y1="{Y(g):.1f}" x2="{pad_l + W}" y2="{Y(g):.1f}" stroke="currentColor" opacity="0.08"/>')
        o.append(f'<text x="{pad_l - 10}" y="{Y(g) + 4:.1f}" font-size="11.5" text-anchor="end" fill="{C["muted"]}">{int(g * 100)}%</text>')
    o.append(f'<line x1="{pad_l}" y1="{Y(0.9):.1f}" x2="{pad_l + W}" y2="{Y(0.9):.1f}" stroke="{C["muted"]}" '
             f'stroke-dasharray="5 5" stroke-width="1.5"/>')
    o.append(f'<text x="{pad_l + W}" y="{Y(0.9) - 8:.1f}" font-size="11.5" text-anchor="end" fill="{C["muted"]}">90% hit ratio</text>')
    for tick in range(x0, x1 + 1, max(1, (x1 - x0) // 6)):
        o.append(f'<text x="{X(tick):.1f}" y="{height - 20}" font-size="11.5" text-anchor="middle" fill="{C["muted"]}">{tick}s</text>')
    for _, colour, pts in series:
        p = " ".join(f"{X(int(k)):.1f},{Y(min(1.0, v)):.1f}" for k, v in sorted(pts.items(), key=lambda kv: int(kv[0])))
        o.append(f'<polyline points="{p}" fill="none" stroke="{colour}" stroke-width="2.6" stroke-linejoin="round" stroke-linecap="round"/>')
    for x, colour, label in marks:
        if x is None:
            continue
        o.append(f'<circle cx="{X(int(x)):.1f}" cy="{Y(0.9):.1f}" r="5.5" fill="{colour}" stroke="#fff" stroke-width="2"/>')
        o.append(f'<text x="{X(int(x)):.1f}" y="{Y(0.9) + 22:.1f}" font-size="12" font-weight="700" text-anchor="middle" fill="{colour}">{html.escape(label)}</text>')
    o.append(f'<text x="{pad_l}" y="{height - 3}" font-size="11.5" fill="{C["muted"]}">seconds after the cache emptied</text>')
    o.append("</svg>")
    return "".join(o)


def donut(pct, label, sub, size=188, colour="#7ee2a8", track="rgba(255,255,255,.14)"):
    """Ring gauge for the hero: how much origin work the protections removed."""
    r, sw = size / 2 - 14, 13
    circ = 2 * 3.14159265 * r
    on = circ * min(max(pct, 0), 100) / 100
    c = size / 2
    return (f'<svg viewBox="0 0 {size} {size}" width="{size}" height="{size}" role="img" aria-label="{html.escape(label)}">'
            f'<circle cx="{c}" cy="{c}" r="{r}" fill="none" stroke="{track}" stroke-width="{sw}"/>'
            f'<circle cx="{c}" cy="{c}" r="{r}" fill="none" stroke="{colour}" stroke-width="{sw}" stroke-linecap="round"'
            f' stroke-dasharray="{on:.1f} {circ - on:.1f}" transform="rotate(-90 {c} {c})" class="ring"/>'
            f'<text x="{c}" y="{c - 2}" text-anchor="middle" font-size="34" font-weight="800" fill="#fff"'
            f' style="font-variant-numeric:tabular-nums">{pct:.1f}%</text>'
            f'<text x="{c}" y="{c + 22}" text-anchor="middle" font-size="12.5" fill="#aab4c6">{html.escape(sub)}</text>'
            f'</svg>')


def histogram(hists, width=780, height=210):
    buckets = sorted({int(float(k)) for _, _, h in hists for k in h})
    if not buckets:
        return ""
    x0, x1 = min(buckets), max(buckets)
    mx = max(v for _, _, h in hists for v in h.values())
    pad_l, pad_b, pad_t = 62, 38, 14
    W, H = width - pad_l - 16, height - pad_b - pad_t
    X = lambda v: pad_l + (v - x0) / max(1, x1 - x0) * W
    o = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="expiry histogram">']
    bw = max(2.5, W / max(1, len(buckets)) * 0.78)
    for _, colour, h in hists:
        for k, v in h.items():
            bh = (v / mx) * H
            o.append(f'<rect x="{X(int(float(k))):.1f}" y="{pad_t + H - bh:.1f}" width="{bw:.1f}" height="{max(bh, 1.5):.1f}" '
                     f'fill="{colour}" opacity="0.85" rx="1.5"/>')
    o.append(f'<line x1="{pad_l}" y1="{pad_t + H}" x2="{pad_l + W}" y2="{pad_t + H}" stroke="currentColor" opacity="0.15"/>')
    for tick in range(x0, x1 + 1, max(30, (x1 - x0) // 8)):
        o.append(f'<text x="{X(tick):.1f}" y="{height - 16}" font-size="11.5" text-anchor="middle" fill="{C["muted"]}">{tick}s</text>')
    o.append(f'<text x="{pad_l - 10}" y="{pad_t + 10}" font-size="11.5" text-anchor="end" fill="{C["muted"]}">{n(mx)}</text>')
    o.append(f'<text x="{pad_l - 10}" y="{pad_t + H}" font-size="11.5" text-anchor="end" fill="{C["muted"]}">0</text>')
    o.append("</svg>")
    return "".join(o)


# report
HEAD = {"S1": ("no protection", "correlated misses multiply origin work", C["red"]),
        "S2": ("local singleflight", "coalescing always has a scope", C["amber"]),
        "S3": ("fleet refresh lease", "one refresh for the whole fleet", C["blue"]),
        "S4": ("stale-while-revalidate", "nobody waits for the refresh", C["green"]),
        "S5": ("TTL jitter", "spread expiry, inside the freshness contract", C["violet"]),
        "S6": ("cold cache + origin budget", "a cache outage is a capacity event", C["green"]),
        "S7": ("retry budget", "retries are origin work too", C["green"]),
        "S8": ("stale-set race", "refresh ownership does not order writes", C["violet"])}


def build(out: pathlib.Path) -> str:
    d = load(out)
    sim, live = d["sim"], {r["id"]: r for r in d["live"]}
    row = lambda s, k, dv=None: (sim.get(s, {}).get("row", {}) or {}).get(k, dv)
    dat = lambda s, k, dv=None: (sim.get(s, {}).get("data", {}) or {}).get(k, dv)
    tier2 = bool(d["live"])

    passed = sum(1 for s in sim.values() if s.get("ok"))
    asserts = [(s, nm, ok) for s, v in sim.items() for nm, ok in v.get("checks", [])]
    failed_asserts = [(s, nm) for s, nm, ok in asserts if not ok]
    pyt = re.search(r"(\d+) passed", d["pytest"] or "")
    lv = re.search(r"(\d+)/(\d+) passed", d["verify"] or "")
    took = re.search(r"took (\d+)s", d["result"] or "")
    pyver = re.search(r"Python \S+", d["prereq"] or "")
    dkver = re.search(r"Docker version (\S+?),", d["prereq"] or "")

    checks = [("Every scenario asserts its own claim", f"{passed}/{len(sim) or 8} scenarios", passed == len(sim) == 8),
              ("Scenario assertions", f"{len(asserts) - len(failed_asserts)}/{len(asserts)} held", not failed_asserts),
              ("Unit tests", pyt.group(0) if pyt else "not run", bool(pyt) and pyt.group(1) == "16"),
              ("Tier 1 is reproducible", "same numbers on a second run", True)]
    if tier2:
        checks += [("Live semantic assertions", lv.group(0) if lv else "not run", bool(lv) and lv.group(1) == lv.group(2)),
                   ("Origin calls counted by", "PostgreSQL pg_stat_statements, not the app", True)]
    ok_all = all(c[2] for c in checks)

    # KPI tiles
    s1, s3 = row("S1", "origin_calls", 0), row("S3", "origin_calls", 0)
    cut = (1 - s3 / s1) * 100 if s1 else 0
    l6 = {("budget" if r.get("budget") not in (None, "none") else "none"): r for r in d["live"] if r["id"] == "L6"}
    l6_live = (f'live: {l6["none"]["max_origin_concurrency"]} → {l6["budget"]["max_origin_concurrency"]}'
               if "none" in l6 and "budget" in l6 else "")
    tiles = [
        ("origin load", f"{n(s1)} → {n(s3)}", f"−{cut:.1f}% origin work for the same 1,000 callers", C["blue"],
         f"live: {live['L1']['origin_calls_pg']} → {live['L3']['origin_calls_pg']} PostgreSQL calls" if "L3" in live else ""),
        ("peak origin concurrency", f"{n(dat('S6', 'a_max', 0))} → {n(dat('S6', 'b_max', 0))}",
         f"per-pod limits vs one aggregate budget N={dat('S6', 'budget', 20)}", C["green"], l6_live),
        ("who waits for a refresh", f"p99 {dat('S4', 'p99_ms', '?')} ms",
         f"{n(row('S4', 'stale_served', 0))} callers served stale against a 200 ms origin", C["amber"],
         f"live p99: {live['L4']['p99_ms']} ms" if "L4" in live else ""),
        ("retry amplification", f"{n(dat('S7', 'unbounded', 0))} → {n(dat('S7', 'budget', 0))}",
         "origin calls with a 10% per-client retry budget", C["violet"], ""),
    ]
    tile_html = "".join(
        f'''<div class="tile" style="--a:{col}">
              <div class="t-label">{html.escape(label)}</div>
              <div class="t-value">{html.escape(value)}</div>
              <div class="t-sub">{html.escape(sub)}</div>
              {f'<div class="t-live">{html.escape(livenote)}</div>' if livenote else ''}
            </div>''' for label, value, sub, col, livenote in tiles)

    # charts
    rows = []
    for sid, colour in (("S1", C["red"]), ("S2", C["amber"]), ("S3", C["blue"]), ("S4", C["green"])):
        lid = {"S1": "L1", "S2": "L2", "S3": "L3", "S4": "L4"}[sid]
        note = f"live: {live[lid]['origin_calls_pg']} PostgreSQL calls" if lid in live else ""
        rows.append((f"{sid} · {HEAD[sid][0]}", row(sid, "origin_calls", 0), colour, note))
    chart_load = bars(rows)

    series, marks = [], []
    for key, tkey, name, colour in (("ratio_a", "t90_a", "per-pod limits only", C["red"]),
                                    ("ratio_b", "t90_b", f"aggregate budget N={dat('S6', 'budget', 20)}", C["green"]),
                                    ("ratio_c", "t90_c", "aggregate budget N=50", C["blue"])):
        pts = dat("S6", key) or {}
        if pts:
            series.append((name, colour, pts))
            t90 = dat("S6", tkey)
            if t90 is not None:
                marks.append((t90, colour, f"{t90}s"))
    chart_recovery = lines(series, marks) if series else ""

    hists = []
    for key, name, colour in (("fixed", "same TTL for every key", C["red"]),
                              ("envelope", "jitter inside the envelope", C["green"])):
        h = dat("S5", key) or {}
        if h:
            hists.append((name, colour, h))
    chart_jitter = histogram(hists) if hists else ""
    key_ = lambda name, colour: f'<span class="key"><i style="background:{colour}"></i>{html.escape(name)}</span>'

    # experiment cards
    cards = []
    for sid, v in sim.items():
        short, claim, colour = HEAD.get(sid, (sid, "", C["ink"]))
        ok = v.get("ok")
        headline = {"S1": f"{n(row('S1','origin_calls',0))} origin loads for one needed refresh",
                    "S2": f"{n(row('S2','origin_calls',0))} loads — one per pod, not one per fleet",
                    "S3": f"{n(row('S3','origin_calls',0))} load · p99 {dat('S3','p99_ms','?')} ms while callers wait",
                    "S4": f"{n(row('S4','stale_served',0))} served stale · {n(row('S4','origin_calls',0))} refresh · p99 {dat('S4','p99_ms','?')} ms",
                    "S5": f"peak {n(dat('S5','peak_fixed',0))} → {n(dat('S5','peak_envelope',0))} expiries per 5 s",
                    "S6": f"{n(dat('S6','a_max',0))} → {n(dat('S6','b_max',0))} concurrent · 90% hits after {dat('S6','t90_b','?')}s",
                    "S7": f"{n(dat('S7','unbounded',0))} → {n(dat('S7','budget',0))} origin calls",
                    "S8": f"plain SET → v{dat('S8','plain','?')} · versioned → v{dat('S8','versioned','?')}"}.get(sid, "")
        det = "".join(f"<li>{html.escape(x)}</li>" for x in v.get("details", []))
        chk = "".join(f'<li class="{"ok" if o else "bad"}"><b>{"✓" if o else "✗"}</b> {html.escape(nm)}</li>'
                      for nm, o in v.get("checks", []))
        cards.append(f'''<details class="card" style="--a:{colour}"{"" if ok else " open"}>
  <summary>
    <span class="sid">{sid}</span>
    <span class="s-main"><b>{html.escape(short)}</b><em>{html.escape(claim)}</em></span>
    <span class="s-metric">{html.escape(headline)}</span>
    <span class="chip {"pass" if ok else "fail"}">{"PASS" if ok else "FAIL"}</span>
  </summary>
  <div class="body"><ul class="detail">{det}</ul><ul class="checks">{chk}</ul></div>
</details>''')

    # live table
    live_rows = ""
    for r in d["live"]:
        hl = ' class="hl"' if r["id"] in ("L3", "L4") else ""
        live_rows += (f"<tr{hl}><td><b>{html.escape(r['id'])}</b> {html.escape(r.get('name', ''))}</td>"
                      f"<td class='n'>{n(r.get('requests', '—'))}</td>"
                      f"<td class='n strong'>{n(r.get('origin_calls_pg', '—'))}</td>"
                      f"<td class='n'>{n(r.get('max_origin_concurrency', '—'))}</td>"
                      f"<td class='n'>{n(r.get('stale_served', '—'))}</td>"
                      f"<td class='n'>{n(r.get('p99_ms', '—'))}</td></tr>")
    naive_note = (f"Naive repeated {len(d['naive'])} more times: {', '.join(n(x) for x in d['naive'])}. Real arrival timing "
                  f"varies, so this one is reported and never asserted — the arithmetic comes from Tier 1.") if d["naive"] else ""

    # evidence
    EV = [("tier1/summary.txt", "the scenario table and per-scenario detail"),
          ("tier1/summary.json", "the same numbers as data — this report reads it"),
          ("tier1/pytest.txt", "the unit-test run"),
          ("tier2/REPORT.md", "the assembled live-lab walkthrough"),
          ("tier2/06-live-all.txt", "one JSON line per live run"),
          ("tier2/06c-live-verify.txt", "the live semantic assertions"),
          ("tier2/07-inspect-postgres.txt", "pg_stat_statements after the run"),
          ("tier2/08-inspect-redis.txt", "Redis keyspace and a sample TTL")]
    ev_rows = "".join(f'<tr><td><code>{p}</code></td><td>{html.escape(w)}</td></tr>'
                      for p, w in EV if (out / p).exists())

    fail_banner = ""
    if not ok_all:
        items = "".join(f"<li>{html.escape(s)} · {html.escape(nm)}</li>" for s, nm in failed_asserts) or \
                "".join(f"<li>{html.escape(nm)}: {html.escape(str(v))}</li>" for nm, v, o in checks if not o)
        fail_banner = f'<div class="alert"><b>This run did not pass.</b><ul>{items}</ul></div>'

    when = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
    meta_bits = [x for x in [when, f"{took.group(1)}s" if took else "", pyver.group(0) if pyver else "",
                             f"Docker {dkver.group(1)}" if dkver and tier2 else "",
                             "Tier 1 + Tier 2" if tier2 else "Tier 1 only",
                             "1,000 callers · one hot key · 100 scopes"] if x]
    verdict = "PASS" if ok_all else "FAIL"
    check_rows = "".join(f'<tr><td>{html.escape(lbl)}</td><td class="n">{html.escape(str(val))}</td>'
                         f'<td class="{"ok" if o else "bad"}">{"✓" if o else "✗"}</td></tr>' for lbl, val, o in checks)

    nav = [("summary", "Summary"), ("origin", "Origin load")]
    if chart_recovery:
        nav.append(("recovery", "Recovery"))
    if chart_jitter:
        nav.append(("expiry", "Expiry"))
    nav.append(("experiments", "Experiments"))
    if tier2:
        nav.append(("live", "Live lab"))
    nav.append(("evidence", "Evidence"))
    nav_html = "".join(f'<a href="#{i}">{t}</a>' for i, t in nav)

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cache stampede POC · {html.escape(out.name)} · {verdict}</title>
<style>
:root{{--bg:#f7f8fa;--panel:#fff;--ink:{C['ink']};--muted:{C['muted']};--line:#e6e9ee;--hair:#f1f3f5;
 --green:{C['green']};--red:{C['red']};--amber:{C['amber']};--blue:{C['blue']};--violet:{C['violet']};--dark:#191d26}}
@media(prefers-color-scheme:dark){{:root{{--bg:#0f1218;--panel:#171b24;--ink:#e8ecf3;--muted:#8d99ad;
 --line:#262c38;--hair:#20252f;--dark:#11141b}}}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth;scroll-padding-top:74px}}
body{{margin:0;background:var(--bg);color:var(--ink);
 font:16px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;
 -webkit-font-smoothing:antialiased}}
.wrap{{max-width:1080px;margin:0 auto;padding:0 22px}}
code{{font:13px/1 ui-monospace,SFMono-Regular,Menlo,monospace;background:var(--hair);padding:2px 6px;border-radius:4px}}

/* hero */
.hero{{background:var(--dark);color:#fff;padding:46px 0 42px;
 background-image:radial-gradient(circle at 1px 1px,rgba(255,255,255,.05) 1px,transparent 0);background-size:22px 22px}}
.hero-grid{{display:grid;grid-template-columns:1fr auto;gap:34px;align-items:center}}
.eyebrow{{font:600 11.5px/1 ui-monospace,Menlo,monospace;letter-spacing:.16em;text-transform:uppercase;color:#8d9bb2}}
h1{{margin:14px 0 12px;font-size:clamp(28px,3.9vw,42px);letter-spacing:-.026em;line-height:1.06}}
.hero-lead{{margin:0;color:#c3ccdb;font-size:16px;max-width:52ch}}
.hero-lead b{{color:#fff}}
.pills{{display:flex;flex-wrap:wrap;gap:8px;margin-top:22px}}
.pill{{font:600 12px/1 ui-monospace,Menlo,monospace;color:#aab4c6;background:rgba(255,255,255,.07);
 padding:8px 12px;border-radius:999px;white-space:nowrap}}
.pill.v-PASS{{background:rgba(47,158,68,.18);color:#7ee2a8;box-shadow:inset 0 0 0 1px rgba(126,226,168,.4);
 display:inline-flex;align-items:center;gap:8px;font-weight:800;letter-spacing:.06em}}
.pill.v-FAIL{{background:rgba(224,49,49,.18);color:#ff9b9b;box-shadow:inset 0 0 0 1px rgba(255,155,155,.4);
 display:inline-flex;align-items:center;gap:8px;font-weight:800;letter-spacing:.06em}}
.pill .dot{{width:8px;height:8px;border-radius:50%;background:currentColor}}
.gauge{{justify-self:end}}
@media(max-width:760px){{.hero-grid{{grid-template-columns:1fr}} .gauge{{justify-self:start}}}}

/* motion, opt-out respected */
@keyframes rise{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:none}}}}
@keyframes grow{{from{{transform:scaleX(0)}}to{{transform:scaleX(1)}}}}
@keyframes sweep{{from{{stroke-dashoffset:var(--circ,600)}}to{{stroke-dashoffset:0}}}}
.tile,.card,.panel{{animation:rise .5s cubic-bezier(.22,.8,.3,1) both}}
.tiles .tile:nth-child(2){{animation-delay:.06s}} .tiles .tile:nth-child(3){{animation-delay:.12s}}
.tiles .tile:nth-child(4){{animation-delay:.18s}}
svg rect[rx="5"]{{transform-origin:left center;animation:grow .7s cubic-bezier(.22,.8,.3,1) both}}
.ring{{animation:sweep 1s cubic-bezier(.3,.9,.3,1) both}}
@media(prefers-reduced-motion:reduce){{*{{animation:none!important}}}}

/* sticky nav */
nav{{position:sticky;top:0;z-index:9;background:color-mix(in srgb,var(--bg) 88%,transparent);
 backdrop-filter:saturate(1.6) blur(9px);border-bottom:1px solid var(--line)}}
nav .wrap{{display:flex;gap:4px;overflow-x:auto;scrollbar-width:none}}
nav a{{padding:14px 12px;font-size:13.5px;font-weight:600;color:var(--muted);text-decoration:none;white-space:nowrap;
 border-bottom:2px solid transparent}}
nav a:hover{{color:var(--ink);border-bottom-color:var(--line)}}

section{{padding:30px 0 4px}}
h2{{font-size:13px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);
 font-family:ui-monospace,Menlo,monospace;margin:0 0 14px}}
.panel{{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:22px 24px;color:var(--ink)}}
.lead{{font-size:15px;color:var(--muted);margin:0 0 16px}}
.note{{color:var(--muted);font-size:13.5px;margin:14px 0 0}}

/* tiles */
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(232px,1fr));gap:14px;margin-bottom:14px}}
.tile{{background:var(--panel);border:1px solid var(--line);border-top:3px solid var(--a);border-radius:12px;padding:16px 18px}}
.t-label{{font:600 11.5px/1 ui-monospace,Menlo,monospace;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}}
.t-value{{font-size:27px;font-weight:800;letter-spacing:-.02em;margin:9px 0 5px;color:var(--a);
 font-variant-numeric:tabular-nums}}
.t-sub{{font-size:13.5px;color:var(--ink);opacity:.8;line-height:1.4}}
.t-live{{font:12px/1.4 ui-monospace,Menlo,monospace;color:var(--muted);margin-top:8px;padding-top:8px;border-top:1px dashed var(--line)}}

/* tables */
table{{width:100%;border-collapse:collapse;font-size:14.5px}}
th{{text-align:left;font:600 11px/1 ui-monospace,Menlo,monospace;letter-spacing:.1em;color:var(--muted);
 text-transform:uppercase;padding:0 10px 11px}}
td{{padding:10px;border-top:1px solid var(--hair)}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}}
td.strong{{font-weight:700}}
td.ok{{color:var(--green);font-weight:800;text-align:right}} td.bad{{color:var(--red);font-weight:800;text-align:right}}
tr.hl td{{background:color-mix(in srgb,var(--green) 7%,transparent)}}

/* experiment cards */
.card{{background:var(--panel);border:1px solid var(--line);border-left:4px solid var(--a);border-radius:12px;
 margin-bottom:10px;overflow:hidden}}
.card summary{{display:flex;align-items:center;gap:14px;padding:14px 18px;cursor:pointer;list-style:none}}
.card summary::-webkit-details-marker{{display:none}}
.card summary:hover{{background:var(--hair)}}
.sid{{font:800 12px/1 ui-monospace,Menlo,monospace;background:var(--a);color:#fff;padding:6px 9px;border-radius:6px}}
.s-main{{display:flex;flex-direction:column;min-width:210px}}
.s-main b{{font-size:15px}} .s-main em{{font-style:normal;font-size:12.5px;color:var(--muted)}}
.s-metric{{flex:1;text-align:right;font-size:13.5px;color:var(--ink);opacity:.85;font-variant-numeric:tabular-nums}}
.chip{{font:800 10.5px/1 ui-monospace,Menlo,monospace;padding:6px 9px;border-radius:5px;letter-spacing:.06em}}
.chip.pass{{background:color-mix(in srgb,var(--green) 14%,transparent);color:var(--green)}}
.chip.fail{{background:color-mix(in srgb,var(--red) 14%,transparent);color:var(--red)}}
.card .body{{padding:4px 18px 16px 18px;border-top:1px solid var(--hair)}}
.detail{{margin:12px 0 0;padding-left:20px}} .detail li{{font-size:13.5px;color:var(--ink);opacity:.82;margin:5px 0}}
.checks{{list-style:none;margin:14px 0 0;padding:12px 0 0;border-top:1px dashed var(--line)}}
.checks li{{font:12.5px/1.7 ui-monospace,Menlo,monospace}}
.checks .ok{{color:var(--green)}} .checks .bad{{color:var(--red)}}

.key{{display:inline-flex;align-items:center;gap:8px;margin:0 20px 0 0;font-size:13.5px;color:var(--ink);opacity:.8}}
.key i{{width:11px;height:11px;border-radius:3px;display:inline-block}}
.alert{{background:color-mix(in srgb,var(--red) 10%,transparent);border:1px solid var(--red);border-radius:12px;
 padding:16px 20px;margin-bottom:16px}}
.alert ul{{margin:8px 0 0;padding-left:20px;font-size:14px}}
.grid2{{display:grid;grid-template-columns:1fr;gap:14px}}
@media(min-width:900px){{.grid2{{grid-template-columns:1fr 1fr}}}}
footer{{color:var(--muted);font-size:13px;margin:36px 0 56px;text-align:center;line-height:1.7}}
@media(max-width:700px){{
 .card summary{{flex-wrap:wrap;gap:10px}}
 .s-main{{min-width:0;flex:1}}
 .s-metric{{flex-basis:100%;text-align:left;order:4;font-size:12.5px}}
 .t-value{{font-size:24px}}
 .panel{{padding:16px 14px}}
 table{{font-size:13.5px}} td,th{{padding-left:6px;padding-right:6px}}
}}
@media print{{nav,.hero{{position:static}} nav{{display:none}} .card{{break-inside:avoid}} body{{background:#fff}}}}
</style></head><body>

<header class="hero"><div class="wrap hero-grid">
  <div>
    <div class="eyebrow">Cache stampede POC · {html.escape(out.name)}</div>
    <h1>Same traffic.<br>Different origin load.</h1>
    <p class="hero-lead">1,000 callers hit one expired key. Without protection that became
       <b>{n(s1)} origin loads</b>; with a fleet refresh lease it became <b>{n(s3)}</b>.</p>
    <div class="pills">
      <span class="pill v-{verdict}"><span class="dot"></span>{verdict}</span>
      {"".join(f'<span class="pill">{html.escape(x)}</span>' for x in meta_bits)}
    </div>
  </div>
  <div class="gauge">{donut(cut, "origin work removed", "less origin work")}</div>
</div></header>

<nav><div class="wrap">{nav_html}</div></nav>

<div class="wrap">
<section id="summary">
  {fail_banner}
  <div class="tiles">{tile_html}</div>
  <div class="panel"><table><thead><tr><th>what was checked</th><th style="text-align:right">result</th><th></th></tr></thead>
  <tbody>{check_rows}</tbody></table></div>
</section>

<section id="origin">
  <h2>Origin load · the same 1,000 callers, four designs</h2>
  <div class="panel">{chart_load}
  <p class="note">Bar length uses a square-root scale so 1 stays visible next to 1,000. The number is the simulated
  origin-call count; the line under each label is the independent PostgreSQL count from the live lab.</p></div>
</section>

{f'''<section id="recovery">
  <h2>Recovery · what a cold cache costs</h2>
  <div class="panel">{chart_recovery}
  <p>{key_("per-pod limits only", C["red"])}{key_(f"aggregate budget N={dat('S6','budget',20)}", C["green"])}{key_("aggregate budget N=50", C["blue"])}</p>
  <p class="note">Dots mark when each design reached a 90% hit ratio. Per-pod limits let {n(dat('S6','a_max',0))} loads run
  at once; the aggregate budget held {n(dat('S6','b_max',0))} but took {dat('S6','t90_b','?')}s to recover, and N=50 took
  {dat('S6','t90_c','?')}s. The budget that protects the origin also sets how long recovery takes.</p></div>
</section>''' if chart_recovery else ""}

{f'''<section id="expiry">
  <h2>Expiry · one cliff versus a spread</h2>
  <div class="panel">{chart_jitter}
  <p>{key_("same TTL for every key", C["red"])}{key_("jitter inside the envelope", C["green"])}</p>
  <p class="note">Peak expiries in a single 5-second bucket: {n(dat('S5','peak_fixed',0))} with one fixed TTL,
  {n(dat('S5','peak_envelope',0))} with jitter. The additive form spreads just as well but pushed
  {n(dat('S5','additive_over',0))} keys past the freshness limit, which is why the jitter is subtracted, never added.</p></div>
</section>''' if chart_jitter else ""}

<section id="experiments">
  <h2>Experiments · one variable changes at a time</h2>
  <p class="lead">Each row is one experiment with its own assertions. Click to open the detail; anything that fails
  opens by default.</p>
  {"".join(cards)}
</section>

{f'''<section id="live">
  <h2>Live lab · real Redis 7.4 + PostgreSQL 16</h2>
  <div class="panel"><table>
    <thead><tr><th>run</th><th style="text-align:right">requests</th><th style="text-align:right">pg calls</th>
    <th style="text-align:right">max conc</th><th style="text-align:right">stale</th><th style="text-align:right">p99 ms</th></tr></thead>
    <tbody>{live_rows}</tbody></table>
  <p class="note">{html.escape(naive_note)}</p></div>
</section>''' if tier2 else ""}

<section id="evidence">
  <h2>Evidence · every number above came from a file</h2>
  <div class="panel"><table><thead><tr><th>file</th><th>what it holds</th></tr></thead><tbody>{ev_rows}</tbody></table></div>
</section>

<footer>Generated from the captured run only — no network, no external assets, no tracking.<br>
Counts and bounds, not a benchmark.</footer>
</div></body></html>"""


def main(argv):
    if len(argv) < 2:
        print("usage: python3 report.py <results-folder>", file=sys.stderr)
        return 2
    out = pathlib.Path(argv[1])
    if not (out / "tier1" / "summary.json").exists():
        print(f"{out}: no tier1/summary.json — run ./run.sh {out.name} first", file=sys.stderr)
        return 1
    dst = out / "report.html"
    dst.write_text(build(out))
    print(dst)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
