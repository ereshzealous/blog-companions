"""Native HTML panels shared by the saved benchmark report and articles."""
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
import hashlib
import html
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
RESULTS = ROOT / "benchmarks/results"
ESC = html.escape
CLASSES = {
    "01": "JMHScopedValueVsThreadLocalBenchmark",
    "02": "JMHSingleFlightCacheStampedeBenchmark",
    "03": "JMHAdaptiveLimiterBenchmark",
}
TITLES = {"01": "The cost of binding a context", "02": "One hot key, ten thousand readers",
          "03": "What the exchange saw during a surge"}


@lru_cache
def captured():
    return json.loads((RESULTS / "jmh-results.json").read_text())


def entries(key):
    return [e for e in captured() if f".{CLASSES[key]}." in e["benchmark"]]


def select(key, method=None, **params):
    matches = [e for e in entries(key) if (method is None or e["benchmark"].endswith("." + method))
               and all(e.get("params", {}).get(k) == str(v) for k, v in params.items())]
    if len(matches) != 1:
        raise ValueError(f"Expected one captured result: {key} {method} {params}; found {len(matches)}")
    return matches[0]


def mean(e, name=None):
    metric = e["secondaryMetrics"][name] if name else e["primaryMetric"]
    raw = [Decimal(str(v)) for fork in metric["rawData"] for v in fork]
    if not raw or any(not v.is_finite() or v < 0 for v in raw):
        raise ValueError(f"Invalid measurement: {e['benchmark']} {name}")
    return sum(raw) / len(raw)


def fmt(value, digits=1):
    d = Decimal(str(value)).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return f"{d:,.{digits}f}"


def metric_table(headers, rows, caption):
    out = [f'<div class="b-table-wrap"><table class="b-table" role="table"><caption>{ESC(caption)}</caption>',
           '<thead role="rowgroup"><tr role="row">']
    out += [f'<th scope="col" role="columnheader">{ESC(h)}</th>' for h in headers]
    out.append('</tr></thead><tbody role="rowgroup">')
    for row in rows:
        out.append('<tr role="row">')
        out += [f'<td role="cell" data-label="{ESC(h, quote=True)}">{ESC(str(v))}</td>'
                for h, v in zip(headers, row, strict=True)]
        out.append('</tr>')
    return ''.join(out) + '</tbody></table></div>'


def bars(title, subtitle, items, unit, digits=1, maximum=None):
    """Visible labels carry exact values; decorative CSS bars always start at zero."""
    maximum = Decimal(str(maximum)) if maximum is not None else max(Decimal(str(v)) for _, v, _ in items)
    rows = []
    for label, value, color in items:
        if not 0 <= Decimal(str(value)) <= maximum:
            raise ValueError(f"Bar outside scale: {label}")
        width = Decimal(str(value)) / maximum * 100 if maximum else Decimal(0)
        rows.append(f'<div class="b-bar-row"><div class="b-bar-label"><span>{ESC(label)}</span>'
                    f'<strong>{fmt(value, digits)}<small> {ESC(unit)}</small></strong></div>'
                    f'<div class="b-track" aria-hidden="true"><span class="b-fill b-{color}" '
                    f'style="width:{width:.5f}%"></span></div></div>')
    return (f'<div class="b-chart"><h4>{ESC(title)}</h4><p class="b-chart-note">{ESC(subtitle)}</p>'
            + ''.join(rows) + f'<div class="b-scale"><span>0</span><span>{fmt(maximum, digits)} {ESC(unit)}</span></div></div>')


def flow(steps):
    return '<ol class="b-flow">' + ''.join(
        f'<li><strong>{ESC(title)}</strong><span>{ESC(detail)}</span></li>' for title, detail in steps) + '</ol>'


def callout(title, text, warning=False):
    return f'<p class="b-takeaway{" b-warning" if warning else ""}"><strong>{ESC(title)}</strong> {ESC(text)}</p>'


def code(command):
    return f'<pre class="b-code"><code>{ESC(command)}</code></pre>'


def raw_details(label, text):
    return f'<details class="b-details b-raw"><summary>{ESC(label)}</summary>{code(text)}</details>'


def metadata(key):
    e = entries(key)[0]
    warmup = (f"Warm-up: {e['warmupIterations']} × {e['warmupTime']}" if e['mode'] == 'avgt'
              else f"Warm-up: {e['warmupIterations']} episode{'s' if e['warmupIterations'] != 1 else ''}")
    measured = (f"Measured: {e['measurementIterations']} × {e['measurementTime']}" if e['mode'] == 'avgt'
                else f"{e['measurementIterations']} measured episodes / configuration")
    return (f'<div class="b-meta"><span>JMH {ESC(e["jmhVersion"])}</span><span>JDK {ESC(e["jdkVersion"])}</span>'
            f'<span>{e["forks"]} JVM fork</span><span>{ESC(warmup)}</span><span>{ESC(measured)}</span></div>')


def run_details(key, project_href):
    include = {"01": "ScopedValue", "02": "Stampede", "03": "AdaptiveLimiter"}[key]
    e = entries(key)[0]
    command = f"cd java_virtual_threads\n./gradlew jmh -PjmhInclude={include}"
    flags = " ".join(dict.fromkeys(e['jvmArgs']))
    body = '<p>From the repository root, with JDK 21 installed:</p>' + code(command)
    body += ('<p>The Gradle task compiles the benchmark, starts a separate JVM, performs warm-up, '
             'then records the measured iterations with the GC profiler. These component benchmarks '
             'do not require the Docker stack.</p>'
             f'<p><strong>Recorded fork flags:</strong> <code>{ESC(flags)}</code></p>'
             '<p><strong>Automatic capture:</strong> each run writes JSON, terminal output, a manifest and an HTML export '
             'to its own <code>benchmarks/runs/&lt;run-id&gt;/</code> directory. These survive <code>gradlew clean</code>.</p>'
             '<p>Start the live console once, then run benchmarks in another terminal. New runs appear automatically:</p>'
             + code('cd java_virtual_threads\npython3 tools/benchmark_console.py serve')
             + '<p><a href="http://127.0.0.1:8090">Open the live bidder, Docker and benchmark console</a>. '
             'The console captures live telemetry while it is running; benchmark evidence is captured by Gradle even when the console is closed.</p>'
             '<p><strong>This page:</strong> uses the saved <code>benchmarks/results/jmh-results.json</code>. '
             'A new benchmark run does not silently replace the saved evidence. '
             'To regenerate this report from the saved measurements, run from the project directory:</p>'
             + code('python3 tools/build_benchmark_report.py'))
    if key == '03':
        body += ('<p>To capture a new representative timeline separately from the five-episode JMH means:</p>'
                 + code('cd java_virtual_threads\n./gradlew surgeBenchmark')
                 + '<p>This automatically captures timeline CSV, outcomes CSV, terminal output and a report in the new run directory. '
                   'A single episode will vary from the JMH mean.</p>')
    body += f'<p><a href="{project_href}/src/test/java/com/rtb/dsp/benchmarks/{CLASSES[key]}.java">Read the benchmark source</a></p>'
    return '<details class="b-details b-run"><summary>Run this benchmark · commands and output files</summary>' + body + '</details>'


def binding_results():
    charts, rows = [], []
    for suffix, label in [('reusedThread', 'One reused thread'), ('virtualThreadPerTask', 'A new virtual thread per task')]:
        items = []
        for binding, name, color in [('threadLocal', 'ThreadLocal', 'neutral'), ('scopedValue', 'ScopedValue', 'purple')]:
            e = select('01', f'{binding}_{suffix}')
            value = mean(e)
            items.append((name, value, color))
            rows.append([label, name, f'{fmt(value)} ± {fmt(e["primaryMetric"]["scoreError"])} ns',
                         f'{fmt(mean(e, "gc.alloc.rate.norm"), 0)} B'])
        charts.append(bars(label, 'Mean time per task · lower is faster', items, 'ns'))
    return (
        flow([('Prepare', '100,000 tasks per invocation; read the bound context four call frames deep.'),
              ('Compare', 'ThreadLocal set/get/remove versus ScopedValue binding, in two thread shapes.'),
              ('Measure', 'Elapsed time normalized per task, plus allocated bytes from the GC profiler.')])
        + '<div class="b-charts">' + ''.join(charts) + '</div>'
        + metric_table(['Task shape', 'Binding', 'Mean ± JMH error', 'Allocated / task'], rows,
                       'Captured measurements · JMH error is the 99.9% confidence-interval half-width')
        + callout('Read the result:', 'ScopedValue is faster on a reused thread, but allocates more. With a new virtual '
                  'thread per task, the timing error bars overlap. These normalized batch costs are not HTTP request latencies.')
    )


def stampede_results():
    charts, rows = [], []
    strategies = [('NAIVE', 'Naive', 'neutral'), ('SINGLE_FLIGHT', 'SingleFlight', 'green'),
                  ('CAFFEINE_COMPUTE', 'Caffeine compute', 'orange')]
    for latency in [2, 20]:
        items = []
        for strategy, name, color in strategies:
            e = select('02', queryMillis=latency, strategy=strategy)
            items.append((name, mean(e), color))
            rows.append([f'{latency} ms', name, f'{fmt(mean(e))} ms', fmt(mean(e, 'dbQueries'), 0),
                         fmt(mean(e, 'poolTimeouts'), 0), fmt(mean(e, 'slaMisses'), 0),
                         f'{fmt(mean(e, "bystanderStallMs"))} ms'])
        charts.append(bars(f'{latency} ms simulated replica', 'Mean time to finish the stampede · lower is faster', items, 'ms'))
    return (
        flow([('Prepare', 'Park 10,000 virtual-thread readers on a latch; clear one hot cache key.'),
              ('Release', 'Let all readers compete for a simulated 40-connection pool; connection timeout 250 ms.'),
              ('Measure', 'Completion time, store queries, pool timeouts, reads over 50 ms and bystander stalls.')])
        + '<div class="b-charts">' + ''.join(charts) + '</div>'
        + metric_table(['Replica time', 'Strategy', 'Stampede time', 'Queries', 'Pool timeouts', 'Reads >50 ms', 'Bystander stall'],
                       rows, 'Means per stampede across 20 measured episodes · counts rounded to whole readers or queries')
        + callout('Read the result:', 'Both coalescing strategies issue one store query. At 20 ms service time, SingleFlight '
                  'also has a shorter bystander stall than Caffeine compute. The store and its pool are simulated, not live Postgres.')
    )


def surge_results():
    configs = [('STATIC_SEMAPHORE_5000', 'Static 5,000', 'neutral'),
               ('AIMD_STEP8_MIN50', 'AIMD +8 / 50', 'orange'),
               ('AIMD_STEP32_MIN16', 'AIMD +32 / 16', 'purple')]
    rows, metrics = [], {k: [] for k in ['p99AllMs', 'timeoutPct', 'shedPct', 'goodputPct']}
    for gate, name, color in configs:
        e = select('03', gate=gate)
        for m in metrics:
            metrics[m].append((name, mean(e, m), color))
        rows.append([name, f'{fmt(mean(e, "p99AllMs"), 0)} ms', f'{fmt(mean(e, "p99AdmittedMs"), 0)} ms',
                     f'{fmt(mean(e, "timeoutPct"))}%', f'{fmt(mean(e, "shedPct"))}%',
                     f'{fmt(mean(e, "goodputPct"))}%', fmt(mean(e, 'peakInFlight'), 0),
                     f'{fmt(mean(e, "recoveryMs"), 0)} ms'])
    charts = [bars('P99 of all responses', 'Includes fast sheds · lower is better', metrics['p99AllMs'], 'ms', 0),
              bars('Responses past the deadline', 'Over the 80 ms exchange timeout · lower is better', metrics['timeoutPct'], '%'),
              bars('Requests shed at admission', 'Refused work · lower is better when capacity is healthy', metrics['shedPct'], '%', maximum=100),
              bars('Bids answered within 50 ms', 'Useful goodput · higher is better', metrics['goodputPct'], '%', maximum=100)]
    return (
        flow([('Send', '10,000 scheduled arrivals/s for 3.5 s: 35,000 requests per episode, open loop.'),
              ('Slow the dependency', '256 service slots; 5 ms → 150 ms from t=1 s to t=2 s → 5 ms again.'),
              ('Count outcomes', 'Latency starts at scheduled arrival. Goodput means a bid within 50 ms; timeout means over 80 ms.')])
        + '<div class="b-charts">' + ''.join(charts) + '</div>'
        + metric_table(['Gate', 'P99 all', 'P99 admitted', 'Timeouts', 'Shed', 'Goodput', 'Peak in flight', 'Recovery'],
                       rows, 'Five-episode means · percentile columns are means of episode percentiles, not pooled percentiles')
        + callout('The trade-off:', 'The tuned gate reduces timeouts by refusing more work. Admitted-request p99 remains about '
                  '150 ms; the much lower overall p99 includes fast no-bids. Useful goodput stays close to the static gate.', True)
        + '<p class="b-note">Recovery is the time after the surge until a 100 ms arrival bucket reaches 90% of '
          'pre-surge in-SLA bid rate. The separately captured timeline is one episode, not this five-episode average.</p>'
        + raw_details('Captured terminal output · one representative surge episode', (RESULTS / 'surge-summary.txt').read_text())
    )


def render_benchmark(key, heading_level=3, project_href="../.."):
    body = {'01': binding_results, '02': stampede_results, '03': surge_results}[key]()
    digest = hashlib.sha256((RESULTS / 'jmh-results.json').read_bytes()).hexdigest()
    return (f'<section class="benchmark-panel" id="benchmark-{key}" data-benchmark="{key}" aria-labelledby="benchmark-{key}-title">'
            f'<div class="b-eyebrow">Recorded benchmark · Part {int(key)}</div>'
            f'<h{heading_level} id="benchmark-{key}-title">{TITLES[key]}</h{heading_level}>' + metadata(key) + body
            + run_details(key, project_href)
            + raw_details('Inspect the captured JMH JSON · original values', json.dumps(entries(key), indent=2))
            + f'<p class="b-source">Source: <a href="{project_href}/benchmarks/results/jmh-results.json">jmh-results.json</a> '
              f'· SHA-256 <code>{digest[:12]}</code>. Displayed auxiliary counters use the mean of raw episode samples.</p></section>')


def smoke_results(heading_level=3, project_href="../.."):
    names = ['ab-smoke.txt', 'stack-smoke-2.txt']
    rows, throughput, tails = [], [], []
    for i, name in enumerate(names, 1):
        raw = (RESULTS / name).read_text()
        def find(pattern):
            match = re.search(pattern, raw, re.M)
            if not match:
                raise ValueError(f'Missing smoke field {pattern} in {name}')
            return Decimal(match[1])
        count = find(r'^Complete requests:\s+(\d+)')
        rps = find(r'^Requests per second:\s+([\d.]+)')
        p99 = find(r'^\s*99%\s+(\d+)')
        p50 = find(r'^\s*50%\s+(\d+)')
        longest = find(r'^\s*100%\s+(\d+)')
        rows.append([f'Run {i}', fmt(count, 0), fmt(rps, 2), f'{fmt(count/rps, 2)} s',
                     f'{fmt(p50, 0)} ms', f'{fmt(p99, 0)} ms', f'{fmt(longest, 0)} ms'])
        throughput.append((f'Run {i}', rps, 'purple' if i == 2 else 'neutral'))
        tails.append((f'Run {i}', p99, 'orange'))
    return ('<section class="benchmark-panel" id="http-smoke" data-benchmark="smoke" aria-labelledby="http-smoke-title">'
            '<div class="b-eyebrow">Recorded HTTP test · Docker Compose + host JVM</div>'
            f'<h{heading_level} id="http-smoke-title">The full-stack smoke runs</h{heading_level}>'
            '<div class="b-meta"><span>Same-laptop client</span><span>256 concurrent requests</span><span>Keep-alive</span>'
            '<span>Closed-loop ApacheBench</span></div>'
            + flow([('Start the stack', 'Real Postgres primary and standby, Redis and Kafka in Docker; bidder runs on the host.'),
                    ('Warm and send', 'The second run warms up with 20,000 requests, then measures 100,000 bid requests.'),
                    ('Record', 'ApacheBench throughput and latency, with selected application counters and JFR warnings.')])
            + '<div class="b-charts">'
            + bars('Completed requests per second', 'Recorded HTTP response throughput', throughput, 'req/s', 0)
            + bars('Client-observed P99', 'Both runs exceed the 50 ms bidder budget', tails, 'ms', 0) + '</div>'
            + metric_table(['Run', 'Requests', 'Requests / s', 'Approx. duration', 'P50', 'P99', 'Maximum'], rows,
                           'Saved ApacheBench output · approximate duration is calculated as requests ÷ requests/s')
            + callout('Scope of this result:', 'These are roughly three-second smoke tests with client and server on one laptop. '
                      'They do not establish sustainable per-pod capacity or 1M requests/s.', True)
            + '<p class="b-note">The “Failed requests” entries in the saved output are length mismatches caused by variable '
              'response lengths. Connect, receive and exception failures are zero in those excerpts. HTTP 200 counters '
              'do not validate every returned bid’s business correctness.</p>'
            + '<details class="b-details b-run"><summary>How the HTTP run works · commands and capture</summary>'
            + '<p>Start the infrastructure and keep the bidder running in the first terminal:</p>'
            + code('cd java_virtual_threads\ndocker compose up -d --wait\n./gradlew bootRun')
            + '<p>In another terminal, use this comparable request fixture and load command. The original request body '
              'and complete ApacheBench command were not archived, so this is a reproducible example, not an exact replay.</p>'
            + code('mkdir -p build/results/http-smoke\n'
                   "cat > build/results/http-smoke/bid.json <<'JSON'\n"
                   '{"auctionId":"smoke-1","exchange":"x","imp":{"id":"1","banner":{"w":300,"h":250}},'
                   '"deviceGeo":{"country":"US","region":"CA","lat":0,"lon":0},"bidFloorMicros":1000000,"ingressEpochNanos":0}\nJSON\n'
                   'ab -k -l -n 20000 -c 256 -p build/results/http-smoke/bid.json \\\n'
                   '  -T application/json http://localhost:8080/openrtb/2.6/bid\n'
                   'ab -k -l -n 100000 -c 256 -p build/results/http-smoke/bid.json \\\n'
                   '  -T application/json http://localhost:8080/openrtb/2.6/bid \\\n'
                   '  > build/results/http-smoke/ab.txt\n'
                   'curl -fsS http://localhost:8080/actuator/prometheus \\\n'
                   '  > build/results/http-smoke/prometheus.txt')
            + '<p><code>-l</code> accepts variable-length responses. This example sends only bids, not win notices. '
              'Use a separate constant-rate generator and sustained runs for a capacity claim.</p></details>'
            + ''.join(raw_details(f'Captured terminal output · {name}', (RESULTS / name).read_text()) for name in names)
            + f'<p class="b-source">Sources: <a href="{project_href}/benchmarks/results/stack-smoke.md">run notes</a>, '
              f'<a href="{project_href}/benchmarks/results/ab-smoke.txt">first run</a>, '
              f'<a href="{project_href}/benchmarks/results/stack-smoke-2.txt">second run</a>.</p></section>')


BENCHMARK_CSS = """
.benchmark-panel{width:var(--wide);margin:24px 0 32px;padding:26px;border:1px solid #dbe2ec;
border-top:3px solid var(--logic);border-radius:14px;background:#fff;scroll-margin-top:24px;min-width:0}
.benchmark-panel h2,.benchmark-panel h3{margin:6px 0 14px;color:var(--ink);font-family:"Lilita One",sans-serif;font-weight:400;font-size:29px;line-height:1.16}
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
.b-bar-row{margin:0 0 16px}.b-bar-label{display:flex;justify-content:space-between;gap:8px;align-items:baseline;margin-bottom:6px;font-size:13px;line-height:1.45}
.b-bar-label strong{font-variant-numeric:tabular-nums;white-space:nowrap;font-size:15px}.b-bar-label small{font-weight:400;font-size:11px;color:#64748b}
.b-track{height:10px;background:#e9edf3;border-radius:3px;overflow:hidden}.b-fill{display:block;height:100%;border-radius:3px}
.b-neutral{background:#64748b}.b-purple{background:#7c3aed}.b-green{background:#16a34a}.b-orange{background:#d97706}
.b-scale{display:flex;justify-content:space-between;gap:10px;color:#64748b;font-size:10px;font-variant-numeric:tabular-nums}
.b-table-wrap{overflow-x:auto;margin:0 0 20px}.b-table{width:100%;font-size:12px;line-height:1.5;font-variant-numeric:tabular-nums}
.b-table caption{text-align:left;color:#475569;font-size:12px;padding:0 0 10px;caption-side:top}
.b-table th{font-size:11px;white-space:normal;padding:9px 8px;vertical-align:bottom;color:#475569}
.b-table td{padding:10px 8px}.b-table td:first-child{font-weight:700;color:var(--ink)}
.b-takeaway{font-size:14px;line-height:1.6;padding:14px 16px;border-left:3px solid #7c3aed;background:#faf5ff;border-radius:0 8px 8px 0;margin:0 0 16px}
.b-warning{border-color:#d97706;background:#fff7ed}
.b-note,.b-source{font-size:12px;line-height:1.65;color:#475569;margin:14px 0}
.b-source{margin-bottom:0;overflow-wrap:anywhere}.b-source code{font-size:11px}
.b-details{border-top:1px solid var(--rule);padding:12px 0;font-size:14px}
.b-details summary{cursor:pointer;font-weight:700;color:var(--logic);font-size:14px;line-height:1.5}
.b-details[open] summary{margin-bottom:16px}.b-details p{font-size:14px;line-height:1.6}
.b-code{margin:12px 0 18px;padding:16px;background:#0f172a;color:#e2e8f0;border-radius:8px;overflow:auto;font:12px/1.65 "Comic Shanns",monospace}
.b-code code{font:inherit;color:inherit;background:none;border:0;padding:0;overflow-wrap:normal}
.b-raw .b-code{max-height:380px}.b-details summary:focus-visible{outline:2px solid var(--logic);outline-offset:4px}
.benchmark-links{display:flex;flex-wrap:wrap;gap:8px 18px;margin:18px 0;font-size:14px}
.results-edition .masthead{margin-bottom:4px}.results-edition .masthead h1{max-width:880px}
.results-intro{max-width:720px;color:#475569;font-size:17px}
.results-edition .benchmark-panel{margin-top:16px}
@media(max-width:640px){
.benchmark-panel{padding:18px 14px;border-radius:10px}.benchmark-panel h2,.benchmark-panel h3{font-size:25px}
.b-flow,.b-charts{grid-template-columns:1fr}.b-flow{gap:12px}.b-flow li{padding-left:30px;position:relative}.b-flow li::before{position:absolute;left:0;top:1px}
.b-chart-note{min-height:0}.b-chart{padding:14px}.b-meta{margin-bottom:18px}.b-table-wrap{overflow:visible}
.b-table,.b-table tbody,.b-table tr,.b-table td{display:block}.b-table thead{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}
.b-table caption{display:block}.b-table tr{border:1px solid var(--rule);border-radius:8px;margin-bottom:12px;overflow:hidden}
.b-table td{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.2fr);gap:10px;padding:7px 10px;overflow-wrap:anywhere}
.b-table td::before{content:attr(data-label);color:#64748b;font-weight:400;font-size:11px}.b-table td:last-child{border-bottom:0}
.b-table td:first-child{background:#f8fafc}.b-code{font-size:11px;padding:12px}.b-source{font-size:11px}
}
@media print{.benchmark-panel{padding:12pt;border:1px solid #ddd}.b-details:not([open]){display:none}
.b-code{white-space:pre-wrap}.b-charts,.b-flow{break-inside:avoid}.b-raw .b-code{max-height:none}.benchmark-links{display:none}}
"""
