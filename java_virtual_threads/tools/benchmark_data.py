"""Read benchmark evidence into a common model for the live console and HTML exports."""
import csv
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import re

CLASSES = {'01': 'JMHScopedValueVsThreadLocalBenchmark', '02': 'JMHSingleFlightCacheStampedeBenchmark',
           '03': 'JMHAdaptiveLimiterBenchmark'}
TITLES = {'01': 'The cost of binding a context', '02': 'One hot key, ten thousand readers',
          '03': 'What the exchange saw during a surge'}


def number(v):
    n = Decimal(str(v))
    if not n.is_finite() or n < 0:
        raise ValueError(f'Invalid nonnegative measurement: {v}')
    return n


def mean(entry, metric=None):
    m = entry['secondaryMetrics'][metric] if metric else entry['primaryMetric']
    samples = [number(v) for fork in m['rawData'] for v in fork]
    if not samples:
        raise ValueError(f'No measured samples: {entry["benchmark"]} / {metric}')
    return sum(samples) / len(samples)


def fmt(value, digits=1):
    n = Decimal(str(value))
    if not n.is_finite(): return 'unavailable'
    return f'{n.quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP):,.{digits}f}'


def chart(title, unit, items, digits=1, maximum=None):
    return dict(title=title, unit=unit, digits=digits, maximum=maximum, kind='bar',
                series=[dict(label=label, value=float(number(value)), color=color) for label, value, color in items])


def panel(key, title, columns, rows, charts, notes=(), steps=(), meta=()):
    return dict(id=key, title=title, columns=columns, rows=rows, charts=charts,
                notes=list(notes), steps=list(steps), meta=list(meta))


def jmh_panel(key, entries):
    """Only supplied configurations appear; no fallback to historical measurements."""
    charts, rows = [], []
    e = entries[0]
    meta = [f'JMH {e["jmhVersion"]}', 'JDK ' + ', '.join(sorted({x['jdkVersion'] for x in entries})),
            f'{e["forks"]} JVM fork(s)', f'Warm-up: {e["warmupIterations"]} × {e["warmupTime"]}',
            f'Measured: {e["measurementIterations"]} × {e["measurementTime"]}']
    samples = sorted({sum(map(len, x['primaryMetric']['rawData'])) for x in entries})
    notes = [f'Measured samples per configuration: {", ".join(map(str, samples))}. '
             'Auxiliary event counters are means of raw episode samples, not JMH event totals.']
    if key == '01':
        for shape, title in [('reusedThread', 'One reused thread'), ('virtualThreadPerTask', 'A new virtual thread per task')]:
            items = []
            for binding, label, color in [('threadLocal', 'ThreadLocal', 'neutral'), ('scopedValue', 'ScopedValue', 'purple')]:
                matches = [x for x in entries if x['benchmark'].endswith(f'.{binding}_{shape}')]
                for x in matches:
                    if x['primaryMetric']['scoreUnit'] != 'ns/op': raise ValueError('Binding units must be ns/op')
                    items.append((label, mean(x), color))
                    rows.append([title, label, f'{fmt(mean(x))} ± {fmt(x["primaryMetric"]["scoreError"])} ns',
                                 f'{fmt(mean(x, "gc.alloc.rate.norm"), 0)} B'])
            if items: charts.append(chart(title, 'ns', items))
        notes += ['Time is normalized per task across a batch, not HTTP latency. The ± value is JMH’s 99.9% '
                  'confidence-interval half-width; unavailable means there were too few samples.']
        steps = [('Prepare', '100,000 tasks per invocation; context read four frames deep.'),
                 ('Compare', 'ThreadLocal and ScopedValue in the selected thread shapes.'),
                 ('Measure', 'Normalized task time and allocated bytes; lower means less cost.')]
        columns = ['Task shape', 'Binding', 'Mean ± JMH error', 'Allocated / task']
    elif key == '02':
        names = {'NAIVE': ('Naive', 'neutral'), 'SINGLE_FLIGHT': ('SingleFlight', 'green'),
                 'CAFFEINE_COMPUTE': ('Caffeine compute', 'orange')}
        for latency in sorted({x['params']['queryMillis'] for x in entries}, key=Decimal):
            items = []
            for strategy, (label, color) in names.items():
                for x in entries:
                    if x['params'] != dict(queryMillis=latency, strategy=strategy): continue
                    if x['primaryMetric']['scoreUnit'] != 'ms/op': raise ValueError('Stampede units must be ms/op')
                    items.append((label, mean(x), color))
                    rows.append([f'{latency} ms', label, f'{fmt(mean(x))} ms', fmt(mean(x, 'dbQueries'), 0),
                                 fmt(mean(x, 'poolTimeouts'), 0), fmt(mean(x, 'slaMisses'), 0),
                                 f'{fmt(mean(x, "bystanderStallMs"))} ms'])
            if items: charts.append(chart(f'{latency} ms simulated replica', 'ms', items))
        columns = ['Replica time', 'Strategy', 'Stampede time', 'Queries', 'Pool timeouts', 'Reads >50 ms', 'Bystander stall']
        notes += ['The store and connection pool are simulated. Times and counts are means per measured episode.']
        steps = [('Prepare', '10,000 readers wait on a latch with one hot key absent.'),
                 ('Release', 'Simulated 40-connection pool with a 250 ms connection timeout.'),
                 ('Measure', 'Completion, queries, timeouts, SLA misses and bystander stalls.')]
    else:
        names = {'STATIC_SEMAPHORE_5000': ('Static 5,000', 'neutral'), 'AIMD_STEP8_MIN50': ('AIMD +8 / 50', 'orange'),
                 'AIMD_STEP32_MIN16': ('AIMD +32 / 16', 'purple')}
        metrics = {m: [] for m in ['p99AllMs', 'timeoutPct', 'shedPct', 'goodputPct']}
        for gate, (label, color) in names.items():
            for x in entries:
                if x['params'].get('gate') != gate: continue
                for m in metrics: metrics[m].append((label, mean(x, m), color))
                rows.append([label, f'{fmt(mean(x, "p99AllMs"), 0)} ms', f'{fmt(mean(x, "p99AdmittedMs"), 0)} ms',
                             f'{fmt(mean(x, "timeoutPct"))}%', f'{fmt(mean(x, "shedPct"))}%',
                             f'{fmt(mean(x, "goodputPct"))}%', fmt(mean(x, 'peakInFlight'), 0),
                             f'{fmt(mean(x, "recoveryMs"), 0)} ms'])
        for metric, title, unit, digits, maximum in [('p99AllMs', 'P99 of all responses', 'ms', 0, None),
                ('timeoutPct', 'Past the 80 ms deadline', '%', 1, 100), ('shedPct', 'Shed at admission', '%', 1, 100),
                ('goodputPct', 'Bids answered within 50 ms', '%', 1, 100)]:
            charts.append(chart(title, unit, metrics[metric], digits, maximum))
        columns = ['Gate', 'P99 all', 'P99 admitted', 'Timeouts', 'Shed', 'Goodput', 'Peak in flight', 'Recovery']
        notes += ['P99 values are means of episode percentiles, not pooled percentiles. Overall P99 includes fast sheds. '
                  'Recovery measures return to 90% of pre-surge goodput. This simulation does not measure HTTP capacity.']
        steps = [('Send', '10,000 scheduled arrivals/s for 3.5 s, open loop.'),
                 ('Slow', '256 service slots; 5 ms → 150 ms from t=1 to t=2 s → 5 ms.'),
                 ('Measure', 'Latency from scheduled arrival; useful bids within 50 ms, timeouts over 80 ms.')]
    if len(rows) != len(entries): raise ValueError(f'Unsupported or duplicate Part {key} configuration')
    return panel(key, TITLES[key], columns, rows, charts, notes, steps, meta)


def parse_ab(text, name):
    def get(pattern):
        m = re.search(pattern, text, re.M)
        if not m: raise ValueError(f'{name}: missing ApacheBench field {pattern}')
        return number(m[1])
    count = get(r'^Complete requests:\s+(\d+)')
    rate = get(r'^Requests per second:\s+([\d.]+)')
    if not rate: raise ValueError(f'{name}: zero throughput')
    p50, p99 = [get(r'^\s*' + n + r'%\s+(\d+)') for n in ['50', '99']]
    failed = get(r'^Failed requests:\s+(\d+)')
    return panel('ab-' + name, name, ['Requests', 'Requests/s', 'Approx. seconds', 'P50', 'P99', 'ab failed'],
        [[fmt(count, 0), fmt(rate, 2), fmt(count/rate, 2), f'{fmt(p50, 0)} ms', f'{fmt(p99, 0)} ms', fmt(failed, 0)]],
        [chart('Completed HTTP requests', 'req/s', [(name, rate, 'purple')], 0),
         chart('Client-observed P99', 'ms', [(name, p99, 'orange')], 0)],
        ['Closed-loop ApacheBench result. Approximate duration = requests ÷ throughput. Check the raw failure breakdown: '
         'length mismatches, network failures and business correctness are different checks. This alone does not prove sustained capacity.'])


def parse_csv(path):
    with path.open() as f:
        reader = csv.DictReader(f); rows = list(reader); fields = set(reader.fieldnames or [])
    if not rows: raise ValueError(f'{path.name}: empty CSV')
    gates = sorted({r.get('gate', '') for r in rows})
    colors = ['neutral', 'orange', 'purple']
    def color(gate, i):
        if 'static' in gate.lower(): return 'neutral'
        if '+8' in gate: return 'orange'
        if '+32' in gate: return 'purple'
        return colors[i % 3]
    if {'gate', 'bucket_ms', 'arrivals', 'bid_in_sla', 'late_bid', 'timed_out', 'shed'} <= fields:
        summary, good, timeouts = [], [], []
        for i, gate in enumerate(gates):
            selected = [r for r in rows if r['gate'] == gate]
            for r in selected:
                if number(r['arrivals']) != sum(number(r[k]) for k in ['bid_in_sla', 'late_bid', 'timed_out', 'shed']):
                    raise ValueError(f'{path.name}: inconsistent outcome counts')
            if len({r['bucket_ms'] for r in selected}) != len(selected): raise ValueError('Duplicate outcome buckets')
            totals = {k: sum(number(r[k]) for r in selected) for k in ['arrivals', 'bid_in_sla', 'late_bid', 'timed_out', 'shed']}
            if not totals['arrivals']: raise ValueError('No arrivals')
            summary.append([gate, *[fmt(totals[k], 0) for k in totals]])
            good.append((gate, totals['bid_in_sla']/totals['arrivals']*100, color(gate, i)))
            timeouts.append((gate, totals['timed_out']/totals['arrivals']*100, color(gate, i)))
        return panel('outcomes', 'Surge outcomes · recorded buckets', ['Gate', 'Arrivals', 'In-SLA bid', 'Late bid', 'Timeout', 'Shed'],
                     summary, [chart('In-SLA bids', '%', good, maximum=100), chart('Timeouts', '%', timeouts, maximum=100)],
                     ['Totals across the supplied CSV buckets. Each gate is a separate episode; this is not the JMH multi-episode mean.'])
    if {'gate', 't_ms', 'limit', 'in_flight', 'queued', 'served', 'shed'} <= fields:
        charts = []
        for metric, title in [('limit', 'Admission limit'), ('in_flight', 'Requests in flight'), ('queued', 'Requests queued')]:
            series = []
            for i, gate in enumerate(gates):
                selected = sorted([r for r in rows if r['gate'] == gate], key=lambda r: number(r['t_ms']))
                points = [[float(number(r['t_ms'])), float(number(r[metric]))] for r in selected]
                if len({p[0] for p in points}) != len(points): raise ValueError('Duplicate timeline ticks')
                series.append(dict(label=gate, color=color(gate, i), points=points))
            charts.append(dict(kind='line', title=title, unit='requests', xUnit='ms', series=series))
        return panel('timeline', 'Surge timeline · captured samples', [], [], charts,
                     ['Each trace uses its own episode clock. Missing timestamps are not invented.'])
    raise ValueError(f'{path.name}: unsupported CSV columns')


def artifacts_for(directory):
    return [dict(name=p.name, sha256=hashlib.sha256(p.read_bytes()).hexdigest(), text=p.read_text()[:120000],
                 truncated=p.stat().st_size > 120000)
            for p in sorted(Path(directory).iterdir()) if p.is_file() and p.suffix in {'.json', '.jsonl', '.csv', '.txt', '.log', '.md'}
            and p.name not in {'manifest.json', 'report.json'}]


def telemetry_panel(path):
    samples = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not samples: return None
    charts = []
    metrics = [('Bid endpoint throughput', 'req/s', lambda s: s.get('rps')),
               ('Bid response interval mean', 'ms', lambda s: s.get('mean_ms')),
               ('Bidder JVM heap', 'MiB', lambda s: (s.get('bidder') or {}).get('heap_bytes'))]
    for title, unit, getter in metrics:
        points = [[float(number(s['epoch'])), None if getter(s) is None else float(number(getter(s)))/(1048576 if unit == 'MiB' else 1)] for s in samples]
        charts.append(dict(kind='line', title=title, unit=unit, xUnit='time', series=[dict(label='Observed bidder', color='purple', points=points)]))
    return panel('live-capture', 'Live bidder captured during this run', [], [], charts,
                 ['These samples describe the running bidder JVM and Compose services, not the forked JMH JVM. '
                  'Unavailable samples remain gaps. The raw JSONL also retains Docker CPU, memory and connection errors.'],
                 meta=[f'{len(samples)} timestamped samples'])


def load_evidence(directory):
    directory = Path(directory)
    panels, raw, entries, seen = [], [], [], set()
    for path in sorted(directory.iterdir()):
        if path.suffix not in {'.json', '.jsonl', '.csv', '.txt', '.log', '.md'} or path.name in {'manifest.json', 'report.json', 'live-metrics.jsonl'}: continue
        text = path.read_text()
        raw.append(dict(name=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                        text=text[:120000], truncated=len(text) > 120000))
        if path.suffix in {'.json', '.jsonl'}:
            data = ([json.loads(line) for line in text.splitlines() if line.strip()]
                    if path.suffix == '.jsonl' else json.loads(text))
            if not isinstance(data, list) or not data or not all(isinstance(x, dict) and 'primaryMetric' in x for x in data):
                raise ValueError(f'{path.name}: expected JMH JSON array or one JMH object per JSONL line')
            for entry in data:
                identity = (entry['benchmark'], json.dumps(entry.get('params', {}), sort_keys=True))
                if identity in seen: raise ValueError(f'Duplicate JMH result: {identity}')
                seen.add(identity); entries.append(entry)
        elif path.suffix == '.csv': panels.append(parse_csv(path))
        elif re.search(r'^Requests per second:', text, re.M) and re.search(r'^Complete requests:', text, re.M):
            panels.append(parse_ab(text, path.name))
    for key, name in CLASSES.items():
        selected = [e for e in entries if f'.{name}.' in e['benchmark']]
        if selected: panels.insert(int(key)-1, jmh_panel(key, selected))
    unknown = [e for e in entries if not any(f'.{c}.' in e['benchmark'] for c in CLASSES.values())]
    if unknown: raise ValueError('Unsupported JMH class: ' + unknown[0]['benchmark'])
    if not panels: raise ValueError('No supported benchmark results found')
    live = directory/'live-metrics.jsonl'
    if live.exists():
        captured = telemetry_panel(live)
        if captured: panels.append(captured)
    return dict(panels=panels, artifacts=artifacts_for(directory), configurations=len(entries))
