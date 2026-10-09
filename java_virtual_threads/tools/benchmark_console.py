#!/usr/bin/env python3
"""Local, read-only live console; benchmark run capture and standalone exports. Python 3.10+."""
import argparse
from contextlib import contextmanager
from collections import deque
from datetime import datetime, timezone
import hashlib
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import threading
import time
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen
import uuid

from benchmark_data import load_evidence, artifacts_for

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / 'tools/console'
RUNS = ROOT / 'benchmarks/runs'


def now(): return datetime.now(timezone.utc).isoformat()


def atomic(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(value); temp.replace(path)


def write_json(path, data): atomic(path, json.dumps(data, indent=2, allow_nan=False) + '\n')


def source_hashes():
    files = [*ROOT.glob('src/**/*.java'), *ROOT.glob('src/**/*.yml'), *ROOT.glob('*.gradle.kts'),
             *ROOT.glob('tools/benchmark_*.py'), *ASSETS.glob('*')]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files) if p.is_file()}


@contextmanager
def capture_lock(run_dir):
    with (run_dir/'.capture.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)


def begin(run_dir, kind, command):
    run_dir = Path(run_dir)
    if run_dir.exists() and any(run_dir.iterdir()): raise ValueError('Refusing to reuse a nonempty run directory')
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = dict(id=run_dir.name, kind=kind, status='running', started_at=now(), command=command,
                    host=dict(system=platform.system(), release=platform.release(), machine=platform.machine(),
                              logical_cpus=os.cpu_count()), source_sha256=source_hashes())
    write_json(run_dir/'manifest.json', manifest)
    print('Recording benchmark:', run_dir)


def run_data(run_dir):
    manifest_path = run_dir/'manifest.json'
    if manifest_path.exists(): manifest = json.loads(manifest_path.read_text())
    else: manifest = dict(id='archived', status='archived', kind='Saved benchmark evidence', started_at=None,
                         command='Historical capture; see the original run notes')
    result = dict(manifest=manifest, panels=[], artifacts=artifacts_for(run_dir), configurations=0)
    if manifest['status'] in {'succeeded', 'archived'}: result.update(load_evidence(run_dir))
    return result


def export_html(data, path):
    payload = json.dumps(data, allow_nan=False).replace('<', '\\u003c')
    template = (ASSETS/'index.html').read_text()
    template = template.replace('<link rel="stylesheet" href="/console.css">',
                                '<style>' + (ASSETS/'console.css').read_text() + '</style>')
    template = template.replace('<script src="/console.js" defer></script>',
                                '<script id="saved-report" type="application/json">' + payload + '</script>'
                                '<script defer>' + (ASSETS/'console.js').read_text() + '</script>')
    atomic(path, template)


def finish(run_dir, failed=False):
    run_dir = Path(run_dir)
    manifest = json.loads((run_dir/'manifest.json').read_text())
    if manifest['status'] != 'running': raise ValueError('Run is already finalized')
    manifest.update(ended_at=now(), status='failed' if failed else 'succeeded')
    manifest['source_changed_during_run'] = manifest['source_sha256'] != source_hashes()
    failure = None
    with capture_lock(run_dir):
        if not failed:
            try: load_evidence(run_dir)
            except (ValueError, KeyError, OSError) as error:
                manifest.update(status='failed', error=str(error)); failure = error
        manifest['artifact_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                       for p in sorted(run_dir.iterdir()) if p.is_file() and p.name != 'manifest.json' and not p.name.startswith('.')}
        write_json(run_dir/'manifest.json', manifest)
    data = run_data(run_dir)
    write_json(run_dir/'report.json', data)
    export_html(data, run_dir/'report.html')
    export_html(data, ROOT/'build/reports/benchmarks/latest.html')
    print(f'Benchmark {manifest["status"]}: {run_dir / "report.html"}')
    print('Live console: http://127.0.0.1:8090 (python3 tools/benchmark_console.py serve)')
    if failure: raise ValueError(f'Result capture failed: {failure}')


def parse_prometheus(text):
    rows = []
    for line in text.splitlines():
        match = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+([^\s]+)', line)
        if not match: continue
        try: value = float(match[3])
        except ValueError: continue
        if not (-float('inf') < value < float('inf')): continue
        labels = dict(re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', match[2] or ''))
        rows.append((match[1], labels, value))
    def total(name, **labels):
        values = [v for n, tags, v in rows if n == name and all(tags.get(k) == v2 for k, v2 in labels.items())]
        return sum(values) if values else None
    return dict(requests=total('http_server_requests_seconds_count', uri='/openrtb/2.6/bid'),
                duration_seconds=total('http_server_requests_seconds_sum', uri='/openrtb/2.6/bid'),
                shed=total('rtb_admission_shed_total'), cpu=total('process_cpu_usage'),
                heap_bytes=total('jvm_memory_used_bytes', area='heap'), threads=total('jvm_threads_live_threads'),
                uptime=total('process_uptime_seconds'), limit=total('rtb_admission_limit'),
                gc_seconds=total('jvm_gc_pause_seconds_sum'), pinned=total('rtb_vthread_pinned_duration_seconds_count'))


def rates(previous, current):
    result = dict(rps=None, mean_ms=None, shed_per_second=None, gc_ms_per_second=None)
    if not previous or not previous.get('bidder') or not current.get('bidder'): return result
    old, new = previous['bidder'], current['bidder']
    if new.get('uptime') is not None and old.get('uptime') is not None and new['uptime'] < old['uptime']: return result
    dt = current['epoch'] - previous['epoch']
    if dt <= 0: return result
    def delta(key):
        if old.get(key) is None or new.get(key) is None or new[key] < old[key]: return None
        return new[key] - old[key]
    count, duration = delta('requests'), delta('duration_seconds')
    if count is not None: result['rps'] = count/dt
    if count and duration is not None: result['mean_ms'] = duration/count*1000
    for key, output, factor in [('shed', 'shed_per_second', 1), ('gc_seconds', 'gc_ms_per_second', 1000)]:
        d = delta(key)
        if d is not None: result[output] = d/dt*factor
    return result


class Capture:
    def __init__(self, bidder, interval, runs):
        self.bidder, self.interval, self.runs = bidder, interval, runs
        self.history, self.lock, self.stop = deque(maxlen=180), threading.Lock(), threading.Event()
        self.directory = ROOT/'benchmarks/captures'/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory/'live-metrics.jsonl'

    def sample(self):
        sample = dict(at=now(), epoch=time.time(), bidder=None, docker=[], errors={})
        try:
            with urlopen(self.bidder + '/actuator/prometheus', timeout=2) as response:
                text = response.read().decode()
            sample['bidder'] = parse_prometheus(text)
            atomic(self.directory/'prometheus-latest.txt', text)
        except Exception as error: sample['errors']['bidder'] = str(error)
        try:
            ids = subprocess.run(['docker', 'ps', '-q', '--filter', 'label=com.docker.compose.project=rtb-dsp'],
                                 check=True, capture_output=True, text=True, timeout=3).stdout.split()
            if ids:
                output = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *ids],
                                        check=True, capture_output=True, text=True, timeout=4).stdout
                sample['docker'] = [json.loads(line) for line in output.splitlines() if line.strip()]
        except Exception as error: sample['errors']['docker'] = str(error)
        with self.lock:
            sample.update(rates(self.history[-1] if self.history else None, sample))
            self.history.append(sample)
        line = json.dumps(sample, allow_nan=False) + '\n'
        with self.path.open('a') as f: f.write(line)
        for path in self.runs.glob('*/manifest.json'):
            try:
                with capture_lock(path.parent):
                    if json.loads(path.read_text())['status'] == 'running':
                        with (path.parent/'live-metrics.jsonl').open('a') as f: f.write(line)
            except (OSError, ValueError, KeyError): pass

    def loop(self):
        while not self.stop.is_set():
            started = time.monotonic()
            try: self.sample()
            except OSError as error: print('Capture write failed:', error, flush=True)
            self.stop.wait(max(.1, self.interval - (time.monotonic()-started)))

    def snapshot(self):
        with self.lock: return dict(samples=list(self.history), capture=str(self.path), interval=self.interval)


def serve(args):
    runs = Path(args.runs).resolve(); runs.mkdir(parents=True, exist_ok=True)
    capture = Capture(args.bidder.rstrip('/'), args.interval, runs)
    thread = threading.Thread(target=capture.loop, daemon=True)

    def directory(run_id):
        if run_id == 'archived': return ROOT/'benchmarks/results'
        if not re.fullmatch(r'[A-Za-z0-9_-]+', run_id): raise ValueError('Invalid run ID')
        path = runs/run_id
        if not path.is_dir() or path.is_symlink(): raise ValueError('Run not found')
        return path

    class Handler(BaseHTTPRequestHandler):
        def reply(self, value, content_type='application/json', status=200):
            body = value if isinstance(value, bytes) else (json.dumps(value, allow_nan=False).encode()
                    if content_type == 'application/json' else value.encode())
            self.send_response(status); self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body))); self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff'); self.end_headers(); self.wfile.write(body)

        def do_GET(self):
            try:
                path = unquote(urlsplit(self.path).path)
                if path == '/api/live': return self.reply(capture.snapshot())
                if path == '/api/runs':
                    records = []
                    for p in runs.glob('*/manifest.json'):
                        if p.parent.is_symlink(): continue
                        m = json.loads(p.read_text())
                        records.append({k: m.get(k) for k in ['id', 'status', 'kind', 'started_at', 'ended_at', 'command', 'error']})
                    records.sort(key=lambda m: m['started_at'] or '', reverse=True)
                    records.append(dict(id='archived', kind='Saved benchmark evidence', status='archived', started_at=None))
                    return self.reply(records)
                if path.startswith('/api/run/'):
                    run_id = path.removeprefix('/api/run/')
                    return self.reply(run_data(directory(run_id)))
                if path.startswith('/raw/'):
                    run_id, filename = path.removeprefix('/raw/').split('/', 1)
                    if Path(filename).name != filename: raise ValueError('Invalid artifact')
                    artifact = directory(run_id)/filename
                    if artifact.is_symlink() or artifact.suffix not in {'.json', '.jsonl', '.csv', '.txt', '.log', '.md'}:
                        raise ValueError('Invalid artifact')
                    return self.reply(artifact.read_bytes(), 'text/plain; charset=utf-8')
                routes = {'/': ('index.html', 'text/html'), '/console.js': ('console.js', 'text/javascript'),
                          '/console.css': ('console.css', 'text/css')}
                if path in routes:
                    name, mime = routes[path]; return self.reply((ASSETS/name).read_bytes(), mime)
                self.reply({'error': 'Not found'}, status=404)
            except (ValueError, KeyError, OSError) as error: self.reply({'error': str(error)}, status=400)
            except BrokenPipeError: pass

        def log_message(self, *args): pass

    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    thread.start()
    print(f'Live console: http://127.0.0.1:{args.port}', flush=True)
    print(f'Capturing bidder + Docker metrics: {capture.path}', flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: capture.stop.set(); server.server_close(); thread.join(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='action', required=True)
    start = subs.add_parser('begin'); start.add_argument('--run-dir', required=True)
    start.add_argument('--kind', required=True); start.add_argument('--command', required=True)
    end = subs.add_parser('finish'); end.add_argument('--run-dir', required=True); end.add_argument('--failed', action='store_true')
    export = subs.add_parser('export'); export.add_argument('--input', required=True); export.add_argument('--output', required=True)
    server = subs.add_parser('serve'); server.add_argument('--port', type=int, default=8090)
    server.add_argument('--bidder', default='http://127.0.0.1:8080'); server.add_argument('--runs', default=str(RUNS))
    server.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    if args.action == 'begin': begin(args.run_dir, args.kind, args.command)
    elif args.action == 'finish': finish(args.run_dir, args.failed)
    elif args.action == 'export': export_html(run_data(Path(args.input)), args.output)
    else:
        if args.interval < 2: parser.error('Capture interval must be at least 2 seconds')
        serve(args)


if __name__ == '__main__':
    try: main()
    except (ValueError, OSError, KeyError) as error: raise SystemExit(str(error))
