#!/usr/bin/env python3
"""Rebuild the saved benchmark report with embedded fonts, tables and CSS charts."""
import argparse
import html
import os
from pathlib import Path

from benchmark_report import BENCHMARK_CSS, render_benchmark, smoke_results

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / 'tools/report'
DEFAULT_OUTPUT = ROOT / 'benchmarks/results/benchmark-results.html'


def build(output):
    output = Path(output).resolve()
    project_href = html.escape(Path(os.path.relpath(ROOT, output.parent)).as_posix(), quote=True)
    body = (ASSETS / 'masthead.html').read_text()
    body += smoke_results(heading_level=2, project_href=project_href)
    body += ''.join(render_benchmark(key, heading_level=2, project_href=project_href)
                    for key in ('01', '02', '03'))
    links = [('README.md', 'Project', 'Run the bidder'),
             ('benchmarks/README.md', 'Evidence', 'Measurements and limits'),
             ('src/test/java/com/rtb/dsp/benchmarks', 'Source', 'Read the benchmarks'),
             ('tools/build_benchmark_report.py', 'Report', 'Regenerate this page')]
    body += '<nav class="series" aria-label="Project resources">' + ''.join(
        f'<a href="{project_href}/{path}"><span>{label}</span><strong>{title}</strong></a>'
        for path, label, title in links) + '</nav>'
    body += ('<footer class="end"><span>Java at Scale · Benchmark results</span>'
             f'<a href="{project_href}/benchmarks/README.md">Evidence and capacity-test plan</a></footer>')
    doc = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
           '<meta name="viewport" content="width=device-width,initial-scale=1">'
           '<title>Benchmark results — Java at Scale</title>'
           '<meta name="description" content="Captured Java benchmark results, native charts, run commands and raw evidence.">'
           f'<style>{(ASSETS / "fonts.css").read_text()}</style>'
           f'<style>{(ASSETS / "report.css").read_text()}{BENCHMARK_CSS}</style>'
           f'</head><body class="results-edition">{body}</body></html>')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(doc)
    print(f'Generated {output}: 4 result panels, {len(doc.encode()):,} bytes')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    build(parser.parse_args().output)
