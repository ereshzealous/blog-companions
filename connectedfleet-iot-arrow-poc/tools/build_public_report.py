"""Build the public reading page from saved reports. Standard library only; no POC imports."""
from pathlib import Path
import hashlib
import html
import json
import re

ROOT=Path(__file__).resolve().parents[1]
RESULTS=ROOT/'benchmarks/results'
LAB01='lab01-20261010T162903Z-nogit'
LAB02='lab02-20261010T164622Z-nogit'
CITED='lab03-confirm-20261011T031053Z-434b2a4'
esc=html.escape

def disclosure(relative,title):
    p=RESULTS/relative
    text=p.read_text()
    digest=hashlib.sha256(p.read_bytes()).hexdigest()
    return f'<details class="b-details"><summary>{title}</summary><p class="b-source">Saved file: <a href="{relative}">{relative}</a> · SHA-256 <code>{digest}</code></p><pre class="b-code"><code>{esc(text)}</code></pre></details>'

def build():
    saved=(RESULTS/'benchmark-results.html').read_text()
    styles=''.join(re.findall(r'<style>[\s\S]*?</style>',saved))
    sections=dict(re.findall(r'(<section class="benchmark-panel" id="([^"]+)">[\s\S]*?</section>)',saved))
    sections={key:value for value,key in sections.items()}
    if len(sections)!=9:raise ValueError('Expected nine saved report sections')
    checks=json.loads((RESULTS/f'lab-02/runs/{LAB02}/run.json').read_text())['checks']
    def cell(value):return esc(value if isinstance(value,str) else json.dumps(value,ensure_ascii=False))
    columns=['Check','Lane','Result','Expected','Observed']
    rows=[]
    for c in checks:
        values=[esc(c['name']),esc(c['lane'] or 'all'),'PASS' if c['ok'] else 'FAIL',cell(c['expected']),cell(c['observed'])]
        rows.append('<tr>'+''.join(f'<td data-label="{label}">{value}</td>' for label,value in zip(columns,values))+'</tr>')
    table='<div class="b-table-wrap"><table class="b-table"><caption>All 48 recorded checks · complete expected and observed values</caption><thead><tr>'+''.join(f'<th scope="col">{c}</th>' for c in columns)+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>'
    sections['lab02']=re.sub(r'<details class="b-details"><summary>All 48 checks</summary>[\s\S]*?</details>','<details class="b-details"><summary>All 48 checks · expected and observed values</summary>'+table+'</details>',sections['lab02'])
    sections['lab02']=sections['lab02'].replace('all three lanes stored identical data; Arrow changed nothing about correctness.','all three lanes agreed on the tested logical content. The lane-B crash left 120 logical events and 170 physical rows; aggregates still need deduplicated input.')
    sections['verdict']=sections['verdict'].replace('no code to own','no custom ingestion worker to maintain').replace('its own buffer made it 12–16× slower than tuned','the combined connector and fetch settings made it 12–16× slower than tuned').replace('<strong>Bottom line:</strong>','<strong>Read the result:</strong>')
    additions={
        'lab01':disclosure(f'lab-01/runs/{LAB01}/summary.txt','Original Lab 01 output · full summary'),
        'lab02':disclosure(f'lab-02/runs/{LAB02}/run.json','Original Lab 02 output · full run record'),
        'e1':disclosure(f'runs/{CITED}/validation.json','Original validator output · full validation record')+disclosure(f'runs/{CITED}/run.json','Original Lab 03 measurements · full run record'),
    }
    for key,extra in additions.items():sections[key]=sections[key].replace('</section>',extra+'</section>')
    for key in ['lab01','lab02']:
        sections[key]=sections[key].replace('</section>','<p class="b-note">This run predates the first commit; no code SHA is assigned. Its run ID identifies the saved evidence.</p></section>')
    labels=[('verdict','Verdict'),('lab01','Lab 01 · worker costs'),('lab02','Lab 02 · correctness'),('e1','E1 · ingestion'),('latency','Latency'),('e2','E2 · compute × format'),('e3','E3 · CSV'),('e4','E4 · failures'),('runs','Run history')]
    nav=''.join(f'<a href="#{key}">{label}</a>' for key,label in labels)
    body=f'''<header class="masthead"><div class="eyebrow">ConnectedFleet · saved benchmark evidence</div><h1>All benchmark results</h1><p class="results-intro">The recorded measurements, correctness checks and original output behind the Apache Arrow investigation.</p><nav class="benchmark-links" aria-label="Report sections">{nav}</nav><p class="b-takeaway b-warning"><strong>One laptop, one node of each service.</strong> These results do not establish production capacity or high availability.</p><p class="b-note">All Lab 03 conclusions use <code>{CITED}</code>, benchmarked commit <code>434b2a4</code>. <strong>238/238 means validator-accepted cases, including deliberate failures.</strong> Earlier runs are process history. Worker process CPU and connector JVM-container CPU are different scopes.</p></header>'''
    body+=''.join(sections[key] for key,_ in labels)
    body+='<section class="benchmark-panel" id="curated-report"><div class="b-eyebrow">Saved technical narrative</div><h2>Curated report and provenance</h2>'+disclosure('REPORT.md','Read the complete curated report')+'<p class="b-note">Publication copies replace local workstation path prefixes with portable labels. Measured values are unchanged; <a href="../publication-provenance.json">the provenance record</a> lists original and publication hashes.</p></section>'
    body+='<footer class="end"><span>ConnectedFleet · Apache Arrow</span><a href="../report_assets/licenses/Nunito-OFL.txt">Font licenses</a></footer>'
    css='<style>.b-table th{white-space:normal}.b-table td{overflow-wrap:anywhere}.b-code{max-width:100%}.b-source a{overflow-wrap:anywhere}</style>'
    output='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ConnectedFleet · All benchmark results</title>'+styles+css+'</head><body class="results-edition">'+body+'</body></html>\n'
    output=output.replace('Run these experiments · commands and output files','Recorded workflow · commands and output files (reference only)')
    (RESULTS/'index.html').write_text(output)
    print('Built benchmarks/results/index.html from saved files only.')

if __name__=='__main__':build()
