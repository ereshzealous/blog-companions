/* Shared renderer: live API data in the console, embedded JSON in a standalone export. */
(() => {
const $ = (q) => document.querySelector(q);
const colors = {neutral:'#64748b',purple:'#7c3aed',orange:'#d97706',green:'#16a34a'};
const el = (tag, cls, text) => { const n=document.createElement(tag); if(cls)n.className=cls; if(text!==undefined)n.textContent=text; return n; };
const fmt = (v, digits=1) => v===null || v===undefined || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('en-US',{minimumFractionDigits:digits,maximumFractionDigits:digits});
const badge = (s) => el('span','badge '+s,s);
const saved = $('#saved-report');
let selected=null, signature='', runListSignature='', followLatest=true;
function table(columns, rows) {
  const wrap=el('div','table-wrap'), t=el('table'); t.setAttribute('role','table');
  const head=el('thead'), hr=el('tr'); columns.forEach(c=>{const th=el('th','',c);th.scope='col';hr.append(th)});head.append(hr);t.append(head);
  const body=el('tbody'); rows.forEach(row=>{const tr=el('tr');row.forEach((v,i)=>{const td=el('td','',v);td.dataset.label=columns[i];tr.append(td)});body.append(tr)});t.append(body);wrap.append(t);return wrap;
}
function drawChart(c) {
  const box=el('div','chart'); box.append(el('h4','',c.title));
  if(c.kind==='line') {
    const points=c.series.flatMap(s=>s.points).filter(p=>p[1]!==null && Number.isFinite(p[1]));
    if(!points.length){box.append(el('p','empty','Waiting for measurements'));return box}
    const allX=c.series.flatMap(s=>s.points.map(p=>p[0]));
    const minX=Math.min(...allX),maxX=Math.max(...allX), maxY=Math.max(...points.map(p=>p[1]),1);
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 600 160');svg.setAttribute('role','img');svg.setAttribute('aria-label',c.title+'; scale 0 to '+fmt(maxY)+' '+c.unit);
    const legend=el('div','legend');
    c.series.forEach(s=>{
      let segment=[]; const flush=()=>{if(!segment.length)return;const p=document.createElementNS(svg.namespaceURI,'polyline');p.setAttribute('points',segment.join(' '));p.setAttribute('fill','none');p.setAttribute('stroke',colors[s.color]||colors.purple);p.setAttribute('stroke-width','2.5');svg.append(p);segment=[]};
      s.points.forEach(([x,y])=>{if(y===null || !Number.isFinite(y)){flush();return} segment.push(`${5+(x-minX)/(maxX-minX||1)*590},${150-y/maxY*140}`)});flush();
      const label=el('span','',s.label);label.style.setProperty('--series-color',colors[s.color]||colors.purple);legend.append(label);
    });
    const axis=el('div','axis');axis.append(el('span','',c.xUnit==='time'?new Date(minX*1000).toLocaleTimeString():fmt(minX,0)+' '+c.xUnit),el('span','',`0–${fmt(maxY)} ${c.unit}`),el('span','',c.xUnit==='time'?new Date(maxX*1000).toLocaleTimeString():fmt(maxX,0)+' '+c.xUnit));
    box.append(svg,axis,legend);return box;
  }
  const max=c.maximum??Math.max(...c.series.map(s=>s.value),0);
  c.series.forEach(s=>{const row=el('div','bar'),label=el('div','bar-label');label.append(el('span','',s.label),el('strong','',fmt(s.value,c.digits)+' '+c.unit));const track=el('div','track'),fill=el('span','fill');track.setAttribute('aria-hidden','true');fill.style.width=(max?s.value/max*100:0)+'%';fill.style.background=colors[s.color]||colors.purple;track.append(fill);row.append(label,track);box.append(row)});
  const axis=el('div','axis');axis.append(el('span','','0'),el('span','',fmt(max,c.digits)+' '+c.unit));box.append(axis);return box;
}
function renderPanel(p){
  const box=el('section','panel');box.dataset.panel=p.id;box.append(el('div','eyebrow','RECORDED BENCHMARK'),el('h2','',p.title));
  const meta=el('div','meta');p.meta.forEach(m=>meta.append(el('span','',m)));box.append(meta);
  if(p.steps.length){const steps=el('ol','steps');p.steps.forEach(([title,detail])=>{const li=el('li');li.append(el('strong','',title),document.createTextNode(detail));steps.append(li)});box.append(steps)}
  const charts=el('div','charts');p.charts.forEach(c=>charts.append(drawChart(c)));box.append(charts);
  if(p.rows.length)box.append(table(p.columns,p.rows));p.notes.forEach(n=>box.append(el('p','callout',n)));return box;
}
function renderRun(data){
  const container=$('#run-content');container.replaceChildren();const m=data.manifest;
  const heading=el('div','run-title'), title=el('div');title.append(el('h2','',m.kind||'Benchmark run'),el('p','',m.id),el('p','',m.started_at?new Date(m.started_at).toLocaleString():'Archived evidence · original capture environment'));heading.append(title,badge(m.status));container.append(heading);
  if(m.command){const d=el('details');d.append(el('summary','','Run command'),el('pre','',m.command));container.append(d)}
  if(m.source_changed_during_run)container.append(el('p','callout','Source files changed during the run. Inspect the manifest before comparing results.'));
  if(m.status==='running')container.append(el('p','empty','Running. The console will load the result when capture completes. Live bidder and Docker measurements are being recorded while this console is open.'));
  if(m.status==='failed')container.append(el('p','empty','Run failed. '+(m.error||'Inspect the captured logs and Gradle output. No previous result is substituted.')));
  const cards=el('div','cards');[['JMH configurations',data.configurations],['Result panels',data.panels.length],['Raw artifacts',data.artifacts.length]].forEach(([label,value])=>{const card=el('div','card');card.append(el('span','',label),el('strong','',String(value)));cards.append(card)});container.append(cards);
  data.panels.forEach(p=>container.append(renderPanel(p)));
  const evidence=el('section','panel');evidence.append(el('h2','','Raw evidence'));
  data.artifacts.forEach(a=>{const d=el('details');d.append(el('summary','',a.name),el('p','note','SHA-256 '+a.sha256+(a.truncated?' · preview truncated; original file retained':'')),el('pre','',a.text));if(!saved){const link=el('a','','Open original artifact');link.href='/raw/'+encodeURIComponent(m.id)+'/'+encodeURIComponent(a.name);link.target='_blank';link.rel='noopener';d.append(link)}evidence.append(d)});
  const manifest=el('details');manifest.append(el('summary','','Run manifest'),el('pre','',JSON.stringify(m,null,2)));evidence.append(manifest);container.append(evidence);
}
function setView(view){$('#live-view').hidden=view!=='live';$('#runs-view').hidden=view!=='runs';document.querySelectorAll('[data-view]').forEach(b=>b.classList.toggle('active',b.dataset.view===view));}
async function api(url){const response=await fetch(url,{cache:'no-store'});const data=await response.json();if(!response.ok)throw new Error(data.error||response.status);return data}
async function refreshRun(force=false){
  const runs=await api('/api/runs');if(!selected||followLatest)selected=runs[0]?.id;
  const listKey=JSON.stringify([runs,selected]);
  if(listKey!==runListSignature){runListSignature=listKey;$('#run-history').replaceChildren();const follow=el('button','',followLatest?'Following newest run':'Follow newest run');follow.onclick=()=>{followLatest=true;runListSignature='';refreshRun(true).catch(showError)};$('#run-history').append(follow);runs.forEach(m=>{const button=el('button',m.id===selected?'active':'');button.dataset.run=m.id;button.append(el('strong','',m.kind),el('small','',m.started_at?new Date(m.started_at).toLocaleString():'Published historical results'),badge(m.status));button.onclick=()=>{followLatest=false;selected=m.id;signature='';refreshRun(true).catch(showError)};$('#run-history').append(button)})}
  const current=runs.find(r=>r.id===selected);const key=JSON.stringify(current);
  if(selected && (force||key!==signature)){renderRun(await api('/api/run/'+encodeURIComponent(selected)));signature=key}
}
function renderLive(data){
  const samples=data.samples,last=samples.at(-1);if(!last)return;
  $('#sample-time').textContent='Captured '+new Date(last.at).toLocaleTimeString()+' · '+data.interval+' s sampling';
  $('#connection').textContent=last.bidder?'Live · bidder connected':'Live console · bidder unavailable';
  const b=last.bidder||{};$('#live-cards').replaceChildren();
  [['Bid endpoint',last.rps,'req/s'],['HTTP interval mean',last.mean_ms,'ms'],['Bidder process CPU',b.cpu==null?null:b.cpu*100,'%'],['JVM heap used',b.heap_bytes==null?null:b.heap_bytes/1048576,'MiB'],['Live JVM threads',b.threads,''],['Admission limit',b.limit,''],['Admission sheds',last.shed_per_second,'/s'],['GC pause time',last.gc_ms_per_second,'ms/s']].forEach(([label,value,unit])=>{const card=el('div','card');const val=el('strong','',fmt(value));val.append(el('small','',' '+unit));card.append(el('span','',label),val);$('#live-cards').append(card)});
  const charts=[['Bid throughput','req/s',s=>s.rps],['Bid response interval mean','ms',s=>s.mean_ms],['Bidder JVM heap','MiB',s=>s.bidder?.heap_bytes==null?null:s.bidder.heap_bytes/1048576],['Bidder process CPU','%',s=>s.bidder?.cpu==null?null:s.bidder.cpu*100]];
  $('#live-charts').replaceChildren(...charts.map(([title,unit,get])=>drawChart({kind:'line',title,unit,xUnit:'time',series:[{label:'Observed',color:'purple',points:samples.map(s=>[s.epoch,get(s)??null])}]})));
  $('#docker').replaceChildren(last.docker.length?table(['Container','CPU','Memory','Memory %','Network I/O','Block I/O','PIDs'],last.docker.map(c=>[c.Name,c.CPUPerc,c.MemUsage,c.MemPerc,c.NetIO,c.BlockIO,c.PIDs])):el('p','empty','No running rtb-dsp Compose containers detected.'));
  $('#capture-path').textContent=data.capture;$('#live-errors').textContent=Object.keys(last.errors).length?JSON.stringify(last.errors,null,2):'Bidder and Docker collection available.';
}
function showError(error){$('#error').hidden=false;$('#error').textContent='Connection or data error: '+error.message;$('#connection').textContent='Disconnected · last sample retained'}
async function poll(){try{const [live]=await Promise.all([api('/api/live'),refreshRun()]);renderLive(live);$('#error').hidden=true}catch(e){showError(e)}finally{setTimeout(poll,3000)}}
function init(){
  document.querySelectorAll('[data-view]').forEach(b=>b.onclick=()=>setView(b.dataset.view));
  if(saved){$('#tabs').hidden=true;$('#run-history').hidden=true;$('.run-layout').style.gridTemplateColumns='1fr';$('#connection').textContent='Captured run · offline export';setView('runs');renderRun(JSON.parse(saved.textContent))}
  else{setView(location.hash==='#runs'?'runs':'live');poll()}
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
