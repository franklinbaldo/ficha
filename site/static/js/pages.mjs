import { archiveView, archivalFileEntries, catalogueTotals, competenceLabel,
  countKinds, fetchManifest, formatBytes, rowCountSeries, safeExternalURL,
  snapshotsDescending } from './catalog.mjs';
import { canonicalCnpj, compareRecords, displayValue, fetchRecordAt, RECORD_FIELDS } from './record.mjs';

const BASE = '/ficha/';
const byId = (doc, id) => doc.getElementById(id);
function create(doc, tag, text = '', className = '') {
  const node = doc.createElement(tag);
  if (text !== '') node.textContent = text;
  if (className) node.className = className;
  return node;
}
function add(doc, parent, tag, text = '', className = '') {
  const child = create(doc, tag, text, className);
  parent.append(child);
  return child;
}
function anchor(doc, parent, label, url, external = false) {
  const safe = external ? safeExternalURL(url) : url;
  if (!safe) return add(doc, parent, 'span', 'Link não declarado');
  const link = add(doc, parent, 'a', label);
  link.href = safe;
  if (external) { link.target = '_blank'; link.rel = 'noopener noreferrer'; }
  return link;
}
function option(doc, select, value, label) {
  const item = create(doc,'option',label);
  item.value = value;
  select.append(item);
}
function status(node, message, error = false) {
  node.textContent = message;
  node.dataset.kind = error ? 'error' : 'info';
}
function setParams(params) {
  const url = new URL(globalThis.location.href);
  for (const [key, val] of Object.entries(params)) {
    if (val === null || val === '') url.searchParams.delete(key);
    else url.searchParams.set(key, val);
  }
  globalThis.history.replaceState(null, '', url);
}
function params() { return new URLSearchParams(globalThis.location.search); }
function showProvenance(doc, parent, date, url) {
  const strip = add(doc,parent,'div','','provenance-strip');
  add(doc,strip,'span','Competência ' + date);
  add(doc,strip,'span','Receita Federal · snapshot arquivado');
  anchor(doc,strip,'Arquivo de origem',url,true);
  add(doc,parent,'p','O registro corresponde à competência indicada, não necessariamente à situação atual. O acesso ao arquivo não certifica seu conteúdo integral.','fineprint');
}
function populateDateSelect(doc, select, manifest, selected) {
  select.replaceChildren();
  for (const snap of snapshotsDescending(manifest)) {
    option(doc, select, snap.date,
      competenceLabel(snap.date) + ' · ' + snap.date + (snap.date === manifest.current ? ' · referência' : ''));
  }
  if ([...select.options].some(o => o.value === selected)) select.value = selected;
}
function renderFieldList(doc, parent, record) {
  const list = add(doc,parent,'dl','','record-grid');
  for (const [field, label] of RECORD_FIELDS) {
    const cell = add(doc,list,'div','','record-field');
    add(doc,cell,'dt',label);
    add(doc,cell,'dd',displayValue(record[field]));
  }
}
export function renderCompanyRecord(doc, parent, result) {
  parent.replaceChildren();
  if (!result.data) {
    add(doc,parent,'h2','Nenhum registro encontrado nesta competência');
    add(doc,parent,'p','A ausência na consulta não comprova encerramento, inexistência histórica ou mudança cadastral.');
    showProvenance(doc,parent,result.date,result.source);
    return;
  }
  add(doc,parent,'p','FICHA CADASTRAL','eyebrow');
  add(doc,parent,'h2',displayValue(result.data.razao_social),'record-heading');
  const header = add(doc,parent,'div','','record-actions');
  add(doc,header,'span','CNPJ ' + result.cnpj,'mono');
  anchor(doc,header,'Comparar competências',BASE+'historia/?cnpj='+encodeURIComponent(result.cnpj));
  renderFieldList(doc,parent,result.data);
  showProvenance(doc,parent,result.date,result.source);
}

export async function mountCompany(doc, {loadManifest = fetchManifest, lookup = fetchRecordAt} = {}) {
  const input = byId(doc,'empresa-cnpj'), select = byId(doc,'empresa-competencia');
  const form = byId(doc,'empresa-form'), state = byId(doc,'empresa-estado');
  const output = byId(doc,'empresa-resultado'), submit = byId(doc,'empresa-enviar');
  if (![input,select,form,state,output,submit].every(Boolean)) return;
  let manifest;
  try {
    manifest = await loadManifest();
    populateDateSelect(doc,select,manifest,params().get('competencia') || manifest.current);
    status(state,'Selecione uma competência e informe um CNPJ completo.');
  } catch (error) {
    status(state,'Não foi possível carregar o catálogo: '+error.message,true);
    submit.disabled = true;
    return;
  }
  let busy = false;
  async function lookupSubmit(event) {
    event?.preventDefault();
    if (busy) return;
    const cnpj = canonicalCnpj(input.value);
    if (!cnpj) { status(state,'Informe um CNPJ completo, numérico ou alfanumérico.',true); output.replaceChildren(); return; }
    const date = select.value;
    const stillCurrent = () => canonicalCnpj(input.value) === cnpj && select.value === date;
    const discarded = 'CNPJ ou competência alterados durante a consulta. Pesquise novamente.';
    busy = true;
    submit.disabled = true;
    output.replaceChildren();
    status(state,'Consultando o registro na competência '+date+'…');
    try {
      const result = await lookup(manifest,date,cnpj);
      // A delayed archival response must not be presented as the new selection.
      if (!stillCurrent()) { status(state,discarded); return; }
      setParams({cnpj,competencia:date});
      renderCompanyRecord(doc,output,result);
      status(state,'Consulta concluída para '+date+'.');
    } catch (error) {
      if (!stillCurrent()) { status(state,discarded); return; }
      setParams({cnpj,competencia:date});
      status(state,'Consulta indisponível: '+error.message,true);
      add(doc,output,'p','Os arquivos continuam referenciados no catálogo. A leitura remota depende de um transporte compatível com CORS e HTTP Range.');
      anchor(doc,output,'Consultar o catálogo de arquivos',BASE+'acervo/?competencia='+date);
    } finally { busy = false; submit.disabled = false; }
  }
  form.addEventListener('submit',lookupSubmit);
  const initial = params().get('cnpj');
  if (initial) { input.value = initial; await lookupSubmit(); }
}

export function renderHistoryComparison(doc, parent, older, newer) {
  parent.replaceChildren();
  const title = add(doc,parent,'h2','Comparação de registros observados');
  title.className = 'section-title';
  const provenance = add(doc,parent,'p','Competências '+older.date+' e '+newer.date+'. As diferenças foram observadas entre snapshots; não representam datas exatas de alteração.');
  provenance.className='fineprint';
  if (!older.data || !newer.data) {
    add(doc,parent,'p','Um ou ambos os snapshots não retornaram registro para este CNPJ. Não é possível concluir que a empresa nasceu, encerrou atividades ou mudou nesta data.');
  } else {
    const table = add(doc,parent,'table','','comparison-table');
    const thead = add(doc,table,'thead'); const headers=add(doc,thead,'tr');
    for(const text of ['Campo',older.date,newer.date]) add(doc,headers,'th',text);
    const tbody = add(doc,table,'tbody');
    for(const row of compareRecords(older.data,newer.data)) {
      const tr = add(doc,tbody,'tr');
      if(row.changed) tr.className='changed';
      add(doc,tr,'th',row.label);
      add(doc,tr,'td',row.before);
      add(doc,tr,'td',row.after);
    }
  }
  const sources = add(doc,parent,'div','','source-links');
  anchor(doc,sources,'Fonte '+older.date,older.source,true);
  anchor(doc,sources,'Fonte '+newer.date,newer.source,true);
}

export async function mountHistory(doc,{loadManifest=fetchManifest,lookup=fetchRecordAt}={}) {
  const list=byId(doc,'historia-competencias'), from=byId(doc,'historia-inicio');
  const to=byId(doc,'historia-fim'), form=byId(doc,'historia-form');
  const input=byId(doc,'historia-cnpj'), button=byId(doc,'historia-enviar');
  const state=byId(doc,'historia-estado'), output=byId(doc,'historia-resultado');
  if (![list,from,to,form,input,button,state,output].every(Boolean)) return;
  let manifest;
  try { manifest=await loadManifest(); }
  catch(error){ status(state,'Catálogo indisponível: '+error.message,true); button.disabled=true; return; }
  const snaps=snapshotsDescending(manifest);
  list.replaceChildren();
  for (const snap of snaps) {
    const li=add(doc,list,'li','','timeline-item');
    add(doc,li,'span',snap.date,'mono');
    add(doc,li,'strong',competenceLabel(snap.date));
    if(snap.date===manifest.current) add(doc,li,'small','Referência do manifesto');
  }
  populateDateSelect(doc,from,manifest,params().get('de') || snaps.at(-1)?.date);
  populateDateSelect(doc,to,manifest,params().get('ate') || manifest.current);
  const initial=params().get('cnpj');
  if(initial) input.value=initial;
  if(snaps.length<2){
    status(state,'O manifesto contém somente uma competência. A comparação exige pelo menos duas observações reais.');
    button.disabled=true;
    return;
  }
  status(state,'Escolha duas competências existentes. A comparação só será executada após solicitar.');
  let busy = false;
  form.addEventListener('submit',async event=>{
    event.preventDefault();
    if (busy) return;
    const cnpj=canonicalCnpj(input.value);
    output.replaceChildren();
    if(!cnpj){status(state,'Informe um CNPJ completo.',true);return;}
    const start=from.value, end=to.value;
    if(start===end){status(state,'Selecione duas competências diferentes.',true);return;}
    if(start>end){status(state,'A competência inicial deve ser anterior à competência final.',true);return;}
    const stillCurrent = () => canonicalCnpj(input.value) === cnpj &&
      from.value === start && to.value === end;
    const discarded = 'CNPJ ou competências alterados durante a comparação. Pesquise novamente.';
    busy = true;
    button.disabled=true;
    status(state,'Consultando os dois snapshots de forma sequencial…');
    try {
      const older=await lookup(manifest,start,cnpj);
      // Avoid a second archival GET if the user changed the requested comparison.
      if (!stillCurrent()) { status(state,discarded); return; }
      const newer=await lookup(manifest,end,cnpj);
      if (!stillCurrent()) { status(state,discarded); return; }
      setParams({cnpj,de:start,ate:end});
      renderHistoryComparison(doc,output,older,newer);
      status(state,'Comparação concluída. Alterações são diferenças entre observações, não datas de eventos.');
    }catch(error){
      if (!stillCurrent()) { status(state,discarded); return; }
      setParams({cnpj,de:start,ate:end});
      status(state,'Comparação indisponível: '+error.message,true);
      anchor(doc,output,'Ver competências disponíveis',BASE+'acervo/');
    }finally{busy=false;button.disabled=false;}
  });
}

export function renderArchiveSelection(doc, parent, manifest, date) {
  const {selected,entries,totals}=archiveView(manifest,date);
  parent.replaceChildren();
  add(doc,parent,'h2',competenceLabel(selected.date)+' · '+selected.date,'section-title');
  const summary=add(doc,parent,'div','','stat-strip');
  add(doc,summary,'div',String(totals.fileCount)+' arquivos declarados');
  add(doc,summary,'div',formatBytes(totals.bytes)+' em arquivos com tamanho declarado');
  if(selected.date===manifest.current) add(doc,parent,'p','Esta é a competência de referência declarada no manifesto.','fineprint');
  add(doc,parent,'p','Os links abaixo são declarados pelo manifesto. A existência de uma URL não comprova acessibilidade nem valida o conteúdo remoto.','fineprint');
  const table=add(doc,parent,'div','','archive-list');
  for (const file of entries) {
    const detail=add(doc,table,'details','','archive-file');
    const head=add(doc,detail,'summary');
    add(doc,head,'strong',file.key);
    add(doc,head,'span',formatBytes(file.size));
    const body=add(doc,detail,'div','','archive-detail');
    if(file.sha256) add(doc,body,'code','SHA-256 declarado: '+file.sha256);
    else if(file.sha1) add(doc,body,'code','SHA-1 declarado: '+file.sha1);
    else add(doc,body,'p','Digest não declarado.');
    if(file.url) anchor(doc,body,'Abrir arquivo no repositório de origem',file.url,true);
    else add(doc,body,'p','URL HTTPS não declarada.');
  }
  if(!entries.length) add(doc,parent,'p','Nenhum arquivo listado nesta competência.');
}

export async function mountArchive(doc,{loadManifest=fetchManifest}={}) {
  const select=byId(doc,'acervo-competencia'), output=byId(doc,'acervo-resultado');
  const state=byId(doc,'acervo-estado'), list=byId(doc,'acervo-competencias');
  if(![select,output,state,list].every(Boolean)) return;
  try{
    const manifest=await loadManifest();
    const snaps=snapshotsDescending(manifest);
    populateDateSelect(doc,select,manifest,params().get('competencia') || manifest.current);
    list.replaceChildren();
    for(const snap of snaps){
      const total=catalogueTotals(snap);
      const row=add(doc,list,'li','','archive-snapshot');
      add(doc,row,'strong',snap.date);
      add(doc,row,'span',String(total.fileCount)+' arquivos');
      add(doc,row,'span',formatBytes(total.bytes));
      if(snap.date===manifest.current) add(doc,row,'small','Referência');
    }
    const paint=()=>{
      renderArchiveSelection(doc,output,manifest,select.value);
      setParams({competencia:select.value});
    };
    select.addEventListener('change',paint);
    paint();
    status(state, snaps.length+' competência(s) declarada(s) no manifesto publicado.');
  }catch(error){status(state,'Não foi possível abrir o acervo: '+error.message,true);}
}

export function renderExploreAssets(doc,parent,manifest,date,filter='') {
  parent.replaceChildren();
  const snapshot=snapshotsDescending(manifest).find(s=>s.date===date);
  if(!snapshot) {add(doc,parent,'p','Competência não publicada.');return;}
  const term=filter.trim().toLowerCase();
  const entries=archivalFileEntries(snapshot).filter(e=>e.key.toLowerCase().includes(term));
  const max=Math.max(1,...entries.map(e=>e.size ?? 0));
  const title=add(doc,parent,'p',entries.length+' arquivos correspondentes · '+date,'fineprint');
  title.className='fineprint';
  for(const file of entries) {
    const row=add(doc,parent,'div','','asset-row');
    const line=add(doc,row,'div','','asset-row-label');
    add(doc,line,'strong',file.key);
    add(doc,line,'span',formatBytes(file.size));
    const track=add(doc,row,'div','','asset-track');
    const bar=add(doc,track,'span','','asset-bar');
    const percentage=file.size===null || file.size===0 ? 0 : Math.max(1,Math.round(100*file.size/max));
    bar.style.width=percentage+'%';
    track.setAttribute('role','img');
    track.setAttribute('aria-label',file.key+': '+formatBytes(file.size));
  }
  if(!entries.length) add(doc,parent,'p','Nenhum conjunto corresponde ao filtro.');
}

export function renderCountSeries(doc,parent,manifest,kind) {
  parent.replaceChildren();
  const rows=rowCountSeries(manifest,kind);
  const max=Math.max(1,...rows.map(row=>row.count ?? 0));
  const fmt=new Intl.NumberFormat('pt-BR');
  for(const row of rows) {
    const item=add(doc,parent,'div','','series-row');
    const header=add(doc,item,'div','','series-label');
    add(doc,header,'span',row.date,'mono');
    add(doc,header,'strong',row.count===null ? 'Não declarado' : fmt.format(row.count)+' linhas');
    const track=add(doc,item,'div','','series-track');
    const bar=add(doc,track,'span','','series-bar');
    bar.style.width=(row.count===null || row.count===0 ? 0 : Math.max(1,Math.round(100*row.count/max)))+'%';
    track.setAttribute('role','img');
    track.setAttribute('aria-label',row.date+': '+(row.count===null ? 'não declarado' : fmt.format(row.count)+' linhas'));
  }
}

export async function mountExplore(doc,{loadManifest=fetchManifest}={}) {
  const select=byId(doc,'explorar-competencia'), filter=byId(doc,'explorar-filtro');
  const state=byId(doc,'explorar-estado'), output=byId(doc,'explorar-grafico');
  const counter=byId(doc,'explorar-contador');
  const kindSelect=byId(doc,'explorar-serie'), series=byId(doc,'explorar-serie-grafico');
  if(![select,filter,state,output,counter,kindSelect,series].every(Boolean)) return;
  try{
    const manifest=await loadManifest();
    const snaps=snapshotsDescending(manifest);
    populateDateSelect(doc,select,manifest,params().get('competencia') || manifest.current);
    const kinds=countKinds(manifest);
    for(const kind of kinds) option(doc,kindSelect,kind,kind.replaceAll('_',' '));
    if(kinds.includes('cnpjs')) kindSelect.value='cnpjs';
    const repaintSeries=()=>renderCountSeries(doc,series,manifest,kindSelect.value);
    kindSelect.addEventListener('change',repaintSeries);
    repaintSeries();
    const paint=()=>{
      const snap=snaps.find(s=>s.date===select.value);
      const totals=catalogueTotals(snap);
      counter.textContent=totals.fileCount+' arquivos · '+formatBytes(totals.bytes)+' declarados';
      renderExploreAssets(doc,output,manifest,select.value,filter.value);
      setParams({competencia:select.value});
    };
    select.addEventListener('change',paint);
    filter.addEventListener('input',paint);
    paint();
    status(state,'Visualização dos tamanhos dos arquivos no catálogo. Não representa quantidade de empresas, nem indicadores por UF.');
  }catch(error){status(state,'Exploração indisponível: '+error.message,true);}
}

export function mountPages(doc=document) {
  if(byId(doc,'empresa-form')) return mountCompany(doc);
  if(byId(doc,'historia-form')) return mountHistory(doc);
  if(byId(doc,'acervo-competencia')) return mountArchive(doc);
  if(byId(doc,'explorar-competencia')) return mountExplore(doc);
}
if(typeof document!=='undefined') mountPages(document);
