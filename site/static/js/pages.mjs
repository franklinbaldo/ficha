import { archiveView, archivalFileEntries, catalogueTotals, competenceLabel,
  countKinds, fetchManifest, formatBytes, rowCountSeries, safeExternalURL,
  snapshotsDescending } from './catalog.mjs';
import { canonicalCnpj, displayValue, RECORD_FIELDS } from './record.mjs';
import { fetchCompanyByCnpj } from './atomic-browser.mjs';
import { CnpjInputError } from './atomic-record.mjs';
import {BASE,byId,create,add,anchor,option,status,setParams,params,watchSelection,populateDateSelect} from './page-helpers.mjs';
import {mountHistory} from './history.mjs';
export {mountHistory,renderHistoryComparison,fetchHistoryRecordAt} from './history.mjs';

function showProvenance(doc, parent, date, url) {
  const strip = add(doc,parent,'div','','provenance-strip');
  add(doc,strip,'span','Competência ' + date);
  add(doc,strip,'span','Origem declarada: Receita Federal · competência no catálogo');
  anchor(doc,strip,'Arquivo indicado no catálogo',url,true);
  add(doc,parent,'p','A consulta se refere à competência indicada, não necessariamente à situação atual. O acesso ao arquivo não certifica seu conteúdo integral.','fineprint');
}
function renderFieldList(doc, parent, record) {
  const list = add(doc,parent,'dl','','record-grid');
  for (const [field, label] of RECORD_FIELDS) {
    const cell = add(doc,list,'div','','record-field');
    add(doc,cell,'dt',label);
    add(doc,cell,'dd',displayValue(record[field]));
  }
}
/** An unrecognised or contradictory reader state is not a negative CNPJ search. */
function requireCompanyObservation(result) {
  if (!result || !['found', 'absent-in-snapshot', 'artifact-unavailable'].includes(result.state)) {
    throw new Error('Estado do registro Protobuf desconhecido; consulta inconclusiva');
  }
  if (result.state === 'found' &&
      (!result.data || typeof result.data !== 'object' || Array.isArray(result.data))) {
    throw new Error('Registro marcado como encontrado sem dados; consulta inconclusiva');
  }
  if (result.state !== 'found' && result.data != null) {
    throw new Error('Estado do registro contradiz os dados retornados; consulta inconclusiva');
  }
  if (result.state === 'found' && result.data.cnpj !== result.cnpj) {
    throw new Error('CNPJ do registro divergente da consulta; consulta inconclusiva');
  }
  return result;
}

export function renderCompanyRecord(doc, parent, result) {
  requireCompanyObservation(result);
  parent.replaceChildren();
  if (result.state === 'artifact-unavailable') {
    add(doc,parent,'h2','Arquivo indisponível nesta competência');
    add(doc,parent,'p','O arquivo do registro não respondeu (HTTP 404). Consulta inconclusiva: isso não comprova ausência da empresa.');
    showProvenance(doc,parent,result.date,result.archiveUrl || result.source);
    return;
  }
  if (!result.data) {
    add(doc,parent,'h2',result.state === 'absent-in-snapshot'
      ? 'CNPJ não localizado no registro Protobuf desta competência'
      : 'Nenhum registro encontrado nesta competência');
    add(doc,parent,'p','A ausência nesta consulta não comprova encerramento, inexistência histórica ou mudança cadastral.');
    showProvenance(doc,parent,result.date,result.archiveUrl || result.source);
    return;
  }
  add(doc,parent,'p','FICHA CADASTRAL','eyebrow');
  add(doc,parent,'h2',displayValue(result.data.razao_social),'record-heading');
  const header = add(doc,parent,'div','','record-actions');
  add(doc,header,'span','CNPJ ' + result.cnpj,'mono');
  anchor(doc,header,'Comparar competências',BASE+'historia/?cnpj='+encodeURIComponent(result.cnpj));
  renderFieldList(doc,parent,result.data);
  if (result.partial) {
    add(doc,parent,'p','Porte empresarial: Não determinado. O código Protobuf legado não permite afirmar o porte RFB. Simples/MEI não informados também permanecem indeterminados.','fineprint');
  }
  showProvenance(doc,parent,result.date,result.archiveUrl || result.source);
}

export async function mountCompany(doc, {loadManifest = fetchManifest, lookup = fetchCompanyByCnpj} = {}) {
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
  const selection = watchSelection(
    [[input, 'input'], [select, 'change']], state, output,
    'CNPJ ou competência alterados durante a consulta. Aguarde a conclusão para pesquisar novamente.',
    ['cnpj', 'competencia']);
  async function lookupSubmit(event) {
    event?.preventDefault();
    if (busy) return;
    const cnpj = canonicalCnpj(input.value);
    if (!cnpj) { setParams({cnpj:null,competencia:null}); status(state,'Informe um CNPJ completo, numérico ou alfanumérico.',true); output.replaceChildren(); selection.markTerminal(); return; }
    const date = select.value;
    const version = selection.begin();
    const stillCurrent = () => selection.current(version) &&
      canonicalCnpj(input.value) === cnpj && select.value === date;
    const discarded = 'CNPJ ou competência alterados durante a consulta. Pesquise novamente.';
    busy = true;
    // Preserve keyboard focus while the archival request is pending.
    // The busy guard still prevents duplicate lookups.
    submit.setAttribute('aria-disabled', 'true');
    output.replaceChildren();
    status(state,'Consultando o registro na competência '+date+'…');
    try {
      const result = await lookup(manifest,date,cnpj);
      // A delayed archival response must not be presented as the new selection.
      if (!stillCurrent()) { status(state,discarded); return; }
      if (!result || result.cnpj !== cnpj || result.date !== date) {
        throw new Error('Identidade ou competência divergente na resposta; consulta inconclusiva');
      }
      setParams({cnpj,competencia:date});
      renderCompanyRecord(doc,output,result);
      if (result.state === 'artifact-unavailable') {
        status(state,'Consulta inconclusiva: arquivo indisponível nesta competência (HTTP 404).',true);
      } else if (result.state === 'absent-in-snapshot') {
        status(state,'Consulta concluída: CNPJ não localizado no registro desta competência.');
      } else {
        status(state,'Consulta concluída para '+date+'.');
      }
    } catch (error) {
      if (!stillCurrent()) { status(state,discarded); return; }
      if (error instanceof CnpjInputError) {
        // Validation rejected this input before decoder or GET: do not
        // announce an archival outage or share a URL as a completed query.
        setParams({cnpj:null,competencia:null});
        status(state,error.message,true);
        add(doc,output,'p','Corrija o CNPJ antes de pesquisar novamente.');
      } else {
        setParams({cnpj,competencia:date});
        status(state,'Consulta indisponível: '+error.message,true);
        add(doc,output,'p','Não foi possível concluir a leitura remota. Isso não demonstra ausência de CNPJ no acervo.');
        anchor(doc,output,'Consultar o catálogo de arquivos',BASE+'acervo/?competencia='+date);
      }
    } finally { busy = false; selection.finish(); submit.removeAttribute('aria-disabled'); }
  }
  form.addEventListener('submit',lookupSubmit);
  const initial = params().get('cnpj');
  if (initial) { input.value = initial; await lookupSubmit(); }
}

export function renderArchiveSelection(doc, parent, manifest, date, filter='') {
  const {selected,entries,totals}=archiveView(manifest,date);
  const term=filter.trim().toLocaleLowerCase('pt-BR');
  const shown=entries.filter(file=>file.key.toLocaleLowerCase('pt-BR').includes(term));
  parent.replaceChildren();
  add(doc,parent,'h2',competenceLabel(selected.date)+' · '+selected.date,'section-title');
  const summary=add(doc,parent,'div','','stat-strip');
  add(doc,summary,'div',String(totals.fileCount)+' arquivos declarados');
  add(doc,summary,'div',formatBytes(totals.bytes)+' em arquivos com tamanho declarado');
  if(selected.date===manifest.current) add(doc,parent,'p','Esta é a competência de referência declarada no manifesto.','fineprint');
  add(doc,parent,'p','Os links abaixo são declarados pelo manifesto. A existência de uma URL não comprova acessibilidade nem valida o conteúdo remoto.','fineprint');
  const table=add(doc,parent,'div','','archive-list');
  for (const file of shown) {
    const detail=add(doc,table,'details','','archive-file');
    const head=add(doc,detail,'summary');
    const row=add(doc,head,'span','','archive-summary-row');
    add(doc,row,'strong',file.key);
    add(doc,row,'span',formatBytes(file.size),'archive-summary-size');
    const body=add(doc,detail,'div','','archive-detail');
    if(file.sha256) add(doc,body,'code','SHA-256 declarado: '+file.sha256);
    else if(file.sha1) add(doc,body,'code','SHA-1 declarado: '+file.sha1);
    else add(doc,body,'p','Digest não declarado.');
    if(file.url) anchor(doc,body,'Abrir arquivo indicado no catálogo: '+file.key,file.url,true);
    else add(doc,body,'p','URL HTTPS não declarada.');
  }
  if(!entries.length) add(doc,parent,'p','Nenhum arquivo listado nesta competência.');
  else if(!shown.length) add(doc,parent,'p','Nenhum arquivo corresponde ao filtro nesta competência.');
  return {shown:shown.length,total:entries.length};
}

export async function mountArchive(doc,{loadManifest=fetchManifest}={}) {
  const select=byId(doc,'acervo-competencia'), output=byId(doc,'acervo-resultado');
  const state=byId(doc,'acervo-estado'), list=byId(doc,'acervo-competencias');
  const filter=byId(doc,'acervo-filtro'), count=byId(doc,'acervo-contagem');
  const clear=byId(doc,'acervo-limpar');
  if(![select,output,state,list].every(Boolean)) return;
  // All archive controls stay inert until the catalogue has loaded successfully.
  select.disabled=true;
  if(filter) filter.disabled=true;
  if(clear) clear.disabled=true;
  try{
    const manifest=await loadManifest();
    const snaps=snapshotsDescending(manifest);
    populateDateSelect(doc,select,manifest,params().get('competencia') || manifest.current);
    // Native options show only the date so they stay legible at 200% CSS text
    // in a 320px viewport. The catalogue list separately marks the reference.
    for(const item of select.options) {
      item.textContent=item.value;
    }
    list.replaceChildren();
    for(const snap of snaps){
      const total=catalogueTotals(snap);
      const row=add(doc,list,'li','','archive-snapshot');
      add(doc,row,'strong',snap.date);
      add(doc,row,'span',String(total.fileCount)+' arquivos');
      add(doc,row,'span',formatBytes(total.bytes));
      if(snap.date===manifest.current) add(doc,row,'small','Referência');
    }
    select.disabled=false;
    if(filter) filter.disabled=false;
    const paint=(updateUrl=true)=>{
      const {shown,total}=renderArchiveSelection(doc,output,manifest,select.value,filter?.value||'');
      if(count) status(count,shown+' de '+total+' arquivo'+(total===1?'':'s')+' exibido'+(total===1?'':'s'));
      if(clear) clear.disabled=!(filter?.value||'');
      if(updateUrl) setParams({competencia:select.value});
    };
    select.addEventListener('change',()=>{
      if(filter) filter.value='';
      paint();
    });
    filter?.addEventListener('input',()=>paint(false));
    clear?.addEventListener('click',()=>{
      if(!filter) return;
      filter.value='';
      paint(false);
      filter.focus();
    });
    paint();
    status(state, snaps.length+' competência'+(snaps.length===1?'':'s')+
      ' declarada'+(snaps.length===1?'':'s')+' no manifesto publicado.');
  }catch(error){
    select.disabled=true;
    if(filter) filter.disabled=true;
    if(clear) clear.disabled=true;
    if(count) status(count,'Lista de arquivos indisponível: filtro desativado.',true);
    status(state,'Não foi possível abrir o acervo: '+error.message,true);
  }
}

export function renderExploreAssets(doc,parent,manifest,date,filter='') {
  parent.replaceChildren();
  const snapshot=snapshotsDescending(manifest).find(s=>s.date===date);
  if(!snapshot) {add(doc,parent,'p','Competência não publicada.');return;}
  const term=filter.trim().toLowerCase();
  const allEntries=archivalFileEntries(snapshot);
  const entries=allEntries.filter(e=>e.key.toLowerCase().includes(term));
  // Keep the within-competence scale stable when a filter hides the largest file.
  // Otherwise the same file appears to grow merely because the list was filtered.
  const max=Math.max(1,...allEntries.map(e=>e.size ?? 0));
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
  return entries.length;
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
      const visible=renderExploreAssets(doc,output,manifest,select.value,filter.value);
      setParams({competencia:select.value});
      status(state,'Exibindo '+visible+' de '+totals.fileCount+' arquivos declarados em '+select.value+'. Tamanhos do manifesto, não quantidade de empresas nem indicadores por UF.');
    };
    select.addEventListener('change',paint);
    filter.addEventListener('input',paint);
    paint();
  }catch(error){status(state,'Exploração indisponível: '+error.message,true);}
}

export function mountPages(doc=document) {
  if(byId(doc,'empresa-form')) return mountCompany(doc);
  if(byId(doc,'historia-form')) return mountHistory(doc);
  if(byId(doc,'acervo-competencia')) return mountArchive(doc);
  if(byId(doc,'explorar-competencia')) return mountExplore(doc);
}
if(typeof document!=='undefined') mountPages(document);
