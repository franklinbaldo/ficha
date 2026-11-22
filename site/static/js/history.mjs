/** Protobuf v1 numeric-only history; no implicit Parquet fallback. */
import {fetchManifest,snapshotsDescending,competenceLabel} from './catalog.mjs';
import {canonicalCnpj,compareRecords} from './record.mjs';
import {fetchCompanyByCnpj} from './atomic-browser.mjs';
import {CnpjInputError} from './atomic-record.mjs';
import {BASE,byId,add,anchor,status,setParams,params,watchSelection,populateDateSelect} from './page-helpers.mjs';

export function renderHistoryComparison(doc, parent, older, newer) {
  parent.replaceChildren();
  const title = add(doc,parent,'h2','Comparação de registros observados');
  title.className = 'section-title';
  const provenance = add(doc,parent,'p','Competências '+older.date+' e '+newer.date+'. As diferenças foram observadas entre snapshots; não representam datas exatas de alteração.');
  provenance.className='fineprint';
  if (older.partial || newer.partial) {
    add(doc,parent,'p','A comparação numérica utiliza Protobuf v1 declarado no manifesto. Campos desconhecidos permanecem não informados, e o porte histórico não pode ser determinado por este wire.','fineprint');
  }
  if (older.state === 'absent-in-snapshot' || newer.state === 'absent-in-snapshot') {
    add(doc,parent,'h3','Comparação incompleta: CNPJ não localizado');
    for (const entry of [older,newer]) {
      if (entry.state === 'absent-in-snapshot') {
        add(doc,parent,'p','CNPJ não localizado no registro Protobuf de ' + entry.date + '.');
      }
    }
    add(doc,parent,'p','Uma diferença de presença entre snapshots não comprova nascimento, encerramento ou data de mudança cadastral.');
  } else {
    add(doc,parent,'p','Em telas estreitas, role a tabela horizontalmente para ler as duas competências. Pelo teclado, pressione Tab até a tabela e use as setas.','fineprint comparison-hint');
    const scroll = add(doc,parent,'div','','comparison-scroll');
    scroll.setAttribute('role','region');
    scroll.setAttribute('aria-label','Tabela de comparação das competências '+older.date+' e '+newer.date);
    scroll.setAttribute('tabindex','0');
    const table = add(doc,scroll,'table','','comparison-table');
    add(doc,table,'caption','Campos cadastrais em '+older.date+' e '+newer.date);
    const thead = add(doc,table,'thead'); const headers=add(doc,thead,'tr');
    for(const text of ['Campo',older.date,newer.date]) {
      add(doc,headers,'th',text).setAttribute('scope','col');
    }
    const tbody = add(doc,table,'tbody');
    for(const row of compareRecords(older.data,newer.data)) {
      const tr = add(doc,tbody,'tr');
      if(row.changed) tr.className='changed';
      add(doc,tr,'th',row.label).setAttribute('scope','row');
      add(doc,tr,'td',row.before);
      add(doc,tr,'td',row.after);
    }
  }
  const sources = add(doc,parent,'div','','source-links');
  anchor(doc,sources,'Fonte '+older.date,older.source,true);
  anchor(doc,sources,'Fonte '+newer.date,newer.source,true);
}

/** Numeric full-CNPJ history reads the same bounded, canonical Protobuf v1
 * as the company page. Alphanumeric history is rejected before all network I/O.
 * No implicit fallback to Parquet when a declared PB shard is unavailable.
 */
export async function fetchHistoryRecordAt(manifest, date, cnpj, {
  atomic = fetchCompanyByCnpj,
} = {}) {
  if (!/^\d{14}$/.test(cnpj)) {
    throw new CnpjInputError('Comparação alfanumérica ainda não disponível. Informe um CNPJ numérico completo (14 dígitos).');
  }
  return atomic(manifest, date, cnpj);
}

function requireHistoryObservation(result, date, cnpj) {
  if (!result || result.date !== date || result.cnpj !== cnpj) {
    throw new Error('Identidade da observação divergente; comparação inconclusiva');
  }
  if (result?.state === 'artifact-unavailable') {
    throw new Error('Consulta inconclusiva em ' + result.date +
      ': arquivo Protobuf não respondeu (HTTP 404); ausência não comprovada');
  }
  if (!result || !['found', 'absent-in-snapshot'].includes(result.state)) {
    throw new Error('Estado de consulta ausente ou desconhecido; comparação inconclusiva');
  }
  if (result.state === 'found' && (!result.data || typeof result.data !== 'object')) {
    throw new Error('Registro encontrado sem dados; comparação inconclusiva');
  }
  if (result.state === 'found' && result.data.cnpj !== cnpj) {
    throw new Error('Identidade do CNPJ no registro divergente; comparação inconclusiva');
  }
  if (result.state === 'absent-in-snapshot' && result.data != null) {
    throw new Error('Resposta de ausência contraditória; comparação inconclusiva');
  }
  return result;
}

export async function mountHistory(doc,{loadManifest=fetchManifest,lookup=fetchHistoryRecordAt}={}) {
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
    if (snap.files?.companies?.shard_by !== 'cnpj_base_prefix_2' ||
        (snap.files.companies.schema_version != null &&
         snap.files.companies.schema_version !== '1.0.0') ||
        !Array.isArray(snap.files.companies.shards)) {
      add(doc,li,'small','Sem índice para comparação numérica');
    }
  }
  const protobufMonths = snaps.filter(s => s.files?.companies?.shard_by ===
    'cnpj_base_prefix_2' &&
    (s.files.companies.schema_version == null ||
      s.files.companies.schema_version === '1.0.0') &&
    Array.isArray(s.files.companies.shards));
  // Keep the full archival timeline; prevent selecting legacy or future-wire months.
  if (protobufMonths.length < 2) {
    from.replaceChildren(); to.replaceChildren();
    status(state,'Há menos de duas competências com camada Protobuf numérica v1. Comparação indisponível.');
    button.disabled=true;
    return;
  }
  const comparableManifest={...manifest,snapshots:protobufMonths,
    current:protobufMonths.some(s=>s.date===manifest.current)
      ? manifest.current : protobufMonths[0].date};
  populateDateSelect(doc,from,comparableManifest,params().get('de') ||
    protobufMonths[1]?.date);
  populateDateSelect(doc,to,comparableManifest,params().get('ate') ||
    comparableManifest.current);
  const initial=params().get('cnpj');
  if(initial) input.value=initial;
  status(state,'Escolha duas competências com índice Protobuf numérico v1.');
  let busy = false;
  const selection = watchSelection(
    [[input, 'input'], [from, 'change'], [to, 'change']], state, output,
    'CNPJ ou competências alterados durante a comparação. Aguarde a conclusão para pesquisar novamente.',
    ['cnpj', 'de', 'ate']);
  form.addEventListener('submit',async event=>{
    event.preventDefault();
    if (busy) return;
    const cnpj=canonicalCnpj(input.value);
    output.replaceChildren();
    if(!cnpj){
      setParams({cnpj:null,de:null,ate:null});
      status(state,'Informe um CNPJ completo.',true);
      selection.markTerminal();
      return;
    }
    // The published history supports only numeric Protobuf v1. Reject locally:
    // the legacy Parquet reader cannot supply the required typed state.
    if(!/^\d{14}$/.test(cnpj)){
      setParams({cnpj:null,de:null,ate:null});
      status(state,'Comparação alfanumérica ainda não disponível. Informe um CNPJ numérico completo (14 dígitos).',true);
      selection.markTerminal();
      return;
    }
    const start=from.value, end=to.value;
    if (start === end || start > end) {
      // An invalid pair must not remain shareable from a preloaded deep link.
      if (['cnpj', 'de', 'ate'].some(key => params().has(key))) {
        setParams({cnpj:null,de:null,ate:null});
      }
      status(state, start === end
        ? 'Selecione duas competências diferentes.'
        : 'A competência inicial deve ser anterior à competência final.', true);
      selection.markTerminal();
      return;
    }
    const version = selection.begin();
    const stillCurrent = () => selection.current(version) &&
      canonicalCnpj(input.value) === cnpj && from.value === start && to.value === end;
    const discarded = 'CNPJ ou competências alterados durante a comparação. Pesquise novamente.';
    busy = true;
    // Keep the button in the keyboard tab order during slow archival lookups.
    // The busy guard rejects repeated submits without disabling focus.
    button.setAttribute('aria-disabled','true');
    status(state,'Consultando competência inicial ('+start+')…');
    try {
      const older=requireHistoryObservation(await lookup(manifest,start,cnpj), start, cnpj);
      // Avoid a second archival GET if the user changed the requested comparison.
      if (!stillCurrent()) { status(state,discarded); return; }
      status(state,older.state === 'absent-in-snapshot'
        ? 'CNPJ não localizado no registro Protobuf de '+start+'. Consultando competência final ('+end+')…'
        : 'Observação de '+start+' recebida. Consultando competência final ('+end+')…');
      const newer=requireHistoryObservation(await lookup(manifest,end,cnpj), end, cnpj);
      if (!stillCurrent()) { status(state,discarded); return; }
      setParams({cnpj,de:start,ate:end});
      renderHistoryComparison(doc,output,older,newer);
      if (older.state === 'absent-in-snapshot' || newer.state === 'absent-in-snapshot') {
        status(state,'Comparação incompleta: CNPJ não localizado em uma das competências.');
      } else {
        status(state,'Comparação concluída. Alterações são diferenças entre observações, não datas de eventos.');
      }
    }catch(error){
      if (!stillCurrent()) { status(state,discarded); return; }
      if (error instanceof CnpjInputError) {
        setParams({cnpj:null,de:null,ate:null});
        status(state,error.message,true);
        add(doc,output,'p','Corrija o CNPJ antes de comparar.');
      } else {
        setParams({cnpj,de:start,ate:end});
        status(state,'Comparação indisponível: '+error.message,true);
        anchor(doc,output,'Ver competências disponíveis',BASE+'acervo/');
      }
    }finally{busy=false;selection.finish();button.removeAttribute('aria-disabled');}
  });
}

