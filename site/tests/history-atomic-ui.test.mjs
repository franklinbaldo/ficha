import test from 'node:test';
import assert from 'node:assert/strict';
import {mountHistory} from '../static/js/pages.mjs';

class Element {
  constructor(tag='div') {
    this.tag=tag; this.children=[]; this.dataset={}; this.attributes={};
    this.listeners=new Map(); this.textContent=''; this.value=''; this.disabled=false;
  }
  append(node) {
    this.children.push(node);
    if(this.tag==='select' && this.children.length===1) this.value=node.value;
  }
  replaceChildren(){this.children=[];this.textContent='';}
  get options(){return this.children;}
  addEventListener(type,handler){this.listeners.set(type,handler);}
  setAttribute(name,value){this.attributes[name]=value;}
}
function documentFor() {
  const ids=['historia-competencias','historia-inicio','historia-fim',
    'historia-form','historia-cnpj','historia-enviar','historia-estado','historia-resultado'];
  const nodes=new Map(ids.map(id=>[id,new Element(
    id==='historia-inicio'||id==='historia-fim'?'select':'div')]));
  return {createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id),
    at:id=>nodes.get(id)};
}
const cnpj='33000167000101';
function manifest() {
  const snap=date=>({date,files:{companies:{
    shard_by:'cnpj_base_prefix_2',schema_version:'1.0.0',
    shards:[{shard:'33',url:'https://archive.org/download/ficha-'+date+
      '/companies-33.zip'}]}}});
  const v11=snap('2026-05');
  v11.files.companies.schema_version='1.1.0';
  return {current:'2026-07',snapshots:[
    {date:'2026-04',files:{cnpjs:{url:'https://archive.org/legacy.parquet'}}},
    v11,snap('2026-06'),snap('2026-07'),
  ]};
}
function outcome(date,state) {
  return {date,cnpj,partial:true,state,
    source:'https://archive.org/download/ficha-'+date+'/companies-33.zip/33/000/167.pb',
    archiveUrl:'https://archive.org/download/ficha-'+date+'/companies-33.zip',
    data:state==='found'?{cnpj,razao_social:'EMPRESA '+date,porte_empresa:null}:null};
}
function textOf(node){return [node.textContent,...node.children.map(textOf)].join(' ');}
async function withLocation(fn) {
  const oldLocation=globalThis.location,oldHistory=globalThis.history,oldFetch=globalThis.fetch;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/historia/');
  globalThis.history={replaceState(_state,_title,url){
    globalThis.location=new URL(String(url));
  }};
  try{return await fn();}
  finally {
    if(oldLocation===undefined)delete globalThis.location;
    else globalThis.location=oldLocation;
    if(oldHistory===undefined)delete globalThis.history;
    else globalThis.history=oldHistory;
    if(oldFetch===undefined)delete globalThis.fetch;
    else globalThis.fetch=oldFetch;
  }
}
async function setup(lookup) {
  const doc=documentFor();
  await mountHistory(doc,{loadManifest:async()=>manifest(),...(lookup?{lookup}:{})});
  doc.at('historia-cnpj').value=cnpj;
  return doc;
}
async function submit(doc) {
  return doc.at('historia-form').listeners.get('submit')({preventDefault(){}});
}

test('só índices v1 entram nos seletores; abril e wire 1.1 ficam na cronologia',async()=>{
  await withLocation(async()=>{
    const doc=await setup(async(_m,date)=>outcome(date,'found'));
    const from=doc.at('historia-inicio'),to=doc.at('historia-fim');
    assert.deepEqual(from.options.map(x=>x.value),['2026-07','2026-06']);
    assert.equal(from.value,'2026-06');
    assert.equal(to.value,'2026-07');
    assert.match(textOf(doc.at('historia-competencias')),/2026-04.*Sem índice para comparação numérica/);
    assert.match(textOf(doc.at('historia-competencias')),/2026-05.*Sem índice para comparação numérica/);
  });
});

test('404 de uma competência torna comparação inconclusiva, jamais concluída',async()=>{
  await withLocation(async()=>{
    const calls=[];
    const doc=await setup(async(_m,date)=>{calls.push(date);
      return outcome(date,date==='2026-06'?'artifact-unavailable':'found');});
    await submit(doc);
    assert.deepEqual(calls,['2026-06'],'404 no primeiro mês proíbe segundo GET');
    assert.match(doc.at('historia-estado').textContent,/inconclusiva.*404/i);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/concluída/i);
    assert.doesNotMatch(textOf(doc.at('historia-resultado')),/empresa nasceu|empresa encerrou/i);
  });
});

test('filial ausente em um PB íntegro não gera comparação histórica concluída',async()=>{
  await withLocation(async()=>{
    const doc=await setup(async(_m,date)=>
      outcome(date,date==='2026-06'?'absent-in-snapshot':'found'));
    await submit(doc);
    assert.match(doc.at('historia-estado').textContent,/Comparação incompleta/);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/concluída/i);
    assert.match(textOf(doc.at('historia-resultado')),/CNPJ não localizado/);
  });
});

test('dois registros encontrados permitem comparação explícita e fontes distintas',async()=>{
  await withLocation(async()=>{
    const doc=await setup(async(_m,date)=>outcome(date,'found'));
    await submit(doc);
    const shown=textOf(doc.at('historia-resultado'));
    assert.match(doc.at('historia-estado').textContent,/Comparação concluída/);
    assert.match(shown,/EMPRESA 2026-06/);
    assert.match(shown,/EMPRESA 2026-07/);
    assert.match(shown,/não permite determinar o porte/);
    assert.ok(new URLSearchParams(globalThis.location.search).has('cnpj'));
  });
});

test('CNPJ numérico de DV inválido não faz GET nem cria deep link histórico',async()=>{
  await withLocation(async()=>{
    let network=0;
    globalThis.fetch=async()=>{network++;throw Error('GET indevido');};
    const doc=await setup();
    doc.at('historia-cnpj').value='33000167000102';
    await submit(doc);
    assert.equal(network,0);
    assert.match(doc.at('historia-estado').textContent,/dígitos verificadores/i);
    assert.equal(doc.at('historia-estado').dataset.kind,'error');
    assert.equal(globalThis.location.search,'');
  });
});

test('found sem payload não é comparação concluída e não faz segunda consulta',async()=>{
  await withLocation(async()=>{
    const calls=[];
    const doc=await setup(async(_m,date)=>{
      calls.push(date);
      return {...outcome(date,'found'),data:null};
    });
    await submit(doc);
    assert.deepEqual(calls,['2026-06']);
    assert.match(doc.at('historia-estado').textContent,/inconclusiva/i);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/Comparação concluída/i);
  });
});

test('resposta sem estado é inconclusiva e não autoriza segunda consulta',async()=>{
  await withLocation(async()=>{
    const calls=[];
    const doc=await setup(async(_m,date)=>{
      calls.push(date);
      const result=outcome(date,'found');
      delete result.state;
      return result;
    });
    await submit(doc);
    assert.deepEqual(calls,['2026-06']);
    assert.match(doc.at('historia-estado').textContent,/inconclusiva/i);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/Comparação concluída/i);
  });
});
