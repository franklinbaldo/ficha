import test from 'node:test';
import assert from 'node:assert/strict';
import * as pages from '../static/js/pages.mjs';
import {CnpjInputError} from '../static/js/atomic-record.mjs';

class Element {
  constructor(tag='div') {
    this.tag=tag; this.children=[]; this.textContent=''; this.value='';
    this.disabled=false; this.dataset={}; this.attributes={}; this.listeners=new Map();
  }
  append(child) { this.children.push(child); if(this.tag==='select'&&this.children.length===1)this.value=child.value; }
  replaceChildren() { this.children=[]; this.textContent=''; }
  get options() { return this.children; }
  addEventListener(event, fn) { this.listeners.set(event,fn); }
  setAttribute(key,value) { this.attributes[key]=value; }
}
const ids=['historia-competencias','historia-inicio','historia-fim','historia-form',
  'historia-cnpj','historia-enviar','historia-estado','historia-resultado'];
const numeric='33000167000101';
const manifest={current:'2026-07',snapshots:['2026-07','2026-06','2026-05','2026-04']
  .map(date=>({date,files:{...(date==='2026-04'?{}:
    {companies:{shard_by:'cnpj_base_prefix_2',shards:[{shard:'33',url:'https://archive.org/33'}]}})}}))};
const treeText=node=>[node.textContent,...node.children.map(treeText)].join(' ');
function docFor() {
  const elements=new Map(ids.map(id=>[id,new Element(id.includes('inicio')||id.includes('fim')?'select':'div')]));
  return {createElement:tag=>new Element(tag),getElementById:id=>elements.get(id),
    at:id=>elements.get(id)};
}
async function withUrl(fn) {
  const oldLoc=globalThis.location,oldHistory=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/historia/');
  globalThis.history={replaceState(_state,_title,url){globalThis.location=new URL(String(url));}};
  try {return await fn();} finally {
    if(oldLoc===undefined)delete globalThis.location;else globalThis.location=oldLoc;
    if(oldHistory===undefined)delete globalThis.history;else globalThis.history=oldHistory;
  }
}
const observe=(date,state='found')=>({date,cnpj:numeric,state,partial:true,
  source:'https://archive.org/download/ficha-'+date+'/companies-33.zip/33/000/167.pb',
  data:state==='found'?{cnpj:numeric,razao_social:'PETROLEO BRASILEIRO',opcao_mei:null}:null});
async function mount(doc, lookup) {
  await pages.mountHistory(doc,{loadManifest:async()=>manifest,lookup});
  doc.at('historia-cnpj').value=numeric;
  doc.at('historia-inicio').value='2026-06';
  doc.at('historia-fim').value='2026-07';
}
const submit=doc=>doc.at('historia-form').listeners.get('submit')({preventDefault(){}});
test('comparador sugere por padrão duas competências com camada Protobuf v1',async()=>{
  await withUrl(async()=>{
    const doc=docFor();
    await pages.mountHistory(doc,{loadManifest:async()=>manifest,lookup:async()=>observe('2026-06')});
    assert.equal(doc.at('historia-inicio').value,'2026-06');
    assert.equal(doc.at('historia-fim').value,'2026-07');
  });
});
test('404 do membro é inconclusivo: interrompe segundo GET e não declara comparação concluída',async()=>{
  await withUrl(async()=>{
    const doc=docFor();let calls=0;
    await mount(doc,async (_m,date)=>{calls++;return observe(date,'artifact-unavailable');});
    await submit(doc);
    assert.equal(calls,1);
    assert.match(doc.at('historia-estado').textContent,/inconclusiva/i);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/concluída/i);
    assert.equal(doc.at('historia-resultado').children.length,1,'somente link para catálogo');
  });
});
test('ausência PB íntegra é condicional à competência, nunca nascimento/encerramento',async()=>{
  await withUrl(async()=>{
    const doc=docFor();
    await mount(doc,async (_m,date)=>observe(date,date==='2026-06'?'absent-in-snapshot':'found'));
    await submit(doc);
    const text=treeText(doc.at('historia-resultado'));
    assert.match(text,/não localizado no registro Protobuf de 2026-06/i);
    assert.match(text,/não comprova nascimento, encerramento/i);
    assert.match(text,/porte histórico não pode ser determinado/i);
    assert.equal(doc.at('historia-estado').dataset.kind,'info');
  });
});
test('validação CNPJ do PB é local e não deixa deep link de consulta inválida',async()=>{
  await withUrl(async()=>{
    const doc=docFor();
    await mount(doc,async()=>{throw new CnpjInputError('CNPJ inválido: dígitos verificadores');});
    await submit(doc);
    assert.match(doc.at('historia-estado').textContent,/CNPJ inválido/i);
    assert.doesNotMatch(doc.at('historia-estado').textContent,/Comparação indisponível/i);
    assert.equal(globalThis.location.search,'');
  });
});
test('dispatcher rejeita alfa antes de acionar qualquer transporte',async()=>{
  const route=pages.fetchHistoryRecordAt;
  assert.equal(typeof route,'function');
  let atom=0,old=0;
  const deps={atomic:async()=>{atom++;return observe('2026-06');},
    legacy:async()=>{old++;throw Error('legado não deve ser acionado');}};
  const result=await route(manifest,'2026-06',numeric,deps);
  assert.equal(result.state,'found');assert.equal(atom,1);assert.equal(old,0);
  await assert.rejects(route(manifest,'2026-06','AB123456000158',deps),
    /Comparação alfanumérica ainda não disponível/);
  assert.equal(atom,1);assert.equal(old,0);
});

test('tabela histórica tem legenda, cabeçalhos com scope e região rolável por teclado',()=> {
  const doc=docFor(), parent=new Element('div');
  pages.renderHistoryComparison(doc,parent,observe('2026-06'),observe('2026-07'));
  assert.match(treeText(parent),/Pelo teclado, pressione Tab até a tabela e use as setas/);
  const region=parent.children.find(node=>node.attributes.role==='region');
  assert.ok(region,'a tabela está em região navegável');
  assert.equal(region.attributes.tabindex,'0','rolagem acessível por teclado');
  assert.match(region.attributes['aria-label'],/2026-06.*2026-07/);
  const table=region.children.find(node=>node.tag==='table');
  assert.ok(table);
  assert.match(table.children.find(node=>node.tag==='caption').textContent,
    /2026-06.*2026-07/);
  const head=table.children.find(node=>node.tag==='thead');
  assert.deepEqual(head.children[0].children.map(node=>[node.textContent,node.attributes.scope]),
    [['Campo','col'],['2026-06','col'],['2026-07','col']]);
  const body=table.children.find(node=>node.tag==='tbody');
  assert.ok(body.children.length>0,'a tabela mantém as linhas de comparação');
  assert.ok(body.children.every(row=>row.children[0].attributes.scope==='row'));
});
