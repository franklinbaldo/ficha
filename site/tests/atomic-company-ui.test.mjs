import test from 'node:test';
import assert from 'node:assert/strict';
import {mountCompany, renderCompanyRecord} from '../static/js/pages.mjs';
import {fetchCompanyByCnpj} from '../static/js/atomic-browser.mjs';

class Element {
  constructor(tag='div') {
    this.tag=tag;this.children=[];this.dataset={};this.attributes={};
    this.listeners=new Map();this.textContent='';this.value='';this.disabled=false;
  }
  append(child) {this.children.push(child);
    if(this.tag==='select'&&this.children.length===1)this.value=child.value;}
  replaceChildren(){this.children=[];this.textContent='';}
  get options(){return this.children;}
  addEventListener(type,fn){this.listeners.set(type,fn);}
  setAttribute(name,value){this.attributes[name]=value;}
  removeAttribute(name){delete this.attributes[name];}
}
function documentFor(ids=[]) {
  const items=new Map(ids.map(id=>[id,new Element(id==='empresa-competencia'?'select':'div')]));
  return {createElement:tag=>new Element(tag),getElementById:id=>items.get(id),
    at:id=>items.get(id)};
}
function treeText(node){
  return [node.textContent,...(node.children||[]).map(treeText)].join(' ');
}
const date='2026-07',cnpj='33000167000101';
const source='https://archive.org/download/ficha-2026-07/companies-33.zip/33/000/167.pb';
const manifest={current:date,snapshots:[{date,files:{}}]};
const base={cnpj,date,source,archiveUrl:'https://archive.org/download/ficha-2026-07/companies-33.zip',
  partial:true};

test('renderizador distingue 404 de ausência no PB sem alegar integridade de todo o acervo',()=>{
  const doc=documentFor(),output=new Element();
  renderCompanyRecord(doc,output,{...base,state:'artifact-unavailable',data:null,httpStatus:404});
  assert.match(treeText(output),/Arquivo indisponível/);
  assert.match(treeText(output),/Consulta inconclusiva/);
  assert.doesNotMatch(treeText(output),/Nenhum registro encontrado/);
  renderCompanyRecord(doc,output,{...base,state:'absent-in-snapshot',data:null});
  assert.match(treeText(output),/não localizado no registro Protobuf/);
  assert.doesNotMatch(treeText(output),/registro íntegro|integridade (?:completa|verificada)/i);
});

test('estados desconhecidos e dados contraditórios não viram ausência nem ficha válida',()=>{
  const doc=documentFor(),output=new Element();
  for(const response of [
    {...base,state:'pending',data:null},
    {...base,state:'found',data:null},
    {...base,state:'absent-in-snapshot',data:{cnpj}},
    {...base,state:'artifact-unavailable',data:{cnpj}},
    null,
  ]){
    assert.throws(()=>renderCompanyRecord(doc,output,response),/inconclusiva/);
    assert.doesNotMatch(treeText(output),/Nenhum registro encontrado|FICHA CADASTRAL/);
  }
});

test('estado inesperado do leitor é consulta inconclusiva, com alternativa ao acervo',async()=>{
  const oldLocation=globalThis.location,oldHistory=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/empresa/');
  globalThis.history={replaceState(_s,_t,url){globalThis.location=new URL(String(url));}};
  try{
    const ids=['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar'];
    const doc=documentFor(ids);
    await mountCompany(doc,{loadManifest:async()=>manifest,
      lookup:async()=>({...base,state:'unrecognised-wire',data:null})});
    doc.at('empresa-cnpj').value=cnpj;
    await doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    assert.match(doc.at('empresa-estado').textContent,/Consulta indisponível.*consulta inconclusiva/);
    assert.equal(doc.at('empresa-estado').dataset.kind,'error');
    const content=treeText(doc.at('empresa-resultado'));
    assert.match(content,/Não foi possível concluir a leitura remota/);
    assert.match(content,/Consultar o catálogo de arquivos/);
    assert.doesNotMatch(content,/Nenhum registro encontrado|FICHA CADASTRAL/);
  }finally{
    if(oldLocation===undefined)delete globalThis.location;else globalThis.location=oldLocation;
    if(oldHistory===undefined)delete globalThis.history;else globalThis.history=oldHistory;
  }
});

test('registro encontrado não atribui porte desconhecido ao enum Protobuf',()=>{
  const doc=documentFor(),output=new Element();
  renderCompanyRecord(doc,output,{...base,state:'found',
    data:{cnpj,razao_social:'EMPRESA TESTE',municipio_nome:'SÃO PAULO'}});
  assert.match(treeText(output),/EMPRESA TESTE/);
  assert.match(treeText(output),/Porte empresarial: Não determinado/);
});

test('404 não anuncia consulta concluída; 503 não produz ausência; consulta recupera',async()=>{
  const oldLocation=globalThis.location,oldHistory=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/empresa/');
  globalThis.history={replaceState(_s,_t,url){globalThis.location=new URL(String(url));}};
  try{
    const ids=['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar'];
    const doc=documentFor(ids);
    let requests=0;
    await mountCompany(doc,{loadManifest:async()=>manifest,lookup:async()=>{
      requests++;
      if(requests===1)return {...base,state:'artifact-unavailable',data:null};
      if(requests===2)throw Error('HTTP 503');
      return {...base,state:'found',data:{cnpj,razao_social:'OK'}};
    }});
    doc.at('empresa-cnpj').value=cnpj;
    const submit=()=>doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    await submit();
    assert.match(doc.at('empresa-estado').textContent,/inconclusiva/i);
    assert.doesNotMatch(doc.at('empresa-estado').textContent,/concluída/i);
    assert.doesNotMatch(treeText(doc.at('empresa-resultado')),/Nenhum registro encontrado/);
    await submit();
    assert.match(doc.at('empresa-estado').textContent,/indisponível/i);
    assert.doesNotMatch(treeText(doc.at('empresa-resultado')),/Nenhum registro encontrado/);
    await submit();
    assert.match(doc.at('empresa-estado').textContent,/concluída/i);
    assert.match(treeText(doc.at('empresa-resultado')),/OK/);
    assert.equal(requests,3);
  }finally{
    if(oldLocation===undefined)delete globalThis.location;else globalThis.location=oldLocation;
    if(oldHistory===undefined)delete globalThis.history;else globalThis.history=oldHistory;
  }
});

test('CNPJ rejeitado não vira indisponibilidade nem deep link consultável',async()=>{
  const oldLocation=globalThis.location, oldHistory=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/empresa/');
  globalThis.history={replaceState(_s,_t,url){globalThis.location=new URL(String(url));}};
  try{
    const ids=['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar'];
    const doc=documentFor(ids);
    let network=0,decoder=0;
    await mountCompany(doc,{loadManifest:async()=>manifest,lookup:(m,d,cnpj)=>
      fetchCompanyByCnpj(m,d,cnpj,{
        fetchImpl:async()=>{network++;throw Error('GET indevido');},
        loadDecoder:async()=>{decoder++;throw Error('decoder indevido');},
      }),
    });
    const submit=()=>doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    for(const [value,message] of [
      ['33000167000102',/dígitos verificadores/],
      ['AB123456000158',/alfanumérico/],
    ]){
      doc.at('empresa-cnpj').value=value;
      await submit();
      assert.match(doc.at('empresa-estado').textContent,message);
      assert.doesNotMatch(doc.at('empresa-estado').textContent,/Consulta indisponível/i);
      assert.equal(doc.at('empresa-estado').dataset.kind,'error');
      assert.equal(new URL(globalThis.location.href).searchParams.has('cnpj'),false);
      assert.equal(network,0);
      assert.equal(decoder,0);
    }
  }finally{
    if(oldLocation===undefined)delete globalThis.location;else globalThis.location=oldLocation;
    if(oldHistory===undefined)delete globalThis.history;else globalThis.history=oldHistory;
  }
});


test('registro encontrado com CNPJ interno divergente não vira ficha de outra empresa',()=>{
  const doc=documentFor(), output=new Element();
  const incompatible={...base,state:'found',
    data:{cnpj:'60701190000104',razao_social:'REGISTRO DE OUTRA EMPRESA'}};
  assert.throws(()=>renderCompanyRecord(doc,output,incompatible),/CNPJ.*inconclusiva/i);
  assert.doesNotMatch(treeText(output),/REGISTRO DE OUTRA EMPRESA|FICHA CADASTRAL/);
});

test('resposta de outra consulta não vira resultado positivo ou ausência para a seleção atual',async()=>{
  const oldLocation=globalThis.location,oldHistory=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/empresa/');
  globalThis.history={replaceState(_s,_t,url){globalThis.location=new URL(String(url));}};
  try{
    const ids=['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar'];
    const doc=documentFor(ids);
    let requests=0;
    await mountCompany(doc,{loadManifest:async()=>manifest,
      lookup:async()=>{requests++;return {
        ...base,date:'2026-06',state:'absent-in-snapshot',data:null};
      }});
    doc.at('empresa-cnpj').value=cnpj;
    await doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    assert.equal(requests,1);
    assert.match(doc.at('empresa-estado').textContent,/Consulta indisponível.*inconclusiva/i);
    assert.doesNotMatch(doc.at('empresa-estado').textContent,/Consulta concluída/i);
    assert.doesNotMatch(treeText(doc.at('empresa-resultado')),/CNPJ não localizado|FICHA CADASTRAL/);
    assert.match(treeText(doc.at('empresa-resultado')),/Consultar o catálogo de arquivos/);
  }finally{
    if(oldLocation===undefined)delete globalThis.location;else globalThis.location=oldLocation;
    if(oldHistory===undefined)delete globalThis.history;else globalThis.history=oldHistory;
  }
});
