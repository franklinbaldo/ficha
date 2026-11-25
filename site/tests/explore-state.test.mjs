import test from 'node:test';
import assert from 'node:assert/strict';
import {mountExplore} from '../static/js/pages.mjs';
import {fetchManifest} from '../static/js/catalog.mjs';

const ids = ['explorar-competencia','explorar-filtro','explorar-serie',
  'explorar-estado','explorar-grafico','explorar-contador','explorar-serie-grafico'];
class Element {
  constructor(tag) {
    this.tag=tag; this.children=[]; this.value=''; this.disabled=false;
    this.dataset={}; this.style={}; this.listeners=new Map(); this.ownText='';
  }
  get textContent(){return this.ownText+this.children.map(c=>c.textContent).join('');}
  set textContent(v){this.ownText=String(v);this.children=[];}
  append(child){this.children.push(child);if(this.tag==='select'&&this.children.length===1)this.value=child.value;}
  replaceChildren(){this.children=[];this.ownText='';if(this.tag==='select')this.value='';}
  get options(){return this.children;}
  addEventListener(event,fn){this.listeners.set(event,fn);}
  setAttribute(){}
  emit(event){this.listeners.get(event)?.();}
}
function fixture(counts=true) {
  return {current:'2026-06',snapshots:[
    {date:'2026-06',files:{cnpjs:{size:0},socios:{size:20}},...(counts?{row_counts:{cnpjs:0}}:{})},
    {date:'2026-07',files:{cnpjs:{size:200}},...(counts?{row_counts:{socios:8}}:{})},
  ]};
}
async function withPage(query,fn) {
  const previous=[globalThis.location,globalThis.history];
  globalThis.location=new URL('https://example.test/ficha/explorar/'+query);
  globalThis.history={replaceState(_state,_title,next){globalThis.location=new URL(String(next));}};
  const nodes=ids.map((_,i)=>new Element(i===0||i===2?'select':i===1?'input':'div'));
  let failTag=null;
  const doc={getElementById:id=>nodes[ids.indexOf(id)]??null,createElement(tag){
    if(tag===failTag){failTag=null;throw Error('falha de renderização injetada');}
    return new Element(tag);
  }};
  try {await fn({doc,nodes,failNext:tag=>{failTag=tag;}});}
  finally {for(const [i,k] of ['location','history'].entries()) {
    if(previous[i]===undefined)delete globalThis[k];else globalThis[k]=previous[i];
  }}
}
const disabled = nodes => nodes.slice(0,3).map(n=>n.disabled);
function assertUnavailable(nodes,original) {
  assert.deepEqual(disabled(nodes),[true,true,true]);
  assert.equal(nodes[3].dataset.kind,'error');
  assert.deepEqual(nodes.slice(4).map(n=>n.textContent),['','','']);
  assert.equal(nodes[0].options.length,0);
  assert.equal(nodes[2].options.length,0);
  assert.equal(globalThis.location.href,original);
  nodes[0].emit('change');nodes[1].emit('input');nodes[2].emit('change');
  assert.deepEqual(nodes.slice(4).map(n=>n.textContent),['','','']);
}
test('Explorar desativa controles durante o GET e após HTTP 503 sem alterar URL',async()=>{
  await withPage('?competencia=2026-06&origem=teste',async({doc,nodes})=>{
    const original=globalThis.location.href;
    let release,gets=0;const gate=new Promise(r=>release=r);
    const pending=mountExplore(doc,{loadManifest:()=>fetchManifest({fetchImpl:async()=>{
      gets++;await gate;return {status:503};
    }})});
    try {assert.deepEqual(disabled(nodes),[true,true,true]);}
    finally {release();await pending;}
    assert.equal(gets,1);assertUnavailable(nodes,original);
  });
});
test('Explorar recusa colisão de identidade em competência não selecionada por HTTP 200',async()=>{
  await withPage('?competencia=2026-06',async({doc,nodes})=>{
    const original=globalThis.location.href,data=fixture();
    data.snapshots[1].files={companies:{shards:[{shard:'00'}]},'companies/00':{}};
    await mountExplore(doc,{loadManifest:()=>fetchManifest({fetchImpl:async()=>({status:200,json:async()=>data})})});
    assertUnavailable(nodes,original);
  });
});
test('Explorar só publica fallback explícito e preserva zero, ausente e filtro local',async()=>{
  await withPage('?competencia=2026-13&origem=teste',async({doc,nodes})=>{
    let gets=0;await mountExplore(doc,{loadManifest:()=>fetchManifest({fetchImpl:async()=>{
      gets++;return {status:200,json:async()=>fixture()};
    }})});
    assert.deepEqual(disabled(nodes),[false,false,false]);
    assert.equal(nodes[0].value,'2026-06');
    assert.equal(globalThis.location.searchParams.get('origem'),'teste');
    assert.match(nodes[3].textContent,/solicitada indisponível/);
    assert.match(nodes[6].textContent,/0 linhas/);
    assert.match(nodes[6].textContent,/Não declarado/);
    nodes[1].value='socios';nodes[1].emit('input');
    assert.match(nodes[3].textContent,/solicitada indisponível/);
    assert.doesNotMatch(nodes[4].textContent,/cnpjs/);
    assert.equal(gets,1);
    nodes[0].value='2026-07';nodes[0].emit('change');
    assert.doesNotMatch(nodes[3].textContent,/solicitada indisponível/);
  });
});
test('Explorar sem séries mantém catálogo e filtro disponíveis sem série artificial',async()=>{
  await withPage('',async({doc,nodes})=>{
    await mountExplore(doc,{loadManifest:async()=>fixture(false)});
    assert.deepEqual(disabled(nodes),[false,false,true]);
    assert.match(nodes[6].textContent,/Nenhuma série de linhas declarada/);
    nodes[1].value='socios';nodes[1].emit('input');
    assert.match(nodes[4].textContent,/socios/);
    assert.doesNotMatch(nodes[4].textContent,/cnpjs/);
  });
});
test('Explorar não mantém gráfico parcial após falha de renderização durante filtro',async()=>{
  await withPage('?competencia=2026-06',async({doc,nodes,failNext})=>{
    await mountExplore(doc,{loadManifest:async()=>fixture()});
    const original=globalThis.location.href;
    failNext('strong');nodes[1].value='socios';nodes[1].emit('input');
    assertUnavailable(nodes,original);
  });
});
test('Explorar conserva a contagem de arquivos declarados quando o filtro muda',async()=>{
  await withPage('?competencia=2026-06',async({doc,nodes})=>{
    await mountExplore(doc,{loadManifest:async()=>fixture()});
    assert.equal(nodes[5].textContent,'2 arquivos · 20 B declarados');
    nodes[1].value='socios';nodes[1].emit('input');
    assert.match(nodes[3].textContent,/Exibindo 1 de 2 arquivos/);
    assert.equal(nodes[5].textContent,'2 arquivos · 20 B declarados');
  });
});
