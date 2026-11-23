import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {mountArchive,renderArchiveSelection} from '../static/js/pages.mjs';

class FakeElement {
  constructor(tag='div') {
    this.tag=tag;
    this.children=[];
    this.textContent='';
    this.value='';
    this.disabled=false;
    this.dataset={};
    this.attributes={};
    this.listeners=new Map();
    this.focused=false;
  }
  append(child) {
    this.children.push(child);
    if(this.tag==='select' && this.children.length===1) this.value=child.value;
  }
  replaceChildren() {
    this.children=[];
    this.textContent='';
    if(this.tag==='select') this.value='';
  }
  get options() { return this.children; }
  addEventListener(event,callback) { this.listeners.set(event,callback); }
  setAttribute(key,value) { this.attributes[key]=value; }
  focus() { this.focused=true; }
}

function documentWith(ids=[]) {
  const nodes=new Map(ids.map(id=>[id,new FakeElement(
    id==='acervo-competencia'?'select':id==='acervo-filtro'?'input':'div'
  )]));
  return {
    createElement:tag=>new FakeElement(tag),
    getElementById:id=>nodes.get(id)??null,
    at:id=>nodes.get(id),
  };
}
function descendants(node) {
  return [node,...node.children.flatMap(descendants)];
}
const ARCHIVE_IDS=['acervo-competencia','acervo-resultado','acervo-estado',
  'acervo-competencias','acervo-filtro','acervo-contagem','acervo-limpar'];

function manifest() {
  const shards=Array.from({length:100},(_,i)=>({
    shard:String(i).padStart(2,'0'),
    size:500+i,
    sha256:'a'.repeat(64),
    url:'https://archive.org/download/example/companies-'+i+'.zip',
  }));
  return {current:'2026-07',snapshots:[
    {date:'2026-06',files:{cnpjs:{size:12,url:'https://archive.org/download/old/cnpjs.zip'}}},
    {date:'2026-07',files:{
      companies:{shards},
      partners:{shards:Array.from({length:7},(_,i)=>({
        shard:String(i).padStart(2,'0'),size:i+1,
        url:'https://archive.org/download/example/partners-'+i+'.zip',
      }))},
      cnpjs:{size:11,url:'javascript:alert(1)'},
    }},
  ]};
}

async function withUrl(fn) {
  const location=globalThis.location,history=globalThis.history;
  globalThis.location=new URL('https://franklinbaldo.github.io/ficha/acervo/?origem=teste');
  globalThis.history={replaceState(_state,_title,url) { globalThis.location=new URL(String(url)); }};
  try { await fn(); }
  finally {
    if(location===undefined) delete globalThis.location;
    else globalThis.location=location;
    if(history===undefined) delete globalThis.history;
    else globalThis.history=history;
  }
}

test('Acervo filtra somente chave, sem perder totais, digest ou URL segura',()=>{
  const doc=documentWith(), root=new FakeElement();
  const data=manifest();
  assert.deepEqual(renderArchiveSelection(doc,root,data,'2026-07','COMPANIES/00'),
    {shown:1,total:108});
  const nodes=descendants(root);
  const summaries=nodes.filter(node=>node.tag==='summary');
  assert.equal(summaries.length,1);
  assert.equal(summaries[0].children[0].textContent,'companies/00');
  assert.equal(nodes.filter(node=>node.tag==='code').length,1);
  assert.match(nodes.find(node=>node.tag==='code').textContent,/^SHA-256 declarado: a{64}$/);
  assert.equal(nodes.find(node=>node.tag==='a').href,
    'https://archive.org/download/example/companies-0.zip');
  assert.equal(nodes.find(node=>node.tag==='div' &&
    node.children[0]?.textContent==='108 arquivos declarados').children[0].textContent,
    '108 arquivos declarados');
  assert.deepEqual(renderArchiveSelection(doc,root,data,'2026-07','nome-inexistente'),
    {shown:0,total:108});
  assert.equal(descendants(root).filter(node=>node.tag==='details').length,0);
  assert.ok(descendants(root).some(node=>/Nenhum arquivo corresponde ao filtro/.test(node.textContent)));
  assert.deepEqual(renderArchiveSelection(doc,root,data,'2026-07','cnpjs'),
    {shown:1,total:108});
  assert.equal(descendants(root).filter(node=>node.tag==='a').length,0,
    'URL javascript não vira link mesmo após filtro');
});

test('Acervo filtra sem GET/alteração da URL, anuncia X/Y e restaura foco ao limpar',async()=>{
  await withUrl(async()=>{
    let loads=0;
    const doc=documentWith(ARCHIVE_IDS);
    await mountArchive(doc,{loadManifest:async()=>{loads++;return manifest();}});
    const input=doc.at('acervo-filtro'), count=doc.at('acervo-contagem');
    const clear=doc.at('acervo-limpar'), select=doc.at('acervo-competencia');
    assert.equal(count.textContent,'108 de 108 arquivos exibidos');
    assert.equal(input.disabled,false);
    assert.equal(clear.disabled,true);
    const originalUrl=globalThis.location.href;
    input.value='PARTNERS/0';
    input.listeners.get('input')();
    assert.equal(count.textContent,'7 de 108 arquivos exibidos');
    assert.equal(descendants(doc.at('acervo-resultado')).filter(n=>n.tag==='details').length,7);
    assert.equal(clear.disabled,false);
    assert.equal(globalThis.location.href,originalUrl,'filtro é local, não entra no deep link');
    assert.equal(loads,1,'filtro não refaz a leitura do manifesto');
    input.value='sem-resultados';
    input.listeners.get('input')();
    assert.equal(count.textContent,'0 de 108 arquivos exibidos');
    clear.listeners.get('click')();
    assert.equal(input.value,'');
    assert.equal(count.textContent,'108 de 108 arquivos exibidos');
    assert.equal(input.focused,true);
    assert.equal(clear.disabled,true);
    input.value='companies/00';
    select.value='2026-06';
    select.listeners.get('change')();
    assert.equal(input.value,'','troca de competência elimina filtro antigo');
    assert.equal(count.textContent,'1 de 1 arquivo exibido');
    assert.equal(new URL(globalThis.location).searchParams.get('competencia'),'2026-06');
    assert.equal(new URL(globalThis.location).searchParams.get('origem'),'teste');
    assert.equal(loads,1);
  });
});

test('Acervo preserva estado sem arquivos e não confunde filtro com ausência da competência',()=>{
  const doc=documentWith(),root=new FakeElement();
  const data={current:'2026-07',snapshots:[{date:'2026-07',files:{}}]};
  assert.deepEqual(renderArchiveSelection(doc,root,data,'2026-07','qualquer'),
    {shown:0,total:0});
  assert.ok(descendants(root).some(node=>node.textContent==='Nenhum arquivo listado nesta competência.'));
});

test('Acervo falha fechado no carregamento: estado anunciado e controles desativados',async()=>{
  await withUrl(async()=>{
    const doc=documentWith(ARCHIVE_IDS);
    let rejectLoad;
    const pending=new Promise((_,reject)=>{rejectLoad=reject;});
    const mounting=mountArchive(doc,{loadManifest:()=>pending});
    const filter=doc.at('acervo-filtro'), clear=doc.at('acervo-limpar');
    const count=doc.at('acervo-contagem'), state=doc.at('acervo-estado');
    assert.equal(filter.disabled,true,'filtro deve iniciar indisponível');
    assert.equal(clear.disabled,true,'limpar deve iniciar indisponível');
    rejectLoad(new Error('falha simulada de manifesto'));
    await mounting;
    assert.equal(filter.disabled,true);
    assert.equal(clear.disabled,true);
    assert.equal(count.textContent,'Lista de arquivos indisponível: filtro desativado.');
    assert.equal(count.dataset.kind,'error');
    assert.match(state.textContent,/Não foi possível abrir o acervo: falha simulada de manifesto/);
    assert.equal(filter.listeners.has('input'),false);
    assert.equal(clear.listeners.has('click'),false);
    assert.equal(descendants(doc.at('acervo-resultado')).filter(n=>n.tag==='details').length,0);
  });
});

test('Fonte SSG fornece rótulo, contagem viva e limpeza acessíveis',()=>{
  const html=readFileSync(new URL('../content/acervo.md',import.meta.url),'utf8');
  assert.match(html,/<label for="acervo-filtro">Filtrar arquivos desta competência<\/label>/);
  assert.match(html,/<input id="acervo-filtro"[^>]* disabled \/>/);
  assert.match(html,/<button type="button" id="acervo-limpar"[^>]* disabled>/);
  assert.match(html,/<p id="acervo-contagem" role="status" aria-live="polite" aria-atomic="true"/);
});
