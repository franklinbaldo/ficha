import test from 'node:test';
import assert from 'node:assert/strict';
import { mountSearch } from '../static/js/search-app.mjs';

class Element {
  constructor() { this.children = []; this.textContent = ''; this.listeners = {}; this.dataset = {}; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  addEventListener(type, cb) { this.listeners[type] = cb; }
  dispatch(type) { return this.listeners[type]?.({ preventDefault() {} }); }
}
function mockDOM() {
  const nodes = Object.fromEntries(['busca-form','busca-termo','busca-enviar',
    'consulta-estado','consulta-lista'].map(k => [k,new Element()]));
  return {nodes, documentRef:{
    getElementById:id=>nodes[id], createElement:()=>new Element(),
  }};
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((ok,no)=>{resolve=ok;reject=no;});
  return {promise, resolve, reject};
}
const dataset = {date:'2026-07', files:{cnpjs:{url:'https://example.test/cnpjs.parquet',size:1}}};

for (const outcome of ['success', 'failure']) {
  test(`descarta resposta antiga após alterar entrada (resultado ${outcome})`, async () => {
    const {nodes,documentRef} = mockDOM();
    const waiting = deferred();
    const queries = [];
    mountSearch({documentRef, fetchDataset:async()=>dataset, probeRange:async()=>{},
      createEngine:async()=>({db:{},duckdb:{}}), attach:async()=>{},
      query:async (_,term)=> {
        queries.push(term);
        if (term==='EMPRESA A') return waiting.promise;
        return [{cnpj:'00000000E08G12',razao_social:'EMPRESA B',uf:'RO'}];
      },
    });
    nodes['busca-termo'].value='EMPRESA A';
    const pendingA = nodes['busca-form'].dispatch('submit');
    // Ensure the query for A is pending before changing the input.
    for (let i=0; i<10 && queries.length===0; i++) await Promise.resolve();
    assert.deepEqual(queries,['EMPRESA A']);
    nodes['busca-termo'].value='EMPRESA B';
    nodes['busca-termo'].dispatch('input');
    if(outcome==='success') waiting.resolve([{cnpj:'11222333000181',razao_social:'EMPRESA A',uf:'SP'}]);
    else waiting.reject(new Error('Erro da consulta A'));
    await pendingA;
    assert.equal(nodes['consulta-lista'].children.length,0, 'A não pode aparecer para entrada B');
    assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
    assert.equal(nodes['busca-enviar'].disabled,false);
    await nodes['busca-form'].dispatch('submit');
    assert.equal(nodes['consulta-lista'].children.length,1);
    assert.equal(nodes['consulta-lista'].children[0].children[0].textContent,'EMPRESA B');
    assert.deepEqual(queries,['EMPRESA A','EMPRESA B']);
  });
}

test('edição durante preparação da base impede consulta para CNPJ antigo', async () => {
  const {nodes,documentRef}=mockDOM();
  const initializing=deferred();
  let queried=0,fetched=0;
  mountSearch({documentRef,fetchDataset:async()=>{fetched++;return initializing.promise;},
    probeRange:async()=>{},createEngine:async()=>({db:{},duckdb:{}}),attach:async()=>{},
    query:async()=>{queried++;return [];},
  });
  nodes['busca-termo'].value='11222333000181';
  const pending=nodes['busca-form'].dispatch('submit');
  nodes['busca-termo'].value='00000000E08G12';
  nodes['busca-termo'].dispatch('input');
  initializing.resolve(dataset);
  await pending;
  assert.equal(queried,0);
  assert.equal(fetched,1);
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  await nodes['busca-form'].dispatch('submit');
  assert.equal(queried,1);
  assert.equal(fetched,1, 'sessão válida deve ser reaproveitada');
});

for (const [label, rows] of [
  ['com resultado', [{cnpj:'00000000E08G12',razao_social:'EMPRESA A',uf:'RO'}]],
  ['sem resultado', []],
]) {
  test(`editar entrada limpa estado de busca concluída ${label}, sem I/O extra`, async () => {
    const {nodes,documentRef}=mockDOM();
    let fetches=0,queries=0;
    mountSearch({documentRef,
      fetchDataset:async()=>{fetches++;return dataset;},
      probeRange:async()=>{},
      createEngine:async()=>({db:{},duckdb:{}}),
      attach:async()=>{},
      query:async()=>{queries++;return rows;},
    });
    nodes['busca-termo'].value='EMPRESA A';
    await nodes['busca-form'].dispatch('submit');
    assert.match(nodes['consulta-estado'].textContent,/Competência 2026-07/);
    assert.equal(nodes['consulta-lista'].children.length, rows.length);
    nodes['busca-termo'].value='EMPRESA B';
    nodes['busca-termo'].dispatch('input');
    assert.equal(nodes['consulta-lista'].children.length,0);
    assert.match(nodes['consulta-estado'].textContent,/Termo alterado.*Pesquise novamente/);
    const announcement=nodes['consulta-estado'].textContent;
    nodes['busca-termo'].value='EMPRESA BC';
    nodes['busca-termo'].dispatch('input');
    assert.equal(nodes['consulta-estado'].textContent,announcement,
      'não repetir anúncio da região viva a cada tecla');
    assert.deepEqual([fetches,queries],[1,1], 'editar não dispara fetch, WASM ou consulta');
    await nodes['busca-form'].dispatch('submit');
    assert.deepEqual([fetches,queries],[1,2], 'consulta nova mantém sessão reutilizável');
  });
}

test('erro de transporte não permanece atribuído a outro termo após editar', async () => {
  const {nodes,documentRef}=mockDOM();
  let requests=0;
  mountSearch({documentRef,fetchDataset:async()=>{requests++;throw Error('Falha CORS');}});
  nodes['busca-termo'].value='00000000E08G12';
  await nodes['busca-form'].dispatch('submit');
  assert.match(nodes['consulta-estado'].textContent,/Falha CORS/);
  nodes['busca-termo'].value='11222333000181';
  nodes['busca-termo'].dispatch('input');
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado.*Pesquise novamente/);
  const message=nodes['consulta-estado'].textContent;
  nodes['busca-termo'].value='11222333000182';
  nodes['busca-termo'].dispatch('input');
  assert.equal(nodes['consulta-estado'].textContent,message, 'região viva não deve repetir a cada tecla');
  assert.equal(requests,1,'digitação não abre consulta nem WASM');
  await nodes['busca-form'].dispatch('submit');
  assert.equal(requests,2,'o novo termo ainda permite uma tentativa explícita');
  assert.match(nodes['consulta-estado'].textContent,/Falha CORS/);
});

test('erro de validação e manifesto ausente são feedbacks obsoletos após editar', async () => {
  const {nodes,documentRef}=mockDOM();
  let requests=0;
  mountSearch({documentRef,fetchDataset:async()=>{requests++;return null;}});
  nodes['busca-termo'].value='ab';
  await nodes['busca-form'].dispatch('submit');
  assert.match(nodes['consulta-estado'].textContent,/ao menos três/);
  nodes['busca-termo'].value='abc';
  nodes['busca-termo'].dispatch('input');
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  assert.equal(requests,0);
  await nodes['busca-form'].dispatch('submit');
  assert.match(nodes['consulta-estado'].textContent,/manifesto ausente/);
  nodes['busca-termo'].value='abcd';
  nodes['busca-termo'].dispatch('input');
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  assert.equal(requests,1,'a edição não faz retry automático');
});
