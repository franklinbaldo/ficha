import test from 'node:test';
import assert from 'node:assert/strict';
import { mountSearch, renderCompanies } from '../static/js/search-app.mjs';
import { createDuckDBEngine } from '../static/js/duckdb-runtime.mjs';

class Element {
  constructor(tag = '') { this.tag = tag; this.children = []; this.textContent = ''; this.dataset = {}; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  addEventListener(type, cb) { this[type] = cb; }
}
const mockDOM = () => {
  const nodes = Object.fromEntries(['busca-form','busca-termo','busca-enviar',
    'consulta-estado','consulta-lista'].map(k => [k,new Element(k)]));
  return { nodes, doc: {
    getElementById: id => nodes[id],
    createElement: tag => new Element(tag),
  }};
};
test('resultados são texto puro, não HTML interpretado', () => {
  const { nodes,doc } = mockDOM();
  renderCompanies(doc,nodes['consulta-lista'],[
    {cnpj:'00000000E08G12',razao_social:'<img src=x onerror=alert(1)>',uf:'RO'},
  ]);
  const li = nodes['consulta-lista'].children[0];
  assert.equal(li.children[0].textContent, '<img src=x onerror=alert(1)>');
  assert.equal(li.children[1].textContent, '00000000E08G12 · RO');
  assert.equal(li.children.length, 2);
});
test('formulário só abre sessão com entrada válida; reutiliza DB e anuncia resultado', async () => {
  const { nodes,doc } = mockDOM();
  let fetches=0, probed=0, created=0, attached=0, queried=0;
  mountSearch({documentRef:doc,
    fetchDataset:async () => {fetches++;return {date:'2026-04',
      files:{cnpjs:{url:'https://example.org/cnpjs.parquet',size:1}}};},
    probeRange:async () => {probed++;},
    createEngine:async () => {created++;return {db:{},duckdb:{}};},
    attach:async () => {attached++;},
    query:async (_,term) => {queried++;return [{cnpj:term,razao_social:'Empresa',uf:'RO'}];},
  });
  const submit = () => nodes['busca-form'].submit({preventDefault() {}});
  nodes['busca-termo'].value = 'ab';
  await submit();
  assert.match(nodes['consulta-estado'].textContent,/ao menos três/);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'error');
  assert.equal(fetches,0);
  nodes['busca-termo'].value = '00.000.000/E08G-12';
  await submit();
  assert.equal(nodes['consulta-lista'].children.length,1);
  assert.match(nodes['consulta-estado'].textContent,/2026-04: 1 resultado/);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'info');
  nodes['busca-termo'].value = 'Empresa';
  await submit();
  assert.deepEqual([fetches,probed,created,attached,queried],[1,1,1,1,2]);
  assert.equal(nodes['busca-enviar'].disabled,false);
});
test('HTTP/engine indisponível é visível, busca seguinte pode repetir', async () => {
  const { nodes,doc } = mockDOM();
  let attempts=0;
  mountSearch({documentRef:doc,
    fetchDataset:async () => {attempts++;throw Error('HTTP 500');},
  });
  nodes['busca-termo'].value='00000000E08G12';
  const submit = () => nodes['busca-form'].submit({preventDefault() {}});
  await submit();
  assert.match(nodes['consulta-estado'].textContent,/HTTP 500/);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'error');
  assert.equal(nodes['consulta-lista'].children.length,0);
  nodes['busca-termo'].value='00000000E08G13';
  nodes['busca-termo'].input();
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'info');
  await submit();
  assert.equal(attempts,2);
});
test('manifesto 404 não fica em cache: nova tentativa observa HTTP 500 e depois recuperação', async () => {
  const {nodes,doc} = mockDOM();
  let attempts = 0;
  mountSearch({documentRef:doc,
    fetchDataset: async () => {
      attempts++;
      if (attempts === 1) return null;
      if (attempts === 2) throw Error('HTTP 500');
      return {date:'2026-07', files:{cnpjs:{url:'https://example.org/cnpjs.parquet',size:1}}};
    },
    probeRange: async () => {},
    createEngine: async () => ({db:{},duckdb:{}}),
    attach: async () => {},
    query: async () => [{cnpj:'00000000E08G12',razao_social:'Teste'}],
  });
  nodes['busca-termo'].value = '00000000E08G12';
  const submit = () => nodes['busca-form'].submit({preventDefault() {}});
  await submit();
  assert.match(nodes['consulta-estado'].textContent, /manifesto ausente/);
  await submit();
  assert.equal(attempts, 2, 'HTTP 404 anterior deve permitir novo fetch');
  assert.match(nodes['consulta-estado'].textContent, /HTTP 500/);
  await submit();
  assert.equal(attempts, 3);
  assert.match(nodes['consulta-estado'].textContent, /2026-07: 1 resultado/);
  assert.equal(nodes['busca-enviar'].disabled, false);
});

test('edição durante pedido pendente informa imediatamente sem apresentar resposta obsoleta', async () => {
  const {nodes,doc} = mockDOM();
  let resolveDataset, queries=0;
  const pendingDataset = new Promise(resolve => { resolveDataset=resolve; });
  mountSearch({documentRef:doc,
    fetchDataset: () => pendingDataset,
    probeRange: async () => {},
    createEngine: async () => ({db:{},duckdb:{}}),
    attach: async () => {},
    query: async () => {queries++;return [{cnpj:'00000000E08G12'}];},
  });
  nodes['busca-termo'].value='00000000E08G12';
  const pending = nodes['busca-form'].submit({preventDefault() {}});
  assert.equal(nodes['busca-enviar'].disabled,true);
  nodes['busca-termo'].value='00000000E08G13';
  nodes['busca-termo'].input();
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado.*Aguarde/);
  assert.equal(nodes['consulta-estado'].dataset.kind,'info');
  assert.equal(nodes['consulta-lista'].children.length,0);
  resolveDataset({date:'2026-07',files:{cnpjs:{url:'https://example.org/cnpjs.parquet',size:1}}});
  await pending;
  assert.equal(queries,0,'não executar SQL com parâmetro obsoleto');
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  assert.equal(nodes['busca-enviar'].disabled,false);
});

test('DuckDB runtime usa bundle selecionado e encerra falha de instanciação', async () => {
  const observed=[];
  class FakeWorker {constructor(url){observed.push(['worker',url]);}}
  const fakeURL={createObjectURL:()=>'blob:test',revokeObjectURL:url=>observed.push(['revoke',url])};
  class FakeBlob {constructor(){}}
  class AsyncDuckDB {
    async instantiate(){throw Error('instantiation failed');}
    async terminate(){observed.push(['terminate']);}
  }
  await assert.rejects(createDuckDBEngine({
    loadModule:async () => ({
      getJsDelivrBundles:()=>({}),
      selectBundle:async () => ({mainWorker:'https://example.org/worker.js',mainModule:'https://example.org/main.wasm'}),
      ConsoleLogger:class {},AsyncDuckDB,
    }),
    WorkerImpl:FakeWorker, URLImpl:fakeURL, BlobImpl:FakeBlob,
  }),/instantiation failed/);
  assert.deepEqual(observed,[['worker','blob:test'],['revoke','blob:test'],['terminate']]);
});

test('CORS/Range indisponível impede iniciar WASM e permite nova tentativa', async () => {
  const {nodes,doc} = mockDOM();
  let probes=0, created=0;
  mountSearch({documentRef:doc,
    fetchDataset:async () => ({date:'2026-07',
      files:{cnpjs:{url:'https://archive.org/download/ficha-2026-07/cnpjs.parquet',size:6076313170}}}),
    probeRange:async () => {
      probes++;
      if (probes === 1) throw Error('Origem de dados inacessível no navegador (rede ou CORS)');
    },
    createEngine:async () => {created++;return {db:{},duckdb:{}};},
    attach:async () => {},
    query:async () => [],
  });
  nodes['busca-termo'].value='00000000E08G12';
  const submit = () => nodes['busca-form'].submit({preventDefault() {}});
  await submit();
  assert.match(nodes['consulta-estado'].textContent, /rede ou CORS/);
  assert.deepEqual([created,probes],[0,1], 'não importar WASM na falha de origem');
  await submit();
  assert.deepEqual([created,probes],[1,2], 'recuperação sem cachear falha');
  assert.match(nodes['consulta-estado'].textContent,/nenhum resultado/);
});
