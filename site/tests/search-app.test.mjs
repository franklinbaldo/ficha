import test from 'node:test';
import assert from 'node:assert/strict';
import { mountSearch, renderCompanies } from '../static/js/search-app.mjs';
import { createDuckDBEngine } from '../static/js/duckdb-runtime.mjs';

class Element {
  constructor(tag = '') { this.tag = tag; this.children = []; this.textContent = ''; this.attributes = {}; this.dataset = {}; }
  setAttribute(name, value) { this.attributes[name] = value; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  addEventListener(type, cb) { this[type] = cb; }
}
const mockDOM = () => {
  const nodes = Object.fromEntries(['busca-form','busca-termo','busca-enviar',
    'consulta-estado','consulta-lista','consulta-recuperacao'].map(k => [k,new Element(k)]));
  nodes['consulta-recuperacao'].hidden = true;
  return { nodes, doc: {
    getElementById: id => nodes[id],
    createElement: tag => new Element(tag),
  }};
};

test('falha analítica revela alternativa no acervo, edição e recuperação a ocultam', async () => {
  const {nodes,doc}=mockDOM();
  let fetches=0;
  mountSearch({documentRef:doc,
    fetchDataset:async()=> {
      fetches++;
      if(fetches===1)throw Error('Origem de dados inacessível no navegador (rede ou CORS)');
      return {date:'2026-07',files:{cnpjs:{url:'https://example.org/cnpjs.parquet',size:1}}};
    },
    probeRange:async()=>{},
    createEngine:async()=>({db:{},duckdb:{}}),
    attach:async()=>{},
    query:async()=>[{cnpj:'33000167000101',razao_social:'Empresa localizada'}],
  });
  const input=nodes['busca-termo'], recovery=nodes['consulta-recuperacao'];
  const submit=()=>nodes['busca-form'].submit({preventDefault(){}});
  assert.equal(recovery.hidden,true);
  input.value='PETROBRAS';
  await submit();
  assert.equal(fetches,1);
  assert.equal(recovery.hidden,false,'erro de rede revela caminho alternativo');
  assert.equal(nodes['consulta-estado'].dataset.kind,'error');
  assert.match(nodes['consulta-estado'].textContent,/rede ou CORS/);
  assert.match(nodes['consulta-estado'].textContent,/não comprova ausência cadastral.*Acervo/,
    'região de status anuncia a alternativa sem mover o link para dentro dela');
  assert.equal(nodes['consulta-lista'].children.length,0);
  input.value='PETROBRAS SA';
  input.input();
  assert.equal(recovery.hidden,true,'aviso anterior não sobrevive a nova seleção');
  await submit();
  assert.equal(fetches,2);
  assert.equal(recovery.hidden,true,'sucesso não mantém ajuda de erro');
  assert.match(nodes['consulta-estado'].textContent,/1 resultado encontrado/);
  assert.equal(nodes['consulta-lista'].children.length,1);
  const {readFile}=await import('node:fs/promises');
  const html=await readFile(new URL('../content/index.md',import.meta.url),'utf8');
  assert.match(html,/<p id="consulta-recuperacao"[^>]*hidden>/);
  assert.ok(html.includes('<a href="/ficha/acervo/">consultar os arquivos'));
});

test('resposta obsoleta após edição não revela recuperação de busca antiga', async () => {
  const {nodes,doc}=mockDOM();
  let fail;
  mountSearch({documentRef:doc,
    fetchDataset:()=>new Promise((_,reject)=>{fail=reject;}),
  });
  const input=nodes['busca-termo'];
  input.value='PETROBRAS';
  const pending=nodes['busca-form'].submit({preventDefault(){}});
  input.value='PETROBRAS NOVA';
  input.input();
  fail(Error('HTTP 500 da consulta anterior'));
  await pending;
  assert.equal(nodes['consulta-recuperacao'].hidden,true);
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  assert.doesNotMatch(nodes['consulta-estado'].textContent,/HTTP 500/);
  assert.doesNotMatch(nodes['consulta-estado'].textContent,/Acervo/,
    'resposta atrasada não pode anunciar alternativa da consulta obsoleta');
});

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
test('só CNPJs numéricos têm ação acessível para a ficha Protobuf v1', () => {
  const {nodes,doc} = mockDOM();
  renderCompanies(doc, nodes['consulta-lista'], [
    {cnpj:'00.000.000/e08g-12',razao_social:'CNPJ alfanumérico',uf:'RO'},
    {cnpj:'11.222.333/0001-81',razao_social:'Empresa repetida',uf:'RO'},
    {cnpj:'33.000.167/0001-01',razao_social:'Empresa repetida',uf:'RJ'},
    {cnpj:'INVALIDO',razao_social:'Sem link'},
    {cnpj:'12.345.678/0001-99',razao_social:'DV inválido'},
  ]);
  const items = nodes['consulta-lista'].children;
  assert.equal(items.length, 5);
  assert.equal(items[0].children[1].textContent, '00.000.000/e08g-12 · RO');
  assert.equal(items[0].children[1].children.length, 0,
    'resultado alfanumérico permanece legível, mas não abre leitor apenas numérico');
  const expected = ['11222333000181', '33000167000101'];
  for (const [index, cnpj] of expected.entries()) {
    const details = items[index + 1].children[1];
    assert.equal(details.children[0], ' · ', 'separador antes da ação');
    const link = details.children[1];
    assert.equal(link.tag, 'a');
    assert.equal(link.textContent, 'Abrir ficha');
    assert.equal(link.href, '/ficha/empresa/?cnpj=' + cnpj);
    assert.equal(link.attributes['aria-label'], 'Abrir ficha do CNPJ ' + cnpj);
  }
  assert.equal(items[3].children[1].children.length, 0,
    'não criar atalho para CNPJ malformado');
  assert.equal(items[4].children[1].children.length, 0,
    'CNPJ numérico com dígitos verificadores inválidos não deve oferecer ficha');
  assert.notEqual(items[1].children[1].children[1].attributes['aria-label'],
    items[2].children[1].children[1].attributes['aria-label']);
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
  assert.match(nodes['consulta-estado'].textContent, /Acervo/,
    'manifesto ausente também anuncia alternativa na região viva');
  await submit();
  assert.equal(attempts, 2, 'HTTP 404 anterior deve permitir novo fetch');
  assert.match(nodes['consulta-estado'].textContent, /HTTP 500/);
  await submit();
  assert.equal(attempts, 3);
  assert.match(nodes['consulta-estado'].textContent, /2026-07: 1 resultado/);
  assert.doesNotMatch(nodes['consulta-estado'].textContent, /Acervo/,
    'sucesso posterior não herda texto de recuperação');
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


test('home encaminha somente CNPJ numérico exato à ficha PB, sem abrir Parquet/WASM', async () => {
  const {nodes,doc} = mockDOM();
  const routes=[], queries=[];
  let fetched=0, probed=0, engines=0;
  mountSearch({
    documentRef:doc,
    openNumericFicha:url=>routes.push(url),
    fetchDataset:async()=>{fetched++;return {date:'2026-07',
      files:{cnpjs:{url:'https://example.org/cnpjs.parquet',size:1}}};},
    probeRange:async()=>{probed++;},
    createEngine:async()=>{engines++;return {db:{},duckdb:{}};},
    attach:async()=>{},
    query:async(_,term)=>{queries.push(term);return [{cnpj:term,razao_social:'Empresa'}];},
  });
  const submit=()=>nodes['busca-form'].submit({preventDefault() {}});
  nodes['busca-termo'].value='11.222.333/0001-81';
  await submit();
  assert.deepEqual(routes,['/ficha/empresa/?cnpj=11222333000181']);
  assert.deepEqual([fetched,probed,engines,queries.length],[0,0,0,0],
    'número completo não toca na origem Parquet nem em WASM');
  assert.equal(nodes['consulta-lista'].children.length,0);
  assert.match(nodes['consulta-estado'].textContent,/Protobuf/);
  assert.equal(nodes['busca-enviar'].disabled,undefined,'navegação não deixa o botão bloqueado');

  // The existing analytics route is still used for alphanumeric CNPJ and names:
  // never misroute these into a numeric-only PB v1 decoder.
  nodes['busca-termo'].value='00.000.000/E08G-12';
  await submit();
  assert.deepEqual(routes,['/ficha/empresa/?cnpj=11222333000181']);
  assert.deepEqual(queries,['00.000.000/E08G-12']);
  nodes['busca-termo'].value='PETROBRAS';
  await submit();
  assert.deepEqual(queries,['00.000.000/E08G-12','PETROBRAS']);
  assert.deepEqual([fetched,probed,engines],[1,1,1]);
});

test('home numérica não mascara erro de navegação, nem contamina consulta subsequente', async () => {
  const {nodes,doc}=mockDOM();
  let fetches=0;
  mountSearch({documentRef:doc,
    openNumericFicha:()=>{throw new Error('navegação recusada');},
    fetchDataset:async()=>{fetches++;throw new Error('CORS');},
  });
  nodes['busca-termo'].value='11222333000181';
  await nodes['busca-form'].submit({preventDefault() {}});
  assert.match(nodes['consulta-estado'].textContent,/navegação recusada/);
  assert.equal(nodes['consulta-estado'].dataset.kind,'error');
  assert.equal(fetches,0);
  nodes['busca-termo'].value='EMPRESA';
  nodes['busca-termo'].input();
  assert.match(nodes['consulta-estado'].textContent,/Termo alterado/);
  await nodes['busca-form'].submit({preventDefault() {}});
  assert.equal(fetches,1,'a busca por nome mantém caminho de falha CORS');
  assert.match(nodes['consulta-estado'].textContent,/CORS/);
});

test('boot da home ativa explicitamente a rota numérica Protobuf', async () => {
  const {readFile} = await import('node:fs/promises');
  const boot = await readFile(new URL('../static/js/boot.mjs',import.meta.url),'utf8');
  assert.match(boot,/openNumericFicha:\s*url\s*=>\s*globalThis\.location\.assign\(url\)/);
});


test('home bloqueia CNPJ numérico com DV inválido antes da rota ou de qualquer consulta remota', async () => {
  const {nodes,doc} = mockDOM();
  const routes = [];
  let fetched = 0, probed = 0, engines = 0;
  mountSearch({
    documentRef: doc,
    openNumericFicha: url => routes.push(url),
    fetchDataset: async () => { fetched++; throw Error('nenhuma busca esperada'); },
    probeRange: async () => { probed++; },
    createEngine: async () => { engines++; throw Error('WASM inesperado'); },
  });
  const submit = () => nodes['busca-form'].submit({preventDefault() {}});
  nodes['busca-termo'].value = '12.345.678/0001-99';
  await submit();
  assert.deepEqual(routes, []);
  assert.deepEqual([fetched,probed,engines], [0,0,0]);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'error');
  assert.match(nodes['consulta-estado'].textContent, /dígitos verificadores/);
  assert.equal(nodes['consulta-lista'].children.length, 0);

  // A rejection cannot poison the next valid, local Protobuf navigation.
  nodes['busca-termo'].value = '33.000.167/0001-01';
  await submit();
  assert.deepEqual(routes, ['/ficha/empresa/?cnpj=33000167000101']);
  assert.deepEqual([fetched,probed,engines], [0,0,0]);
  assert.equal(nodes['consulta-estado'].dataset.kind, 'info');
});
