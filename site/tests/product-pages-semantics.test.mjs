import test from 'node:test';
import assert from 'node:assert/strict';
import { mountArchive, mountCompany, mountExplore, mountHistory,
  renderArchiveSelection, renderExploreAssets } from '../static/js/pages.mjs';

class FakeElement {
  constructor(tag = 'div') {
    this.tag = tag;
    this.children = [];
    this.textContent = '';
    this.value = '';
    this.disabled = false;
    this.dataset = {};
    this.style = {};
    this.listeners = new Map();
    this.attributes = {};
  }
  append(child) {
    this.children.push(child);
    if (this.tag === 'select' && this.children.length === 1) this.value = child.value;
  }
  replaceChildren() { this.children = []; this.textContent = ''; this.value = this.tag === 'select' ? '' : this.value; }
  get options() { return this.children; }
  addEventListener(name, listener) { this.listeners.set(name, listener); }
  setAttribute(name, value) { this.attributes[name] = value; }
}

function fakeDocument(ids = []) {
  const elements = new Map(ids.map(id => [id, new FakeElement(
    id.includes('competencia') || id.endsWith('inicio') || id.endsWith('fim') || id.endsWith('serie') ? 'select' : 'div'
  )]));
  return {
    createElement: tag => new FakeElement(tag),
    getElementById: id => elements.get(id) ?? null,
    at: id => elements.get(id),
  };
}

function manifestWithShards() {
  const snapshot = date => ({
    date, row_counts: { cnpjs: 2 }, files: {
      companies: { shards: [
        { shard: '00', size: 10, url: 'https://archive.org/00' },
        { shard: '01', size: 20, url: 'https://archive.org/01' },
      ] },
      cnpjs: { size: 30, url: 'https://archive.org/cnpjs' },
    },
  });
  return { current: '2026-07', snapshots: [snapshot('2026-06'), snapshot('2026-07')] };
}

async function withPageUrl(fn) {
  const oldLocation = globalThis.location;
  const oldHistory = globalThis.history;
  globalThis.location = new URL('https://franklinbaldo.github.io/ficha/historia/');
  globalThis.history = { replaceState(_state, _title, next) { globalThis.location = new URL(String(next)); } };
  try { return await fn(); }
  finally {
    if (oldLocation === undefined) delete globalThis.location;
    else globalThis.location = oldLocation;
    if (oldHistory === undefined) delete globalThis.history;
    else globalThis.history = oldHistory;
  }
}

test('acervo e exploração contam arquivos expandidos, não conjuntos com shards', async () => {
  const data = manifestWithShards();
  const doc = fakeDocument();
  const acervo = new FakeElement();
  const explorar = new FakeElement();
  renderArchiveSelection(doc, acervo, data, '2026-07');
  renderExploreAssets(doc, explorar, data, '2026-07');
  assert.equal(acervo.children[1].children[0].textContent, '3 arquivos declarados');
  assert.equal(explorar.children[0].textContent, '3 arquivos correspondentes · 2026-07');

  await withPageUrl(async () => {
    const archiveDoc = fakeDocument([
      'acervo-competencia', 'acervo-resultado', 'acervo-estado', 'acervo-competencias',
    ]);
    await mountArchive(archiveDoc, { loadManifest: async () => data });
    assert.equal(archiveDoc.at('acervo-competencias').children[0].children[1].textContent, '3 arquivos');

    const exploreDoc = fakeDocument([
      'explorar-competencia', 'explorar-filtro', 'explorar-estado', 'explorar-grafico',
      'explorar-contador', 'explorar-serie', 'explorar-serie-grafico',
    ]);
    await mountExplore(exploreDoc, { loadManifest: async () => data });
    assert.equal(exploreDoc.at('explorar-contador').textContent, '3 arquivos · 60 B declarados');
  });
});

test('história rejeita competência inicial posterior antes de consultar ou alterar URL', async () => {
  await withPageUrl(async () => {
    const ids = ['historia-competencias', 'historia-inicio', 'historia-fim',
      'historia-form', 'historia-cnpj', 'historia-enviar',
      'historia-estado', 'historia-resultado'];
    const doc = fakeDocument(ids);
    let lookups = 0;
    await mountHistory(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async () => { lookups++; throw new Error('lookup não deve ocorrer'); },
    });
    doc.at('historia-cnpj').value = '00000000E08G12';
    doc.at('historia-inicio').value = '2026-07';
    doc.at('historia-fim').value = '2026-06';
    const originalUrl = globalThis.location.href;
    await doc.at('historia-form').listeners.get('submit')({ preventDefault() {} });
    assert.equal(lookups, 0, 'comparação invertida não deve fazer I/O');
    assert.equal(globalThis.location.href, originalUrl, 'não gravar deep link inválido');
    assert.equal(doc.at('historia-estado').dataset.kind, 'error');
    assert.match(doc.at('historia-estado').textContent, /competência inicial.*anterior.*final/i);
    assert.equal(doc.at('historia-enviar').disabled, false);
    assert.equal(doc.at('historia-resultado').children.length, 0);
  });
});

test('história preserva ordem anterior → posterior numa comparação válida', async () => {
  await withPageUrl(async () => {
    const ids = ['historia-competencias', 'historia-inicio', 'historia-fim',
      'historia-form', 'historia-cnpj', 'historia-enviar',
      'historia-estado', 'historia-resultado'];
    const doc = fakeDocument(ids);
    const lookups = [];
    await mountHistory(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async (_manifest, date, cnpj) => {
        lookups.push([date, cnpj]);
        return { date, cnpj, data: null, source: 'https://archive.org/' + date };
      },
    });
    doc.at('historia-cnpj').value = '00000000E08G12';
    doc.at('historia-inicio').value = '2026-06';
    doc.at('historia-fim').value = '2026-07';
    await doc.at('historia-form').listeners.get('submit')({ preventDefault() {} });
    assert.deepEqual(lookups.map(([date]) => date), ['2026-06', '2026-07']);
    assert.equal(doc.at('historia-estado').dataset.kind, 'info');
    assert.match(doc.at('historia-estado').textContent, /Comparação concluída/);
  });
});

test('ficha descarta resposta tardia após mudança de CNPJ sem publicar URL antiga', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['empresa-cnpj', 'empresa-competencia', 'empresa-form',
      'empresa-estado', 'empresa-resultado', 'empresa-enviar']);
    let finish, lookups = 0;
    await mountCompany(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async () => {
        lookups++;
        return new Promise(resolve => { finish = resolve; });
      },
    });
    doc.at('empresa-cnpj').value = '00000000E08G12';
    const submit = () => doc.at('empresa-form').listeners.get('submit')({ preventDefault() {} });
    const pending = submit();
    assert.equal(doc.at('empresa-enviar').disabled, true);
    await submit(); // a synthetic repeated submit must not start another remote query
    assert.equal(lookups, 1);
    doc.at('empresa-cnpj').value = '00000000E08G13';
    finish({ date: '2026-07', cnpj: '00000000E08G12',
      source: 'https://archive.org/old', data: { razao_social: 'Resultado antigo' } });
    await pending;
    assert.match(doc.at('empresa-estado').textContent, /alterados durante a consulta/i);
    assert.equal(doc.at('empresa-resultado').children.length, 0);
    assert.equal(globalThis.location.search, '', 'deep link must not record the stale request');
    assert.equal(doc.at('empresa-enviar').disabled, false);
  });
});

test('ficha não transforma erro tardio de outra seleção em falha da seleção atual', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['empresa-cnpj', 'empresa-competencia', 'empresa-form',
      'empresa-estado', 'empresa-resultado', 'empresa-enviar']);
    let fail;
    await mountCompany(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async () => new Promise((_, reject) => { fail = reject; }),
    });
    doc.at('empresa-cnpj').value = '00000000E08G12';
    const pending = doc.at('empresa-form').listeners.get('submit')({ preventDefault() {} });
    doc.at('empresa-competencia').value = '2026-06';
    fail(new Error('falha da consulta antiga'));
    await pending;
    assert.match(doc.at('empresa-estado').textContent, /alterados durante a consulta/i);
    assert.doesNotMatch(doc.at('empresa-estado').textContent, /falha da consulta antiga/);
    assert.equal(doc.at('empresa-resultado').children.length, 0);
    assert.equal(globalThis.location.search, '');
  });
});

test('história interrompe segundo lookup quando a seleção muda após o primeiro', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['historia-competencias', 'historia-inicio', 'historia-fim',
      'historia-form', 'historia-cnpj', 'historia-enviar',
      'historia-estado', 'historia-resultado']);
    let finishOlder;
    const lookups = [];
    await mountHistory(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async (_manifest, date, cnpj) => {
        lookups.push([date, cnpj]);
        if (date === '2026-06') return new Promise(resolve => { finishOlder = resolve; });
        throw new Error('second lookup must not run after a selection change');
      },
    });
    doc.at('historia-cnpj').value = '00000000E08G12';
    doc.at('historia-inicio').value = '2026-06';
    doc.at('historia-fim').value = '2026-07';
    const submit = () => doc.at('historia-form').listeners.get('submit')({ preventDefault() {} });
    const pending = submit();
    await submit(); // reject reentrant comparisons during the first lookup
    assert.equal(lookups.length, 1);
    doc.at('historia-fim').value = '2026-06';
    finishOlder({ date: '2026-06', source: 'https://archive.org/old', data: null });
    await pending;
    assert.deepEqual(lookups.map(item => item[0]), ['2026-06']);
    assert.equal(doc.at('historia-resultado').children.length, 0);
    assert.match(doc.at('historia-estado').textContent, /alterados durante a comparação/i);
    assert.equal(globalThis.location.search, '');
    assert.equal(doc.at('historia-enviar').disabled, false);
  });
});

test('história descarta segunda resposta tardia após mudança do CNPJ', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['historia-competencias', 'historia-inicio', 'historia-fim',
      'historia-form', 'historia-cnpj', 'historia-enviar',
      'historia-estado', 'historia-resultado']);
    let finishNewer;
    await mountHistory(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async (_manifest, date, cnpj) => date === '2026-06'
        ? { date, cnpj, source: 'https://archive.org/old', data: null }
        : new Promise(resolve => { finishNewer = resolve; }),
    });
    doc.at('historia-cnpj').value = '00000000E08G12';
    doc.at('historia-inicio').value = '2026-06';
    doc.at('historia-fim').value = '2026-07';
    const pending = doc.at('historia-form').listeners.get('submit')({ preventDefault() {} });
    await Promise.resolve(); // first lookup resolved; second has started
    assert.equal(typeof finishNewer, 'function');
    doc.at('historia-cnpj').value = '00000000E08G13';
    finishNewer({ date: '2026-07', source: 'https://archive.org/new', data: null });
    await pending;
    assert.equal(doc.at('historia-resultado').children.length, 0);
    assert.match(doc.at('historia-estado').textContent, /alterados durante a comparação/i);
    assert.equal(globalThis.location.search, '');
  });
});


test('ficha: editar após resultado limpa deep link anterior mas preserva parâmetros alheios', async () => {
  await withPageUrl(async () => {
    globalThis.location.search = '?origem=exemplo';
    const doc = fakeDocument(['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar']);
    let lookups = 0;
    await mountCompany(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async (_manifest,date,cnpj) => {
        lookups++;
        return {date,cnpj,data:{razao_social:'Empresa anterior'},source:'https://archive.org/old'};
      },
    });
    doc.at('empresa-cnpj').value = '00000000E08G12';
    await doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    assert.ok(doc.at('empresa-resultado').children.length > 0);
    assert.equal(new URLSearchParams(globalThis.location.search).get('cnpj'), '00000000E08G12');
    doc.at('empresa-cnpj').value = '00000000E08G13';
    doc.at('empresa-cnpj').listeners.get('input')();
    assert.equal(doc.at('empresa-resultado').children.length, 0);
    assert.match(doc.at('empresa-estado').textContent,/Seleção alterada.*Pesquise novamente/);
    assert.equal(lookups,1,'editar não consulta automaticamente');
    assert.equal(globalThis.location.search, '?origem=exemplo', 'não manter CNPJ anterior na URL');
    const reloadDoc = fakeDocument(['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar']);
    await mountCompany(reloadDoc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async () => { lookups++; throw new Error('recarregar não deve consultar A'); },
    });
    assert.equal(lookups, 1, 'recarregar com URL limpa não reconsulta a seleção antiga');
  });
});

test('ficha: edição A→B→A durante lookup descarta resposta antiga', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['empresa-cnpj','empresa-competencia','empresa-form',
      'empresa-estado','empresa-resultado','empresa-enviar']);
    let finish;
    await mountCompany(doc, {
      loadManifest: async () => manifestWithShards(),
      lookup: async () => new Promise(resolve => { finish = resolve; }),
    });
    const input = doc.at('empresa-cnpj');
    input.value = '00000000E08G12';
    const pending = doc.at('empresa-form').listeners.get('submit')({preventDefault(){}});
    input.value = '00000000E08G13';
    input.listeners.get('input')();
    input.value = '00000000E08G12';
    input.listeners.get('input')();
    finish({date:'2026-07',cnpj:'00000000E08G12',
      data:{razao_social:'Resultado antigo'},source:'https://archive.org/old'});
    await pending;
    assert.equal(doc.at('empresa-resultado').children.length,0);
    assert.match(doc.at('empresa-estado').textContent,/alterados durante a consulta/i);
    assert.equal(globalThis.location.search,'','não publicar URL de requisição alterada');
  });
});

test('história: editar competência limpa link antigo mas mantém parâmetros alheios', async () => {
  await withPageUrl(async () => {
    globalThis.location.search = '?origem=exemplo';
    const doc = fakeDocument(['historia-competencias','historia-inicio','historia-fim',
      'historia-form','historia-cnpj','historia-enviar','historia-estado','historia-resultado']);
    let calls=0;
    await mountHistory(doc,{
      loadManifest:async()=>manifestWithShards(),
      lookup:async (_manifest,date,cnpj)=>{
        calls++;
        return {date,cnpj,data:null,source:'https://archive.org/'+date};
      },
    });
    doc.at('historia-cnpj').value='00000000E08G12';
    doc.at('historia-inicio').value='2026-06';
    doc.at('historia-fim').value='2026-07';
    await doc.at('historia-form').listeners.get('submit')({preventDefault(){}});
    assert.ok(doc.at('historia-resultado').children.length>0);
    assert.equal(new URLSearchParams(globalThis.location.search).get('de'), '2026-06');
    doc.at('historia-fim').value='2026-06';
    doc.at('historia-fim').listeners.get('change')();
    assert.equal(doc.at('historia-resultado').children.length,0);
    assert.match(doc.at('historia-estado').textContent,/Seleção alterada.*Pesquise novamente/);
    assert.equal(calls,2);
    assert.equal(globalThis.location.search, '?origem=exemplo');
    const reloadDoc = fakeDocument(['historia-competencias','historia-inicio','historia-fim',
      'historia-form','historia-cnpj','historia-enviar','historia-estado','historia-resultado']);
    await mountHistory(reloadDoc, {loadManifest:async()=>manifestWithShards()});
    assert.equal(reloadDoc.at('historia-cnpj').value, '', 'URL limpa não restaura CNPJ antigo');
  });
});

test('história: edição A→B→A enquanto segunda fonte demora descarta comparação', async () => {
  await withPageUrl(async () => {
    const doc = fakeDocument(['historia-competencias','historia-inicio','historia-fim',
      'historia-form','historia-cnpj','historia-enviar','historia-estado','historia-resultado']);
    let finishNewer;
    await mountHistory(doc,{
      loadManifest:async()=>manifestWithShards(),
      lookup:async (_manifest,date,cnpj)=>date==='2026-06'
        ? {date,cnpj,data:null,source:'https://archive.org/old'}
        : new Promise(resolve=>{finishNewer=resolve;}),
    });
    doc.at('historia-cnpj').value='00000000E08G12';
    doc.at('historia-inicio').value='2026-06';
    doc.at('historia-fim').value='2026-07';
    const pending=doc.at('historia-form').listeners.get('submit')({preventDefault(){}});
    await Promise.resolve();
    assert.equal(typeof finishNewer,'function');
    doc.at('historia-cnpj').value='00000000E08G13';
    doc.at('historia-cnpj').listeners.get('input')();
    doc.at('historia-cnpj').value='00000000E08G12';
    doc.at('historia-cnpj').listeners.get('input')();
    finishNewer({date:'2026-07',data:null,source:'https://archive.org/new'});
    await pending;
    assert.equal(doc.at('historia-resultado').children.length,0);
    assert.match(doc.at('historia-estado').textContent,/alterados durante a comparação/i);
    assert.equal(globalThis.location.search,'');
  });
});
