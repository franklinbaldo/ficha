import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { currentDataset, fetchCurrentDataset, probeParquetRange } from '../static/js/dataset.mjs';
import { planCompanyQuery, queryCompanies, attachAnalyticalFiles } from '../static/js/query-core.mjs';

const fixture = async () => JSON.parse(await readFile(
  new URL('../../web/public/manifest.json', import.meta.url), 'utf8'));

test('usa a competência current do manifesto verdadeiro da aplicação', async () => {
  const raw = await fixture();
  const selected = currentDataset(raw);
  assert.equal(selected.date, raw.current);
  assert.equal(selected.files.cnpjs.url, raw.snapshots.find(s => s.date === raw.current).files.cnpjs.url);
});

test('rejeita PWA, current órfão/duplicado e digest inconsistente', async () => {
  const raw = await fixture();
  assert.throws(() => currentDataset({ display:'standalone', start_url:'.' }), /current\/snapshots/);
  assert.throws(() => currentDataset({ ...raw, current:'2099-12' }), /exatamente um snapshot/);
  assert.throws(() => currentDataset({ ...raw, snapshots:[...raw.snapshots, raw.snapshots.find(s => s.date === raw.current)] }), /exatamente um snapshot/);
  const broken = structuredClone(raw);
  const snap = broken.snapshots.find(s => s.date === raw.current);
  snap.files.cnpjs.url = 'javascript:alert(1)';
  assert.throws(() => currentDataset(broken), /protocolo/);
  snap.files.cnpjs.url = 'https://archive.org/cnpjs.parquet';
  snap.files.cnpjs.sha256 = 'broken';
  assert.throws(() => currentDataset(broken), /integridade/);
});

test('busca manifesto sem cache e diferencia 404 de HTTP 500', async () => {
  const raw = await fixture();
  let called;
  const data = await fetchCurrentDataset({fetchImpl: async (...args) => {
    called = args; return {status:200,ok:true,json:async () => raw};
  }});
  assert.equal(data.date, raw.current);
  assert.deepEqual(called, ['/ficha/manifest.json', {cache:'no-store'}]);
  assert.equal(await fetchCurrentDataset({fetchImpl:async () => ({status:404})}), null);
  await assert.rejects(fetchCurrentDataset({fetchImpl:async () => ({status:500,ok:false})}), /HTTP 500/);
});

test('SQL parametrizado preserva CNPJ numérico, alfa e busca por nome', () => {
  for (const [raw,text] of [
    ['11.222.333/0001-81','11222333000181'],
    ['00.000.000/e08g-12','00000000E08G12'],
    ['12.ABC.345/01DE-35','12ABC34501DE35'],
  ]) {
    const q = planCompanyQuery(raw);
    assert.equal(q.param,text); assert.match(q.sql,/WHERE cnpj = \? LIMIT 1/);
  }
  assert.equal(planCompanyQuery('%_\\'),null);
  assert.equal(planCompanyQuery('EMPRESA% TESTE').param,'%EMPRESA TESTE%');
  assert.match(planCompanyQuery('EMPRESA TESTE').sql,/WHERE razao_social ILIKE \? LIMIT 20/);
});

test('fecha statement/conexão inclusive no erro; input curto não consulta', async () => {
  const trace = [];
  const db = {connect:async () => ({
    prepare:async () => ({query:async value => ({toArray:() => [{toJSON:() => ({cnpj:value})}]}),
      close:async () => trace.push('statement')}),
    close:async () => trace.push('connection'),
  })};
  assert.deepEqual(await queryCompanies(db,'00000000E08G12'),[{cnpj:'00000000E08G12'}]);
  assert.deepEqual(trace,['statement','connection']);
  let closed = 0;
  const broken = {connect:async () => ({prepare:async () => {throw Error('failed');},
    close:async () => {closed++;}})};
  assert.deepEqual(await queryCompanies(broken,'ab'),[]);
  await assert.rejects(queryCompanies(broken,'11222333000181'),/failed/);
  assert.equal(closed,1);
});

test('registra cnpjs.parquet pela URL real e rejeita SQL em nomes de lookup', async () => {
  const dataset = currentDataset(await fixture());
  const events = [];
  const db = {
    registerFileURL:async (...args) => events.push(['file',...args]),
    connect:async () => ({query:async sql => events.push(['view',sql]),
      close:async () => events.push(['close'])}),
  };
  const module = {DuckDBDataProtocol:{HTTP:'HTTP'}};
  await attachAnalyticalFiles(db,dataset,module);
  assert.deepEqual(events[0],['file','cnpjs.parquet',dataset.files.cnpjs.url,'HTTP',false]);
  assert.equal(events.at(-1)[0],'close');
  await assert.rejects(attachAnalyticalFiles(db,{...dataset,lookups:{'x; DROP TABLE cnpjs':{url:'https://example.org/x'}}},module),/unsafe lookup/);
});


test('paridade com Astro: curingas não convertem busca textual em CNPJ exato', () => {
  // Classificar o input original, ANTES de remover curingas para o ILIKE.
  for (const [raw, expected] of [
    ['11222333000181%', '%11222333000181%'],
    ['00.000.000/E08G-12%', '%00.000.000/E08G-12%'],
    ['11222333000181_', '%11222333000181%'],
    ['11222333000181\\', '%11222333000181%'],
  ]) {
    const plan = planCompanyQuery(raw);
    assert.equal(plan.mode, 'name', `modo divergente para ${JSON.stringify(raw)}`);
    assert.ok(plan.sql.includes('WHERE razao_social ILIKE ? LIMIT 20'));
    assert.equal(plan.param, expected);
  }
  for (const [raw, canonical] of [
    ['11222333000181', '11222333000181'],
    ['00.000.000/E08G-12', '00000000E08G12'],
  ]) {
    const plan = planCompanyQuery(raw);
    assert.equal(plan.mode, 'cnpj');
    assert.equal(plan.param, canonical);
    assert.ok(plan.sql.includes('WHERE cnpj = ? LIMIT 1'));
  }
});

test('probe remoto pede somente um byte e exige CORS/206/Content-Range visível', async () => {
  const entry = currentDataset(await fixture()).files.cnpjs;
  const calls = [];
  const oneByteBody = () => new ReadableStream({
    start(controller) { controller.enqueue(Uint8Array.of(0x50)); controller.close(); },
  });
  const success = async (url, options) => {
    calls.push([url, options]);
    return {status: 206,
      headers: {get: name => name.toLowerCase() === 'content-range' ?
        `bytes 0-0/${entry.size}` : null},
      body: oneByteBody()};
  };
  await probeParquetRange({url: entry.url, size: entry.size, fetchImpl: success});
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], entry.url);
  const {signal, ...request} = calls[0][1];
  assert.ok(signal instanceof AbortSignal);
  assert.deepEqual(request, {
    method:'GET', mode:'cors', cache:'no-store', headers:{Range:'bytes=0-0'},
  });
  assert.equal(signal.aborted, true);
  await assert.rejects(probeParquetRange({url:entry.url, size:entry.size,
    fetchImpl: async () => {throw new TypeError('Failed to fetch');}}), /rede ou CORS/);
  await assert.rejects(probeParquetRange({url:entry.url, size:entry.size,
    fetchImpl: async () => ({status:200, headers:{get:()=>null}})}), /Range 206/);
  await assert.rejects(probeParquetRange({url:entry.url, size:entry.size,
    fetchImpl: async () => ({status:206, headers:{get:()=>null}})}), /Content-Range/);
  await assert.rejects(probeParquetRange({url:entry.url, size:entry.size,
    fetchImpl: async () => ({status:206,
      headers:{get:()=>`bytes 0-0/${entry.size + 1}`}})}), /compatível/);
});

test('probe Range tem prazo máximo, aborta fetch travado e permite retry', async () => {
  const entry = currentDataset(await fixture()).files.cnpjs;
  let signal;
  await assert.rejects(probeParquetRange({
    url:entry.url, size:entry.size, timeoutMs:25,
    fetchImpl: (_url, options) => {
      signal = options.signal;
      return new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once:true});
      });
    },
  }), /prazo \(timeout\)/);
  assert.equal(signal.aborted, true);
  await assert.rejects(probeParquetRange({
    url:entry.url, size:entry.size, timeoutMs:0,
    fetchImpl: () => {throw Error('não deve alcançar a rede');},
  }), /timeoutMs inválido/);
  // A later attempt must be able to succeed with a fresh request.
  await probeParquetRange({
    url:entry.url, size:entry.size, timeoutMs:25,
    fetchImpl: async () => ({
      status:206, headers:{get:()=>`bytes 0-0/${entry.size}`},
      body:oneByteBody(),
    }),
  });
});

test('206 com headers corretos precisa entregar um byte, jamais vazio ou excessivo', async () => {
  const entry = currentDataset(await fixture()).files.cnpjs;
  const reply = body => ({
    status:206,
    headers:{get:name => name.toLowerCase() === 'content-range' ?
      `bytes 0-0/${entry.size}` : null},
    body,
  });
  const body = chunks => new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(Uint8Array.from(chunk));
      controller.close();
    },
  });
  const check = fakeBody => probeParquetRange({
    url:entry.url, size:entry.size, fetchImpl:async () => reply(fakeBody),
  });
  await assert.rejects(check(null), /sem corpo/);
  await assert.rejects(check(body([])), /exatamente 1 byte/);
  await assert.rejects(check(body([[0x50, 0x41]])), /exatamente 1 byte/);
  await assert.rejects(check(body([[0x50], [0x41]])), /maior que 1 byte/);
  await check(body([[0x50]])); // Positive control: one byte followed by EOF.
});

test('timeout cobre corpo travado mesmo quando o mock ignora AbortSignal', async () => {
  const entry = currentDataset(await fixture()).files.cnpjs;
  let signal;
  await assert.rejects(probeParquetRange({
    url:entry.url, size:entry.size, timeoutMs:25,
    fetchImpl:async (_url, options) => {
      signal = options.signal;
      return {
        status:206, headers:{get:()=>`bytes 0-0/${entry.size}`},
        body:new ReadableStream({
          start(controller) { controller.enqueue(Uint8Array.of(0x50)); },
          // No close(): second read remains pending indefinitely.
        }),
      };
    },
  }), /prazo \(timeout\)/);
  assert.equal(signal.aborted, true);
});
