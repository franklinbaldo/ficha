import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { archivalFileEntries, archiveView, catalogueTotals, competenceLabel,
  fetchManifest, formatBytes, safeExternalURL, snapshotsDescending } from '../static/js/catalog.mjs';
import { canonicalCnpj, compareRecords, fetchRecordAt } from '../static/js/record.mjs';

const read = path => readFile(new URL(path,import.meta.url),'utf8');
const manifest = async () => JSON.parse(await read('../../web/public/manifest.json'));

test('catálogo usa os snapshots reais e não inventa competências', async () => {
  const raw=await manifest();
  const ordered=snapshotsDescending(raw);
  assert.ok(ordered.length>0);
  assert.ok(ordered.some(s=>s.date===raw.current));
  assert.deepEqual(ordered.map(s=>s.date),ordered.map(s=>s.date).sort().reverse());
  assert.match(competenceLabel(raw.current),/de 20\d\d/);
  const result=archiveView(raw,raw.current);
  const selected=raw.snapshots.find(s=>s.date===raw.current);
  assert.equal(result.selected.date,selected.date);
  assert.equal(result.entries.length,Object.values(selected.files)
    .filter(f=>f && typeof f==='object' && !Array.isArray(f)).length);
  assert.equal(result.totals.fileCount,catalogueTotals(selected).fileCount);
  assert.ok(result.entries.some(f=>f.key==='cnpjs'));
});

test('só URLs HTTPS e hashes de 64 caracteres entram na apresentação', () => {
  assert.equal(safeExternalURL('javascript:alert(1)'),null);
  assert.equal(safeExternalURL('http://example.org/x'),null);
  assert.equal(safeExternalURL('not-url'),null);
  assert.equal(safeExternalURL('https://archive.org/a'),'https://archive.org/a');
  const entries=archivalFileEntries({files:{
    toxic:{url:'javascript:alert(1)',sha256:'123',size:12},
    good:{url:'https://example.com/data',sha256:'f'.repeat(64),size:2048},
  }});
  assert.equal(entries.find(x=>x.key==='toxic').url,null);
  assert.equal(entries.find(x=>x.key==='toxic').sha256,null);
  assert.equal(entries.find(x=>x.key==='good').sha256,'f'.repeat(64));
  assert.match(formatBytes(2048),/2 KB/);
  assert.match(formatBytes(null),/não declarado/);
});

test('um HTTP 404 não é interpretado como catálogo vazio', async () => {
  await assert.rejects(fetchManifest({fetchImpl:async()=>({ok:false,status:404})}),/HTTP 404/);
  await assert.rejects(fetchManifest({fetchImpl:async()=>({ok:true,json:async()=>({current:'2026-07',snapshots:[]})})}),/ausente/);
});

test('CNPJ alfanumérico é textual, com validação antes de qualquer I/O', async () => {
  assert.equal(canonicalCnpj('00.000.000/e08g-12'),'00000000E08G12');
  assert.equal(canonicalCnpj('12.ABC.345/01DE-35'),'12ABC34501DE35');
  assert.equal(canonicalCnpj('123'),null);
});

test('leitor de ficha usa snapshot pedido, sonda transporte, consulta e libera WASM', async () => {
  const raw=await manifest();
  const date=raw.current;
  const events=[];
  const expected=raw.snapshots.find(s=>s.date===date).files.cnpjs;
  const result=await fetchRecordAt(raw,date,'00.000.000/E08G-12',{
    probeRange:async arg=>events.push(['probe',arg]),
    createEngine:async()=>{events.push(['wasm']);return {db:{terminate:async()=>events.push(['terminate'])},duckdb:{}};},
    attach:async()=>events.push(['attach']),
    query:async(_db,term)=>{events.push(['query',term]);return [{cnpj:term,razao_social:'Teste de contrato'}];},
  });
  assert.deepEqual(events.map(x=>x[0]),['probe','wasm','attach','query','terminate']);
  assert.deepEqual(events[0][1],{url:expected.url,size:expected.size});
  assert.equal(events[3][1],'00000000E08G12');
  assert.equal(result.source,expected.url);
  assert.equal(result.date,date);
  assert.equal(result.data.razao_social,'Teste de contrato');
});

test('erro na consulta encerra instância e não fabrica resultados', async () => {
  const raw=await manifest();let closed=0,called=0;
  await assert.rejects(fetchRecordAt(raw,raw.current,'00000000E08G12',{
    probeRange:async()=>{called++;},
    createEngine:async()=>({db:{terminate:async()=>{closed++;}},duckdb:{}}),
    attach:async()=>{},
    query:async()=>{throw Error('Sem CORS');},
  }),/Sem CORS/);
  assert.equal(closed,1);
  await assert.rejects(fetchRecordAt(raw,raw.current,'INVÁLIDO',{
    probeRange:async()=>{called++;},
  }),/CNPJ completo/);
  assert.equal(called,1);
});

test('diff é observação, não prova de evento cadastral', () => {
  const rows=compareRecords(
    {cnpj:'00000000E08G12',razao_social:'EMPRESA A',uf:'RO'},
    {cnpj:'00000000E08G12',razao_social:'EMPRESA B',uf:'RO'});
  assert.equal(rows.find(r=>r.field==='razao_social').changed,true);
  assert.equal(rows.find(r=>r.field==='uf').changed,false);
  assert.deepEqual(compareRecords(null,null),[]);
});

test('SSG tem páginas novas com formulários e navegação, sem Astro/Svelte', async () => {
  for(const [path,marker] of [
    ['../content/empresa.md','id="empresa-form"'],
    ['../content/historia.md','id="historia-form"'],
    ['../content/explorar.md','id="explorar-grafico"'],
    ['../content/acervo.md','id="acervo-competencia"'],
  ]){
    const source=await read(path);
    assert.ok(source.includes(marker),path+' sem '+marker);
    assert.match(source,/layout: product/);
    assert.doesNotMatch(source,/astro-island|svelte/i);
  }
  const template=await read('../templates/product.html');
  for(const marker of ['href="/ficha/historia/"','href="/ficha/explorar/"',
    'href="/ficha/acervo/"','/ficha/js/pages.mjs','{{!content}}']) {
    assert.ok(template.includes(marker),marker);
  }
  const home=await read('../content/index.md');
  assert.ok(home.includes('Cada empresa tem'));
  assert.ok(home.includes('id="busca-form"'));
});
