import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {validateManifest,fetchManifest,archiveView,rowCountSeries,snapshotsDescending,countKinds} from '../static/js/catalog.mjs';

const readManifest=async()=>JSON.parse(await readFile(new URL('../../web/public/manifest.json',import.meta.url),'utf8'));

const fixture = () => ({
  current: '2026-07',
  snapshots: [
    {date:'2026-07', files:{companies:{shards:[{shard:'00',size:12},{shard:'01',size:0}]}, cnpjs:{size:5}}, row_counts:{cnpjs:0}},
    {date:'2026-06', files:{companies:{size:7},legacy:{}}, row_counts:{cnpjs:4}},
    {date:'2026-05', files:{}},
  ],
});
const response = manifest => async () => ({status:200,json:async()=>manifest});
const mutations = [
  ['current como array', m=>m.current=[m.current]],
  ['competência como array', m=>m.snapshots[1].date=[m.snapshots[1].date]],
  ['files como array', m=>m.snapshots[1].files=[]],
  ['arquivo nulo', m=>m.snapshots[1].files.legacy=null],
  ['arquivo textual', m=>m.snapshots[1].files.legacy='zip'],
  ['arquivo como array', m=>m.snapshots[1].files.legacy=[]],
  ['shards como objeto', m=>m.snapshots[1].files.legacy={shards:{}}],
  ['shard nulo', m=>m.snapshots[1].files.legacy={shards:[null]}],
  ['prefixo companies inválido', m=>m.snapshots[1].files.companies={shards:[{shard:'x'}]}],
  ['shard duplicado', m=>m.snapshots[1].files.companies={shards:[{shard:'00'},{shard:'00'}]}],
  ['colisão monolítico/shard', m=>{m.snapshots[1].files.companies={shards:[{shard:'00'}]};m.snapshots[1].files['companies/00']={};}],
  ['row_counts como array', m=>m.snapshots[1].row_counts=[4,0]],
  ['row_counts textual', m=>m.snapshots[1].row_counts='4'],
];
for (const [label, mutate] of mutations) {
  test('catálogo: recusa '+label+' antes de exibir competência corrente', async () => {
    const m=fixture();mutate(m);
    assert.throws(()=>validateManifest(m), Error);
    await assert.rejects(fetchManifest({fetchImpl:response(m)}), Error);
    assert.throws(()=>archiveView(m,'2026-07'), Error);
  });
}
test('catálogo: preserva histórico monolítico, shard, vazio e zero declarado', async () => {
  const m=fixture();const before=JSON.stringify(m);
  assert.equal(await fetchManifest({fetchImpl:response(m)}),m);
  assert.deepEqual(archiveView(m,'2026-07').totals,{fileCount:3,bytes:17,sizedCount:3});
  assert.deepEqual(archiveView(m,'2026-06').totals,{fileCount:2,bytes:7,sizedCount:1});
  assert.deepEqual(archiveView(m,'2026-05').totals,{fileCount:0,bytes:0,sizedCount:0});
  assert.deepEqual(rowCountSeries(m,'cnpjs'),[{date:'2026-05',count:null},{date:'2026-06',count:4},{date:'2026-07',count:0}]);
  assert.equal(JSON.stringify(m),before);
});
test('catálogo: manifesto versionado preserva inventário e contagens sem pins históricos no teste', async () => {
  const m=await readManifest();
  const before=JSON.stringify(m);
  assert.equal(await fetchManifest({fetchImpl:response(m)}),m);
  assert.deepEqual(snapshotsDescending(m).map(s=>s.date),m.snapshots.map(s=>s.date).sort().reverse());
  const expectedKinds=new Set();
  for(const s of m.snapshots) {
    const expected=new Map();
    for(const [key,f] of Object.entries(s.files)) {
      const pairs=Array.isArray(f.shards)?f.shards.map(sh=>[key+'/'+sh.shard,sh]):[[key,f]];
      for(const [id,entry] of pairs) {
        assert.equal(expected.has(id),false,'identidade duplicada no manifesto: '+id);
        expected.set(id,Number.isSafeInteger(entry.size)&&entry.size>=0?entry.size:null);
      }
    }
    const entries=archiveView(m,s.date).entries;
    assert.equal(entries.length,expected.size);
    for(const entry of entries) {
      assert.ok(expected.has(entry.key));
      assert.equal(entry.size,expected.get(entry.key));
      expected.delete(entry.key);
    }
    assert.equal(expected.size,0);
    for(const [k,v] of Object.entries(s.row_counts??{})) if(Number.isSafeInteger(v)&&v>=0) expectedKinds.add(k);
  }
  assert.deepEqual([...countKinds(m)].sort(),[...expectedKinds].sort());
  for(const k of expectedKinds) {
    const expected=m.snapshots.map(s=>({date:s.date,count:Number.isSafeInteger(s.row_counts?.[k])&&s.row_counts[k]>=0?s.row_counts[k]:null})).sort((a,b)=>a.date.localeCompare(b.date));
    assert.deepEqual(rowCountSeries(m,k),expected);
  }
  assert.equal(JSON.stringify(m),before);
});
