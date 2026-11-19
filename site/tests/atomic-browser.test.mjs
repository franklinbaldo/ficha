import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fetchCompanyByCnpj, fetchSnapshotLookups} from '../static/js/atomic-browser.mjs';

const date='2026-07', cnpj='33000167000101';
const archive='https://archive.org/download/ficha-2026-07/';
const lookups={schema_version:'1.0.0',snapshot_date:date,
  municipios:{'7107':'SÃO PAULO'},cnaes:{'6201501':'PROGRAMAS DE COMPUTADOR'}};
const bytes=new TextEncoder().encode(JSON.stringify(lookups));
const sha256=bytes=>createHash('sha256').update(bytes).digest('hex');
const manifest={snapshots:[{date,files:{
  companies:{shard_by:'cnpj_base_prefix_2',schema_version:'1.0.0',
    shards:[{shard:'33',url:archive+'companies-33.zip'}]},
  lookups:{url:archive+'lookups.json',size:bytes.byteLength,sha256:sha256(bytes)},
}}]};
const company={cnpj_base:33000167,snapshot_yyyymm:202607,qtd_estabelecimentos:1,
  razao_social:'EMPRESA TESTE',estabelecimentos:[{cnpj_ordem:1,cnpj_dv:1,
    uf:'SP',municipio_codigo:7107,cnae_principal_codigo:6201501,
    opcao_simples:false,opcao_mei:false}]};
const decoder=async()=>({decodeCompany:()=>structuredClone(company)});

test('CNPJ/DV inválido falha antes de buscar o decoder ou chamar a rede', async()=>{
  let loaded=0, fetched=0;
  await assert.rejects(fetchCompanyByCnpj(manifest,date,'33000167000102',{
    loadDecoder:async()=>{loaded++;return decoder();},
    fetchImpl:async()=>{fetched++;throw Error('rede indevida');},
  }),/dígitos verificadores/);
  assert.equal(loaded,0);
  assert.equal(fetched,0);
});

test('registro PB exato recebe município e CNAE do lookup RFB do mês',async()=>{
  const urls=[];
  const result=await fetchCompanyByCnpj(manifest,date,cnpj,{
    loadDecoder:decoder,
    fetchImpl:async url=>{
      urls.push(url);
      return new Response(url.endsWith('.json')?bytes:Uint8Array.of(1,2,3));
    },
    loadLookups:(m,d,opts)=>fetchSnapshotLookups(m,d,{...opts,cacheMap:new Map()}),
  });
  assert.deepEqual(urls,[
    archive+'companies-33.zip/33/000/167.pb',
    archive+'lookups.json',
  ]);
  assert.equal(result.state,'found');
  assert.equal(result.partial,true);
  assert.equal(result.data.municipio_nome,'SÃO PAULO');
  assert.equal(result.data.cnae_principal_descricao,'PROGRAMAS DE COMPUTADOR');
  assert.equal(result.data.porte_empresa,null);
  assert.equal(result.data.opcao_simples,null);
});

test('CNAE RFB com zero inicial mantém sete dígitos no lookup',async()=>{
  const lookup={...lookups,cnaes:{'0600001':'EXTRAÇÃO DE PETRÓLEO'}};
  const body=new TextEncoder().encode(JSON.stringify(lookup));
  const data=structuredClone(manifest);
  data.snapshots[0].files.lookups.size=body.byteLength;
  data.snapshots[0].files.lookups.sha256=sha256(body);
  const modified=structuredClone(company);
  modified.estabelecimentos[0].cnae_principal_codigo=600001;
  const result=await fetchCompanyByCnpj(data,date,cnpj,{
    loadDecoder:async()=>({decodeCompany:()=>modified}),
    fetchImpl:async url=>new Response(url.endsWith('.json')?body:Uint8Array.of(1)),
    loadLookups:(m,d,opts)=>fetchSnapshotLookups(m,d,{...opts,cacheMap:new Map()}),
  });
  assert.equal(result.state,'found');
  assert.equal(result.data.cnae_principal_codigo,600001);
  assert.equal(result.data.cnae_principal_descricao,'EXTRAÇÃO DE PETRÓLEO');
});

test('lookup alterado com mesmo tamanho falha pelo SHA-256 do manifesto',async()=>{
  const corrupted=new Uint8Array(bytes);
  corrupted[corrupted.length-2]^=1;
  await assert.rejects(fetchSnapshotLookups(manifest,date,{
    fetchImpl:async()=>new Response(corrupted),cacheMap:new Map(),
  }),/SHA-256.*difere/);
});

test('lookup com schema incompatível ou ausente é recusado mesmo com SHA e mês corretos',async()=>{
  for (const version of ['2.0.0', null]) {
    const modified=structuredClone(lookups);
    if (version === null) delete modified.schema_version;
    else modified.schema_version=version;
    const payload=new TextEncoder().encode(JSON.stringify(modified));
    const declared=structuredClone(manifest);
    declared.snapshots[0].files.lookups.size=payload.byteLength;
    declared.snapshots[0].files.lookups.sha256=sha256(payload);
    await assert.rejects(fetchSnapshotLookups(declared,date,{
      fetchImpl:async()=>new Response(payload),cacheMap:new Map(),
    }),/versão|schema|incompatível/i);
  }
});

test("cache não ignora tamanho declarado divergente para o mesmo SHA-256",async()=>{
  const cacheMap=new Map();
  let calls=0;
  const fetchImpl=async()=>{calls++;return new Response(bytes);};
  await fetchSnapshotLookups(manifest,date,{fetchImpl,cacheMap});
  const incompatible=structuredClone(manifest);
  incompatible.snapshots[0].files.lookups.size++;
  await assert.rejects(fetchSnapshotLookups(incompatible,date,{fetchImpl,cacheMap}),
    /tamanho divergente/);
  assert.equal(calls,2);
});

test('404 do membro é inconclusivo e não acessa o lookup',async()=>{
  let calls=0;
  const result=await fetchCompanyByCnpj(manifest,date,cnpj,{
    loadDecoder:decoder,
    fetchImpl:async()=>{calls++;return new Response(null,{status:404});},
    loadLookups:async()=>{throw Error('lookup indevido após 404');},
  });
  assert.equal(result.state,'artifact-unavailable');
  assert.equal(result.data,null);
  assert.equal(calls,1);
});

test('503 rejeita, tentativa subsequente é possível sem cache de falha',async()=>{
  let fetched=0;
  const options={loadDecoder:decoder,
    fetchImpl:async()=>{
      fetched++;
      return fetched===1?new Response(null,{status:503}):new Response(Uint8Array.of(1));
    },
    loadLookups:async()=>lookups};
  await assert.rejects(fetchCompanyByCnpj(manifest,date,cnpj,options),/HTTP 503/);
  assert.equal((await fetchCompanyByCnpj(manifest,date,cnpj,options)).state,'found');
  assert.equal(fetched,2);
});

test('404 de lookup, competência trocada e tamanho divergente são erro, não ausência',async()=>{
  const wrong={...lookups,snapshot_date:'2026-06'};
  const payload=new TextEncoder().encode(JSON.stringify(wrong));
  await assert.rejects(fetchSnapshotLookups(manifest,date,{
    fetchImpl:async()=>new Response(null,{status:404}),cacheMap:new Map(),
  }),/Lookup indisponível/);
  // Wrong bytes of the same length must fail on digest before date parsing.
  await assert.rejects(fetchSnapshotLookups(manifest,date,{
    fetchImpl:async()=>new Response(payload),cacheMap:new Map(),
  }),/SHA-256 do lookup difere/);
  // Once the digest is bound to these bytes, the wrong month must fail too.
  const wrongMonthManifest=structuredClone(manifest);
  wrongMonthManifest.snapshots[0].files.lookups.sha256=sha256(payload);
  await assert.rejects(fetchSnapshotLookups(wrongMonthManifest,date,{
    fetchImpl:async()=>new Response(payload),cacheMap:new Map(),
  }),/incompatível/);
  // Size is an independent gate, not merely a side effect of SHA mismatch.
  await assert.rejects(fetchSnapshotLookups(manifest,date,{
    fetchImpl:async()=>new Response(payload.slice(0,-1)),cacheMap:new Map(),
  }),/tamanho divergente/);
  const equalSize=new TextEncoder().encode(JSON.stringify(
    {...lookups,snapshot_date:'2026-08'}));
  const edited=structuredClone(manifest);
  edited.snapshots[0].files.lookups.size=equalSize.byteLength;
  edited.snapshots[0].files.lookups.sha256=sha256(equalSize);
  await assert.rejects(fetchSnapshotLookups(edited,date,{
    fetchImpl:async()=>new Response(equalSize),cacheMap:new Map(),
  }),/incompatível/);
});
