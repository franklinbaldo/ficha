import test from 'node:test';
import assert from 'node:assert/strict';
import {MAX_COMPANY_BYTES, planAtomicRecord, readBoundedMember,
  fetchAtomicRecord} from '../static/js/atomic-record.mjs';

const DATE = '2026-07';
const CNPJ = '33000167000101';
const archive = 'https://archive.org/download/ficha-2026-07/companies-33.zip';
const manifest = { snapshots: [{date:DATE, files:{companies:{
  shard_by:'cnpj_base_prefix_2',schema_version:'1.0.0',
  shards:[{shard:'33',url:archive}],
}}}]};
const company = {cnpj_base:33000167,snapshot_yyyymm:202607,razao_social:'EMPRESA TESTE',
  porte_empresa:1,estabelecimentos:[{cnpj_ordem:1,cnpj_dv:1,uf:'RJ',
    municipio_codigo:7107,cnae_principal_codigo:6201501,opcao_simples:false,
    opcao_mei:true,data_opcao_mei:20260711}],socios:[{tipo:2}]};
function mock(status=200, bytes=[1,2,3]) {
  return async () => new Response(status===404?null:new Uint8Array(bytes),{status});
}
const decodeCompany = () => structuredClone(company);
const lookup=(m=manifest,d=DATE,c=CNPJ,opts={}) => fetchAtomicRecord(m,d,c,{fetchImpl:mock(),decodeCompany,...opts});

test('CNPJ completo resolve somente o membro do shard declarado',()=>{
  assert.equal(planAtomicRecord(manifest,DATE,'33.000.167/0001-01').memberUrl,
    archive+'/33/000/167.pb');
  assert.equal(planAtomicRecord(manifest,DATE,CNPJ).archiveUrl,archive);
});
test('sem rota silenciosa para CNPJ alfanumérico, mês ausente ou legado sem camada',()=>{
  assert.throws(()=>planAtomicRecord(manifest,DATE,'33000167E08G12'),/1\.1/);
  assert.throws(()=>planAtomicRecord(manifest,'2026-04',CNPJ),/não publicada/);
  assert.throws(()=>planAtomicRecord({snapshots:[{date:DATE,files:{companies_zip:{url:archive}}}]},DATE,CNPJ),/não declarada/);
  assert.throws(()=>planAtomicRecord(manifest,DATE,'insuficiente'),/14 caracteres/);
});
test('rejeita troca de URL entre slots, versão indevida e duplicata de competência',()=>{
  const changed=structuredClone(manifest);
  changed.snapshots[0].files.companies.shards[0].url=archive.replace('33.zip','34.zip');
  assert.throws(()=>planAtomicRecord(changed,DATE,CNPJ),/URL divergente/);
  changed.snapshots[0].files.companies.shards[0].url=archive;
  changed.snapshots[0].files.companies.schema_version='1.1.0';
  assert.throws(()=>planAtomicRecord(changed,DATE,CNPJ),/v1 não declarada/);
  assert.throws(()=>planAtomicRecord({snapshots:[...manifest.snapshots,...manifest.snapshots]},DATE,CNPJ),/duplicada/);
});
test('guarda 32 MiB configurada, Content-Length e corpo fragmentado',async()=>{
  assert.equal(MAX_COMPANY_BYTES,32*1024*1024);
  await assert.rejects(readBoundedMember(new Response(Uint8Array.of(1),{
    headers:{'content-length':'100'}}),3),/orçamento/);
  const res = new Response(new ReadableStream({start(controller){
    controller.enqueue(Uint8Array.of(1,2));controller.enqueue(Uint8Array.of(3,4));controller.close();
  }}));
  await assert.rejects(readBoundedMember(res,3),/orçamento/);
  assert.deepEqual([...await readBoundedMember(new Response(Uint8Array.of(1,2,3)),3)],[1,2,3]);
  await assert.rejects(readBoundedMember(new Response(Uint8Array.of(1)),0),/inválido/);
});
test('decoder canônico injetado recebe bytes, sem baixar ZIP ou Parquet',async()=>{
  const urls=[];let observed;
  const result=await lookup(manifest,DATE,CNPJ,{fetchImpl:async(url,options)=>{
    urls.push([url,options.mode]);return new Response(Uint8Array.of(23,42));
  },decodeCompany:bytes=>{observed=[...bytes];return structuredClone(company);}});
  assert.deepEqual(urls,[[archive+'/33/000/167.pb','cors']]);
  assert.deepEqual(observed,[23,42]);
  assert.equal(result.data.cnpj,CNPJ);
  assert.equal(result.partial,true);
  assert.equal(result.data.razao_social,'EMPRESA TESTE');
  assert.equal(result.data.porte_empresa,null,'enum legado não é rótulo RFB confiável');
  assert.equal(result.data.opcao_simples,null,'False pode representar dado ausente');
  assert.equal(result.data.opcao_mei,true);
  assert.equal(result.data.data_opcao_mei,'2026-07-11');
  assert.equal(result.data.municipio_codigo,7107,'não fabricar IBGE 7 dígitos');
});
test('404 de membro, estabelecimento ausente e HTTP 503 diferem',async()=>{
  const missing=await lookup(manifest,DATE,CNPJ,{fetchImpl:mock(404)});
  assert.equal(missing.data,null); assert.equal(missing.missingRoot,true);
  const absent=await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>({
    ...company,estabelecimentos:[{cnpj_ordem:2,cnpj_dv:1}],
  })});
  assert.equal(absent.data,null);assert.equal(absent.missingRoot,false);
  await assert.rejects(lookup(manifest,DATE,CNPJ,{fetchImpl:mock(503)}),/HTTP 503/);
});
test('erro de raiz, competência, duplicata e wire 1.1 não vira registro',async()=>{
  for(const [mutation,expected] of [
    [c=>{c.cnpj_base=33000168},/Raiz/],
    [c=>{delete c.snapshot_yyyymm},/Competência/],
    [c=>{c.estabelecimentos.push({...c.estabelecimentos[0]})},/duplicado/],
    [c=>{c.estabelecimentos[0].cnpj_ordem_text='E08G'},/1\.1/],
    [c=>{c.socios[0].cnpj_socio_text='AB'},/1\.1/],
    [c=>{c.estabelecimentos[0].cnpj_ordem=10000},/Identificador/],
  ]){
    const bad=structuredClone(company);mutation(bad);
    await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>bad}),expected);
  }
  await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>{throw Error('bad bytes')}}),/inválida/);
});
test('cancelamento é propagado ao transporte sem alterar o manifesto',async()=>{
  const controller=new AbortController();let seen;
  const before=structuredClone(manifest);
  await lookup(manifest,DATE,CNPJ,{signal:controller.signal,fetchImpl:async(url,opts)=>{
    seen=opts.signal;return mock()(url);
  }});
  assert.equal(seen,controller.signal);
  assert.deepEqual(manifest,before);
});
test('datas impossíveis são rejeitadas, ausência preserva null',async()=>{
  const wrong=structuredClone(company);
  wrong.estabelecimentos[0].data_opcao_mei=20261341;
  await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>wrong}),/Data impossível/);
  const noDate=structuredClone(company);delete noDate.estabelecimentos[0].data_opcao_mei;
  const result=await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>noDate});
  assert.equal(result.data.data_opcao_mei,null);
});
