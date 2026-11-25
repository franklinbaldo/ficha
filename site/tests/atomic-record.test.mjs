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
  porte_empresa:1,qtd_estabelecimentos:1,estabelecimentos:[{cnpj_ordem:1,cnpj_dv:1,uf:'RJ',
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
test('CNPJ inválido não consulta a rede nem invoca decodificação',async()=>{
  let fetched=0,decoded=0;
  for (const invalid of ['33000167000102','00000000000000','11111111111111']) {
    assert.throws(()=>planAtomicRecord(manifest,DATE,invalid),/CNPJ inválido/);
    await assert.rejects(fetchAtomicRecord(manifest,DATE,invalid,{
      fetchImpl:async()=>{fetched++;return mock()();},
      decodeCompany:()=>{decoded++;return company;},
    }),/CNPJ inválido/);
  }
  assert.equal(fetched,0);
  assert.equal(decoded,0);
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
  let cancelCount=0, pullCount=0;
  const oversize=new Response(new ReadableStream({
    pull(){pullCount++;},
    cancel(){cancelCount++;},
  },{highWaterMark:0}),{headers:{'content-length':'100'}});
  await assert.rejects(readBoundedMember(oversize,3),/orçamento/);
  assert.equal(cancelCount,1,'rejeição antes da leitura deve cancelar o transporte');
  assert.equal(pullCount,0,'rejeição do cabeçalho não deve ler chunks');
  const res = new Response(new ReadableStream({start(controller){
    controller.enqueue(Uint8Array.of(1,2));controller.enqueue(Uint8Array.of(3,4));controller.close();
  }}));
  await assert.rejects(readBoundedMember(res,3),/orçamento/);
  assert.deepEqual([...await readBoundedMember(new Response(Uint8Array.of(1,2,3)),3)],[1,2,3]);
  await assert.rejects(readBoundedMember(new Response(Uint8Array.of(1)),0),/inválido/);
});
test('Content-Length de identidade deve corresponder ao corpo antes do decoder',async()=>{
  // Os dois corpos são decodificáveis no mock, mas nenhum certifica seu tamanho.
  for (const length of [1,3]) {
    let decoded=0;
    await assert.rejects(lookup(manifest,DATE,CNPJ,{
      fetchImpl:async()=>new Response(Uint8Array.of(23,42),{
        status:200,headers:{'content-length':String(length)},
      }),
      decodeCompany:()=>{decoded++;return structuredClone(company);},
    }),/Content-Length incompatível/);
    assert.equal(decoded,0,'não interpretar corpo parcial ou maior que o declarado');
  }
  const complete=await readBoundedMember(new Response(Uint8Array.of(23,42),{
    headers:{'content-length':'2','content-encoding':'identity'},
  }),16);
  assert.deepEqual([...complete],[23,42]);

  // Fetch entrega bytes já descomprimidos; Content-Length codificado não é
  // comparável ao tamanho do stream quando há Content-Encoding real.
  const decodedStream=await readBoundedMember(new Response(Uint8Array.of(23,42),{
    headers:{'content-length':'1','content-encoding':'gzip'},
  }),16);
  assert.deepEqual([...decodedStream],[23,42]);
});

test('stream que reutiliza Uint8Array não corrompe chunks já recebidos',async()=>{
  const reused=Uint8Array.of(1,2);
  let pulls=0;
  const response=new Response(new ReadableStream({
    pull(controller){
      if(pulls++===0) controller.enqueue(reused);
      else if(pulls===2){ reused.set([3,4]); controller.enqueue(reused); }
      else controller.close();
    },
  },{highWaterMark:0}),{headers:{'content-length':'4'}});
  assert.deepEqual([...await readBoundedMember(response,8)],[1,2,3,4],
    'cada leitura precisa preservar os bytes antes de novo pull');
});

test('erro de excesso não aguarda cancelamento de stream que nunca resolve',async()=>{
  let cancelCount=0,releaseCount=0;
  const pending=new Promise(()=>{});
  const response={headers:{get:()=>null},body:{getReader:()=>({
    async read(){return {done:false,value:Uint8Array.of(1,2,3,4)};},
    cancel(){cancelCount++;return pending;},
    releaseLock(){releaseCount++;},
  })}};
  const result=await Promise.race([
    readBoundedMember(response,3).then(
      ()=>({state:'unexpected-success'}),
      error=>({state:'rejected',message:error.message}),
    ),
    new Promise(resolve=>setTimeout(()=>resolve({state:'stalled'}),150)),
  ]);
  assert.deepEqual(result,{state:'rejected',
    message:'Membro Protobuf excede orçamento de leitura'});
  assert.equal(cancelCount,1,'o cancelamento foi solicitado');
  assert.equal(releaseCount,1,'o reader foi liberado mesmo sem teardown');
});

test('decoder canônico injetado recebe bytes, sem baixar ZIP ou Parquet',async()=>{
  const urls=[];let observed;
  const result=await lookup(manifest,DATE,CNPJ,{fetchImpl:async(url,options)=>{
    urls.push([url,options.mode]);return new Response(Uint8Array.of(23,42));
  },decodeCompany:bytes=>{observed=[...bytes];return structuredClone(company);}});
  assert.deepEqual(urls,[[archive+'/33/000/167.pb','cors']]);
  assert.deepEqual(observed,[23,42]);
  assert.equal(result.data.cnpj,CNPJ);
  assert.equal(result.state,'found');
  assert.equal(result.partial,true);
  assert.equal(result.data.razao_social,'EMPRESA TESTE');
  assert.equal(result.data.porte_empresa,null,'enum legado não é rótulo RFB confiável');
  assert.equal(result.data.opcao_simples,null,'False pode representar dado ausente');
  assert.equal(result.data.opcao_mei,true);
  assert.equal(result.data.data_opcao_mei,'2026-07-11');
  assert.equal(result.data.municipio_codigo,7107,'não fabricar IBGE 7 dígitos');
});
test('404 de membro ou ZIP é indisponibilidade, ausência só após PB íntegro, 503 é erro',async()=>{
  let decoderCalled=false;
  const unavailable=await lookup(manifest,DATE,CNPJ,{
    fetchImpl:mock(404),decodeCompany:()=>{decoderCalled=true;return company;},
  });
  assert.equal(unavailable.data,null);
  assert.equal(unavailable.state,'artifact-unavailable');
  assert.equal(unavailable.httpStatus,404);
  assert.equal(decoderCalled,false);
  assert.equal(Object.hasOwn(unavailable,'missingRoot'),false,
    'HTTP 404 não é prova de raiz ausente');
  const absent=await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>({
    ...company,estabelecimentos:[{cnpj_ordem:2,cnpj_dv:92}],
  })});
  assert.equal(absent.data,null);
  assert.equal(absent.state,'absent-in-snapshot');
  await assert.rejects(lookup(manifest,DATE,CNPJ,{fetchImpl:mock(503)}),/HTTP 503/);
});
test('GET Protobuf exige HTTP 200 integral, não 201/206 decodificável',async()=>{
  let decoded=0;
  for (const status of [201,206]) {
    const response = new Response(Uint8Array.of(23,42), {
      status,headers: status===206 ? {'Content-Range':'bytes 0-1/100'} : {},
    });
    await assert.rejects(lookup(manifest,DATE,CNPJ,{
      fetchImpl:async()=>response,
      decodeCompany:()=>{decoded++;return structuredClone(company);},
    }),new RegExp('HTTP '+status));
  }
  assert.equal(decoded,0,
    'um 2xx não integral não pode ser decodificado nem declarar presença');
});

test('HTTP 200 com Content-Range recusa membro parcial antes de ler ou decodificar',async()=>{
  let decoded=0, pulled=0, cancelled=0;
  const response=new Response(new ReadableStream({
    pull(controller){ pulled++; controller.enqueue(Uint8Array.of(23,42)); controller.close(); },
    cancel(){ cancelled++; },
  },{highWaterMark:0}),{
    status:200,
    headers:{'Content-Range':'bytes 0-1/100','Content-Length':'2'},
  });
  await assert.rejects(lookup(manifest,DATE,CNPJ,{
    fetchImpl:async()=>response,
    decodeCompany:()=>{decoded++;return structuredClone(company);},
  }),/Content-Range inesperado/);
  assert.equal(pulled,0,'o 200 parcial deve falhar antes de ler o corpo');
  assert.equal(cancelled,1,'a rejeição descarta o corpo de forma best-effort');
  assert.equal(decoded,0,'bytes parciais não podem declarar CNPJ presente ou ausente');
});

test('contagem de filiais incompatível ou não declarada nunca prova ausência',async()=>{
  for (const mutate of [
    c=>{c.qtd_estabelecimentos=3;c.estabelecimentos=[];},
    c=>{c.qtd_estabelecimentos=2;},
    c=>{delete c.qtd_estabelecimentos;},
  ]) {
    const bad=structuredClone(company);
    mutate(bad);
    await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>bad}),
      /Quantidade de estabelecimentos inconsistente/);
  }
  const absent=structuredClone(company);
  absent.estabelecimentos[0].cnpj_ordem=2;
  absent.estabelecimentos[0].cnpj_dv=92;
  assert.equal((await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>absent})).state,
    'absent-in-snapshot');
});
test('duplicata de outra filial invalida agregado antes de declarar ausência',async()=>{
  const bad=structuredClone(company);
  bad.qtd_estabelecimentos=2;
  bad.estabelecimentos=[
    {...bad.estabelecimentos[0],cnpj_ordem:2,cnpj_dv:92},
    {...bad.estabelecimentos[0],cnpj_ordem:2,cnpj_dv:92},
  ];
  // Count matches list length, but both rows represent the SAME full CNPJ.
  // Querying a different, valid CNPJ must be inconclusive, not 'absent'.
  await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>bad}),
    /Estabelecimento duplicado/);
  const sound=structuredClone(bad);
  sound.estabelecimentos=[bad.estabelecimentos[0]];
  sound.qtd_estabelecimentos=1;
  assert.equal((await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>sound})).state,
    'absent-in-snapshot');
});
test('DV de toda filial é necessário para certificar presença ou ausência',async()=>{
  const corrupt=structuredClone(company);
  corrupt.estabelecimentos[0].cnpj_dv=99;
  await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>corrupt}),
    /Dígitos verificadores de filial inválidos/);
  // Mesmo um CNPJ encontrado não torna uma raiz parcialmente corrompida íntegra.
  corrupt.qtd_estabelecimentos=2;
  corrupt.estabelecimentos=[structuredClone(company.estabelecimentos[0]),
    {cnpj_ordem:2,cnpj_dv:99}];
  await assert.rejects(lookup(manifest,DATE,CNPJ,{decodeCompany:()=>corrupt}),
    /Dígitos verificadores de filial inválidos/);
  const validOther=structuredClone(company);
  validOther.estabelecimentos[0].cnpj_ordem=2;
  validOther.estabelecimentos[0].cnpj_dv=92;
  assert.equal((await lookup(manifest,DATE,CNPJ,{decodeCompany:()=>validOther})).state,
    'absent-in-snapshot');
});
test('erro de raiz, competência, duplicata e wire 1.1 não vira registro',async()=>{
  for(const [mutation,expected] of [
    [c=>{c.cnpj_base=33000168},/Raiz/],
    [c=>{delete c.snapshot_yyyymm},/Competência/],
    [c=>{c.estabelecimentos.push({...c.estabelecimentos[0]});c.qtd_estabelecimentos=2},/duplicado/],
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

test('declaração da camada e perfil do decoder permanecem distintos em todos os estados',async()=>{
  for (const [label,version] of [['ausente',undefined],['null',null],['v1','1.0.0']]) {
    const m=structuredClone(manifest);
    const layer=m.snapshots[0].files.companies;
    if(version===undefined) delete layer.schema_version; else layer.schema_version=version;
    const expected={declaredWire:version??null,decoderProfile:'1.0.0',
      wireProvenance:version==='1.0.0'?'declared':'legacy-assumed',producerSemantics:'unattested'};
    for(const [state,opts] of [
      ['found',{}],
      ['absent-in-snapshot',{decodeCompany:()=>({...company,estabelecimentos:[{cnpj_ordem:2,cnpj_dv:92}]})}],
      ['artifact-unavailable',{fetchImpl:mock(404),decodeCompany:()=>assert.fail('404 não decodifica')}],
    ]) {
      const result=await lookup(m,DATE,CNPJ,opts);
      assert.equal(result.state,state,label);
      for(const [key,value] of Object.entries(expected)) assert.equal(result[key],value,label+' '+state);
      if(state==='found') assert.equal(result.data.porte_empresa,null);
    }
  }
});

test('qualquer declaração própria por shard, selecionado ou não, impede GET e decoder',async()=>{
  for (const layerVersion of [undefined,null,'1.0.0']) {
    for (const index of [0,1]) {
      for (const shardVersion of [null,'1.0.0','1.1.0','9.0.0',undefined]) {
        const m=structuredClone(manifest);
        const layer=m.snapshots[0].files.companies;
        if(layerVersion===undefined) delete layer.schema_version; else layer.schema_version=layerVersion;
        layer.shards.push({shard:'34',url:archive.replace('33.zip','34.zip')});
        layer.shards[index].schema_version=shardVersion;
        let fetched=0,decoded=0;
        await assert.rejects(lookup(m,DATE,CNPJ,{
          fetchImpl:async()=>{fetched++;return mock()();},
          decodeCompany:()=>{decoded++;return company;},
        }),/v1 não declarada/);
        assert.equal(fetched,0);
        assert.equal(decoded,0);
      }
    }
  }
});

test('versão incompatível da camada não é substituída pela versão do snapshot ou ZIP legado',async()=>{
  for(const version of ['1.1.0','9.0.0','',1]) {
    const m=structuredClone(manifest);
    m.snapshots[0].schema_version='1.0.0';
    m.snapshots[0].files.companies_zip={schema_version:'1.0.0'};
    m.snapshots[0].files.companies.schema_version=version;
    await assert.rejects(lookup(m,DATE,CNPJ,{
      fetchImpl:()=>assert.fail('GET indevido'),
      decodeCompany:()=>assert.fail('decoder indevido'),
    }),/v1 não declarada/);
  }
});
