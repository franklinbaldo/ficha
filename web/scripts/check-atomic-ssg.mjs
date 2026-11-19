// Exercise the *built* FICHA SSG route with actual public Protobuf and
// snapshot lookups. No Parquet, writes, credentials or remote side effects.
import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import {chromium} from 'playwright';

const base = process.argv[2] || 'http://127.0.0.1:4321/ficha/';
const out = process.argv[3] || '../accessibility-evidence';
await mkdir(out, {recursive:true});
const browser = await chromium.launch({headless:true});
const results = [];
const MAX_MS = 65_000;
const cases = [
  {cnpj:'18236120000158', name:'Nubank', city:/S[AÃ]O PAULO/i},
  {cnpj:'33000167000101', name:'Petrobras', city:/RIO DE JANEIRO/i},
];
try {
  for (const {cnpj,name,city} of cases) {
    const page = await browser.newPage({serviceWorkers:'block'});
    try {
      const visited = [];
      page.on('request',request=>{
        if(request.url().includes('archive.org/download/')) visited.push(request.url());
      });
      await page.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
      await page.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
      await page.locator('#empresa-cnpj').fill(cnpj);
      await page.locator('#empresa-enviar').click();
      await page.waitForFunction(
        () => /Consulta concluída|Consulta indisponível|Consulta inconclusiva/.test(
          document.getElementById('empresa-estado')?.textContent||''),
        null, {timeout:MAX_MS},
      );
      const message = await page.locator('#empresa-estado').textContent();
      assert.match(message,/Consulta concluída/,name+': '+message);
      const record = await page.locator('#empresa-resultado').textContent();
      assert.ok(record.includes(cnpj),name+': CNPJ da resposta');
      assert.match(record,city,name+': município RFB da competência');
      assert.match(record,/Porte empresarial: Não determinado/);
      assert.ok(visited.some(url=>url.includes('.zip/')&&url.endsWith('.pb')),
        'membro Protobuf remoto não observado');
      assert.ok(visited.some(url=>url.endsWith('/lookups.json')),
        'lookup remoto não observado');
      assert.ok(!visited.some(url=>url.includes('.parquet')),
        'não pode carregar Parquet na ficha numérica');
      results.push({name,cnpj,state:'found',requests:visited.length,status:'PASS'});
    } finally {await page.close();}
  }
  // History uses the same Protobuf transport for both selected months.
  const history = await browser.newPage({serviceWorkers:'block'});
  try {
    const visited=[];
    history.on('request',request=>{
      if(request.url().includes('archive.org/download/')) visited.push(request.url());
    });
    await history.goto(base+'historia/',{waitUntil:'domcontentloaded'});
    await history.locator('#historia-inicio option[value="2026-06"]')
      .waitFor({state:'attached'});
    assert.equal(await history.locator('#historia-inicio option[value="2026-04"]').count(),0,
      '2026-04 não oferece índice numérico v1 para comparação');
    await history.locator('#historia-inicio').selectOption('2026-06');
    await history.locator('#historia-fim').selectOption('2026-07');
    await history.locator('#historia-cnpj').fill('33000167000101');
    await history.locator('#historia-enviar').click();
    await history.locator('#historia-estado')
      .filter({hasText:/Comparação concluída|Comparação indisponível|Comparação inconclusiva|Comparação incompleta/})
      .waitFor({timeout:MAX_MS});
    const message=await history.locator('#historia-estado').textContent();
    assert.match(message,/Comparação concluída/,message);
    const table=await history.locator('#historia-resultado').textContent();
    assert.match(table,/Razão social/);
    assert.match(table,/não permite determinar o porte/i);
    const pb=visited.filter(url=>url.includes('.zip/')&&url.endsWith('.pb'));
    assert.equal(pb.length,2,'comparação exige dois membros Protobuf distintos');
    assert.ok(!visited.some(url=>url.endsWith('.parquet')),
      'histórico numérico não pode abrir Parquet via DuckDB');
    results.push({name:'História 2026-06→07 · Petrobras',state:'compared',
      pbGets:pb.length,status:'PASS'});
  }finally{await history.close();}

  // A 404 in either month invalidates the comparison, not the company's history.
  const history404=await browser.newPage({serviceWorkers:'block'});
  try{
    await history404.route('**/companies-33.zip/**',
      route=>route.fulfill({status:404,body:''}));
    await history404.goto(base+'historia/',{waitUntil:'domcontentloaded'});
    await history404.locator('#historia-inicio option[value="2026-06"]')
      .waitFor({state:'attached'});
    await history404.locator('#historia-inicio').selectOption('2026-06');
    await history404.locator('#historia-fim').selectOption('2026-07');
    await history404.locator('#historia-cnpj').fill('33000167000101');
    await history404.locator('#historia-enviar').click();
    await history404.locator('#historia-estado')
      .filter({hasText:/Comparação inconclusiva/}).waitFor({timeout:MAX_MS});
    assert.match(await history404.locator('#historia-resultado').textContent(),
      /arquivo indisponível/i);
    assert.doesNotMatch(await history404.locator('#historia-estado').textContent(),
      /concluída/i);
    results.push({name:'História 404 não é ausência',status:'PASS'});
  }finally{await history404.close();}

  // Negative DV must fail before *any* archival request.
  const page = await browser.newPage({serviceWorkers:'block'});
  try {
    let archiveGets = 0;
    page.on('request',r=>{if(r.url().includes('archive.org/'))archiveGets++;});
    await page.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
    await page.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
    await page.locator('#empresa-cnpj').fill('33000167000102');
    await page.locator('#empresa-enviar').click();
    await page.locator('#empresa-estado').filter({hasText:/dígitos verificadores/}).waitFor({state:'attached'});
    assert.equal(archiveGets,0,'DV inválido causou GET no IA');
    results.push({name:'DV inválido',archiveGets,status:'PASS'});
  }finally{await page.close();}
  // An HTTP 404 for the member is inconclusive, never a negative company hit.
  const missing = await browser.newPage({serviceWorkers:'block'});
  try {
    await missing.route('**/companies-33.zip/**',route=>route.fulfill({status:404,body:''}));
    await missing.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
    await missing.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
    await missing.locator('#empresa-cnpj').fill('33000167000101');
    await missing.locator('#empresa-enviar').click();
    await missing.locator('#empresa-estado').filter({hasText:/Consulta inconclusiva/}).waitFor({state:'attached'});
    const body=await missing.locator('#empresa-resultado').textContent();
    assert.match(body,/Arquivo indisponível/);
    assert.doesNotMatch(body,/Nenhum registro encontrado/);
    results.push({name:'HTTP 404 inconclusivo',status:'PASS'});
  }finally{await missing.close();}
  // HTTP 503 is not a proof of company absence.
  const unavailable = await browser.newPage({serviceWorkers:'block'});
  try {
    await unavailable.route('**/companies-33.zip/**', route =>
      route.fulfill({status:503, body:'Temporariamente indisponível'}));
    await unavailable.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
    await unavailable.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
    await unavailable.locator('#empresa-cnpj').fill('33000167000101');
    await unavailable.locator('#empresa-enviar').click();
    await unavailable.locator('#empresa-estado')
      .filter({hasText:/Consulta indisponível/}).waitFor({timeout:MAX_MS});
    const body = await unavailable.locator('#empresa-resultado').textContent();
    assert.doesNotMatch(body,/Nenhum registro encontrado|não localizado no registro/);
    results.push({name:'HTTP 503 indisponível',status:'PASS'});
  } finally {await unavailable.close();}
  // An invalid member is a decoding error, never an intact negative result.
  const invalid = await browser.newPage({serviceWorkers:'block'});
  try {
    await invalid.route('**/companies-33.zip/**', route =>
      route.fulfill({status:200,body:Buffer.from([255,255,255])}));
    await invalid.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
    await invalid.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
    await invalid.locator('#empresa-cnpj').fill('33000167000101');
    await invalid.locator('#empresa-enviar').click();
    await invalid.locator('#empresa-estado')
      .filter({hasText:/Consulta indisponível/}).waitFor({timeout:MAX_MS});
    assert.doesNotMatch(await invalid.locator('#empresa-resultado').textContent(),
      /Nenhum registro encontrado|não localizado no registro/);
    results.push({name:'Protobuf inválido não é ausência',status:'PASS'});
  } finally {await invalid.close();}
  // A→B→A while A is in flight must invalidate the old request, even when
  // the final input once again equals the original CNPJ.
  const stale = await browser.newPage({serviceWorkers:'block'});
  let release;
  const held = new Promise(resolve => {release = resolve;});
  try {
    await stale.route('**/companies-33.zip/**', async route => {
      await held;
      await route.fulfill({status:503,body:''}).catch(()=>{});
    });
    await stale.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
    await stale.locator('#empresa-competencia option[value="2026-07"]').waitFor({state:'attached'});
    await stale.locator('#empresa-cnpj').fill('33000167000101');
    const requested = stale.waitForRequest(request =>
      request.url().includes('companies-33.zip/') && request.url().endsWith('.pb'),
    {timeout:20_000});
    await stale.locator('#empresa-enviar').click();
    await requested;
    await stale.locator('#empresa-cnpj').fill('18236120000158');
    await stale.locator('#empresa-cnpj').fill('33000167000101');
    release();
    await stale.locator('#empresa-estado')
      .filter({hasText:/Pesquise novamente/}).waitFor({timeout:MAX_MS});
    assert.doesNotMatch(await stale.locator('#empresa-estado').textContent(),
      /Consulta indisponível|Consulta concluída/);
    assert.equal((await stale.locator('#empresa-resultado').textContent()).trim(),'');
    assert.ok(!new URL(stale.url()).searchParams.has('cnpj'),
      'resposta antiga não pode restaurar o deep link após edição');
    results.push({name:'Resposta obsoleta A→B→A',status:'PASS'});
  } finally {release?.(); await stale.close();}
}finally {
  await browser.close();
  await writeFile(out+'/atomic-ssg-browser-report.json',
    JSON.stringify(results,null,2)+'\n');
}
console.log('FICHA_ATOMIC_SSG_BROWSER_PASS '+JSON.stringify(results));
