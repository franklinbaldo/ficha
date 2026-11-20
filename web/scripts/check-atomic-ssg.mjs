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
  // History: the same real v1 record can be compared across two declared PB
  // snapshots without invoking Parquet, DuckDB-WASM or any monolithic ZIP.
  const history = await browser.newPage({serviceWorkers:'block'});
  try {
    const visited=[];
    history.on('request',r=>{
      if(r.url().includes('archive.org/download/'))visited.push(r.url());
    });
    await history.goto(base+'historia/',{waitUntil:'domcontentloaded'});
    await history.locator('#historia-inicio option[value="2026-06"]').waitFor({state:'attached'});
    assert.equal(await history.locator('#historia-inicio').inputValue(),'2026-06',
      'default comparison starts at a PB-enabled month');
    await history.locator('#historia-cnpj').fill('33000167000101');
    await history.locator('#historia-fim').selectOption('2026-07');
    await history.locator('#historia-enviar').click();
    await history.locator('#historia-estado')
      .filter({hasText:/Comparação concluída|Comparação indisponível/})
      .waitFor({timeout:MAX_MS});
    const status=await history.locator('#historia-estado').textContent();
    assert.match(status,/Comparação concluída/,status);
    const text=await history.locator('#historia-resultado').textContent();
    assert.match(text,/PETROLEO BRASILEIRO/i);
    assert.match(text,/2026-06.*2026-07/s);
    assert.match(text,/porte histórico não pode ser determinado/);
    assert.ok(visited.filter(url=>url.endsWith('.pb')).length >= 2,
      'membros Protobuf das duas competências ausentes');
    assert.ok(!visited.some(url=>/\.parquet|\.wasm/.test(url)),
      'a comparação numérica não deve invocar Parquet ou WASM');
    // Audit the rendered result, not a synthetic accessibility fixture.
    // At mobile width the three-column table must stay readable by scrolling
    // within a labelled, keyboard-focusable region, preserving native headers.
    await history.setViewportSize({width:375,height:812});
    const table=history.locator('.comparison-scroll');
    await table.waitFor({state:'visible'});
    const widths=await table.evaluate(el=>({
      client:el.clientWidth,scroll:el.scrollWidth,
    }));
    assert.ok(widths.scroll>widths.client,'a tabela móvel deve rolar sem achatar as colunas');
    assert.equal(await table.getAttribute('role'),'region');
    assert.match(await table.getAttribute('aria-label'),/2026-06.*2026-07/);
    assert.equal(await table.getAttribute('tabindex'),'0');
    assert.equal(await table.locator('table caption').count(),1);
    assert.equal(await table.locator('thead th[scope="col"]').count(),3);
    assert.ok(await table.locator('tbody th[scope="row"]').count()>0);
    await table.focus();
    assert.equal(await table.evaluate(el=>document.activeElement===el),true);
    await history.keyboard.press('ArrowRight');
    await history.waitForFunction(
      ()=>document.querySelector('.comparison-scroll')?.scrollLeft>0,
      null,{timeout:5000},
    );
    await history.screenshot({path:out+'/history-mobile-keyboard-scroll.png'});
    results.push({name:'História móvel: tabela acessível por teclado',widths,status:'PASS'});
    results.push({name:'História PB real 2026-06/07',requests:visited.length,status:'PASS'});
  } finally {await history.close();}
  const missingHistory = await browser.newPage({serviceWorkers:'block'});
  try {
    let members=0;
    await missingHistory.route('**/companies-33.zip/**',route=>{
      members++;return route.fulfill({status:404,body:''});
    });
    await missingHistory.goto(base+'historia/',{waitUntil:'domcontentloaded'});
    await missingHistory.locator('#historia-inicio option[value="2026-06"]')
      .waitFor({state:'attached'});
    await missingHistory.locator('#historia-cnpj').fill('33000167000101');
    await missingHistory.locator('#historia-enviar').click();
    await missingHistory.locator('#historia-estado')
      .filter({hasText:/inconclusiva/i}).waitFor({timeout:MAX_MS});
    assert.equal(members,1,'404 inconclusivo não deve gerar GET do segundo snapshot');
    assert.doesNotMatch(await missingHistory.locator('#historia-estado').textContent(),
      /Comparação concluída/);
    results.push({name:'História PB 404 inconclusivo',members,status:'PASS'});
  } finally {await missingHistory.close();}
}finally {
  await browser.close();
  await writeFile(out+'/atomic-ssg-browser-report.json',
    JSON.stringify(results,null,2)+'\n');
}
console.log('FICHA_ATOMIC_SSG_BROWSER_PASS '+JSON.stringify(results));
