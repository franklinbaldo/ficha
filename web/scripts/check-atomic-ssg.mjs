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
}finally {
  await browser.close();
  await writeFile(out+'/atomic-ssg-browser-report.json',
    JSON.stringify(results,null,2)+'\n');
}
console.log('FICHA_ATOMIC_SSG_BROWSER_PASS '+JSON.stringify(results));
