// Chromium smoke of the generated shadow artifact, never of the public Pages.
// It deliberately tests offline UI/HTTP failure only: no fake company result.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { resolve, extname, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = resolve(fileURLToPath(new URL('../dist/', import.meta.url)));
const server = createServer(async (req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (!pathname.startsWith('/ficha/')) {
    res.writeHead(404); res.end(); return;
  }
  const relative = pathname.endsWith('/') ? pathname + 'index.html' : pathname;
  const absolute = resolve(root, '.' + relative);
  if (!absolute.startsWith(root + sep)) {
    res.writeHead(403); res.end(); return;
  }
  try {
    const data = await readFile(absolute);
    const type = ({'.html':'text/html', '.css':'text/css', '.mjs':'text/javascript',
      '.json':'application/json'})[extname(absolute)] || 'application/octet-stream';
    res.writeHead(200, {'content-type':type, 'cache-control':'no-store'});
    res.end(data);
  } catch (error) {
    res.writeHead(error.code === 'ENOENT' ? 404 : 500);
    res.end();
  }
});

await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port + '/ficha/';
let browser;
try {
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 375, height: 812 } });
  const errors = [], urls = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => urls.push(request.url()));
  await page.goto(base);
  await page.locator('#busca-form').waitFor();
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement?.getAttribute('href')),
    '#main', 'primeiro Tab deve alcançar o skip-link');
  assert.equal(urls.some(url => url.includes('cdn.jsdelivr.net')), false,
    'o WASM não deve carregar antes da primeira consulta');

  await page.locator('#busca-termo').fill('00.000.000/E08G-12');
  await page.locator('#busca-enviar').click();
  await page.waitForFunction(() =>
    document.getElementById('consulta-estado')?.textContent?.includes('manifesto ausente'));
  assert.equal(await page.locator('#consulta-lista li').count(), 0);
  assert.equal(await page.locator('#busca-enviar').isEnabled(), true);
  assert.equal(urls.some(url => url.includes('cdn.jsdelivr.net')), false,
    'manifesto 404 não deve inicializar o DuckDB');

  await page.route('**/ficha/manifest.json', route => route.fulfill({
    status: 500, contentType: 'application/json', body: '{}',
  }));
  await page.locator('#busca-enviar').click();
  await page.waitForFunction(() =>
    document.getElementById('consulta-estado')?.textContent?.includes('HTTP 500'));
  assert.equal(await page.locator('#consulta-lista li').count(), 0,
    'erro HTTP não pode preservar resultados antigos');
  assert.equal(await page.locator('#busca-enviar').isEnabled(), true);

  // A valid published manifest must NOT trigger a real remote download in CI.
  // Emulate failed cross-origin transport and verify failure before WASM CDN.
  const validManifest = JSON.parse(await readFile(
    new URL('../../web/public/manifest.json', import.meta.url), 'utf8'));
  await page.route('**/ficha/manifest.json', route => route.fulfill({
    status: 200, contentType: 'application/json', body: JSON.stringify(validManifest),
  }));
  await page.route('https://archive.org/**', route => route.abort('failed'));
  await page.locator('#busca-enviar').click();
  await page.waitForFunction(() =>
    document.getElementById('consulta-estado')?.textContent?.includes('rede ou CORS'));
  assert.equal(urls.some(url => url.includes('cdn.jsdelivr.net')), false,
    'erro de CORS/Range não deve baixar WASM');
  assert.equal(await page.locator('#busca-enviar').isEnabled(), true);
  assert.deepEqual(errors, [], 'nenhum erro JavaScript não tratado no boot da home');

  const noJsContext = await browser.newContext({ javaScriptEnabled: false });
  const noJsPage = await noJsContext.newPage();
  await noJsPage.goto(base);
  assert.equal(await noJsPage.locator('noscript').isVisible(), true);
  await noJsContext.close();
  await page.close();
  console.log('Chromium local PASS: 375px, skip-link, ESM, 404, 500, rede/CORS abortada, sem JS');
  console.log('NOT_RUN: Worker/WASM, Parquet real, Protobuf, dados públicos e Pages');
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
}
