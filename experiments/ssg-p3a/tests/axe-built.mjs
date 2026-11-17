/**
 * WCAG audit of the already-built SSG prototype. Browser + axe are required.
 * Axe is served only by this loopback preview; no production assets or data.
 *
 * FICHA_PLAYWRIGHT_MODULE=/abs/path/playwright \
 * FICHA_AXE_MODULE=/abs/path/axe-core \
 * node tests/axe-built.mjs dist
 */
import assert from 'node:assert/strict';
import { existsSync, readFile } from 'node:fs';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { extname, join, resolve, sep } from 'node:path';

const root = resolve(process.argv[2] ?? new URL('../dist', import.meta.url).pathname);
assert.ok(existsSync(join(root, 'ficha/index.html')), 'axe: build SSG ausente; NOT_RUN');
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.FICHA_PLAYWRIGHT_MODULE || 'playwright');
const axe = require(process.env.FICHA_AXE_MODULE || 'axe-core');
assert.ok(typeof axe.source === 'string' && axe.source.length > 1000, 'axe-core ausente');
const mime = { '.html': 'text/html; charset=utf-8', '.css': 'text/css',
  '.mjs': 'text/javascript', '.js': 'text/javascript', '.json': 'application/json' };
const server = createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); }
  catch { res.writeHead(400).end(); return; }
  if (url.pathname === '/ficha/__audit__/axe.js') {
    res.writeHead(200, { 'content-type': 'text/javascript' }).end(axe.source);
    return;
  }
  const relative = url.pathname.endsWith('/') ? url.pathname + 'index.html' : url.pathname;
  if (!relative.startsWith('/ficha/')) { res.writeHead(404).end(); return; }
  const file = resolve(root, '.' + relative);
  if (!file.startsWith(root + sep)) { res.writeHead(403).end(); return; }
  readFile(file, (error, bytes) => {
    if (error) { res.writeHead(404).end(); return; }
    res.writeHead(200, { 'content-type': mime[extname(file)] || 'application/octet-stream' }).end(bytes);
  });
});
async function audit(page, width, stage) {
  const result = await page.evaluate(async () => {
    const report = await window.axe.run(document, {
      runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'] }
    });
    return {
      violations: report.violations.map(v => ({
        id: v.id, impact: v.impact,
        nodes: v.nodes.map(n => ({ target: n.target, summary: n.failureSummary }))
      })),
      incomplete: report.incomplete.map(v => ({ id: v.id, nodes: v.nodes.length }))
    };
  });
  assert.deepEqual(result.violations, [], 'WCAG: ' + JSON.stringify({ width, stage, ...result }));
  console.log('AXE_PASS ' + JSON.stringify({ width, stage, manualReview: result.incomplete }));
}
let browser;
try {
  await new Promise(ok => server.listen(0, '127.0.0.1', ok));
  const base = 'http://127.0.0.1:' + server.address().port + '/ficha/';
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  for (const width of [320, 375, 1440]) {
    const page = await browser.newPage({ viewport: { width, height: 800 } });
    const errors = [], remoteRequests = [];
    page.on('pageerror', e => errors.push(e.message));
    page.on('request', req => {
      if (!req.url().startsWith('http://127.0.0.1:')) remoteRequests.push(req.url());
    });
    assert.equal((await page.goto(base, { waitUntil: 'networkidle' })).status(), 200);
    await page.addScriptTag({ url: base + '__audit__/axe.js' });
    assert.equal(await page.evaluate(() => typeof window.axe.run), 'function', 'axe blocked');
    await audit(page, width, 'idle');

    await page.locator('#search-term').fill('abc');
    await page.locator('#search-form button').click();
    assert.equal(await page.locator('#search-status').getAttribute('data-state'), 'error');
    await audit(page, width, 'invalid');

    await page.locator('#search-term').fill('00000000E08G12');
    await page.locator('#search-form button').click();
    await page.waitForFunction(() => document.querySelector('#search-status')?.dataset.state === 'unavailable');
    await audit(page, width, 'unavailable');

    await page.locator('#search-kind').selectOption('pessoa');
    assert.match(await page.locator('#search-status').textContent(), /Nenhuma consulta realizada/);
    await audit(page, width, 'reset');

    assert.deepEqual(errors, [], 'JS page errors');
    assert.deepEqual(remoteRequests, [], 'PoC nao pode consultar dados externos');
    await page.close();
  }
} catch (error) {
  console.error('AXE_FAIL', error.stack);
  process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  await new Promise(ok => server.close(ok));
}
