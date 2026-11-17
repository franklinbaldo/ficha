import assert from 'node:assert/strict';
import { existsSync, readFile } from 'node:fs';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { extname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const root = resolve(process.argv[2] ?? new URL('../dist', import.meta.url).pathname);
assert.ok(existsSync(join(root, 'ficha/index.html')), 'browser: build SSG ausente; NÃO é PASS');
const { chromium } = require(process.env.FICHA_PLAYWRIGHT_MODULE || 'playwright');
const mime = { '.html':'text/html; charset=utf-8', '.css':'text/css', '.mjs':'text/javascript',
  '.js':'text/javascript', '.json':'application/json' };
const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const relative = url.pathname.endsWith('/') ? url.pathname + 'index.html' : url.pathname;
  const file = resolve(root, '.' + relative);
  if (!file.startsWith(root + sep)) { res.writeHead(403).end(); return; }
  readFile(file, (error, bytes) => {
    if (error) { res.writeHead(404).end(); return; }
    res.writeHead(200, { 'content-type': mime[extname(file)] || 'application/octet-stream' }).end(bytes);
  });
});

let browser;
try {
  await new Promise(ok => server.listen(0, '127.0.0.1', ok));
  const base = `http://127.0.0.1:${server.address().port}/ficha/`;
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  for (const width of [320, 375, 1440]) {
    const page = await browser.newPage({ viewport: { width, height: 800 } });
    const errors = [], remoteRequests = [];
    page.on('pageerror', e => errors.push(e.message));
    page.on('request', req => {
      if (!req.url().startsWith('http://127.0.0.1:')) remoteRequests.push(req.url());
    });
    const response = await page.goto(base, { waitUntil: 'networkidle' });
    assert.equal(response.status(), 200);
    assert.equal(await page.locator('html').getAttribute('lang'), 'pt-BR');
    assert.equal(await page.locator('#search-status').getAttribute('aria-live'), 'polite');
    await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(() => document.activeElement.className), 'skip-link');
    // Prova de semântica por papéis/nomes acessíveis (não equivale a leitor de tela real).
    for (const [role, name] of [
      ['navigation', 'Navegação principal'],
      ['combobox', 'Buscar por'],
      ['textbox', 'CNPJ ou termo'],
      ['button', 'Consultar'],
      ['status', undefined],
    ]) {
      assert.equal(await page.getByRole(role, name ? { name } : {}).count(), 1,
        `papel/nome acessível ausente: ${role}/${name ?? 'status'} em ${width}px`);
    }
    // Ciclo completo de Tab, incluindo link público: o foco deve ficar visível.
    for (const selector of [
      'header nav a.brand',
      'header nav a[href="#consulta"]',
      'header nav a[href="#origin-title"]',
      '#search-kind',
      '#search-term',
      '#search-form button',
      '.origin a[href="https://franklinbaldo.github.io/ficha/manifest.json"]',
    ]) {
      await page.keyboard.press('Tab');
      assert.equal(await page.locator(selector).evaluate(el => el === document.activeElement),
        true, `ordem de Tab incorreta em ${width}px: ${selector}`);
      const focus = await page.locator(selector).evaluate(el => {
        const style = getComputedStyle(el);
        const bounds = el.getBoundingClientRect();
        return { outline: style.outlineStyle, width: Number.parseFloat(style.outlineWidth),
          visible: bounds.width > 0 && bounds.height > 0 };
      });
      assert.ok(focus.visible && focus.outline === 'solid' && focus.width >= 2,
        `foco não perceptível em ${width}px: ${selector} ${JSON.stringify(focus)}`);
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1),
      false, `overflow horizontal em ${width}px`);
    await page.locator('#search-term').fill('00000000E08G12');
    await page.locator('#search-form button').click();
    await page.waitForFunction(() => document.querySelector('#search-status').textContent.includes('indisponível'));
    assert.equal(await page.locator('#search-status').getAttribute('data-state'), 'unavailable');
    assert.equal(await page.locator('#search-result').isVisible(), false);
    await page.locator('#search-term').fill('123');
    assert.match(await page.locator('#search-status').textContent(), /Nenhuma consulta realizada/);
    await page.locator('#search-form button').click();
    assert.match(await page.locator('#search-status').textContent(), /14 caracteres/);
    await page.locator('#search-kind').selectOption('pessoa');
    assert.match(await page.locator('#search-status').textContent(), /Nenhuma consulta realizada/);
    // Teste de ampliação de texto 200%, independente do tamanho normal do viewport.
    // WCAG 1.4.4/1.4.10: não aceitar rolagem horizontal em 320 CSS px.
    await page.evaluate(() => { document.documentElement.style.fontSize = '200%'; });
    assert.equal(
      await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1),
      false, `overflow horizontal com texto a 200% em ${width}px`
    );
    for (const selector of ['#search-kind', '#search-term', '#search-form button', '#search-status']) {
      assert.equal(await page.locator(selector).isVisible(), true,
        `controle oculto com texto 200%: ${selector}`);
    }
    // Submissão pelo teclado também deve preservar o estado de indisponibilidade.
    await page.locator('#search-kind').selectOption('cnpj');
    await page.locator('#search-term').fill('00000000E08G12');
    await page.locator('#search-term').press('Enter');
    await page.waitForFunction(() => document.querySelector('#search-status')?.dataset.state === 'unavailable');
    assert.equal(await page.locator('#search-result').isVisible(), false);
    assert.deepEqual(errors, [], 'erros JS no navegador');
    assert.deepEqual(remoteRequests, [], 'a PoC NÃO pode consultar dados reais');
    console.log(`BROWSER_PASS width=${width} focus=skip-link mock/unavailable/invalid/reset text-200 keyboard-submit tab-order focus-visible accessible-names no-overflow no-remote-fetch`);
    await page.close();
  }
} catch (error) {
  console.error('BROWSER_FAIL', error.stack);
  process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  await new Promise(ok => server.close(ok));
}
