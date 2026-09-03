import { chromium } from 'playwright';
import axe from 'axe-core';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/ficha/';
const outputDir = process.argv[3] ?? '../accessibility-evidence';
await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
await page.goto(url, { waitUntil: 'domcontentloaded' });

// Wait for the same public states accepted by the visual contract.
await page.waitForFunction(() => {
  const text = document.body.innerText;
  return text.includes('Pronto para consultas') ||
    text.includes('A consulta está temporariamente indisponível.') ||
    text.includes('Os dados ainda não foram publicados') ||
    text.includes('Manifest inválido');
}, null, { timeout: 45_000 });

await page.addScriptTag({ content: axe.source });
const axeResult = await page.evaluate(async () => window.axe.run(document, {
  resultTypes: ['violations'],
}));
const serious = axeResult.violations.filter((item) => ['serious', 'critical'].includes(item.impact));

const interactiveSelector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
const expectedControls = await page.evaluate((selector) => {
  return [...document.querySelectorAll(selector)].map((el, index) => {
    const id = `cobogo-a11y-${index}`;
    el.dataset.cobogoA11yId = id;
    return {
      id,
      tag: el.tagName,
      href: el.getAttribute('href'),
      name: el.getAttribute('name'),
      text: (el.textContent || '').trim().slice(0, 80),
    };
  });
}, interactiveSelector);

const expected = expectedControls.length;
const visited = new Set();
const focusTrace = [];
let invisibleFocus = 0;

await page.locator('body').click({ position: { x: 1, y: 1 } });
for (let i = 0; i < expected + 4; i += 1) {
  await page.keyboard.press('Tab');
  const focus = await page.evaluate(() => {
    const el = document.activeElement;
    if (!el || el === document.body) return null;
    const style = getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const id = el.dataset.cobogoA11yId || null;
    const visibleIndicator = style.outlineStyle !== 'none' && parseFloat(style.outlineWidth || '0') > 0 ||
      style.boxShadow !== 'none' || parseFloat(style.borderWidth || '0') > 0;
    return {
      id,
      tag: el.tagName,
      href: el.getAttribute('href'),
      name: el.getAttribute('name'),
      text: (el.textContent || '').trim().slice(0, 80),
      visibleIndicator,
      width: rect.width,
      height: rect.height,
    };
  });
  if (!focus) continue;
  if (focus.id) visited.add(focus.id);
  focusTrace.push(focus);
  if (!focus.visibleIndicator) invisibleFocus += 1;
}

const unreachedControls = expectedControls.filter((control) => !visited.has(control.id));
const result = {
  url,
  commit: process.env.GITHUB_HEAD_SHA || process.env.GITHUB_SHA || null,
  axe_serious_or_critical: serious,
  expected_keyboard_controls: expected,
  reached_keyboard_controls: visited.size,
  unreached_keyboard_controls: unreachedControls,
  invisible_focus_steps: invisibleFocus,
  focus_trace: focusTrace,
};
await writeFile(`${outputDir}/accessibility-evidence.json`, `${JSON.stringify(result, null, 2)}\n`);
await browser.close();

if (serious.length) throw new Error(`axe encontrou ${serious.length} violação(ões) séria(s)/crítica(s)`);
if (visited.size < expected) throw new Error(`teclado alcançou ${visited.size} de ${expected} controles esperados`);
if (invisibleFocus) throw new Error(`${invisibleFocus} passo(s) de foco sem indicador perceptível`);
