import { chromium } from 'playwright';
import axe from 'axe-core';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/ficha/';
const outputDir = process.argv[3] ?? '../accessibility-evidence';
await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
await page.goto(url, { waitUntil: 'domcontentloaded' });

// Preserve diagnostics even if the SSG identity or accessibility assertion fails.
await writeFile(`${outputDir}/served-home.html`, await page.content());
await page.screenshot({ path: `${outputDir}/home-mobile.png`, fullPage: true });
// The SSG is intentionally static. Do not treat old Astro states as SSG evidence.
const siteMode = process.env.FICHA_SITE_MODE ?? 'astro';
if (siteMode === 'ssg') {
  const charset = await page.evaluate(() => document.characterSet);
  if (charset.toUpperCase() !== 'UTF-8') {
    throw new Error(`SSG served with invalid charset: ${charset}`);
  }
  const hasSearch = (await page.$('#busca-form')) !== null;
  if (hasSearch) {
    await page.waitForSelector('#busca-form', { timeout: 15_000 });
    await page.locator('aside[aria-labelledby="referencia-titulo"] #competencia')
      .waitFor({ state: 'visible' });
    for (const viewport of [{ width: 390, height: 844 }, { width: 1440, height: 960 }]) {
      await page.setViewportSize(viewport);
      const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      if (pageWidth > viewport.width) {
        throw new Error(`Home SSG: conteúdo de ${pageWidth}px excede viewport de ${viewport.width}px`);
      }
    }
    await page.locator('footer #ssg-search-btn').waitFor({ state: 'visible' });
    await page.screenshot({ path: `${outputDir}/home-desktop.png`, fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    // Text-only enlargement (not native browser zoom) must reflow at 320/375px.
    for (const width of [320, 375]) {
      await page.setViewportSize({ width, height: 812 });
      const scrollWidth = await page.evaluate(() => {
        document.documentElement.style.fontSize = '200%';
        return document.documentElement.scrollWidth;
      });
      if (scrollWidth > width) {
        throw new Error(`Home SSG: texto 200% transborda ${scrollWidth}px / ${width}px`);
      }
    }
    await page.evaluate(() => document.documentElement.style.removeProperty('font-size'));
    await page.setViewportSize({ width: 390, height: 844 });
  } else {
    await page.getByRole('heading', {
      name: 'Pesquisa por CNPJ ainda indisponível',
      exact: true,
    }).waitFor({ timeout: 15_000 });
  }
} else {
  // Keep the legacy Astro diagnosis usable for historical replays.
  await page.waitForFunction(() => {
    const text = document.body.innerText;
    return text.includes('Pronto para consultas') ||
      text.includes('A consulta está temporariamente indisponível.') ||
      text.includes('Os dados ainda não foram publicados') ||
      text.includes('Manifest inválido');
  }, null, { timeout: 45_000 });
}

await page.addScriptTag({ content: axe.source });
const axeResult = await page.evaluate(async () => window.axe.run(document, {
  resultTypes: ['violations'],
}));
const serious = axeResult.violations.filter((item) => ['serious', 'critical'].includes(item.impact));

const interactiveSelector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';
const expectedControls = await page.evaluate((selector) => {
  return [...document.querySelectorAll(selector)]
    .filter((el) => {
      // Descendants of a closed <details> are not in the tab sequence, even if
      // getBoundingClientRect() reports a box. The owning summary remains focusable.
      const closedDetails = el.closest('details:not([open])');
      if (closedDetails && !(el.tagName === 'SUMMARY' && el.parentElement === closedDetails)) {
        return false;
      }
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
    })
    .map((el, index) => {
      const style = getComputedStyle(el);
      const id = `cobogo-a11y-${index}`;
      el.dataset.cobogoA11yId = id;
      return {
        id,
        tag: el.tagName,
        href: el.getAttribute('href'),
        name: el.getAttribute('name'),
        text: (el.textContent || '').trim().slice(0, 80),
        unfocusedStyle: {
          outline: `${style.outlineStyle} ${style.outlineWidth} ${style.outlineColor}`,
          boxShadow: style.boxShadow,
          border: `${style.borderWidth} ${style.borderColor}`,
          background: style.backgroundColor,
        },
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
    const focusedStyle = {
      outline: `${style.outlineStyle} ${style.outlineWidth} ${style.outlineColor}`,
      outlineVisible: style.outlineStyle !== 'none' && parseFloat(style.outlineWidth || '0') > 0,
      boxShadow: style.boxShadow,
      border: `${style.borderWidth} ${style.borderColor}`,
      background: style.backgroundColor,
    };
    return {
      id,
      tag: el.tagName,
      href: el.getAttribute('href'),
      name: el.getAttribute('name'),
      text: (el.textContent || '').trim().slice(0, 80),
      focusedStyle,
      width: rect.width,
      height: rect.height,
    };
  });
  if (!focus) continue;
  if (focus.id) visited.add(focus.id);
  const unfocused = expectedControls.find((control) => control.id === focus.id)?.unfocusedStyle;
  const focused = focus.focusedStyle;
  // A permanent border or shadow is not a keyboard-focus indicator.
  const outlineChanged = focused.outlineVisible && focused.outline !== unfocused?.outline;
  const shadowChanged = focused.boxShadow !== 'none' && focused.boxShadow !== unfocused?.boxShadow;
  const borderChanged = unfocused && focused.border !== unfocused.border;
  const backgroundChanged = unfocused && focused.background !== unfocused.background;
  focus.visibleIndicator = Boolean(unfocused && (
    outlineChanged || shadowChanged || borderChanged || backgroundChanged
  ));
  focusTrace.push(focus);
  if (!focus.visibleIndicator) invisibleFocus += 1;
}

const unreachedControls = expectedControls.filter((control) => !visited.has(control.id));
let skipLinkResult = null;
if (siteMode === 'ssg') {
  // A skip link that is visible on Tab but does not move the keyboard path is broken.
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.keyboard.press('Tab');
  const skipLinkFocused = await page.evaluate(() =>
    document.activeElement?.matches('a.skip-link[href="#main"]') === true
  );
  await page.keyboard.press('Enter');
  const skipTargetFocused = await page.evaluate(() =>
    location.hash === '#main' && document.activeElement?.id === 'main'
  );
  await page.keyboard.press('Tab');
  const nextTabInsideMain = await page.evaluate(() =>
    document.querySelector('main')?.contains(document.activeElement) === true
  );
  skipLinkResult = {
    skip_link_focused: skipLinkFocused,
    main_focused_after_enter: skipTargetFocused,
    next_tab_inside_main: nextTabInsideMain,
    passed: skipLinkFocused && skipTargetFocused && nextTabInsideMain,
  };
}
const result = {
  site_mode: siteMode,
  url,
  commit: process.env.GITHUB_HEAD_SHA || process.env.GITHUB_SHA || null,
  axe_serious_or_critical: serious,
  expected_keyboard_controls: expected,
  reached_keyboard_controls: visited.size,
  unreached_keyboard_controls: unreachedControls,
  invisible_focus_steps: invisibleFocus,
  skip_link: skipLinkResult,
  focus_trace: focusTrace,
};
await writeFile(`${outputDir}/accessibility-evidence.json`, `${JSON.stringify(result, null, 2)}\n`);
await browser.close();

if (serious.length) throw new Error(`axe encontrou ${serious.length} violação(ões) séria(s)/crítica(s)`);
if (visited.size < expected) throw new Error(`teclado alcançou ${visited.size} de ${expected} controles esperados`);
if (invisibleFocus) throw new Error(`${invisibleFocus} passo(s) de foco sem alteração visual`);
if (skipLinkResult && !skipLinkResult.passed) {
  throw new Error('Skip-link não moveu foco e Tab seguinte para dentro de main');
}
