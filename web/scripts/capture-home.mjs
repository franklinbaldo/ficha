import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/ficha/';
const outputDir = process.argv[3] ?? '../visual-evidence';
const deadlineMs = 45_000;
const pollMs = 500;
const friendlyRemoteMessage = 'A consulta está temporariamente indisponível.';

const states = [
  {
    id: 'hydrated',
    test: (text) => text.includes('Pronto para consultas'),
  },
  {
    id: 'remote-data-unavailable',
    test: (text) =>
      text.includes(friendlyRemoteMessage) ||
      text.includes('Os dados ainda não foram publicados') ||
      text.includes('Manifest inválido'),
  },
];

const viewports = [
  { id: 'desktop', width: 1280, height: 900 },
  { id: 'narrow', width: 390, height: 844 },
];

await mkdir(outputDir, { recursive: true });
const browser = await chromium.launch({ headless: true });
const captures = [];

for (const viewport of viewports) {
  const page = await browser.newPage({ viewport: { width: viewport.width, height: viewport.height } });
  const consoleMessages = [];
  const pageErrors = [];

  page.on('console', (message) => consoleMessages.push(`${message.type()}: ${message.text()}`));
  page.on('pageerror', (error) => pageErrors.push(error.message));

  await page.goto(url, { waitUntil: 'domcontentloaded' });

  const startedAt = Date.now();
  let state = 'incomplete';
  let bodyText = '';
  while (Date.now() - startedAt < deadlineMs) {
    bodyText = await page.locator('body').innerText();
    const match = states.find((candidate) => candidate.test(bodyText));
    if (match) {
      state = match.id;
      break;
    }
    await page.waitForTimeout(pollMs);
  }

  if (bodyText.split('\n').some((line) => line.trim().startsWith('Erro:'))) {
    throw new Error('raw initialization error remains visible in the page');
  }

  if (state === 'remote-data-unavailable' && bodyText.includes(friendlyRemoteMessage)) {
    for (const forbidden of ['NetworkError', 'XMLHttpRequest']) {
      if (bodyText.includes(forbidden)) {
        throw new Error(`technical error leaked into the visible recovery state: ${forbidden}`);
      }
    }
    // The page has another manifest.json link outside the failure state.
    // Checking body text alone is a false positive when recovery disappears
    // or its insertion lags behind the status update on a narrow viewport.
    const recovery = page.locator('[data-remote-recovery]');
    await recovery.waitFor({ state: 'visible', timeout: 5_000 });
    const links = recovery.locator('a');
    if (await links.count() !== 1) {
      throw new Error('recovery state must expose exactly one visible manifest link');
    }
    const link = links.first();
    const href = await link.getAttribute('href');
    const expectedPath = new URL('./manifest.json', page.url()).pathname;
    if (!href || new URL(href, page.url()).pathname !== expectedPath ||
        (await link.textContent())?.trim() !== 'manifest.json') {
      throw new Error('recovery link does not point to the published manifest.json');
    }
    bodyText = await page.locator('body').innerText();
    if (bodyText.split('\n').some((line) => line.trim().startsWith('Erro:'))) {
      throw new Error('raw initialization error reappeared after recovery became visible');
    }
  }

  const filename = `home-${state}-${viewport.width}x${viewport.height}.png`;
  await page.screenshot({ path: `${outputDir}/${filename}`, fullPage: false });

  captures.push({
    viewport,
    url,
    state,
    screenshot: filename,
    waited_ms: Date.now() - startedAt,
    final_status_excerpt: bodyText
      .split('\n')
      .map((line) => line.trim())
      .find((line) =>
        line.includes('Pronto para consultas') ||
        line.includes(friendlyRemoteMessage) ||
        line.includes('Os dados ainda não foram publicados') ||
        line.includes('Manifest inválido') ||
        line.includes('Preparando o mecanismo de consulta') ||
        line.includes('Carregando dados de')
      ) ?? null,
    page_errors: pageErrors,
    console_tail: consoleMessages.slice(-20),
  });

  await page.close();
}

const evidence = {
  url,
  commit: process.env.GITHUB_HEAD_SHA || process.env.GITHUB_SHA || null,
  captures,
};

await writeFile(`${outputDir}/capture-state.json`, `${JSON.stringify(evidence, null, 2)}\n`);
for (const capture of captures) {
  console.log(`capture_viewport=${capture.viewport.id}:${capture.viewport.width}x${capture.viewport.height}`);
  console.log(`capture_state=${capture.state}`);
  console.log(`capture_screenshot=${capture.screenshot}`);
  if (capture.final_status_excerpt) console.log(`capture_status=${capture.final_status_excerpt}`);
}

await browser.close();
