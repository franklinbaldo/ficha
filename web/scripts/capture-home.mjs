import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';

const url = process.argv[2] ?? 'http://127.0.0.1:4321/ficha/';
const outputDir = process.argv[3] ?? '../visual-evidence';
const deadlineMs = 45_000;
const pollMs = 500;

const states = [
  {
    id: 'hydrated',
    test: (text) => text.includes('Pronto para consultas'),
  },
  {
    id: 'remote-data-unavailable',
    test: (text) =>
      text.includes('Erro:') ||
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
        line.startsWith('Erro:') ||
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
  commit: process.env.GITHUB_SHA ?? null,
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
