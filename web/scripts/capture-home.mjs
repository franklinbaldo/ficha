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

await mkdir(outputDir, { recursive: true });
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
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

const filename = `home-${state}-1280x900.png`;
await page.screenshot({ path: `${outputDir}/${filename}`, fullPage: false });

const evidence = {
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
};

await writeFile(`${outputDir}/capture-state.json`, `${JSON.stringify(evidence, null, 2)}\n`);
console.log(`capture_state=${state}`);
console.log(`capture_screenshot=${filename}`);
if (evidence.final_status_excerpt) console.log(`capture_status=${evidence.final_status_excerpt}`);

await browser.close();
