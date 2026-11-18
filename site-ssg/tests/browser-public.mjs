// Read-only Chromium evidence from the real, already-published Astro Pages.
// Not a pass/fail oracle for the SSG shadow. Do not fetch the complete Parquet.
// Every conclusion is observational and scoped to this run.
import { chromium } from 'playwright';
import { mkdir, writeFile, appendFile } from 'node:fs/promises';

const publicUrl = 'https://franklinbaldo.github.io/ficha/';
const output = 'evidence-public';
await mkdir(output, { recursive: true });

const evidence = {
  observedAt: new Date().toISOString(),
  publicUrl,
  branchSHA: process.env.GITHUB_HEAD_SHA || process.env.GITHUB_SHA || null,
  mode: 'PUBLICADO_VERIFICADO: browser, read-only',
  viewports: [],
};

let browser;
try {
  browser = await chromium.launch({headless: true});
  for (const viewport of [
    {name: 'mobile', width: 375, height: 812},
    {name: 'desktop', width: 1440, height: 900},
  ]) {
    const page = await browser.newPage({
      viewport: {width: viewport.width, height: viewport.height},
      serviceWorkers: 'block',
    });
    page.setDefaultTimeout(15000);
    const report = {viewport, status: 'NOT_RUN', console: [], failures: [], archiveRequests: []};
    page.on('console', message => {
      const line = `${message.type()}: ${message.text()}`;
      if (/cors|archive\.org|parquet|duckdb|error/i.test(line)) report.console.push(line.slice(0, 500));
    });
    page.on('pageerror', error => report.console.push(`pageerror: ${error.message}`));
    page.on('requestfailed', request => {
      const url = request.url();
      if (/archive\.org|manifest\.json|parquet/i.test(url)) {
        report.failures.push({url, failure: request.failure()?.errorText || null});
      }
    });
    page.on('response', response => {
      if (/archive\.org|manifest\.json/i.test(response.url())) {
        report.archiveRequests.push({
          url: response.url(), status: response.status(),
          contentRange: response.headers()['content-range'] ?? null,
          allowOrigin: response.headers()['access-control-allow-origin'] ?? null,
        });
      }
    });
    try {
      const response = await page.goto(publicUrl, {
        waitUntil: 'domcontentloaded', timeout: 30000,
      });
      report.documentStatus = response?.status() ?? null;
      await page.locator('main').waitFor({state:'attached'});
      report.title = await page.title();

      // Read the manifest in the browser origin, never substitute the local copy.
      const manifest = await page.evaluate(async () => {
        const href = document.body.dataset.manifestHref || './manifest.json';
        const url = new URL(href, location.href).href;
        const response = await fetch(url, {cache:'no-store'});
        if (!response.ok) return {url, httpStatus: response.status};
        const payload = await response.json();
        const chosen = payload.snapshots?.filter(s => s.date === payload.current) ?? [];
        const parquet = chosen.length === 1 ? chosen[0]?.files?.cnpjs : null;
        return {
          url, httpStatus: response.status, current: payload.current ?? null,
          matches: chosen.length,
          parquetUrl: parquet?.url ?? null,
          parquetSize: parquet?.size ?? null,
          archiveSha256Declared: parquet?.sha256 ?? null,
        };
      });
      report.manifest = manifest;

      // This is a genuine browser CORS/Range probe of ONE BYTE.
      // Do not read, buffer or download the Parquet.
      if (manifest.parquetUrl && Number.isSafeInteger(manifest.parquetSize)) {
        report.rangeProbe = await page.evaluate(async url => {
          const controller = new AbortController();
          const deadline = setTimeout(() => controller.abort(), 10000);
          try {
            const response = await fetch(url, {
              method: 'GET', mode: 'cors', cache: 'no-store',
              headers: {Range:'bytes=0-0'}, signal: controller.signal,
            });
            const result = {
              httpStatus: response.status,
              contentRangeVisible: response.headers.get('content-range'),
              allowOriginVisible: response.headers.get('access-control-allow-origin'),
              responseUrl: response.url,
            };
            await response.body?.cancel();
            return result;
          } catch (error) {
            return {blocked: true, errorName: error?.name ?? 'Error',
              message: String(error?.message ?? error).slice(0, 240)};
          } finally { clearTimeout(deadline); }
        }, manifest.parquetUrl);
      }

      // Give initialization a chance to reach a terminal user-visible state.
      // Capturing during "Preparando..." would misclassify the recovery link.
      try {
        await page.waitForFunction(() => {
          const status = document.querySelector('.status')?.textContent ?? '';
          return /Pronto para consultas|temporariamente indisponível|dados ainda não foram publicados|Manifest inválido|^Erro/i.test(status);
        }, undefined, {timeout:20000});
        report.initializationSettled = true;
      } catch {
        report.initializationSettled = false; // still record what the user saw
      }

      // Check the existing search UI and its accessible recovery link.
      const input = page.locator('#busca-principal');
      try {
        await input.waitFor({state:'visible', timeout:15000});
        report.searchInputVisible = true;
        report.searchInputEnabled = await input.isEnabled();
        const button = page.locator('.input-group button').first();
        report.searchButtonEnabled = await button.isEnabled();
        if (report.searchButtonEnabled) {
          await input.fill('00000000000191');
          await button.click({timeout:3000});
          report.searchAttempted = true;
          await page.waitForTimeout(1000);
        } else report.searchAttempted = false;
      } catch (error) {
        report.searchInputVisible = false;
        report.searchUiError = error?.message?.slice(0, 280) ?? String(error);
      }
      report.statusText = await page.locator('.status').first().textContent().catch(() => null);
      report.recoveryText = await page.locator('[data-remote-recovery]').first()
        .textContent().catch(() => null);
      report.recoveryLinkVisible = await page.locator('a[href*="manifest.json"]').first()
        .isVisible().catch(() => false);
      report.resultsCount = await page.locator('.results-list > *').count();
      await page.screenshot({path:`${output}/public-${viewport.name}.png`});
      report.status = 'BROWSER_OBSERVED';
    } catch (error) {
      report.status = 'INCOMPLETE';
      report.error = error?.message?.slice(0, 500) ?? String(error);
    } finally {
      evidence.viewports.push(report);
      await page.close();
    }
  }
} catch (error) {
  evidence.error = error?.message ?? String(error);
} finally {
  await browser?.close();
  await writeFile(`${output}/evidence.json`, JSON.stringify(evidence, null, 2) + '\n');
  console.log('PUBLIC_PAGES_CHROMIUM_EVIDENCE=' + JSON.stringify(evidence));
  if (process.env.GITHUB_STEP_SUMMARY) {
    await appendFile(process.env.GITHUB_STEP_SUMMARY,
      '\n### Public Pages Chromium: observational, not SSG parity\n' +
      '\n\`evidence-public/evidence.json\` records the published manifest, one-byte CORS/Range probe, visible search/recovery and console by viewport. ' +
      'A successful job does not imply working CNPJ search.\n');
  }
}
