// Read-only Chromium evidence from the real, already-published Astro Pages.
// Not a pass/fail oracle for the SSG shadow. Do not fetch the complete Parquet.
// Every conclusion is observational and scoped to this run.
import { chromium } from 'playwright';
import { mkdir, writeFile, appendFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';

const publicUrl = 'https://franklinbaldo.github.io/ficha/';
const output = 'evidence-public';
await mkdir(output, { recursive: true });

const evidence = {
  observedAt: new Date().toISOString(),
  publicUrl,
  // GITHUB_SHA on pull_request is often a synthetic merge ref, not the checked-out head.
  branchSHA: execFileSync('git', ['rev-parse', 'HEAD'], {encoding:'utf8'}).trim(),
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
    const report = {viewport, status: 'NOT_RUN', console: [], failures: [], archiveRequests: [], blockedArchiveRequests: []};
    // The live Astro app may request many ranges or even a full Parquet if CORS
    // becomes available. Block that application's Archive traffic. A separate
    // sterile page below performs the *only* permitted remote one-byte probe.
    await page.route(/^https:\/\/[^/]*archive\.org\//, async route => {
      report.blockedArchiveRequests.push({
        url: route.request().url(),
        range: route.request().headers().range ?? null,
      });
      await route.abort('blockedbyclient');
    });
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
        // Synthetic document *at the real Pages origin*: no Astro script can
        // race this probe, and only the explicit bytes=0-0 fetch reaches IA.
        const probePage = await browser.newPage({serviceWorkers:'block'});
        try {
          await probePage.route(publicUrl, route => route.fulfill({
            status:200, contentType:'text/html',
            body:'<!doctype html><html lang="pt-BR"><title>Probe</title></html>',
          }));
          await probePage.goto(publicUrl, {waitUntil:'domcontentloaded'});
          report.rangeProbe = await probePage.evaluate(async url => {
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
        } finally {
          await probePage.close();
        }
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
      report.recoveryLinkVisible = await page.locator('[data-remote-recovery] a[href*="manifest.json"]').first()
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
