import assert from 'node:assert/strict';
import { existsSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const WIDGET_MARKER = '<!-- SSG Search Widget -->';

function checkGeneratedManifest(file) {
  const manifest = JSON.parse(readFileSync(file, 'utf8'));
  assert.ok(!('current' in manifest) && !('snapshots' in manifest),
    'manifesto RFB: nunca renomear, sobrescrever nem alterar');
  // SSG 0.0.76/staticdatagen 0.0.32 uses the page frontmatter 'name'
  // for the PWA web manifest. Missing 'name' emits an ERROR but exits zero.
  // Refuse that older fallback as well as unknown data-manifest contracts.
  assert.deepEqual(Object.keys(manifest).sort(), [
    'background_color', 'description', 'display', 'name',
    'orientation', 'scope', 'short_name', 'start_url', 'theme_color',
  ], 'formato do manifesto gerado pelo SSG mudou: parar');
  assert.equal(manifest.name, 'FICHA', 'manifesto SSG desconhecido: parar');
  assert.equal(manifest.start_url, '.', 'manifesto SSG mudou: parar');
  assert.equal(manifest.display, 'standalone', 'manifesto SSG mudou: parar');
  assert.equal(manifest.scope, '/', 'manifesto SSG mudou: parar');
  assert.equal(manifest.background_color, '#ffffff', 'manifesto SSG mudou: parar');
  assert.equal(manifest.orientation, 'portrait-primary', 'manifesto SSG mudou: parar');
  assert.equal(manifest.short_name, null, 'manifesto SSG mudou: parar');
  assert.equal(manifest.theme_color, null, 'manifesto SSG mudou: parar');
  assert.ok(typeof manifest.description === 'string'
    && manifest.description.startsWith('Ensaio isolado do shell estático do Ficha'),
    'descrição do manifesto SSG mudou: parar');
}

/**
 * Isolate ssg@0.0.76 extras from the P3-A prototype: strip only the known
 * built-in documentation-search widget and quarantine its synthetic web manifest.
 * Fail closed on any upstream format drift. Never edit web/, IA, or RFB data.
 */
export function finalizeOutput(root = project) {
  const out = join(root, 'dist/ficha');
  const htmlFile = join(out, 'index.html');
  const manifestFile = join(out, 'manifest.json');
  const quarantined = join(out, 'ssg-generated-webmanifest.json');
  let html = readFileSync(htmlFile, 'utf8');
  const start = html.indexOf(WIDGET_MARKER);
  let widgetRemoved = false;

  if (start !== -1) {
    assert.equal(html.indexOf(WIDGET_MARKER, start + WIDGET_MARKER.length), -1,
      'widget SSG duplicado');
    const end = html.lastIndexOf('</body>');
    assert.ok(end > start && html.indexOf('</body>') === end, 'widget fora do rodapé do body');
    const widget = html.slice(start, end);
    assert.match(widget, /id="ssg-search-widget"/, 'marcador do widget mudou');
    assert.match(widget, /id="ssg-search-overlay"/, 'estrutura do widget mudou');
    const scripts = [...widget.matchAll(/<script\b[^>]*src="(\/ficha\/_csp\/[a-f0-9]+\.js)"[^>]*><\/script>/g)];
    assert.equal(scripts.length, 1, 'script inesperado no widget');
    const script = readFileSync(join(out, scripts[0][1].slice('/ficha/'.length)), 'utf8');
    assert.match(script, /ssg-search-overlay/, 'script não corresponde ao widget');
    const links = [...html.slice(0, start).matchAll(/<link\b[^>]*href="(\/ficha\/_csp\/[a-f0-9]+\.css)"[^>]*>/g)];
    assert.equal(links.length, 1, 'CSS do widget mudou');
    const css = readFileSync(join(out, links[0][1].slice('/ficha/'.length)), 'utf8');
    assert.match(css, /#ssg-search-btn/, 'CSS não corresponde ao widget');
    html = html.slice(0, start).replace(links[0][0], '') + html.slice(end);
    widgetRemoved = true;
  }
  assert.doesNotMatch(html, /ssg-search-(widget|btn|overlay|input)/,
    'fragmento de busca genérica permanece no HTML');

  if (existsSync(manifestFile)) {
    assert.ok(!existsSync(quarantined), 'manifesto isolado já existe; build não foi limpo');
    checkGeneratedManifest(manifestFile);
    // All checks succeeded before the first write.
    writeFileSync(htmlFile, html);
    renameSync(manifestFile, quarantined);
  } else {
    assert.ok(existsSync(quarantined), 'manifesto SSG ausente sem isolamento documentado');
    checkGeneratedManifest(quarantined);
    assert.ok(!widgetRemoved, 'HTML alterado sem manifesto original: build inconsistente');
  }
  return { widgetRemoved, manifestIsolated: true };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    console.log('SSG_FINALIZE_PASS ' + JSON.stringify(finalizeOutput()));
  } catch (error) {
    console.error('SSG_FINALIZE_FAIL ' + error.message);
    process.exitCode = 1;
  }
}
