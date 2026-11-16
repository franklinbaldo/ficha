import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function readRequired(dir, relative) {
  const file = join(dir, relative);
  assert.ok(existsSync(file), `arquivo de build ausente: ${relative}`);
  return readFileSync(file, 'utf8');
}

export function verifyBuiltShell(dir) {
  const html = readRequired(dir, 'index.html');
  const css = readRequired(dir, 'css/style.css');
  const js = readRequired(dir, 'js/main.mjs');
  const consult = readRequired(dir, 'js/consult.mjs');
  assert.match(html, /<html\b[^>]*\blang=["']pt-BR["']/i, 'idioma da página');
  assert.doesNotMatch(html, /\{\{!?[^}]+\}\}|\{%[^%]+%\}/, 'template não resolvido');
  for (const id of ['main-content', 'search-form', 'search-kind', 'search-term', 'search-status', 'search-result']) {
    assert.match(html, new RegExp(`\\bid=["']${id}["']`), `elemento ${id} ausente`);
  }
  assert.match(html, /href=["']\/ficha\/css\/style\.css["']/, 'CSS fora da base /ficha/');
  assert.match(html, /src=["']\/ficha\/js\/main\.mjs["']/, 'módulo JS não conectado ao HTML');
  assert.match(html, /href=["']\/ficha\/manifest\.json["']/, 'link do manifesto precisa manter base /ficha/');
  assert.match(html, /aria-live=["']polite["']/, 'resultado sem live region');
  assert.match(css, /:focus-visible/, 'indicação de foco ausente');
  assert.match(js, /createLookupController/, 'módulo de UI não conectado ao adaptador');
  assert.match(consult, /export function createLookupController/, 'módulo de consulta não emitido');
  return { htmlBytes: Buffer.byteLength(html), cssBytes: Buffer.byteLength(css), jsBytes: Buffer.byteLength(js) + Buffer.byteLength(consult) };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const output = resolve(process.argv[2] || join(project, 'dist/ficha'));
  try {
    const result = verifyBuiltShell(output);
    console.log(`SSG_OUTPUT_PASS ${JSON.stringify({ output, ...result })}`);
  } catch (error) {
    console.error(`SSG_OUTPUT_FAIL ${error.message}`);
    process.exitCode = 1;
  }
}
