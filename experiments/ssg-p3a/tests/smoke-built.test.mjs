import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { verifyBuiltShell } from './smoke-built.mjs';

const validHtml = `<!doctype html><html lang="pt-BR"><head>
<link rel="stylesheet" href="/ficha/css/style.css">
<script type="module" src="/ficha/js/main.mjs"></script></head>
<body><a href="#main-content">Pular</a><main id="main-content">
<form id="search-form"><select id="search-kind"></select><input id="search-term"></form>
<p id="search-status" aria-live="polite"></p><section id="search-result"></section>
<a href="/ficha/manifest.json">manifesto</a></main></body></html>`;

function fixture(change = () => {}) {
  const dir = mkdtempSync(join(tmpdir(), 'ficha-ssg-smoke-'));
  for (const sub of ['css', 'js']) mkdirSync(join(dir, sub));
  writeFileSync(join(dir, 'index.html'), validHtml);
  writeFileSync(join(dir, 'css/style.css'), 'a:focus-visible{outline:2px solid red}');
  writeFileSync(join(dir, 'js/main.mjs'), 'const createLookupController=()=>null;');
  writeFileSync(join(dir, 'js/consult.mjs'), 'export function createLookupController(){}');
  change(dir);
  return dir;
}
function withFixture(change, fn) {
  const dir = fixture(change);
  try { fn(dir); } finally { rmSync(dir, {recursive:true, force:true}); }
}
test('oracle: shell completo sintético passa (não é SSG real)', () => {
  withFixture(() => {}, dir => assert.ok(verifyBuiltShell(dir).htmlBytes > 0));
});
test('oracle negativo: JS não publicado falha', () => {
  withFixture(dir => rmSync(join(dir, 'js/main.mjs')), dir => assert.throws(() => verifyBuiltShell(dir), /arquivo de build ausente: js\/main\.mjs/));
});
test('oracle negativo: módulo omitido no HTML falha', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('<script type="module" src="/ficha/js/main.mjs"></script>', '')), dir => assert.throws(() => verifyBuiltShell(dir), /módulo JS não conectado/));
});
test('oracle negativo: base de CSS trocada falha', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('/ficha/css/style.css','/css/style.css')), dir => assert.throws(() => verifyBuiltShell(dir), /CSS fora da base/));
});
test('oracle negativo: token de template não resolvido falha', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml + '{{!content}}'), dir => assert.throws(() => verifyBuiltShell(dir), /template não resolvido/));
});
test('oracle negativo: live region ausente falha', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('aria-live="polite"','')), dir => assert.throws(() => verifyBuiltShell(dir), /resultado sem live region/));
});
