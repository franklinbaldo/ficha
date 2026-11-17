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
<a href="https://franklinbaldo.github.io/ficha/manifest.json">manifesto</a></main></body></html>`;

function fixture(change = () => {}) {
  const dir = mkdtempSync(join(tmpdir(), 'ficha-ssg-smoke-'));
  for (const sub of ['css', 'js']) mkdirSync(join(dir, sub));
  writeFileSync(join(dir, 'index.html'), validHtml);
  writeFileSync(join(dir, 'css/style.css'), 'a:focus-visible{outline:2px solid red}');
  writeFileSync(join(dir, 'js/main.mjs'), 'const createLookupController=()=>null;');
  writeFileSync(join(dir, 'js/consult.mjs'), 'export function createLookupController(){}');
  writeFileSync(join(dir, 'ssg-generated-webmanifest.json'), JSON.stringify({name:'Static Site'}));
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

test('oracle negativo: widget genérico sobreviveu à finalização', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('</body>', '<div id="ssg-search-widget"></div></body>')),
    dir => assert.throws(() => verifyBuiltShell(dir), /busca genérica do SSG/));
});
test('oracle negativo: manifesto falso ocupa contrato RFB', () => {
  withFixture(dir => writeFileSync(join(dir, 'manifest.json'), JSON.stringify({name:'Static Site',start_url:'/'})),
    dir => assert.throws(() => verifyBuiltShell(dir), /não é um contrato de dados RFB/));
});

test('oracle negativo: âncora interna aponta para ID ausente', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('href="#main-content"', 'href="#inexistente"')),
    dir => assert.throws(() => verifyBuiltShell(dir), /âncora sem destino/));
});
test('oracle negativo: link local sai do prefixo /ficha/', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('</body>', '<a href="/admin/">externo</a></body>')),
    dir => assert.throws(() => verifyBuiltShell(dir), /URL local fora da base/));
});
test('oracle negativo: path traversal escapa de /ficha/', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('</body>', '<a href="/ficha/../admin">perigoso</a></body>')),
    dir => assert.throws(() => verifyBuiltShell(dir), /URL local escapa da base/));
});
test('oracle negativo: asset sob /ficha/ mas ausente no build', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'), validHtml.replace('</body>', '<script src="/ficha/js/fantasma.mjs"></script></body>')),
    dir => assert.throws(() => verifyBuiltShell(dir), /asset local ausente/));
});

test('oracle negativo: script remoto não é permitido no shell offline', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'),
    validHtml.replace('</body>', '<script src="https://cdn.example.invalid/poc.js"></script></body>')),
  dir => assert.throws(() => verifyBuiltShell(dir), /URL externa não autorizada/));
});
test('oracle negativo: link HTTPS arbitrário não é exceção para asset ou origem', () => {
  withFixture(dir => writeFileSync(join(dir, 'index.html'),
    validHtml.replace('</body>', '<a href="https://example.invalid/ficha/manifest.json">origem não permitida</a></body>')),
  dir => assert.throws(() => verifyBuiltShell(dir), /URL externa não autorizada/));
});
