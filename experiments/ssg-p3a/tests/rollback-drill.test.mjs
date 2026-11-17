import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { inventory, rehearseRollback } from '../scripts/rollback-drill.mjs';

const html = `<!doctype html><html lang="pt-BR"><head><link rel="stylesheet" href="/ficha/css/style.css"><script type="module" src="/ficha/js/main.mjs"></script></head><body><main id="main-content"><form id="search-form"><select id="search-kind"></select><input id="search-term"></form><p id="search-status" aria-live="polite"></p><div id="search-result"></div><a href="https://franklinbaldo.github.io/ficha/manifest.json">manifesto</a></main></body></html>`;
function fixture(fn) {
  const root = mkdtempSync(join(tmpdir(), 'ficha-p3a-rollback-test-'));
  const old = join(root, 'old');
  const next = join(root, 'next');
  mkdirSync(old);
  mkdirSync(next);
  mkdirSync(join(next, 'css'));
  mkdirSync(join(next, 'js'));
  writeFileSync(join(old, 'index.html'), '<title>OLD ASTRO STAND-IN</title>');
  writeFileSync(join(old, 'manifest.json'), '{"current":"SYNTHETIC","snapshots":[]}');
  writeFileSync(join(next, 'index.html'), html);
  writeFileSync(join(next, 'css/style.css'), ':focus-visible{outline:2px solid blue}');
  writeFileSync(join(next, 'js/main.mjs'), 'const createLookupController = true;');
  writeFileSync(join(next, 'js/consult.mjs'), 'export function createLookupController(){}');
  writeFileSync(join(next, 'ssg-generated-webmanifest.json'), '{"name":"FICHA"}');
  try { fn(old, next); } finally { rmSync(root, { recursive: true, force: true }); }
}
test('rollback: candidate SSG enters only sandbox; original previous and candidate bytes survive', () => {
  fixture((old, next) => {
    const before = inventory(old).sha256;
    const fresh = inventory(next).sha256;
    const result = rehearseRollback(old, next, {expectedPreviousSha256: before, simulateFailure: true});
    assert.equal(result.switchedInSandbox, true);
    assert.equal(result.simulatedPostSwitchFailure, true);
    assert.equal(result.restoredSha256, before);
    assert.equal(inventory(next).sha256, fresh);
    assert.equal(readFileSync(join(old, 'manifest.json'), 'utf8'), '{"current":"SYNTHETIC","snapshots":[]}');
  });
});
test('rollback negativo: checksum anterior errado aborta antes da troca', () => {
  fixture((old, next) => assert.throws(() => rehearseRollback(old, next,
    {expectedPreviousSha256: '0'.repeat(64)}), /hash anterior divergente/));
});
test('rollback negativo: adulteração do candidato em staging aborta antes da troca', () => {
  fixture((old, next) => assert.throws(() => rehearseRollback(old, next, {
    expectedPreviousSha256: inventory(old).sha256,
    afterStage(staged) { writeFileSync(join(staged, 'index.html'), '<h1>tampered</h1>'); },
  }), /candidato adulterado/));
});
test('rollback negativo: manifesto SSG colidindo com contrato RFB aborta', () => {
  fixture((old, next) => {
    writeFileSync(join(next, 'manifest.json'), '{"name":"Static Site"}');
    assert.throws(() => rehearseRollback(old, next, {expectedPreviousSha256: inventory(old).sha256}), /contrato de dados RFB/);
  });
});
test('rollback negativo: symlink no artefato anterior é recusado', () => {
  fixture((old, next) => {
    symlinkSync(join(old, 'index.html'), join(old, 'alias.html'));
    assert.throws(() => inventory(old), /symlink proibido/);
  });
});
test('rollback negativo: artefatos de origem sobrepostos são recusados', () => {
  fixture((old) => assert.throws(() => rehearseRollback(old, old, {
    expectedPreviousSha256: inventory(old).sha256,
  }), /artefatos sobrepostos/));
});
