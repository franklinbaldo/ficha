import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { materializeShellAssets } from '../scripts/materialize-assets.mjs';

const assets = ['css/style.css', 'js/main.mjs', 'js/consult.mjs'];

function fixture(t, { html = true, absent = '', symlink = '' } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'ficha-p3a-assets-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  mkdirSync(join(dir, 'dist/ficha'), { recursive: true });
  if (html) writeFileSync(join(dir, 'dist/ficha/index.html'), '<html lang="pt-BR"></html>');
  writeFileSync(join(dir, 'dist/ficha/manifest.json'), '{"ssg_only":true}\n');
  for (const asset of assets) {
    if (asset === absent) continue;
    const path = join(dir, 'static', asset);
    mkdirSync(join(dir, 'static', asset.split('/')[0]), { recursive: true });
    if (asset === symlink) symlinkSync(join(dir, 'dist/ficha/index.html'), path);
    else writeFileSync(path, 'asset:' + asset + '\n');
  }
  return dir;
}

test('copies all assets only after real SSG HTML exists', (t) => {
  const dir = fixture(t);
  assert.deepEqual(materializeShellAssets(dir), assets);
  for (const asset of assets) {
    assert.equal(readFileSync(join(dir, 'dist/ficha', asset), 'utf8'), 'asset:' + asset + '\n');
  }
  assert.equal(readFileSync(join(dir, 'dist/ficha/manifest.json'), 'utf8'), '{"ssg_only":true}\n');
});

test('copy is idempotent and preserves generated manifest', (t) => {
  const dir = fixture(t);
  materializeShellAssets(dir);
  materializeShellAssets(dir);
  assert.equal(readFileSync(join(dir, 'dist/ficha/manifest.json'), 'utf8'), '{"ssg_only":true}\n');
});

test('without SSG HTML fails rather than fabricating a build', (t) => {
  const dir = fixture(t, { html: false });
  assert.throws(() => materializeShellAssets(dir), /HTML SSG ausente/);
  assert.equal(existsSync(join(dir, 'dist/ficha/css/style.css')), false);
});

test('missing JS rejects before copying any asset', (t) => {
  const dir = fixture(t, { absent: 'js/consult.mjs' });
  assert.throws(() => materializeShellAssets(dir), /js\/consult\.mjs ausente/);
  assert.equal(existsSync(join(dir, 'dist/ficha/css/style.css')), false);
});

test('symlinked source is rejected', (t) => {
  const dir = fixture(t, { symlink: 'js/main.mjs' });
  assert.throws(() => materializeShellAssets(dir), /js\/main\.mjs ausente ou não regular/);
  assert.equal(existsSync(join(dir, 'dist/ficha/css/style.css')), false);
});
