import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, lstat, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const cli = fileURLToPath(new URL('../scripts/remove-ssg-manifest.mjs', import.meta.url));
const pwa = {
  background_color: '#ffffff',
  description: 'Dados públicos de CNPJ por competência, preservados no Internet Archive.',
  display: 'standalone', name: 'FICHA', orientation: 'portrait-primary',
  scope: '/', short_name: null, start_url: '.', theme_color: null,
};
async function withFixture(payload, check) {
  const dir = await mkdtemp(join(tmpdir(), 'ficha-ssg-'));
  const path = join(dir, 'manifest.json');
  try {
    if (payload !== null) await writeFile(path, JSON.stringify(payload));
    await check(path, dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
const run = path => spawnSync(process.execPath, [cli, path], { encoding: 'utf8' });

test('remove somente o manifesto PWA exato emitido pelo SSG 0.0.75', async () => {
  await withFixture(pwa, async path => {
    const result = run(path);
    assert.equal(result.status, 0, result.stderr);
    await assert.rejects(lstat(path), { code: 'ENOENT' });
  });
});
test('preserva contrato de dados Receita current/snapshots', async () => {
  const rfb = { current: '2026-04', snapshots: [] };
  await withFixture(rfb, async path => {
    assert.notEqual(run(path).status, 0);
    assert.deepEqual(JSON.parse(await readFile(path, 'utf8')), rfb);
  });
});
test('preserva manifesto desconhecido, mesmo semelhante ao PWA', async () => {
  const unknown = { ...pwa, future_field: true };
  await withFixture(unknown, async path => {
    assert.notEqual(run(path).status, 0);
    assert.deepEqual(JSON.parse(await readFile(path, 'utf8')), unknown);
  });
});
test('recusa apagar caminho simbólico', async () => {
  await withFixture(null, async (path, dir) => {
    const target = join(dir, 'pwa.json');
    await writeFile(target, JSON.stringify(pwa));
    await symlink(target, path);
    assert.notEqual(run(path).status, 0);
    assert.deepEqual(JSON.parse(await readFile(target, 'utf8')), pwa);
  });
});
test('manifesto ausente: nenhum arquivo criado', async () => {
  await withFixture(null, async path => {
    assert.equal(run(path).status, 0);
    await assert.rejects(lstat(path), { code: 'ENOENT' });
  });
});
