import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, test } from 'node:test';

const originalScript = await readFile(new URL('./finalize.mjs', import.meta.url));
const dirs = [];
const sample = () => ({
  current: '2026-07',
  snapshots: [{ date: '2026-07', files: { cnpjs: {
    url: 'https://archive.org/download/ficha-2026-07/cnpjs.parquet',
    sha256: 'a'.repeat(64), size: 6_076_313_170,
  } } }],
});
const pwa = {
  background_color: '#ffffff',
  description: 'Dados públicos de CNPJ por competência, preservados no Internet Archive.',
  display: 'standalone', name: 'FICHA', orientation: 'portrait-primary',
  scope: '/', short_name: null, start_url: '.', theme_color: null,
};
const nav = '<a href="/ficha/">Início</a><a href="/ficha/sobre/">Sobre</a><link href="/ficha/css/site.css">';
const index = `<!doctype html><html lang="pt-BR"><body>${nav}<main id="main">Pesquisa por CNPJ ainda indisponível
  <span>__FICHA_CURRENT_COMPETENCE__</span><a href="/ficha/manifest.json">Dados</a>
  <a href="__FICHA_CURRENT_SOURCE_URL__">Fonte</a><span>__FICHA_CURRENT_SOURCE_SIZE__ GB</span>
</main></body></html>`;
const about = `<!doctype html><html lang="pt-BR"><body>${nav}<main id="main">Sobre o FICHA</main></body></html>`;

async function fixture(mutator = async () => {}) {
  const root = await mkdtemp(join(tmpdir(), 'ficha-finalize-'));
  dirs.push(root);
  const file = path => join(root, path);
  for (const dir of ['web/public', 'site/scripts', 'site/static/css', 'site/dist/ficha/sobre']) {
    await mkdir(file(dir), { recursive: true });
  }
  const manifest = sample();
  await mutator({ manifest, file });
  const canonical = Buffer.from(JSON.stringify(manifest, null, 2) + '\n');
  await writeFile(file('web/public/manifest.json'), canonical);
  await writeFile(file('site/scripts/finalize.mjs'), originalScript);
  await writeFile(file('site/static/css/site.css'), 'a:focus-visible{outline:2px solid}');
  await writeFile(file('site/dist/ficha/index.html'), index);
  await writeFile(file('site/dist/ficha/sobre/index.html'), about);
  await writeFile(file('site/dist/ficha/manifest.json'), JSON.stringify(pwa));
  return { file, canonical, manifest };
}
function run(f) {
  return spawnSync(process.execPath, [f.file('site/scripts/finalize.mjs')], { encoding: 'utf8' });
}
function rejected(f, pattern) {
  const result = run(f);
  assert.notEqual(result.status, 0, result.stdout);
  assert.match(result.stderr, pattern);
}
afterEach(async () => {
  await Promise.all(dirs.splice(0).map(d => rm(d, { recursive: true, force: true })));
});

test('preserva exatamente os bytes da origem no destino e anuncia a competência', async () => {
  const f = await fixture();
  const result = run(f);
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(await readFile(f.file('site/dist/ficha/manifest.json')), f.canonical);
  const html = await readFile(f.file('site/dist/ficha/index.html'), 'utf8');
  assert.match(html, /julho de 2026 \(2026-07\)/);
  assert.match(html, /6,08 GB/);
  assert.doesNotMatch(html, /__FICHA_CURRENT_/);
});

test('rejeita manifesto PWA inesperado sem sobrescrevê-lo', async () => {
  const f = await fixture();
  const other = JSON.stringify({ name: 'outro PWA' });
  await writeFile(f.file('site/dist/ficha/manifest.json'), other);
  rejected(f, /SSG produziu manifest.json desconhecido/);
  assert.equal(await readFile(f.file('site/dist/ficha/manifest.json'), 'utf8'), other);
});

test('rejeita manifesto de destino como link simbólico', async () => {
  const f = await fixture();
  await rm(f.file('site/dist/ficha/manifest.json'));
  await symlink(f.file('web/public/manifest.json'), f.file('site/dist/ficha/manifest.json'));
  rejected(f, /Arquivo regular exigido/);
});

test('rejeita fonte canônica como link simbólico', async () => {
  const f = await fixture();
  await writeFile(f.file('web/public/source.json'), f.canonical);
  await rm(f.file('web/public/manifest.json'));
  await symlink(f.file('web/public/source.json'), f.file('web/public/manifest.json'));
  rejected(f, /Arquivo regular exigido/);
});

for (const [name, change] of [
  ['SHA-256 ausente', m => { delete m.snapshots[0].files.cnpjs.sha256; }],
  ['SHA-256 inválido', m => { m.snapshots[0].files.cnpjs.sha256 = 'z'.repeat(64); }],
  ['tamanho ausente', m => { delete m.snapshots[0].files.cnpjs.size; }],
  ['tamanho fracionário', m => { m.snapshots[0].files.cnpjs.size = 6.5; }],
  ['tamanho booleano', m => { m.snapshots[0].files.cnpjs.size = true; }],
  ['tamanho não seguro', m => { m.snapshots[0].files.cnpjs.size = 9_007_199_254_740_992; }],
  ['URL sem HTTPS', m => { m.snapshots[0].files.cnpjs.url = 'http://archive.org/file'; }],
  ['current ausente', m => { m.current = '2026-08'; }],
]) {
  test(`falha fechado quando ${name}`, async () => {
    const f = await fixture(({ manifest }) => change(manifest));
    const originalPwa = await readFile(f.file('site/dist/ficha/manifest.json'));
    rejected(f, /Current sem CNPJ HTTPS e identidade sha256\/size completas/);
    assert.deepEqual(await readFile(f.file('site/dist/ficha/manifest.json')), originalPwa);
  });
}

test('rejeita competências duplicadas antes de copiar', async () => {
  const f = await fixture(({ manifest }) => manifest.snapshots.push(structuredClone(manifest.snapshots[0])));
  rejected(f, /Competências duplicadas/);
});

test('rejeita marcador ausente na página', async () => {
  const f = await fixture();
  await writeFile(f.file('site/dist/ficha/index.html'), index.replace('__FICHA_CURRENT_COMPETENCE__',''));
  rejected(f, /Marcador de competência ausente ou repetido/);
});
