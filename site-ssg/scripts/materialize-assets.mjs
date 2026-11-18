import { copyFile, lstat, mkdir, readFile } from 'node:fs/promises';

const root = new URL('../dist/ficha/', import.meta.url);
const home = new URL('index.html', root);
const stat = await lstat(home);
if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('SSG HTML ausente ou symlink');
const body = await readFile(home, 'utf8');
for (const ref of ['/ficha/css/site.css', '/ficha/js/boot.mjs']) {
  if (!body.includes(ref)) throw new Error('HTML SSG não referencia ' + ref);
}
const files = [
  ['static/css/site.css', 'css/site.css'],
  ...['boot.mjs', 'search-app.mjs', 'duckdb-runtime.mjs', 'query-core.mjs', 'dataset.mjs']
    .map(name => ['static/js/' + name, 'js/' + name]),
];
// Fail before copying anything if the generator already created an asset.
for (const [, target] of files) {
  try {
    await lstat(new URL(target, root));
    throw new Error(target + ': destino já existe; não sobrescrever');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
}
for (const [source, target] of files) {
  const destination = new URL(target, root);
  await mkdir(new URL(target.includes('/') ? target.slice(0, target.lastIndexOf('/') + 1) : '', root),
    { recursive: true });
  await copyFile(new URL('../' + source, import.meta.url), destination);
}
console.log('CSS e módulos ES copiados para saída SSG isolada; nenhum deploy executado');
