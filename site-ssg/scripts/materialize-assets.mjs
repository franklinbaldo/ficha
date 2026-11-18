import { copyFile, lstat, mkdir, readFile } from 'node:fs/promises';

const html = new URL('../dist/ficha/index.html', import.meta.url);
const css = new URL('../dist/ficha/css/site.css', import.meta.url);
const source = new URL('../static/css/site.css', import.meta.url);
const stat = await lstat(html);
if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('SSG HTML ausente ou symlink');
const body = await readFile(html, 'utf8');
if (!body.includes('/ficha/css/site.css')) throw new Error('HTML SSG não referencia CSS esperado');
await mkdir(new URL('../dist/ficha/css/', import.meta.url), {recursive:true});
try {
  await lstat(css);
  throw new Error('CSS destino já existe; não sobrescrever asset gerado');
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}
await copyFile(source, css);
console.log('CSS estático copiado para saída isolada; nenhum deploy executado');
