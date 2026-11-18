import { lstat, readFile } from 'node:fs/promises';

const root = new URL('../dist/ficha/', import.meta.url);
async function readRequired(name) {
  const url = new URL(name, root);
  const stat = await lstat(url);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(name + ': arquivo regular obrigatório');
  return readFile(url, 'utf8');
}
const index = await readRequired('index.html');
const about = await readRequired('sobre/index.html');
const css = await readRequired('css/site.css');
for (const [name,html] of [['index',index],['sobre',about]]) {
  for (const fragment of ['lang="pt-BR"','/ficha/css/site.css','href="/ficha/"','href="/ficha/sobre/"','id="main"','href="#main"']) {
    if (!html.includes(fragment)) throw new Error(name + ': ausente ' + fragment);
  }
  if (/astro-island|svelte|\/views\//i.test(html)) throw new Error(name + ': elemento legado na saída');
  if (/\{\{[!\w]|\{%/.test(html)) throw new Error(name + ': template não renderizado');
}
for (const fragment of ['Prévia técnica não publicada', 'id="busca-form"', 'id="busca-termo"',
  'id="busca-enviar"', 'id="consulta-estado"', 'id="consulta-lista"',
  'type="module" src="/ficha/js/boot.mjs"']) {
  if (!index.includes(fragment)) throw new Error('Home SSG sem interface ' + fragment);
}
if (!about.includes('Sobre o FICHA')) throw new Error('Conteúdo Sobre não gerado');
if (about.includes('/ficha/js/boot.mjs')) throw new Error('JS da busca indevido na página Sobre');
for (const name of ['boot.mjs','search-app.mjs','dataset.mjs','query-core.mjs','duckdb-runtime.mjs']) {
  const js = await readRequired('js/' + name);
  if (!js.trim()) throw new Error('Módulo JS vazio: ' + name);
}
if (!css.includes('focus-visible')) throw new Error('CSS acessível ausente');
let manifest;
try {
  manifest = await readRequired('manifest.json');
} catch (e) {
  if (e.code !== 'ENOENT') throw e;
}
if (manifest !== undefined) {
  let parsed;
  try { parsed = JSON.parse(manifest); } catch { throw new Error('Manifesto gerado inválido'); }
  if (!parsed.current || !Array.isArray(parsed.snapshots)) throw new Error('Manifesto PWA do SSG conflita com manifest.json de dados: não publicar');
}
console.log('SSG HTML + JS isolados válidos; consulta remota, Protobuf e paridade navegador NÃO testados');
