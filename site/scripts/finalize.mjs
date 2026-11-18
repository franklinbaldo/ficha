import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { copyFile, lstat, mkdir, readFile, unlink, writeFile } from 'node:fs/promises';

const root = new URL('../dist/ficha/', import.meta.url);
const canonicalURL = new URL('../../web/public/manifest.json', import.meta.url);
const manifestURL = new URL('manifest.json', root);
const cssURL = new URL('css/site.css', root);
async function required(url) {
  const stat = await lstat(url);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('Arquivo regular exigido: ' + url);
  return readFile(url);
}
const original = await required(canonicalURL);
const manifest = JSON.parse(original.toString('utf8'));
if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(manifest.current) || !Array.isArray(manifest.snapshots)) {
  throw new Error('Manifesto RFB sem current/snapshots válidos');
}
const dates = manifest.snapshots.map(s => s?.date);
if (new Set(dates).size !== dates.length) throw new Error('Competências duplicadas');
const matches = manifest.snapshots.filter(s => s?.date === manifest.current);
if (matches.length !== 1 || !/^https:\/\//.test(matches[0]?.files?.cnpjs?.url || '')) {
  throw new Error('Current sem snapshot CNPJ inequívoco');
}
const expectedPwa = {
  background_color:'#ffffff',
  description:'Dados públicos de CNPJ por competência, preservados no Internet Archive.',
  display:'standalone',
  name:'FICHA',
  orientation:'portrait-primary',
  scope:'/',
  short_name:null,
  start_url:'.',
  theme_color:null,
};
try {
  const outputManifest = await required(manifestURL);
  assert.deepStrictEqual(JSON.parse(outputManifest.toString('utf8')), expectedPwa,
    'SSG produziu manifest.json desconhecido; preservar e abortar');
  await unlink(manifestURL);
} catch (err) {
  if (err?.code !== 'ENOENT') throw err;
}
await copyFile(canonicalURL, manifestURL);
assert.deepStrictEqual(await required(manifestURL), original, 'Manifesto RFB não idêntico');
await mkdir(new URL('css/',root), {recursive:true});
try { await lstat(cssURL); throw new Error('CSS já existe; não sobrescrever'); }
catch (err) { if (err?.code !== 'ENOENT') throw err; }
await copyFile(new URL('../static/css/site.css', import.meta.url), cssURL);
const indexURL = new URL('index.html',root);
const aboutURL = new URL('sobre/index.html',root);
let index = (await required(indexURL)).toString('utf8');
const about = (await required(aboutURL)).toString('utf8');
const [year,month] = manifest.current.split('-').map(Number);
const label = new Intl.DateTimeFormat('pt-BR', {month:'long',year:'numeric',timeZone:'UTC'})
  .format(new Date(Date.UTC(year,month-1,1))) + ' (' + manifest.current + ')';
const marker='__FICHA_CURRENT_COMPETENCE__';
if (index.split(marker).length !== 2) throw new Error('Marcador de competência ausente ou repetido');
index=index.replace(marker,label);
await writeFile(indexURL,index,'utf8');
for (const [file,html] of [['index',index],['sobre',about]]) {
  for (const requiredText of ['lang="pt-BR"','href="/ficha/"','href="/ficha/sobre/"','href="/ficha/css/site.css"','id="main"']) {
    if (!html.includes(requiredText)) throw new Error(file + ': falta ' + requiredText);
  }
  if (/astro-island|svelte|<form|type="module"/i.test(html)) throw new Error(file+': elemento de busca/legado indevido');
  if (/\{\{[!\w]|\{%/.test(html)) throw new Error(file + ': template não renderizado');
}
for (const token of ['Pesquisa por CNPJ ainda indisponível',label,'/ficha/manifest.json']) {
  if (!index.includes(token)) throw new Error('Home sem ' + token);
}
if (!about.includes('Sobre o FICHA')) throw new Error('Sobre não renderizado');
const css=(await required(cssURL)).toString('utf8');
if (!css.includes('focus-visible')) throw new Error('CSS sem foco teclado');
const digest=createHash('sha256').update(original).digest('hex');
console.log('SSG MVP válido; current='+manifest.current+'; manifesto byte-identical SHA256='+digest);
