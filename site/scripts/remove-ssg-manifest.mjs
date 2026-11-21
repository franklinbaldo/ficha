import assert from 'node:assert/strict';
import { lstat, readFile, unlink } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

// SSG 0.0.75 emits a PWA manifest at the same path as Ficha's Receita
// snapshot contract. Remove ONLY the exact observed PWA payload.
// Any variation stops the shadow build; never silently overwrite RFB data.
const expected = {
  background_color: '#ffffff',
  description: 'Dados públicos de CNPJ por competência e arquivos declarados no catálogo FICHA.',
  display: 'standalone',
  name: 'FICHA',
  orientation: 'portrait-primary',
  scope: '/',
  short_name: null,
  start_url: '.',
  theme_color: null,
};
const file = process.argv[2]
  ? pathToFileURL(resolve(process.argv[2]))
  : new URL('../dist/ficha/manifest.json', import.meta.url);

let stat;
try {
  stat = await lstat(file);
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
  console.log('Nenhum manifesto PWA emitido; nada para remover.');
  process.exit(0);
}
if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) {
  throw new Error('manifest.json gerado não é arquivo regular pequeno; não remover');
}
const data = JSON.parse(await readFile(file, 'utf8'));
assert.deepStrictEqual(data, expected, 'manifest.json desconhecido ou da Receita: preservar e interromper');
await unlink(file);
console.log('Manifesto PWA conhecido do SSG removido apenas do build shadow; nenhum manifesto RFB criado.');
