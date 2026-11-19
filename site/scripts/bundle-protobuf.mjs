/** Produce a self-contained ES module from the canonical generated decoder.
 * Uses esbuild already locked as a transitive dependency of web/ (Vite).
 * This script is run only after web/bun.lock has been installed frozen.
 */
import { spawnSync } from 'node:child_process';
import { lstat, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const root = new URL('../dist/ficha/js/', import.meta.url);
await mkdir(root, {recursive:true});
const output = new URL('company-decoder.mjs', root);
try {
  await lstat(output);
  throw new Error('Decoder publicado já existe: não sobrescrever');
} catch (error) {
  if (error?.code !== 'ENOENT') throw error;
}
const bin = fileURLToPath(new URL('../../web/node_modules/.bin/esbuild', import.meta.url));
const entry = fileURLToPath(new URL('./canonical-decoder-entry.mjs', import.meta.url));
const dest = fileURLToPath(output);
const process = spawnSync(bin, [entry, '--bundle', '--minify', '--platform=browser',
  '--format=esm', '--target=es2022', '--outfile='+dest], {stdio:'inherit'});
if (process.error) throw process.error;
if (process.status !== 0) throw new Error('Falha no bundle canônico do Protobuf: '+process.status);
const file = await lstat(output);
if (!file.isFile() || file.isSymbolicLink() || file.size < 1000 || file.size > 512000) {
  throw new Error('Bundle Protobuf inesperado');
}
console.log('Decoder canônico gerado: '+file.size+' bytes');
