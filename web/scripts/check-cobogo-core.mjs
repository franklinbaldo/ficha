import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const pin = JSON.parse(await readFile(new URL('../cobogo-core.pin.json', import.meta.url), 'utf8'));
const core = await readFile(new URL('../src/styles/cobogo-core.css', import.meta.url));
const header = await readFile(new URL('../src/components/Header.svelte', import.meta.url), 'utf8');
const home = await readFile(new URL('../src/pages/index.astro', import.meta.url), 'utf8');

const prefix = Buffer.from(`blob ${core.length}\0`);
const blobSha = createHash('sha1').update(prefix).update(core).digest('hex');
if (blobSha !== pin.blob_sha) {
  throw new Error(`Cobogó core snapshot drifted: expected ${pin.blob_sha}, got ${blobSha}`);
}

const coreImport = "import '../styles/cobogo-core.css';";
const mapImport = "import '../styles/ficha-core-map.css';";
const corePos = home.indexOf(coreImport);
const mapPos = home.indexOf(mapImport);
if (corePos < 0 || mapPos < 0 || corePos > mapPos) {
  throw new Error('Expected layer order: cobogo core -> Ficha semantic mapping -> local page CSS');
}

if (home.includes('font-family: sans-serif')) {
  throw new Error('Generic document font ownership returned to the Ficha home');
}
if (home.includes('.access-path a:focus-visible')) {
  throw new Error('Generic access-link focus ring returned to local page CSS');
}
if (!header.includes('data-cobogo-inverted')) {
  throw new Error('Header must declare the inverted region so shared focus tones remain legible');
}
if (/outline\s*:/.test(header)) {
  throw new Error('Generic header focus outline returned to consumer-local CSS');
}

console.log(`Cobogó core pin OK: ${pin.repository}@${pin.commit}:${pin.path} (${blobSha})`);
