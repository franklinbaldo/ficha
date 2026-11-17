import { copyFileSync, existsSync, lstatSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const assets = ['css/style.css', 'js/main.mjs', 'js/consult.mjs'];

function requireRegular(file, description) {
  if (!existsSync(file) || !lstatSync(file).isFile()) {
    throw new Error(description + ' ausente ou não regular: ' + file);
  }
}

/** Copy only PoC-owned static assets after a verified SSG HTML build.
 * No network, manifest rewrite, deployment, or external path arguments.
 */
export function materializeShellAssets(root = project) {
  const output = join(root, 'dist/ficha');
  requireRegular(join(output, 'index.html'), 'HTML SSG');
  const sources = assets.map((asset) => {
    const source = join(root, 'static', asset);
    requireRegular(source, 'asset ' + asset);
    return { source, destination: join(output, asset) };
  });
  for (const { source, destination } of sources) {
    mkdirSync(dirname(destination), { recursive: true });
    copyFileSync(source, destination);
  }
  return assets;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const copied = materializeShellAssets();
    console.log('SSG_ASSETS_PASS copied=' + copied.join(','));
  } catch (error) {
    console.error('SSG_ASSETS_FAIL ' + error.message);
    process.exitCode = 1;
  }
}
