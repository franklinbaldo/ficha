/**
 * Check that the public Ficha data manifest was copied unchanged into the
 * generated site. This is a local/offline build gate, not a remote-file check.
 *
 * Usage:
 *   node check-manifest-byte-identity.mjs <web/public/manifest.json> <dist/.../manifest.json>
 */
import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';

function checkSourceManifest(bytes) {
  let data;
  try {
    data = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new Error('fonte: JSON inválido');
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('fonte: objeto JSON ausente');
  }
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(data.current ?? '') ||
      !Array.isArray(data.snapshots) || data.snapshots.length === 0) {
    throw new Error('fonte: contrato current/snapshots inválido');
  }
  const months = new Set();
  for (const entry of data.snapshots) {
    if (!entry || typeof entry !== 'object' ||
        !/^\d{4}-(0[1-9]|1[0-2])$/.test(entry.date ?? '') ||
        months.has(entry.date)) {
      throw new Error('fonte: competência inválida ou duplicada');
    }
    months.add(entry.date);
  }
  if (!months.has(data.current)) {
    throw new Error('fonte: current não seleciona nenhum snapshot');
  }
  const selected = data.snapshots.find(s => s.date === data.current);
  const cnpjs = selected?.files?.cnpjs;
  if (typeof cnpjs?.url !== 'string' ||
      !/^https:\/\//.test(cnpjs.url) ||
      !/^[a-f0-9]{64}$/.test(cnpjs.sha256 ?? '') ||
      !Number.isSafeInteger(cnpjs.size) || cnpjs.size <= 0) {
    throw new Error('fonte: identidade de cnpjs.parquet ausente/inválida');
  }
  if ('display' in data || 'start_url' in data || 'background_color' in data) {
    throw new Error('fonte: manifesto PWA no lugar do catálogo de dados');
  }
  return { current: data.current, snapshots: data.snapshots.length };
}

async function regularBytes(file, label) {
  const stat = await lstat(file);
  if (!stat.isFile() || stat.isSymbolicLink()) {
    throw new Error(`${label}: arquivo regular obrigatório`);
  }
  return readFile(file);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length !== 2) {
    throw new Error('uso: node check-manifest-byte-identity.mjs <fonte> <artefato>');
  }
  const [source, built] = await Promise.all([
    regularBytes(args[0], 'fonte'), regularBytes(args[1], 'artefato'),
  ]);
  const summary = checkSourceManifest(source);
  if (!source.equals(built)) {
    throw new Error('artefato: manifest.json difere byte a byte da fonte RFB; publicação recusada');
  }
  console.log(`manifesto RFB preservado: current=${summary.current}, ` +
    `snapshots=${summary.snapshots}, bytes=${source.length}, ` +
    `sha256=${createHash('sha256').update(source).digest('hex')}`);
}

main().catch(error => {
  console.error(`manifesto de dados: ${error.message}`);
  process.exitCode = 1;
});
