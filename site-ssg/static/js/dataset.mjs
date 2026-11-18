/** Data-only boundary for the SSG migration. No Astro, Svelte or network on import. */
const DATE = /^\d{4}-(0[1-9]|1[0-2])$/;
const HEX40 = /^[0-9a-f]{40}$/;
const HEX64 = /^[0-9a-f]{64}$/;

function dataURL(value, context) {
  if (typeof value !== 'string') throw new Error(`${context}: URL ausente`);
  let url;
  try { url = new URL(value); } catch { throw new Error(`${context}: URL inválida`); }
  if (url.protocol !== 'https:' && url.protocol !== 'http:') {
    throw new Error(`${context}: protocolo não permitido`);
  }
  return value;
}

function requiredFile(entry, label) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
    throw new Error(`${label}: entrada obrigatória ausente`);
  }
  dataURL(entry.url, label);
  if (!HEX64.test(entry.sha256 ?? '') ||
      (entry.sha1 !== undefined && !HEX40.test(entry.sha1)) ||
      !Number.isSafeInteger(entry.size) || entry.size < 0) {
    throw new Error(`${label}: metadados de integridade inválidos`);
  }
  return entry;
}

/** Selects the *published* current entry, never a guessed latest snapshot. */
export function currentDataset(manifest) {
  if (!manifest || !DATE.test(manifest.current ?? '') ||
      !Array.isArray(manifest.snapshots)) {
    throw new Error('manifest.json: contrato current/snapshots inválido');
  }
  const matching = manifest.snapshots.filter(s => s?.date === manifest.current);
  if (matching.length !== 1) {
    throw new Error('manifest.json: current precisa selecionar exatamente um snapshot');
  }
  const snapshot = matching[0];
  const files = snapshot.files;
  if (!files || typeof files !== 'object') throw new Error('snapshot sem files');
  for (const name of ['cnpjs', 'raizes', 'socios', 'lookups']) {
    requiredFile(files[name], `files.${name}`);
  }
  const lookups = snapshot.lookups ?? {};
  if (!lookups || typeof lookups !== 'object' || Array.isArray(lookups)) {
    throw new Error('lookups: contrato inválido');
  }
  for (const [kind, entry] of Object.entries(lookups)) {
    if (!/^[a-z][a-z0-9_]*$/.test(kind)) {
      throw new Error(`lookup: nome inseguro ${kind}`);
    }
    if (!entry || typeof entry !== 'object') throw new Error(`lookup ${kind}: inválido`);
    dataURL(entry.url, `lookup ${kind}`);
  }
  // This is a subset reader for the analytical search; it does NOT certify
  // full v1/v1.1 manifests, remote files or Protobuf shard identity.
  return { date: snapshot.date, files, lookups, generatedAt: snapshot.generated_at ?? null };
}

/** 404 means unpublished. Other HTTP or schema failures must remain visible. */
export async function fetchCurrentDataset({
  fetchImpl = fetch, url = '/ficha/manifest.json',
} = {}) {
  const response = await fetchImpl(url, { cache: 'no-store' });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`manifest.json: HTTP ${response.status}`);
  return currentDataset(await response.json());
}
