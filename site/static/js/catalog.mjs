/** Read-only helpers for the public, versioned FICHA catalogue. */
const DATE = /^\d{4}-(0[1-9]|1[0-2])$/;
const HEX = /^[0-9a-f]{64}$/;

export function validateManifest(manifest) {
  if (!manifest || !DATE.test(manifest.current ?? '') ||
      !Array.isArray(manifest.snapshots)) {
    throw new Error('Manifesto sem current/snapshots válidos');
  }
  const dates = new Set();
  for (const snapshot of manifest.snapshots) {
    if (!DATE.test(snapshot?.date ?? '') || dates.has(snapshot.date) ||
        !snapshot.files || typeof snapshot.files !== 'object') {
      throw new Error('Snapshot duplicado ou inválido');
    }
    dates.add(snapshot.date);
  }
  if (!dates.has(manifest.current)) throw new Error('Competência current ausente do catálogo');
  return manifest;
}

export async function fetchManifest({fetchImpl = fetch, url = '/ficha/manifest.json'} = {}) {
  const response = await fetchImpl(url, {cache:'no-store'});
  if (!response.ok) throw new Error('Catálogo indisponível: HTTP ' + response.status);
  return validateManifest(await response.json());
}

export function snapshotsDescending(manifest) {
  return [...validateManifest(manifest).snapshots].sort((a,b) => b.date.localeCompare(a.date));
}

export function competenceLabel(date) {
  if (!DATE.test(date ?? '')) return 'Competência desconhecida';
  const [y,m] = date.split('-').map(Number);
  return new Intl.DateTimeFormat('pt-BR', {month:'long',year:'numeric',timeZone:'UTC'})
    .format(new Date(Date.UTC(y,m-1,1)));
}

export function formatBytes(size) {
  if (!Number.isSafeInteger(size) || size < 0) return 'Tamanho não declarado';
  if (size < 1024) return size + ' B';
  const units = ['KB','MB','GB','TB'];
  let value = size, i = -1;
  do { value /= 1024; i++; } while (value >= 1024 && i < units.length-1);
  return new Intl.NumberFormat('pt-BR', {maximumFractionDigits:2}).format(value) + ' ' + units[i];
}

export function archivalFileEntries(snapshot) {
  if (!snapshot || !snapshot.files || typeof snapshot.files !== 'object') return [];
  return Object.entries(snapshot.files)
    .filter(([,file]) => file && typeof file === 'object' && !Array.isArray(file))
    .map(([key,file]) => ({
      key, size: Number.isSafeInteger(file.size) && file.size >= 0 ? file.size : null,
      sha256: typeof file.sha256 === 'string' && HEX.test(file.sha256) ? file.sha256 : null,
      url: safeExternalURL(file.url),
    }))
    .sort((a,b) => a.key.localeCompare(b.key,'pt-BR'));
}

export function safeExternalURL(value) {
  if (typeof value !== 'string') return null;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' ? url.href : null;
  } catch { return null; }
}

export function catalogueTotals(snapshot) {
  const entries = archivalFileEntries(snapshot);
  return {
    fileCount: entries.length,
    bytes: entries.reduce((acc,x) => acc + (x.size ?? 0),0),
    sizedCount: entries.filter(x => x.size !== null).length,
  };
}

export function archiveView(manifest, selectedDate) {
  const snapshots = snapshotsDescending(manifest);
  const selected = snapshots.find(s => s.date === selectedDate) ??
    snapshots.find(s => s.date === manifest.current);
  if (!selected) throw new Error('Snapshot selecionado ausente');
  return {snapshots, selected, entries: archivalFileEntries(selected),
    totals: catalogueTotals(selected)};
}
