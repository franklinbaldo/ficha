/** Read-only helpers for the public, versioned FICHA catalogue. */
const DATE = /^\d{4}-(0[1-9]|1[0-2])$/;
const HEX = /^[0-9a-f]{64}$/;

const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);

function validateSnapshotFiles(files) {
  const keys = new Set();
  const add = key => {
    if (keys.has(key)) throw new Error('Identidade de arquivo duplicada: ' + key);
    keys.add(key);
  };
  for (const [key, file] of Object.entries(files)) {
    if (!isObject(file)) throw new Error('Entrada de arquivo inválida: ' + key);
    if (!own(file, 'shards')) {
      add(key);
      continue;
    }
    if (!Array.isArray(file.shards)) throw new Error('Lista de shards inválida: ' + key);
    const pattern = key === 'companies' ? /^[0-9]{2}$/ : /^[a-zA-Z0-9_-]{1,16}$/;
    for (const shard of file.shards) {
      if (!isObject(shard) || typeof shard.shard !== 'string' || !pattern.test(shard.shard)) {
        throw new Error('Identidade de shard inválida: ' + key);
      }
      add(key + '/' + shard.shard);
    }
  }
}

export function validateManifest(manifest) {
  if (!isObject(manifest) || typeof manifest.current !== 'string' ||
      !DATE.test(manifest.current) || !Array.isArray(manifest.snapshots)) {
    throw new Error('Manifesto sem current/snapshots válidos');
  }
  const dates = new Set();
  for (const snapshot of manifest.snapshots) {
    if (!isObject(snapshot) || typeof snapshot.date !== 'string' ||
        !DATE.test(snapshot.date) || dates.has(snapshot.date) || !isObject(snapshot.files) ||
        (snapshot.row_counts != null && !isObject(snapshot.row_counts))) {
      throw new Error('Snapshot duplicado ou inválido');
    }
    validateSnapshotFiles(snapshot.files);
    dates.add(snapshot.date);
  }
  if (!dates.has(manifest.current)) throw new Error('Competência current ausente do catálogo');
  return manifest;
}

export async function fetchManifest({fetchImpl = fetch, url = '/ficha/manifest.json'} = {}) {
  // This is a full GET, never a byte-range probe: 201/206 cannot certify the catalogue.
  const response = await fetchImpl(url, {cache:'no-store'});
  if (response.status !== 200) throw new Error('Catálogo indisponível: HTTP ' + response.status);
  // Full-object HTTP 200 cannot simultaneously claim partial-content semantics.
  // Fail before JSON parsing even when a response contains a valid catalogue.
  if (response.headers?.get?.('content-range') != null) {
    try { void response.body?.cancel?.().catch(() => {}); } catch { /* Best effort. */ }
    throw new Error('Catálogo HTTP 200 com Content-Range inesperado');
  }
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
  const entries = [];
  const add = (key,file) => entries.push({
    key,
    size: Number.isSafeInteger(file.size) && file.size >= 0 ? file.size : null,
    sha256: typeof file.sha256 === 'string' && HEX.test(file.sha256) ? file.sha256 : null,
    sha1: typeof file.sha1 === 'string' && /^[0-9a-f]{40}$/.test(file.sha1) ? file.sha1 : null,
    url: safeExternalURL(file.url),
  });
  for (const [key,file] of Object.entries(snapshot.files)) {
    if (!file || typeof file !== 'object' || Array.isArray(file)) continue;
    if (Array.isArray(file.shards)) {
      // Each ZIP shard is a real archival file with its own URL and identity.
      for (const shard of file.shards) {
        if (shard && typeof shard === 'object' && typeof shard.shard === 'string' &&
            /^[a-zA-Z0-9_-]{1,16}$/.test(shard.shard)) add(key+'/'+shard.shard,shard);
      }
    } else add(key,file);
  }
  return entries.sort((a,b) => a.key.localeCompare(b.key,'pt-BR'));
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

/** Counts declared by the ETL, not independently verified live statistics. */
export function countKinds(manifest) {
  const names = new Set();
  for (const snap of snapshotsDescending(manifest)) {
    for (const [key,value] of Object.entries(snap.row_counts ?? {})) {
      if (Number.isSafeInteger(value) && value >= 0) names.add(key);
    }
  }
  return [...names].sort((a,b)=>a.localeCompare(b,'pt-BR'));
}

export function rowCountSeries(manifest,key) {
  return [...snapshotsDescending(manifest)].reverse().map(snap => ({
    date:snap.date,
    count: Number.isSafeInteger(snap.row_counts?.[key]) && snap.row_counts[key] >= 0
      ? snap.row_counts[key] : null,
  }));
}
