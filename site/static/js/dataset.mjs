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

/**
 * Verify CORS + Range on the published origin before loading DuckDB-WASM.
 * Require exactly one byte, not only a 206 header, and keep the deadline
 * active through body reads. This is transport evidence, not SHA-256 identity.
 */
export async function probeParquetRange({url, size, fetchImpl = fetch, timeoutMs = 10000} = {}) {
  dataURL(url, 'Parquet');
  if (!Number.isSafeInteger(size) || size < 1) {
    throw new Error('Parquet: tamanho inválido no manifesto');
  }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000) {
    throw new Error('Parquet: timeoutMs inválido');
  }
  const controller = new AbortController();
  const timeoutMessage = 'Origem de dados não respondeu dentro do prazo (timeout)';
  let timedOut = false;
  let timer;
  // Promise.race also covers mocks and non-cooperative body streams: a
  // signal alone does not guarantee that a custom reader will settle.
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
      reject(new Error(timeoutMessage));
    }, timeoutMs);
  });
  const withinDeadline = operation => Promise.race([operation, deadline]);
  try {
    let response;
    try {
      response = await withinDeadline(fetchImpl(url, {
        method: 'GET', mode: 'cors', cache: 'no-store',
        headers: { Range: 'bytes=0-0' }, signal: controller.signal,
      }));
    } catch {
      if (timedOut) throw new Error(timeoutMessage);
      // A browser intentionally hides CORS failures from JavaScript.
      throw new Error('Origem de dados inacessível no navegador (rede ou CORS)');
    }

    // Abort immediately on HTTP 200 so a non-Range origin cannot download
    // a multi-gigabyte Parquet as a fallback.
    if (response.status !== 206 ||
        response.headers?.get('content-range') !== `bytes 0-0/${size}`) {
      throw new Error('Origem de dados sem HTTP Range 206/Content-Range compatível');
    }
    if (!response.body || typeof response.body.getReader !== 'function') {
      throw new Error('Origem de dados sem corpo de 1 byte');
    }
    const reader = response.body.getReader();
    try {
      const first = await withinDeadline(reader.read());
      if (first.done || !(first.value instanceof Uint8Array) ||
          first.value.byteLength !== 1) {
        throw new Error('Origem de dados não entregou corpo de exatamente 1 byte');
      }
      // The Parquet magic prefix starts with 0x50 ('P'). This catches a
      // wrong 206 body cheaply; it is not a SHA-256 or full PAR1 check.
      if (first.value[0] !== 0x50) {
        throw new Error('Origem de dados com assinatura Parquet inicial inválida');
      }
      // Also reject a second chunk, not just an oversized first chunk.
      const rest = await withinDeadline(reader.read());
      if (!rest.done) {
        throw new Error('Origem de dados entregou corpo maior que 1 byte');
      }
    } catch (error) {
      if (timedOut) throw new Error(timeoutMessage);
      throw error;
    } finally {
      // A timeout can leave a custom reader.read() pending; abort below.
      try { reader.releaseLock(); } catch { /* pending read */ }
    }
  } finally {
    clearTimeout(timer);
    // Ends incomplete responses; a fully consumed 1-byte stream is already
    // closed. Never await cancel() on an untrusted remote body.
    controller.abort();
  }
}
