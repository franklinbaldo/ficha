/** Browser bridge for numeric Protobuf v1. The generated decoder is loaded only
 * after a valid CNPJ and an explicitly declared archive shard are selected.
 * No Parquet engine or monolithic ZIP is used for this route.
 */
import {fetchAtomicRecord, planAtomicRecord, readBoundedMember} from './atomic-record.mjs';

const LOOKUP_LIMIT = 1024 * 1024;
const cache = new Map();

export async function fetchSnapshotLookups(manifest, date, {
  fetchImpl = fetch, cacheMap = cache,
} = {}) {
  const snapshot = manifest?.snapshots?.find(s => s?.date === date);
  const url = snapshot?.files?.lookups?.url;
  const expected = `https://archive.org/download/ficha-${date}/lookups.json`;
  if (url !== expected) throw new Error('Lookup da competência ausente ou URL arquivística divergente');
  if (!Number.isSafeInteger(snapshot.files.lookups.size) ||
      snapshot.files.lookups.size < 1 || snapshot.files.lookups.size > LOOKUP_LIMIT) {
    throw new Error('Tamanho do lookup não declarado ou fora do orçamento');
  }
  const sha256 = snapshot.files.lookups.sha256;
  if (!/^[0-9a-f]{64}$/.test(sha256 ?? '')) {
    throw new Error('SHA-256 do lookup ausente ou inválido no manifesto');
  }
  const cacheKey = url + '#' + sha256 + ':' + snapshot.files.lookups.size;
  let promise = cacheMap.get(cacheKey);
  if (!promise) {
    promise = (async () => {
      const response = await fetchImpl(url, {mode:'cors'});
      // A full-file GET is valid only with 200. A 201 or 206 may carry
      // identical bytes in a test but must not attest an archival lookup.
      if (response.status !== 200) throw new Error('Lookup indisponível: HTTP ' + response.status);
      // A full-file response cannot declare byte ranges, even if SHA matches.
      if (response.headers?.get?.('content-range') != null) {
        try { void response.body?.cancel?.().catch(() => {}); } catch { /* Best effort. */ }
        throw new Error('Lookup HTTP 200 com Content-Range inesperado');
      }
      const bytes = await readBoundedMember(response, LOOKUP_LIMIT);
      if (bytes.byteLength !== snapshot.files.lookups.size) {
        throw new Error('Lookup com tamanho divergente do manifesto');
      }
      if (!globalThis.crypto?.subtle) throw new Error('SHA-256 indisponível neste navegador');
      const hash = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', bytes));
      const actual = [...hash].map(byte => byte.toString(16).padStart(2, '0')).join('');
      if (actual !== sha256) throw new Error('SHA-256 do lookup difere do manifesto');
      let value;
      try { value = JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(bytes)); }
      catch { throw new Error('Lookup JSON inválido'); }
      if (value?.schema_version !== '1.0.0' ||
          value?.snapshot_date !== date || !value.municipios ||
          !value.cnaes || typeof value.municipios !== 'object' ||
          typeof value.cnaes !== 'object' || Array.isArray(value.municipios) ||
          Array.isArray(value.cnaes)) {
        throw new Error('Lookup incompatível com a competência');
      }
      return value;
    })();
    cacheMap.set(cacheKey, promise);
    promise.catch(() => { if (cacheMap.get(cacheKey) === promise) cacheMap.delete(cacheKey); });
  }
  return promise;
}

/** The decoder must originate from the generated web/src/generated/company.pb.js
 * bundle, never from an independent hand-coded Protobuf parser.
 */
export async function fetchCompanyByCnpj(manifest, date, cnpj, {
  fetchImpl = fetch,
  loadDecoder = () => import('./company-decoder.mjs'),
  loadLookups = fetchSnapshotLookups,
  signal,
} = {}) {
  // Fail before fetching decoder or data on malformed CNPJ, absent month or bad URL.
  planAtomicRecord(manifest, date, cnpj);
  const {decodeCompany} = await loadDecoder();
  if (typeof decodeCompany !== 'function') throw new Error('Decoder canônico indisponível');
  const result = await fetchAtomicRecord(manifest, date, cnpj,
    {fetchImpl, decodeCompany, signal});
  // HTTP 404 is inconclusive, not a company absence; skip secondary GETs.
  if (result.state !== 'found') return result;
  const lookups = await loadLookups(manifest, date, {fetchImpl});
  const municipio = result.data.municipio_codigo;
  const cnae = result.data.cnae_principal_codigo;
  const municipality = municipio == null ? null : lookups.municipios[String(municipio)];
  const activity = cnae == null ? null : lookups.cnaes[String(cnae).padStart(7, '0')];
  if ((municipio != null && typeof municipality !== 'string') ||
      (cnae != null && typeof activity !== 'string')) {
    throw new Error('Código RFB ausente do lookup da competência');
  }
  return {...result, data:{
    ...result.data,
    municipio_nome:municipality ?? null,
    cnae_principal_descricao:activity ?? null,
  }};
}
