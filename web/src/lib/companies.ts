/**
 * Per-company `.pb` reader — fetches one Company protobuf from the atomic
 * companies layer on Internet Archive via transparent-unzip, decodes it via
 * the protobufjs static module, and adapts it to shapes consumable by existing
 * UI without forcing a refactor.
 *
 * Snapshots históricos podem declarar `companies_zip`; snapshots retomáveis
 * declaram `companies` com 100 shards de prefixo. Nos dois casos o member path
 * interno continua `XX/XXX/XXX.pb`.
 */

import { ficha } from '../generated/company.pb.js';
import type { Snapshot } from '../schemas/v1/manifest';

// The generated `.d.ts` marks I* interfaces as deprecated in favor of
// the class types, but the class types include private fields that make
// plain-object construction awkward. The interfaces are the structural
// contract we actually want — silence the deprecation for the re-exports.
// eslint-disable-next-line @typescript-eslint/no-deprecated
export type Company = ficha.v1.ICompany;
// eslint-disable-next-line @typescript-eslint/no-deprecated
export type Estabelecimento = ficha.v1.IEstabelecimento;
// eslint-disable-next-line @typescript-eslint/no-deprecated
export type Socio = ficha.v1.ISocio;

/** Integer callers may zero-pad; textual roots must already be eight ASCII chars. */
function rootText(cnpjBase: number | string): string {
  const s = typeof cnpjBase === 'number'
    ? String(cnpjBase).padStart(8, '0')
    : cnpjBase;
  if (!/^[A-Z0-9]{8}$/.test(s)) throw new Error('Invalid CNPJ root');
  return s;
}

/** `12345678` → `'12/345/678.pb'` — mirrors ficha_etl.pack.cnpjpath. */
export function cnpjpath(cnpjBase: number | string): string {
  const s = rootText(cnpjBase);
  return `${s.slice(0, 2)}/${s.slice(2, 5)}/${s.slice(5, 8)}.pb`;
}

/** Resolve the historical monolithic companies.zip URL for a given IA item. */
export function companiesZipUrl(iaBase: string, identifier: string): string {
  return `${iaBase.replace(/\/$/, '')}/${identifier}/companies.zip`;
}

type AtomicCompanyFiles = Pick<Snapshot['files'], 'companies' | 'companies_zip'>;

/** Resolve the archive that must contain one cnpj_base. */
export function companyArchiveUrl(
  cnpjBase: number | string,
  options: {
    iaBase?: string;
    identifier: string;
    files?: AtomicCompanyFiles;
  }
): string {
  const { iaBase = 'https://archive.org/download', identifier, files } = options;
  if (files?.companies) {
    const base = rootText(cnpjBase);
    // A legacy prefix-2 index only describes numeric roots. A textual root
    // beginning with digits would otherwise be routed to a plausible old ZIP.
    if (files.companies.shard_by === 'cnpj_base_prefix_2' && /^[0-9]{2}/.test(base) && /[A-Z]/.test(base)) {
      throw new Error('companyArchiveUrl: alphanumeric root requires v1.1 shard mapping');
    }
    // Older manifests remain readable, but the new bucket mapping must explicitly
    // advertise the wire schema that preserves alphanumeric CNPJ text.
    if (files.companies.shard_by === 'cnpj_base_prefix_or_base36_mod_100_v1'
        && files.companies.schema_version !== '1.1.0') {
      throw new Error('companyArchiveUrl: schema 1.1.0 required for alphanumeric shards');
    }
    if (files.companies.shard_by === 'cnpj_base_prefix_2'
        && files.companies.schema_version === '1.1.0') {
      throw new Error('companyArchiveUrl: schema 1.1.0 cannot use legacy numeric shard mapping');
    }
    const rawPrefix = base.slice(0, 2);
    const prefix = files.companies.shard_by === 'cnpj_base_prefix_or_base36_mod_100_v1'
      && !/^[0-9]{2}$/.test(rawPrefix)
      ? String(parseInt(rawPrefix, 36) % 100).padStart(2, '0')
      : rawPrefix;
    const shard = files.companies.shards.find((entry) => entry.shard === prefix);
    if (!shard) {
      throw new Error(`companyArchiveUrl(${cnpjBase}): shard ${prefix} não declarado`);
    }
    return shard.url;
  }
  if (files?.companies_zip) return files.companies_zip.url;
  return companiesZipUrl(iaBase, identifier);
}

/**
 * Public interactive lookup budget, not an assertion about corpus-wide sizes.
 * If a member exceeds it, report an unavailable result instead of silently
 * downloading a whole ZIP (or treating a transport error as absent CNPJ).
 */
export const MAX_ATOMIC_COMPANY_BYTES = 32 * 1024 * 1024;

/** Consume a transparent-unzip PB member with a hard bound even without Content-Length. */
export async function readAtomicMember(
  response: Response,
  maxBytes: number = MAX_ATOMIC_COMPANY_BYTES
): Promise<Uint8Array> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new Error('Protobuf: limite de bytes inválido');
  }
  const tooLarge = () => new Error('Protobuf: membro excede o limite de leitura da interface');
  const advertised = response.headers?.get('content-length');
  if (advertised != null && /^\d+$/.test(advertised) && Number(advertised) > maxBytes) {
    // Rejecting the header must also stop an already-started browser download.
    // Never await cancellation: remote stream teardown must not delay UI feedback.
    if (typeof response.body?.cancel === 'function') {
      void response.body.cancel().catch(() => {});
    }
    throw tooLarge();
  }
  // Mocks without a ReadableStream retain the historical arrayBuffer path.
  // Browsers always expose a response.body stream: that is the bounded path.
  if (!response.body || typeof response.body.getReader !== 'function') {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > maxBytes) throw tooLarge();
    return bytes;
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      if (!(part.value instanceof Uint8Array)) {
        throw new Error('Protobuf: corpo binário inválido');
      }
      total += part.value.byteLength;
      if (total > maxBytes) throw tooLarge();
      chunks.push(part.value);
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes;
  } catch (error) {
    // Cancellation must not block UI feedback if an origin stalls.
    void reader.cancel().catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
}

/**
 * Fetch and decode a single Company by cnpj_base.
 *
 * Quando `files` vem do manifesto, ele é a fonte de verdade para a URL do
 * arquivo atômico. Sem `files`, mantém o fallback histórico por identifier.
 *
 * @returns the decoded Company, or `null` if the path returned 404 (the
 *   CNPJ doesn't exist in this snapshot). Other HTTP errors throw.
 */
export async function fetchCompany(
  cnpjBase: number | string,
  options: {
    iaBase?: string;
    identifier: string;
    files?: AtomicCompanyFiles;
    fetchImpl?: typeof fetch;
  }
): Promise<Company | null> {
  const { fetchImpl = fetch } = options;
  const archive = companyArchiveUrl(cnpjBase, options);
  const url = `${archive}/${cnpjpath(cnpjBase)}`;
  const res = await fetchImpl(url);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(`fetchCompany(${cnpjBase}): HTTP ${res.status} ${url}`);
  }
  const bytes = await readAtomicMember(res);
  const company = normalizeLongs(ficha.v1.Company.decode(bytes));
  // A v1-indexed ZIP must not masquerade as 1.1 just because the numeric root
  // still matches: alphanumeric establishment orders or partners are lossy in v1.
  if (options.files?.companies?.shard_by === 'cnpj_base_prefix_2' &&
      (company.cnpj_base_text ||
       company.estabelecimentos?.some(est => est.cnpj_ordem_text) ||
       company.socios?.some(socio => socio.cnpj_socio_text))) {
    throw new Error('fetchCompany: v1 numeric shard contains v1.1 textual CNPJ fields');
  }
  // A v1 decoder can silently discard unknown 1.1 text fields. At this
  // supported reader boundary, never return a different root as a valid hit.
  const expectedRoot = rootText(cnpjBase);
  if (companyRoot(company) !== expectedRoot) {
    throw new Error(`fetchCompany: root mismatch for ${expectedRoot} (unsupported or corrupt wire)`);
  }
  return company;
}

/** Possible shape of a 64-bit value returned by protobufjs uint64/int64. */
type LongLike = { low: number; high: number; unsigned?: boolean; toNumber?: () => number };

/**
 * Convert a uint64 decode result to a plain number.
 *
 * protobufjs returns 64-bit values as `Long` objects when `util.Long` is
 * configured (the default with `long` installed) or as `{low, high}`
 * pairs otherwise. The generated `.d.ts` declares these fields as
 * `number` (we passed `--force-number` to pbjs), so the runtime value
 * must match — otherwise downstream comparisons, formatting, and JSON
 * shaping break. For CNPJs (≤ 14 digits, ≤ 2^53-1) the conversion is
 * lossless.
 */
function longToNumber(v: unknown): number {
  if (typeof v === 'number') return v;
  if (v && typeof v === 'object') {
    const l = v as LongLike;
    if (typeof l.toNumber === 'function') return l.toNumber();
    if (typeof l.low === 'number' && typeof l.high === 'number') {
      // Reconstruct from low/high 32-bit halves (unsigned: high * 2^32 + low).
      const low = l.low >>> 0;
      const high = l.high >>> 0;
      return high * 0x100000000 + low;
    }
  }
  return 0;
}

/** Coerce 64-bit fields on a decoded Company to plain numbers. */
function normalizeLongs(company: Company): Company {
  for (const s of company.socios ?? []) {
    if (s.cnpj_socio !== undefined && s.cnpj_socio !== null) {
      s.cnpj_socio = longToNumber(s.cnpj_socio);
    }
  }
  return company;
}

// Note: an earlier draft included `companyToEmpresaRows`, a flat-row
// adapter aimed at EstabelecimentoSchema. It was removed because the
// canonical row contract requires zero-padded codes, formatted dates,
// and lookup-decoded descriptions that this layer can't supply on its
// own. When wiring `fetchCompany` into the UI, build the row shape at
// the call site where you have access to the lookup tables.

/** Text fields in schema 1.1 override the legacy numeric mirrors. */
export function companyRoot(company: Company): string {
  return company.cnpj_base_text || String(company.cnpj_base ?? 0).padStart(8, '0');
}

export function estabelecimentoCnpj(company: Company, est: Estabelecimento): string {
  const order = est.cnpj_ordem_text || String(est.cnpj_ordem ?? 0).padStart(4, '0');
  return companyRoot(company) + order + String(est.cnpj_dv ?? 0).padStart(2, '0');
}

export function socioCnpj(socio: Socio): string {
  return socio.cnpj_socio_text || String(socio.cnpj_socio ?? 0).padStart(14, '0');
}
