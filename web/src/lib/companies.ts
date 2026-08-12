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

/** `12345678` → `'12/345/678.pb'` — mirrors ficha_etl.pack.cnpjpath. */
export function cnpjpath(cnpjBase: number | string): string {
  const s = String(cnpjBase).padStart(8, '0');
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
    const prefix = cnpjpath(cnpjBase).slice(0, 2);
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
  const bytes = new Uint8Array(await res.arrayBuffer());
  return normalizeLongs(ficha.v1.Company.decode(bytes));
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
