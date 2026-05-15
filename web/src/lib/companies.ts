/**
 * Per-company `.pb` reader — fetches one Company protobuf from
 * `companies.zip` on Internet Archive via transparent-unzip, decodes it
 * via the protobufjs static module, and adapts it to shapes consumable
 * by existing UI (e.g. EmpresaFicha.svelte) without forcing a refactor.
 *
 * IA serves any member of a public ZIP at
 *   {item}/companies.zip/{member}
 * — so a per-company fetch is a single HTTP GET of ~1–10 KB rather than
 * a range-read over the multi-GB cnpjs.parquet. See ADR around
 * `pack.py` (etl/src/ficha_etl/pack.py) for the layout contract.
 */

import { ficha } from '../generated/company.pb.js';

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

/** Resolve the companies.zip base URL for a given IA item. */
export function companiesZipUrl(iaBase: string, identifier: string): string {
  return `${iaBase.replace(/\/$/, '')}/${identifier}/companies.zip`;
}

/**
 * Fetch and decode a single Company by cnpj_base.
 *
 * @returns the decoded Company, or `null` if the path returned 404 (the
 *   CNPJ doesn't exist in this snapshot). Other HTTP errors throw.
 */
export async function fetchCompany(
  cnpjBase: number | string,
  options: {
    iaBase?: string;
    identifier: string;
    fetchImpl?: typeof fetch;
  }
): Promise<Company | null> {
  const { iaBase = 'https://archive.org/download', identifier, fetchImpl = fetch } = options;
  const url = `${companiesZipUrl(iaBase, identifier)}/${cnpjpath(cnpjBase)}`;
  const res = await fetchImpl(url);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(`fetchCompany(${cnpjBase}): HTTP ${res.status} ${url}`);
  }
  const bytes = new Uint8Array(await res.arrayBuffer());
  return ficha.v1.Company.decode(bytes);
}

// Note: an earlier draft included `companyToEmpresaRows`, a flat-row
// adapter aimed at EstabelecimentoSchema. It was removed because the
// canonical row contract requires zero-padded codes, formatted dates,
// and lookup-decoded descriptions that this layer can't supply on its
// own. When wiring `fetchCompany` into the UI, build the row shape at
// the call site where you have access to the lookup tables.
