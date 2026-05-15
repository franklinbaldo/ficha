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

/**
 * Adapt a Company (per-cnpj_base) into one row per Estabelecimento, in
 * the shape EmpresaFicha.svelte currently expects (`empresa: any`).
 *
 * The current `cnpjs.parquet` row shape is denormalized — one row per
 * establishment with `cnpj` + most root fields copied across. We replay
 * that by mapping each `company.estabelecimentos[i]` into a flat object,
 * pulling cnpj_base + razao_social + capital_social etc. from the root.
 *
 * `cnae_principal_descricao` etc. require lookup decoding — leave codes
 * as-is so the caller can apply the same lookup_{kind} DuckDB views the
 * search flow already uses.
 */
export function companyToEmpresaRows(company: Company): Record<string, unknown>[] {
  // proto3 doesn't distinguish "unset" from "0", so cnpj_base == 0 here
  // means either pack.py packed a bogus row (impossible — it asserts the
  // opposite) or a hand-crafted Company was passed in. Either way the
  // padded "00000000" would yield a malformed CNPJ; fail loudly instead.
  if (!company.cnpj_base) {
    throw new Error('companyToEmpresaRows: Company is missing cnpj_base');
  }
  const base = String(company.cnpj_base).padStart(8, '0');
  return (company.estabelecimentos ?? []).map((e) => {
    const ordem = String(e.cnpj_ordem ?? 0).padStart(4, '0');
    const dv = String(e.cnpj_dv ?? 0).padStart(2, '0');
    return {
      cnpj: `${base}${ordem}${dv}`,
      cnpj_base: base,
      cnpj_ordem: ordem,
      cnpj_dv: dv,
      identificador_matriz_filial: e.tipo,
      razao_social: company.razao_social ?? '',
      razao_social_normalizada: company.razao_social_normalizada ?? '',
      natureza_juridica_codigo: company.natureza_juridica_codigo ?? 0,
      capital_social: company.capital_social ?? 0,
      porte_empresa: company.porte_empresa ?? 0,
      ente_federativo_responsavel: company.ente_federativo_responsavel ?? '',
      nome_fantasia: e.nome_fantasia ?? '',
      situacao_cadastral: e.situacao_cadastral ?? 0,
      data_situacao_cadastral: e.data_situacao_cadastral ?? 0,
      motivo_situacao_cadastral_codigo: e.motivo_situacao_cadastral_codigo ?? 0,
      data_inicio_atividade: e.data_inicio_atividade ?? 0,
      cnae_principal_codigo: e.cnae_principal_codigo ?? 0,
      cnaes_secundarios_codigos: e.cnaes_secundarios_codigos ?? [],
      tipo_logradouro: e.tipo_logradouro ?? '',
      logradouro: e.logradouro ?? '',
      numero: e.numero ?? '',
      complemento: e.complemento ?? '',
      bairro: e.bairro ?? '',
      cep: e.cep ?? 0,
      uf: e.uf ?? '',
      municipio_codigo: e.municipio_codigo ?? 0,
      opcao_simples: e.opcao_simples ?? false,
      opcao_mei: e.opcao_mei ?? false,
    };
  });
}
