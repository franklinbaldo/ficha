/** Public record access. Never substitute another source or invent data. */
import { attachAnalyticalFiles, queryCompanies, stripCnpj } from './query-core.mjs';

export const RECORD_FIELDS = Object.freeze([
  ['razao_social','Razão social'],
  ['nome_fantasia','Nome fantasia'],
  ['cnpj','CNPJ'],
  ['uf','UF'],
  ['municipio_nome','Município'],
  ['cnae_principal_codigo','CNAE principal'],
  ['cnae_principal_descricao','Atividade principal'],
  ['capital_social','Capital social (valor cadastral)'],
  ['opcao_simples','Opção pelo Simples'],
  ['opcao_mei','Opção pelo MEI'],
  ['data_opcao_simples','Data da opção pelo Simples'],
  ['data_exclusao_simples','Data da exclusão do Simples'],
  ['data_opcao_mei','Data da opção pelo MEI'],
]);

export function canonicalCnpj(value) {
  const result = stripCnpj(String(value ?? '').trim());
  return /^[A-Z0-9]{12}[0-9]{2}$/.test(result) ? result : null;
}

export function displayValue(value) {
  if (value === null || value === undefined || value === '') return 'Não informado';
  if (value === true) return 'Sim';
  if (value === false) return 'Não';
  return String(value);
}

export function compareRecords(before, after) {
  if (!before && !after) return [];
  return RECORD_FIELDS.map(([field,label]) => ({
    field, label,
    before: displayValue(before?.[field]),
    after: displayValue(after?.[field]),
    // Presentation collapses null, empty strings and some scalar types.
    // Compare observed values, never their labels.
    changed: before?.[field] !== after?.[field],
  }));
}

/** One lookup per archived snapshot, only following a successful 1-byte transport probe.
 * The caller must label this as an observation at the given competence.
 * The reader does not claim SHA-256 verification of the complete remote Parquet.
 */
export async function fetchRecordAt(manifest, date, cnpj, {
  probeRange, createEngine,
  attach = attachAnalyticalFiles, query = queryCompanies,
} = {}) {
  const canonical = canonicalCnpj(cnpj);
  if (!canonical) throw new Error('Informe um CNPJ completo, numérico ou alfanumérico');
  const snapshot = manifest?.snapshots?.find(s => s.date === date);
  if (!snapshot) throw new Error('Competência não publicada no manifesto');
  // Legacy Parquet is an explicit, opt-in path. Keep the Protobuf company and
  // history pages free of dataset probing and DuckDB module downloads at load.
  const { currentDataset, probeParquetRange } = await import('./dataset.mjs');
  // Keep the original snapshot set: reducing it to [snapshot] would bypass
  // currentDataset's fail-closed check against duplicate competence dates.
  const dataset = currentDataset({ ...manifest, current: date });
  await (probeRange ?? probeParquetRange)({url:dataset.files.cnpjs.url, size:dataset.files.cnpjs.size});
  const engineFactory = createEngine ?? (await import('./duckdb-runtime.mjs')).createDuckDBEngine;
  const engine = await engineFactory();
  try {
    await attach(engine.db, dataset, engine.duckdb);
    const rows = await query(engine.db, canonical);
    return {cnpj:canonical, date, data:rows[0] ?? null, source:dataset.files.cnpjs.url};
  } finally {
    await engine.db?.terminate?.();
  }
}
