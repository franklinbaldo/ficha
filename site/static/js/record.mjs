/** Public record access. Never substitute another source or invent data. */
import { currentDataset, probeParquetRange } from './dataset.mjs';
import { createDuckDBEngine } from './duckdb-runtime.mjs';
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
    changed: displayValue(before?.[field]) !== displayValue(after?.[field]),
  }));
}

/** One lookup per archived snapshot, only following a successful 1-byte transport probe.
 * The caller must label this as an observation at the given competence.
 * The reader does not claim SHA-256 verification of the complete remote Parquet.
 */
export async function fetchRecordAt(manifest, date, cnpj, {
  probeRange = probeParquetRange, createEngine = createDuckDBEngine,
  attach = attachAnalyticalFiles, query = queryCompanies,
} = {}) {
  const canonical = canonicalCnpj(cnpj);
  if (!canonical) throw new Error('Informe um CNPJ completo, numérico ou alfanumérico');
  const snapshot = manifest?.snapshots?.find(s => s.date === date);
  if (!snapshot) throw new Error('Competência não publicada no manifesto');
  const dataset = currentDataset({ current: date, snapshots:[snapshot] });
  await probeRange({url:dataset.files.cnpjs.url, size:dataset.files.cnpjs.size});
  const engine = await createEngine();
  try {
    await attach(engine.db, dataset, engine.duckdb);
    const rows = await query(engine.db, canonical);
    return {cnpj:canonical, date, data:rows[0] ?? null, source:dataset.files.cnpjs.url};
  } finally {
    await engine.db?.terminate?.();
  }
}
