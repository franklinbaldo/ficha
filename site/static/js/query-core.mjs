/** Browser-neutral DuckDB-WASM query core. The caller supplies an instantiated db. */
const FIELDS = `cnpj, razao_social, nome_fantasia, uf,
  cnae_principal_codigo, cnae_principal_descricao, municipio_nome,
  capital_social, opcao_simples, data_opcao_simples, data_exclusao_simples,
  opcao_mei, data_opcao_mei`;
const EXACT = `SELECT ${FIELDS} FROM cnpjs WHERE cnpj = ? LIMIT 1`;
const BY_NAME = `SELECT ${FIELDS} FROM cnpjs WHERE razao_social ILIKE ? LIMIT 20`;

/** Presentation punctuation is removable; letters must never become numbers. */
export const stripCnpj = value => value.toUpperCase().replace(/[.\/\-\s]/g, '');

/** Matches the current Svelte home semantics, including name fallback. */
export function planCompanyQuery(input) {
  const sanitized = input.trim().replace(/[%_\\]/g, '');
  // Legacy Astro classifies the original input before stripping LIKE wildcards.
  // Otherwise a trailing %/_/backslash incorrectly becomes an exact CNPJ.
  const cnpj = stripCnpj(input.trim());
  if (/^[A-Z0-9]{12}[0-9]{2}$/.test(cnpj)) {
    return { mode: 'cnpj', sql: EXACT, param: cnpj };
  }
  if (sanitized.length < 3) return null;
  return { mode: 'name', sql: BY_NAME, param: `%${sanitized}%` };
}

/** Same registration path as web/src/lib/analytical.ts, without a second WASM runtime. */
export async function attachAnalyticalFiles(db, dataset, duckdb) {
  if (!duckdb?.DuckDBDataProtocol?.HTTP) throw new Error('DuckDB HTTP protocol required');
  await db.registerFileURL('cnpjs.parquet', dataset.files.cnpjs.url,
    duckdb.DuckDBDataProtocol.HTTP, false);
  // The current CNPJ/name queries read only cnpjs.parquet. Its descriptions
  // are denormalized by the ETL (ADR 0019). Do not eagerly register the six
  // archival lookup files: they introduce extra CORS/Range dependencies even
  // when the user requests only a CNPJ.
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW cnpjs AS SELECT * FROM 'cnpjs.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Prepared statements preserve alphanumeric CNPJ as text and close resources. */
export async function queryCompanies(db, input) {
  const plan = planCompanyQuery(input);
  if (!plan) return [];
  const conn = await db.connect();
  let stmt;
  try {
    stmt = await conn.prepare(plan.sql);
    const result = await stmt.query(plan.param);
    return result.toArray().map(row => row.toJSON());
  } finally {
    try { if (stmt) await stmt.close(); } finally { await conn.close(); }
  }
}
