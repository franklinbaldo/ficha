import * as duckdb from '@duckdb/duckdb-wasm';

/**
 * Instancia uma DuckDB-WASM via JsDelivr bundle (CDN público; sem precisar
 * empacotar o WASM no nosso build).
 *
 * Devolve já uma `AsyncDuckDB` pronta. Ainda sem `cnpjs.parquet` carregado —
 * usar `attachCnpjs(db, url)` separadamente.
 */
export async function createDuckDB(): Promise<duckdb.AsyncDuckDB> {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  const worker = new Worker(
    URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], { type: 'text/javascript' })
    )
  );

  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

/**
 * Registra a URL do `cnpjs.parquet` e cria a VIEW `cnpjs` apontando pra ela.
 *
 * DuckDB-WASM lê via HTTP range requests — só baixa as colunas/row groups
 * que cada query precisa, não o arquivo inteiro.
 */
export async function attachCnpjs(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('cnpjs.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW cnpjs AS SELECT * FROM 'cnpjs.parquet'`);
  } finally {
    await conn.close();
  }
}
