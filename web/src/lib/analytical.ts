import * as duckdb from '@duckdb/duckdb-wasm';
import type { Snapshot } from '../schemas/v1/manifest';

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

/** Registra `cnpjs.parquet` como a relação lógica `cnpjs`. */
export async function attachCnpjs(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('cnpjs.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW cnpjs AS SELECT * FROM 'cnpjs.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Registra `raizes.parquet` como a relação lógica `raizes`. */
export async function attachRaizes(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('raizes.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW raizes AS SELECT * FROM 'raizes.parquet'`);
  } finally {
    await conn.close();
  }
}

/**
 * Registra cada parquet de lookup definido no manifest como file URL
 * no DuckDB e cria a respectiva VIEW `lookup_{kind}`.
 */
export async function attachLookups(db: duckdb.AsyncDuckDB, manifest: Snapshot): Promise<void> {
  if (!manifest.lookups) return;
  const conn = await db.connect();
  try {
    for (const [kind, info] of Object.entries(manifest.lookups)) {
      await db.registerFileURL(`${kind}.parquet`, info.url, duckdb.DuckDBDataProtocol.HTTP, false);
      await conn.query(`CREATE OR REPLACE VIEW lookup_${kind} AS SELECT * FROM '${kind}.parquet'`);
    }
  } finally {
    await conn.close();
  }
}

/**
 * Registra `enderecos.parquet` e cria a VIEW `enderecos`.
 *
 * Parquet ordenado por (uf, municipio_codigo, logradouro_normalizado, numero).
 */
export async function attachEnderecos(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('enderecos.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW enderecos AS SELECT * FROM 'enderecos.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Registra `pessoas.parquet` e cria a VIEW `pessoas`. */
export async function attachPessoas(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('pessoas.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW pessoas AS SELECT * FROM 'pessoas.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Registra `socios.parquet` e cria a VIEW `socios`. */
export async function attachSocios(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('socios.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW socios AS SELECT * FROM 'socios.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Registra `cnpj_contatos.parquet` e cria a VIEW `cnpj_contatos`. */
export async function attachCnpjContatos(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('cnpj_contatos.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW cnpj_contatos AS SELECT * FROM 'cnpj_contatos.parquet'`);
  } finally {
    await conn.close();
  }
}

/** Registra `cnpj_cnaes.parquet` e cria a VIEW `cnpj_cnaes`. */
export async function attachCnpjCnaes(db: duckdb.AsyncDuckDB, url: string): Promise<void> {
  await db.registerFileURL('cnpj_cnaes.parquet', url, duckdb.DuckDBDataProtocol.HTTP, false);
  const conn = await db.connect();
  try {
    await conn.query(`CREATE OR REPLACE VIEW cnpj_cnaes AS SELECT * FROM 'cnpj_cnaes.parquet'`);
  } finally {
    await conn.close();
  }
}
