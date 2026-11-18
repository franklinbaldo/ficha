/** Browser-only DuckDB loader. Exact version pinned; remote CDN loading is a
 * temporary migration boundary and is NOT an offline/integrity guarantee. */
const DUCKDB_ESM = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev45.0/+esm';

export async function createDuckDBEngine({
  loadModule = () => import(DUCKDB_ESM),
  WorkerImpl = Worker,
  URLImpl = URL,
  BlobImpl = Blob,
} = {}) {
  const duckdb = await loadModule();
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  if (!bundle?.mainWorker || !bundle.mainModule) {
    throw new Error('DuckDB-WASM: bundle incompleto');
  }
  // Mirrors web/src/lib/analytical.ts: worker script URL comes from the
  // selected DuckDB bundle, not from user input or the dataset manifest.
  const source = 'importScripts(' + JSON.stringify(bundle.mainWorker) + ');';
  const blobURL = URLImpl.createObjectURL(new BlobImpl([source], { type: 'text/javascript' }));
  let worker;
  try {
    worker = new WorkerImpl(blobURL);
  } finally {
    URLImpl.revokeObjectURL(blobURL);
  }
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return { db, duckdb };
  } catch (error) {
    await db.terminate();
    throw error;
  }
}
