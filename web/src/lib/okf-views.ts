import type * as duckdb from '@duckdb/duckdb-wasm';
import { okfConvenienceViews } from '../generated/ficha-okf.views';

export function getOkfConvenienceView(name: string) {
  const view = okfConvenienceViews.find((item) => item.name === name);
  if (!view) throw new Error(`Unknown OKF convenience view: ${name}`);
  return view;
}

export async function queryOkfConvenienceView(db: duckdb.AsyncDuckDB, name: string) {
  const view = getOkfConvenienceView(name);
  const conn = await db.connect();
  try {
    return await conn.query(view.sql);
  } finally {
    await conn.close();
  }
}

export { okfConvenienceViews };
