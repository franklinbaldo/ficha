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

export async function previewOkfConvenienceView(
  db: duckdb.AsyncDuckDB,
  name: string,
  limit = 5
) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) {
    throw new Error('OKF view preview limit must be an integer between 1 and 50');
  }
  const view = getOkfConvenienceView(name);
  const conn = await db.connect();
  try {
    // A view OKF representa a relação completa; a superfície pública só pede
    // uma amostra limitada para não materializar milhões de linhas no browser.
    return await conn.query(`SELECT * FROM (${view.sql}) AS okf_view_preview LIMIT ${limit}`);
  } finally {
    await conn.close();
  }
}

export { okfConvenienceViews };
