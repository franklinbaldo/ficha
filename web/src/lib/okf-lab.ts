import type * as duckdb from '@duckdb/duckdb-wasm';
import { attachCnpjs, attachRaizes, attachSocios, createDuckDB } from './analytical';
import { currentSnapshot, fetchManifest } from './manifest';

export type OkfLab = {
  db: duckdb.AsyncDuckDB;
  snapshotDate: string;
};

export async function createOkfLab(): Promise<OkfLab> {
  const manifest = await fetchManifest();
  if (!manifest) throw new Error('Dados ainda não publicados');

  const snapshot = currentSnapshot(manifest);
  if (!snapshot) throw new Error('Manifesto sem snapshot corrente válido');

  const db = await createDuckDB();
  await attachCnpjs(db, snapshot.files.cnpjs.url);
  await attachRaizes(db, snapshot.files.raizes.url);
  await attachSocios(db, snapshot.files.socios.url);
  return { db, snapshotDate: snapshot.date };
}
