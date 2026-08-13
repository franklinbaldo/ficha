import type * as duckdb from '@duckdb/duckdb-wasm';
import { previewOkfConvenienceView, validateOkfConvenienceViewRows } from './okf-views';

export type OkfViewBenchmark = {
  name: string;
  samplesMs: number[];
  firstMs: number;
  warmMeanMs: number;
};

export async function benchmarkOkfConvenienceView(
  db: duckdb.AsyncDuckDB,
  name: string,
  runs = 3
): Promise<OkfViewBenchmark> {
  if (!Number.isInteger(runs) || runs < 2 || runs > 10) {
    throw new Error('OKF view benchmark runs must be an integer between 2 and 10');
  }

  const samplesMs: number[] = [];
  for (let i = 0; i < runs; i += 1) {
    const startedAt = performance.now();
    const table = await previewOkfConvenienceView(db, name, 5);
    validateOkfConvenienceViewRows(
      name,
      table.toArray().map((row) => row.toJSON())
    );
    samplesMs.push(performance.now() - startedAt);
  }

  const warm = samplesMs.slice(1);
  return {
    name,
    samplesMs,
    firstMs: samplesMs[0],
    warmMeanMs: warm.reduce((sum, value) => sum + value, 0) / warm.length,
  };
}
