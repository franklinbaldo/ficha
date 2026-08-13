import { describe, expect, it } from 'vitest';
import { SnapshotEntrySchema } from './manifest';

const base = {
  date: '2026-05',
  schema_version: '1.0.0',
  rfb_layout_date: null,
  generated_at: '2026-05-31T00:00:00Z',
  generator: 'ficha-etl',
  row_counts: { cnpjs: 1, raizes: 1, socios: 1 },
  files: {
    cnpjs: {
      url: 'https://archive.org/download/ficha-2026-05/cnpjs.parquet',
      sha1: '1'.repeat(40),
      sha256: 'a'.repeat(64),
      size: 10,
    },
    raizes: {
      url: 'https://archive.org/download/ficha-2026-05/raizes.parquet',
      sha1: '2'.repeat(40),
      sha256: 'b'.repeat(64),
      size: 10,
    },
    socios: {
      url: 'https://archive.org/download/ficha-2026-05/socios.parquet',
      sha1: '3'.repeat(40),
      sha256: 'c'.repeat(64),
      size: 10,
    },
    lookups: {
      url: 'https://archive.org/download/ficha-2026-05/lookups.json',
      sha1: '4'.repeat(40),
      sha256: 'd'.repeat(64),
      size: 10,
    },
  },
};

describe('lookup parquet identity', () => {
  it('accepts size + sha1 + sha256 for new lookup parquets', () => {
    const parsed = SnapshotEntrySchema.parse({
      ...base,
      lookups: {
        municipios: {
          url: 'https://archive.org/download/ficha-2026-05/lookups/municipios.parquet',
          sha1: '5'.repeat(40),
          sha256: 'e'.repeat(64),
          size: 123,
        },
      },
    });

    expect(parsed.lookups?.municipios).toMatchObject({
      sha1: '5'.repeat(40),
      sha256: 'e'.repeat(64),
      size: 123,
    });
  });

  it('temporarily accepts historical URL-only lookup entries', () => {
    const parsed = SnapshotEntrySchema.parse({
      ...base,
      date: '2026-04',
      lookups: {
        municipios: {
          url: 'https://archive.org/download/ficha-2026-04/lookups/municipios.parquet',
        },
      },
    });

    expect(parsed.lookups?.municipios?.url).toContain('2026-04');
  });
});
