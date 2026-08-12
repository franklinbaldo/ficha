import { describe, expect, it } from 'vitest';
import { SnapshotEntrySchema } from './manifest';

function baseSnapshot() {
  return {
    date: '2026-05',
    schema_version: '1.0.0',
    rfb_layout_date: null,
    generated_at: '2026-08-12T00:00:00Z',
    generator: 'ficha-etl',
    row_counts: { cnpjs: 1, raizes: 1, socios: 1 },
    files: {
      cnpjs: { url: 'https://example.test/cnpjs', sha256: 'a'.repeat(64), size: 1 },
      raizes: { url: 'https://example.test/raizes', sha256: 'b'.repeat(64), size: 1 },
      socios: { url: 'https://example.test/socios', sha256: 'c'.repeat(64), size: 1 },
      lookups: { url: 'https://example.test/lookups', sha256: 'd'.repeat(64), size: 1 },
    },
  };
}

function shards() {
  return Array.from({ length: 100 }, (_, value) => {
    const shard = String(value).padStart(2, '0');
    return {
      shard,
      url: `https://archive.org/download/ficha-2026-05/companies-${shard}.zip`,
      sha1: 'e'.repeat(40),
      size: value + 1,
    };
  });
}

function shardedSnapshot(companyShards = shards()) {
  const base = baseSnapshot();
  return {
    ...base,
    files: {
      ...base.files,
      companies: {
        shard_by: 'cnpj_base_prefix_2' as const,
        shards: companyShards,
      },
    },
  };
}

describe('SnapshotEntrySchema companies shards', () => {
  it('accepts the complete 00..99 set', () => {
    expect(SnapshotEntrySchema.parse(shardedSnapshot()).files.companies?.shards).toHaveLength(100);
  });

  it('keeps historical snapshots valid without an atomic companies layer', () => {
    expect(() => SnapshotEntrySchema.parse(baseSnapshot())).not.toThrow();
  });

  it('rejects duplicate/missing prefixes even when the array still has 100 entries', () => {
    const broken = shards();
    broken[99] = { ...broken[98]! };
    expect(() => SnapshotEntrySchema.parse(shardedSnapshot(broken))).toThrow();
  });
});
