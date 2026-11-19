import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { ManifestSchema } from '../schemas/v1/manifest';

type Shard = { shard: string; url: string };
type Snapshot = {
  date: string;
  files: { companies?: { shards: Shard[] }; companies_zip?: { url: string } };
};
type Catalog = { current: string; snapshots: Snapshot[] };

function checkedInManifest(): Catalog {
  return JSON.parse(readFileSync(
    new URL('../../public/manifest.json', import.meta.url), 'utf8'
  )) as Catalog;
}

/**
 * A publication-specific check, not a generic Zod URL policy. A future
 * query_url/CORS mirror must not silently replace the archive custody URL.
 */
function assertArchiveShardBindings(catalog: Catalog): number {
  const months = new Set<string>();
  let checked = 0;
  for (const snapshot of catalog.snapshots) {
    if (months.has(snapshot.date)) throw Error('duplicate snapshot date');
    months.add(snapshot.date);
    const shards = snapshot.files.companies?.shards;
    if (!shards) continue; // 2026-04 has no atomic layer.
    if (shards.length !== 100) throw Error('incomplete shard list');
    const slots = new Set<string>();
    for (const entry of shards) {
      if (!/^\d{2}$/.test(entry.shard) || slots.has(entry.shard)) {
        throw Error('duplicate or invalid shard slot');
      }
      slots.add(entry.shard);
      const expected = 'https://archive.org/download/ficha-' +
        snapshot.date + '/companies-' + entry.shard + '.zip';
      if (entry.url !== expected) {
        throw Error('archive mismatch for ' + snapshot.date + '/' + entry.shard);
      }
      checked += 1;
    }
    for (let number = 0; number < 100; number += 1) {
      if (!slots.has(String(number).padStart(2, '0'))) {
        throw Error('missing shard slot ' + number);
      }
    }
  }
  if (!months.has(catalog.current)) throw Error('current snapshot missing');
  return checked;
}

describe('checked-in IA custody URLs for the atomic company layer (#208)', () => {
  it('validates all published shard-to-month bindings without dereferencing IA', () => {
    const manifest = checkedInManifest();
    expect(ManifestSchema.safeParse(manifest).success).toBe(true);
    expect(assertArchiveShardBindings(manifest)).toBeGreaterThanOrEqual(300);
    for (const date of ['2026-05', '2026-06', '2026-07']) {
      expect(manifest.snapshots.find(snapshot => snapshot.date === date)
        ?.files.companies?.shards).toHaveLength(100);
    }
  });

  it('rejects swapped distinct URLs even with 100 unique shard IDs', () => {
    const manifest = checkedInManifest();
    const shards = manifest.snapshots.find(snapshot =>
      snapshot.date === '2026-07')!.files.companies!.shards;
    const first = shards[0]!;
    const second = shards[1]!;
    const temporary = first.url;
    first.url = second.url;
    second.url = temporary;
    expect(new Set(shards.map(shard => shard.url)).size).toBe(100);
    expect(() => assertArchiveShardBindings(manifest)).toThrow(/archive mismatch/);
  });

  it('rejects a correct slot pointing to another snapshot', () => {
    const manifest = checkedInManifest();
    const july = manifest.snapshots.find(snapshot => snapshot.date === '2026-07')!;
    july.files.companies!.shards[37]!.url =
      'https://archive.org/download/ficha-2026-06/companies-37.zip';
    expect(() => assertArchiveShardBindings(manifest)).toThrow(/2026-07\/37/);
  });

  it('rejects query strings, including an empty question mark, on custody URLs', () => {
    for (const suffix of ['?download=1', '?']) {
      const manifest = checkedInManifest();
      const july = manifest.snapshots.find(snapshot => snapshot.date === '2026-07')!;
      july.files.companies!.shards[0]!.url += suffix;
      expect(() => assertArchiveShardBindings(manifest)).toThrow(/2026-07\/00/);
    }
  });
});
