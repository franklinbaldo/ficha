import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { ManifestSchema as LegacyV1ManifestSchema } from './fixtures/manifest-v1-at-021ec8b5.castor';
import { ManifestSchema as UpdatedManifestSchema } from '../schemas/v1/manifest';

/**
 * This imports the complete, byte-for-byte v1 Zod schema from main@021ec8b5
 * (Git blob 7e64bb60c7718b18ea889c46bf84e48820005032).
 * It is NOT a reimplementation of the old parser and MUST NOT be updated
 * when the current manifest schema changes.
 *
 * All 1.1 entries/URLs below are synthetic and are never published.
 */
function compatibilityFixtures() {
  const published = JSON.parse(
    readFileSync(new URL('../../public/manifest.json', import.meta.url), 'utf8')
  );
  const july = published.snapshots.find((snapshot: { date: string }) => snapshot.date === '2026-07');
  if (!july?.files?.companies?.shards) {
    throw new Error('published fixture must contain July 2026 companies shards');
  }

  const future = structuredClone(july);
  future.date = '2026-08';
  future.files.companies = {
    ...future.files.companies,
    schema_version: '1.1.0',
    shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
    shards: future.files.companies.shards.map((shard: { shard: string; url: string }) => ({
      ...shard,
      // Proposed, not implemented: distinct wire namespace that v1 cannot
      // discover from its unchanged manifest.
      url: `https://example.invalid/ficha-2026-08/wire-1.1/companies-${shard.shard}.zip`,
    })),
  };
  return { published, future };
}

describe('Castor P0-A: actual historical v1 manifest parser compatibility', () => {
  it('still accepts the currently versioned public numeric index', () => {
    const { published } = compatibilityFixtures();
    expect(published.current).toBe('2026-07');
    expect(LegacyV1ManifestSchema.safeParse(published).success).toBe(true);
  });

  it('rejects the ENTIRE mixed index in the actual legacy parser, even with current=2026-07', () => {
    const { published, future } = compatibilityFixtures();
    const mixed = { ...published, snapshots: [future, ...published.snapshots] };
    expect(UpdatedManifestSchema.safeParse(mixed).success).toBe(true);
    const oldResult = LegacyV1ManifestSchema.safeParse(mixed);
    expect(oldResult.success).toBe(false);
    if (!oldResult.success) {
      expect(oldResult.error.issues.some(issue => issue.path.includes('shard_by'))).toBe(true);
    }
    // This is a demonstrable availability regression if the v1 URL is reused.
    // Never construe this fail-closed behavior as old decoders supporting wire 1.1.
  });

  it('a separately discovered fixture leaves the entire original v1 index and URLs intact', () => {
    const { published, future } = compatibilityFixtures();
    const separate = { current: '2026-08', snapshots: [future] };
    const oldUrls = new Set<string>(
      published.snapshots.flatMap((snapshot: typeof future) =>
        snapshot.files.companies?.shards.map((shard: { url: string }) => shard.url) ?? []
      )
    );
    const newUrls = future.files.companies.shards.map((shard: { url: string }) => shard.url);
    expect(newUrls).toHaveLength(100);
    expect(new Set(newUrls).size).toBe(100);
    expect(newUrls.every((url: string) => !oldUrls.has(url))).toBe(true);
    expect(LegacyV1ManifestSchema.safeParse(published).success).toBe(true);
    expect(UpdatedManifestSchema.safeParse(separate).success).toBe(true);
    expect(LegacyV1ManifestSchema.safeParse(separate).success).toBe(false);
    // This test is a contract oracle only. No production discovery route exists yet.
  });
});
