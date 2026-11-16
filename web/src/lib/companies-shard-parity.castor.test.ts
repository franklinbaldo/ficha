import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { companyArchiveUrl } from './companies';

const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const expectedDigest = '3f9eed1068515339d6528137d19acac79c4259ed3341b1b8b83f6b05821aca6e';

describe('Castor P0-A: complete Python ↔ JS shard mapping', () => {
  it('maps all 1296 ASCII prefixes to the same 100 buckets, keeping numeric v1 stable', () => {
    const files = {
      companies: {
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
        schema_version: '1.1.0' as const,
        shards: Array.from({ length: 100 }, (_, n) => {
          const shard = String(n).padStart(2, '0');
          return { shard, url: `https://example.org/companies-${shard}.zip`, sha1: 'a'.repeat(40), size: 1 };
        }),
      },
    };
    const transcript: string[] = [];
    for (const first of alphabet) {
      for (const second of alphabet) {
        const prefix = first + second;
        const actual = companyArchiveUrl(prefix + '000000', { identifier: 'fixture', files });
        const match = /companies-(\\d{2})\\.zip$/.exec(actual);
        expect(match, prefix).not.toBeNull();
        const bucket = match![1]!;
        const expected = /^[0-9]{2}$/.test(prefix)
          ? prefix
          : String(Number.parseInt(prefix, 36) % 100).padStart(2, '0');
        expect(bucket, prefix).toBe(expected);
        transcript.push(`${prefix}:${bucket}\\n`);
      }
    }
    expect(transcript).toHaveLength(1296);
    expect(createHash('sha256').update(transcript.join(''), 'ascii').digest('hex')).toBe(expectedDigest);
  });

  it('does not map an invalid root to a shard', () => {
    const files = {
      companies: {
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
        shards: [{ shard: '00', url: 'https://example.org/companies-00.zip', sha1: 'a'.repeat(40), size: 1 }],
      },
    };
    for (const root of ['ab000000', 'ZZ00000', 'ZZ0000000', 'AB!00000']) {
      expect(() => companyArchiveUrl(root, { identifier: 'fixture', files })).toThrow('Invalid CNPJ root');
    }
  });
});
