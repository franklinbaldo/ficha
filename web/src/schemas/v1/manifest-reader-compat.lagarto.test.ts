/** P0-D offline contract tests. No Internet Archive requests or real manifest writes. */
import { describe, expect, it } from 'vitest';
import { ficha } from '../../generated/company.pb.js';
import { companyArchiveUrl, companyRoot, estabelecimentoCnpj, socioCnpj } from '../../lib/companies';
import { currentSnapshot } from '../../lib/manifest';
import { ManifestSchema, SnapshotEntrySchema } from './manifest';

const archive = 'https://archive.org/download';
const file = (name: string) => ({
  url: archive + '/ficha-2026-04/' + name,
  sha256: 'a'.repeat(64),
  size: 7,
});

function snapshot(date: string) {
  return {
    date,
    schema_version: '1.0.0',
    rfb_layout_date: null,
    generated_at: '2026-08-01T00:00:00Z',
    generator: 'fixture-offline',
    row_counts: { cnpjs: 1, raizes: 1, socios: 1 },
    files: {
      cnpjs: file('cnpjs.parquet'),
      raizes: file('raizes.parquet'),
      socios: file('socios.parquet'),
      lookups: file('lookups.json'),
    },
  };
}

function shards() {
  return Array.from({ length: 100 }, (_, i) => {
    const shard = String(i).padStart(2, '0');
    return {
      shard,
      url: archive + '/ficha-2026-08/companies-' + shard + '.zip',
      sha1: 'b'.repeat(40),
      size: i + 1,
    };
  });
}

function snapshot11() {
  return {
    ...snapshot('2026-08'),
    schema_version: '1.1.0',
    files: {
      ...snapshot('2026-08').files,
      companies: {
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
        schema_version: '1.1.0' as const,
        shards: shards(),
      },
    },
  };
}

describe('Lagarto P0-D: manifest v1/v1.1 and reader contract', () => {
  it('reads v1 sha256-only monolithic snapshots and legacy numeric bytes', () => {
    const old = {
      ...snapshot('2026-04'),
      files: { ...snapshot('2026-04').files, companies_zip: file('companies.zip') },
    };
    const manifest = ManifestSchema.parse({ current: '2026-04', snapshots: [old] });
    const selected = currentSnapshot(manifest);
    expect(selected?.date).toBe('2026-04');
    expect(companyArchiveUrl('00000001', {
      identifier: 'ficha-2026-04',
      files: selected!.files,
    })).toBe(archive + '/ficha-2026-04/companies.zip');

    const wire = ficha.v1.Company.encode({ cnpj_base: 1234 }).finish();
    expect(companyRoot(ficha.v1.Company.decode(wire))).toBe('00001234');
  });

  it('accepts published v1 100-shard geometry with numeric prefixes', () => {
    const legacy = {
      ...snapshot('2026-07'),
      files: {
        ...snapshot('2026-07').files,
        companies: {
          shard_by: 'cnpj_base_prefix_2' as const,
          shards: shards(),
        },
      },
    };
    const parsed = SnapshotEntrySchema.parse(legacy);
    expect(parsed.files.companies?.schema_version).toBeUndefined();
    expect(companyArchiveUrl('00001234', {
      identifier: 'ficha-2026-07',
      files: parsed.files,
    })).toBe(archive + '/ficha-2026-08/companies-00.zip');
  });

  it('parses v1.1 alongside v1 without auto-promoting the latest date', () => {
    const manifest = ManifestSchema.parse({
      current: '2026-04',
      snapshots: [snapshot('2026-04'), snapshot11()],
    });
    expect(manifest.snapshots).toHaveLength(2);
    expect(currentSnapshot(manifest)?.date).toBe('2026-04');
    expect(manifest.snapshots[0]?.schema_version).toBe('1.0.0');
    expect(manifest.snapshots[1]?.files.companies?.schema_version).toBe('1.1.0');
    expect(companyArchiveUrl('A0000000', {
      identifier: 'ficha-2026-08',
      files: manifest.snapshots[1]!.files,
    })).toBe(archive + '/ficha-2026-08/companies-60.zip');
  });

  it('prefers v1.1 literal alpha fields over numeric mirrors after wire decoding', () => {
    const wire = ficha.v1.Company.encode({
      cnpj_base: 0,
      cnpj_base_text: '00000000',
      estabelecimentos: [{ cnpj_ordem: 0, cnpj_ordem_text: 'E08G', cnpj_dv: 12 }],
      socios: [{ cnpj_socio: 0, cnpj_socio_text: 'A123456789AB34' }],
    }).finish();
    const decoded = ficha.v1.Company.decode(wire);
    expect(companyRoot(decoded)).toBe('00000000');
    expect(estabelecimentoCnpj(decoded, decoded.estabelecimentos[0]!)).toBe('00000000E08G12');
    expect(socioCnpj(decoded.socios[0]!)).toBe('A123456789AB34');
  });

  it('routes every numeric 00..99 prefix to its historical shard', () => {
    const parsed = SnapshotEntrySchema.parse(snapshot11());
    for (let i = 0; i < 100; i++) {
      const prefix = String(i).padStart(2, '0');
      expect(companyArchiveUrl(prefix + '000000', {
        identifier: 'ficha-2026-08',
        files: parsed.files,
      })).toBe(archive + '/ficha-2026-08/companies-' + prefix + '.zip');
    }
  });

  it('rejects incomplete, duplicated, corrupt hash and zero-byte shards', () => {
    const partial = structuredClone(snapshot11());
    partial.files.companies.shards.pop();
    expect(SnapshotEntrySchema.safeParse(partial).success).toBe(false);

    const duplicate = structuredClone(snapshot11());
    duplicate.files.companies.shards[99] = { ...duplicate.files.companies.shards[98]! };
    expect(SnapshotEntrySchema.safeParse(duplicate).success).toBe(false);

    const badHash = structuredClone(snapshot11());
    badHash.files.companies.shards[0]!.sha1 = 'NOT-A-HASH';
    expect(SnapshotEntrySchema.safeParse(badHash).success).toBe(false);

    const zero = structuredClone(snapshot11());
    zero.files.companies.shards[0]!.size = 0;
    expect(SnapshotEntrySchema.safeParse(zero).success).toBe(false);
  });

  it('exposes the unsupported v1 numeric-only reconstruction of v1.1 alphanumeric wire', () => {
    const wire = ficha.v1.Company.encode({
      cnpj_base: 0,
      cnpj_base_text: '00000000',
      estabelecimentos: [{ cnpj_ordem_text: 'E08G', cnpj_dv: 12 }],
    }).finish();
    const decoded = ficha.v1.Company.decode(wire);
    const establishment = decoded.estabelecimentos[0]!;
    // A v1 numeric-only projection ignores the 1.1 text field, losing E08G.
    // This test does NOT claim an actual v1 decoder rejects the new bytes.
    const legacyNumericOnly = String(decoded.cnpj_base ?? 0).padStart(8, '0')
      + String(establishment.cnpj_ordem ?? 0).padStart(4, '0')
      + String(establishment.cnpj_dv ?? 0).padStart(2, '0');
    const authoritative = estabelecimentoCnpj(decoded, establishment);
    expect(legacyNumericOnly).toBe('00000000000012');
    expect(authoritative).toBe('00000000E08G12');
    expect(legacyNumericOnly).not.toBe(authoritative);
  });

  it('refuses an alphanumeric root when a legacy numeric-prefix shard manifest is selected', () => {
    const legacy = SnapshotEntrySchema.parse({
      ...snapshot('2026-07'),
      files: {
        ...snapshot('2026-07').files,
        companies: { shard_by: 'cnpj_base_prefix_2', shards: shards() },
      },
    });
    expect(() => companyArchiveUrl('A0000000', {
      identifier: 'ficha-2026-07',
      files: legacy.files,
    })).toThrow(/shard A0/);
    const current = SnapshotEntrySchema.parse(snapshot11());
    expect(companyArchiveUrl('A0000000', {
      identifier: 'ficha-2026-08',
      files: current.files,
    })).toBe(archive + '/ficha-2026-08/companies-60.zip');
  });
});
