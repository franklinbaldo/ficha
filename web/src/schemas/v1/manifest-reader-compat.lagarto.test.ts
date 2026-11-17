/** P0-D offline contract tests. No Internet Archive requests or real manifest writes. */
import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { Field, Root, Type } from 'protobufjs';
import { z } from 'zod';

import { ficha } from '../../generated/company.pb.js';
import { companyArchiveUrl, companyRoot, estabelecimentoCnpj, socioCnpj } from '../../lib/companies';
import { currentSnapshot } from '../../lib/manifest';
import { fetchAtomicV11Company, parseAtomicV11Manifest } from '../../lib/companies-atomic-v11';
import { ManifestSchema, SnapshotEntrySchema } from './manifest';

const archive = 'https://archive.org/download';
const packerCaMemberHex = '22084341303030303031a20108100c2a0445303847aa01104a0e3030303030303030453038473132a006f0ae0c';
const packerCaMemberSha256 = '041e2b5e6f039959b03fd185021493f422ce884ea155590c9e99916ca769ec48';
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
        shards: shards().map((entry) => ({
          ...entry,
          url: entry.url.replace('/companies-', '/wire-1.1/companies-'),
        })),
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
    })).toBe(archive + '/ficha-2026-08/wire-1.1/companies-60.zip');
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
      })).toBe(archive + '/ficha-2026-08/wire-1.1/companies-' + prefix + '.zip');
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
    })).toBe(archive + '/ficha-2026-08/wire-1.1/companies-60.zip');
  });
  it('proves a real v1-only protobuf decoder silently drops v1.1 alpha identity fields', () => {
    // This is a minimal *actual* protobufjs v1 wire schema, not a numeric projection.
    // Its v1 field tags match the published pre-1.1 Company/Estabelecimento tags.
    const legacy = new Root();
    const pkg = legacy.define('ficha.v1');
    pkg.add(new Type('Estabelecimento')
      .add(new Field('cnpj_ordem', 1, 'uint32'))
      .add(new Field('cnpj_dv', 2, 'uint32')));
    pkg.add(new Type('Company')
      .add(new Field('cnpj_base', 1, 'uint32'))
      .add(new Field('estabelecimentos', 20, 'Estabelecimento', 'repeated')));
    legacy.resolveAll();

    const wire = ficha.v1.Company.encode({
      cnpj_base: 0,
      cnpj_base_text: '00000000',
      estabelecimentos: [{ cnpj_ordem_text: 'E08G', cnpj_dv: 12 }],
    }).finish();
    const v1Type = legacy.lookupType('ficha.v1.Company');
    const old = v1Type.toObject(v1Type.decode(wire)) as {
      cnpj_base?: number;
      estabelecimentos: Array<{ cnpj_ordem?: number; cnpj_dv?: number }>;
    };
    expect(old.estabelecimentos).toHaveLength(1);
    expect(old.estabelecimentos[0]!.cnpj_ordem).toBeUndefined();

    const legacyCnpj = String(old.cnpj_base ?? 0).padStart(8, '0')
      + String(old.estabelecimentos[0]!.cnpj_ordem ?? 0).padStart(4, '0')
      + String(old.estabelecimentos[0]!.cnpj_dv ?? 0).padStart(2, '0');
    const updated = ficha.v1.Company.decode(wire);
    expect(legacyCnpj).toBe('00000000000012');
    expect(estabelecimentoCnpj(updated, updated.estabelecimentos[0]!))
      .toBe('00000000E08G12');
    // Observed silent corruption proves forward compatibility is NOT guaranteed.
    // This test cannot force out-of-repository old clients to negotiate a version.
  });

  it('proves a mixed v1/1.1 discovery manifest breaks legacy routing before current selection', () => {
    // Extracted routing contract from the v1 manifest parser on main:
    // its shard_by is z.literal('cnpj_base_prefix_2'), not an enum.
    // Model the original routing boundary, NOT a deployed external client.
    const v1ShardSchema = z.object({
      shard_by: z.literal('cnpj_base_prefix_2'),
      shards: z.array(z.object({
        shard: z.string().regex(/^\d{2}$/),
        url: z.string().url(),
        sha1: z.string().regex(/^[0-9a-f]{40}$/),
        size: z.number().int().positive(),
      })).length(100),
    });
    const v1DiscoverySchema = z.object({
      current: z.string().regex(/^\d{4}-\d{2}$/),
      snapshots: z.array(z.object({
        date: z.string().regex(/^\d{4}-\d{2}$/),
        files: z.object({ companies: v1ShardSchema.optional() }),
      })),
    });
    const legacy = {
      ...snapshot('2026-07'),
      files: {
        ...snapshot('2026-07').files,
        companies: {
          shard_by: 'cnpj_base_prefix_2' as const,
          shards: shards().map((s) => ({
            ...s,
            url: s.url.replace('/ficha-2026-08/', '/ficha-2026-07/'),
          })),
        },
      },
    };
    const alpha = snapshot11();
    // A single mixed index is valid to the UPDATED parser but invalid to v1,
    // even though current still points to the old 2026-07 snapshot.
    const mixed = { current: '2026-07', snapshots: [legacy, alpha] };
    expect(ManifestSchema.safeParse(mixed).success).toBe(true);
    expect(v1DiscoverySchema.safeParse(mixed).success).toBe(false);

    // Explicit discovery separation is a *candidate contract*, not implemented
    // production routing: a v1 index must not list v1.1 shard URLs.
    const v1Only = { current: '2026-07', snapshots: [legacy] };
    const v11Only = { current: '2026-08', snapshots: [alpha] };
    expect(v1DiscoverySchema.safeParse(v1Only).success).toBe(true);
    expect(ManifestSchema.safeParse(v11Only).success).toBe(true);
    const oldUrls = legacy.files.companies.shards.map((s) => s.url);
    const newUrls = alpha.files.companies.shards.map((s) => s.url);
    expect(oldUrls).toHaveLength(100);
    expect(newUrls).toHaveLength(100);
    expect(oldUrls.every((url) => url.includes('/ficha-2026-07/'))).toBe(true);
    expect(newUrls.every((url) => url.includes('/ficha-2026-08/wire-1.1/'))).toBe(true);
    expect(oldUrls.filter((url) => newUrls.includes(url))).toHaveLength(0);
    // A client that manually fetches v1.1 wire still silently loses letters;
    // separating discovery cannot retroactively change legacy decoders.
  });


  it('rejects a cross-version shard URL collision before any member request', async () => {
    // Candidate opt-in discovery must compare its URLs against the *v1* index;
    // uniqueness within v1.1 alone cannot prevent reuse of legacy wire paths.
    const legacy = {
      ...snapshot('2026-07'),
      files: {
        ...snapshot('2026-07').files,
        companies: {
          shard_by: 'cnpj_base_prefix_2' as const,
          shards: shards().map((entry) => ({
            ...entry,
            url: entry.url.replace('/ficha-2026-08/', '/ficha-2026-07/'),
          })),
        },
      },
    };
    const optIn = snapshot11();
    expect(SnapshotEntrySchema.safeParse(legacy).success).toBe(true);
    expect(SnapshotEntrySchema.safeParse(optIn).success).toBe(true);
    const v1Urls = new Set(legacy.files.companies.shards.map((entry) => entry.url));
    expect(optIn.files.companies.shards.filter((entry) => v1Urls.has(entry.url)))
      .toHaveLength(0);

    // Deliberately reuse ONE legacy archive while retaining 100 distinct
    // v1.1 bucket URLs. The current parser checks only intra-index uniqueness.
    const collision = structuredClone(optIn);
    collision.files.companies.shards[42]!.url = legacy.files.companies.shards[42]!.url;
    expect(new Set(collision.files.companies.shards.map((entry) => entry.url)).size)
      .toBe(100);
    expect(SnapshotEntrySchema.safeParse(collision).success).toBe(true);
    expect(collision.files.companies.shards.filter((entry) => v1Urls.has(entry.url)))
      .toHaveLength(1);

    // Generic schema accepts this invalid cross-version index. The real opt-in
    // reader must reject it before attempting to fetch any ZIP member.
    const validOptIn = { current: optIn.date, snapshots: [optIn] };
    expect(parseAtomicV11Manifest(validOptIn).current).toBe('2026-08');
    const ambiguousIndex = ManifestSchema.parse({
      current: collision.date,
      snapshots: [collision],
    });
    expect(() => parseAtomicV11Manifest(ambiguousIndex))
      .toThrow(/unversioned or noncanonical URL/);
    const fetchImpl = vi.fn(async () => {
      throw new Error('network must not run on invalid opt-in index');
    }) as unknown as typeof fetch;
    await expect(fetchAtomicV11Company('00000000', ambiguousIndex, { fetchImpl }))
      .rejects.toThrow(/unversioned or noncanonical URL/);
    expect(fetchImpl).not.toHaveBeenCalled();
  });


  it('reads the exact Python-generated CA member through the opt-in reader', async () => {
    // This is the *same* member asserted from sharded_pack._pack_rows by the
    // Python P0-D test, not a separately encoded protobufjs approximation.
    const wire = Uint8Array.from(
      packerCaMemberHex.match(/../g)!.map((value) => Number.parseInt(value, 16))
    );
    expect(createHash('sha256').update(wire).digest('hex')).toBe(packerCaMemberSha256);
    const payload = new ArrayBuffer(wire.byteLength);
    new Uint8Array(payload).set(wire);
    const manifest = ManifestSchema.parse({ current: '2026-08', snapshots: [snapshot11()] });
    const fetchImpl = vi.fn(async () => new Response(payload)) as unknown as typeof fetch;
    const company = await fetchAtomicV11Company('CA000001', manifest, { fetchImpl });

    expect(company).not.toBeNull();
    expect(companyRoot(company!)).toBe('CA000001');
    expect(estabelecimentoCnpj(company!, company!.estabelecimentos![0]!))
      .toBe('CA000001E08G12');
    expect(socioCnpj(company!.socios![0]!)).toBe('00000000E08G12');
    expect(fetchImpl).toHaveBeenCalledOnce();
    const requested = String((fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]);
    expect(requested).toBe(
      'https://archive.org/download/ficha-2026-08/wire-1.1/companies-42.zip/CA/000/001.pb'
    );

    // Even when the genuine bytes are available, a legacy URL must be
    // rejected *before* a fetch. This is routing isolation, not byte integrity.
    const collision = structuredClone(manifest);
    collision.snapshots[0]!.files.companies!.shards[42]!.url =
      'https://archive.org/download/ficha-2026-08/companies-42.zip';
    const forbidden = vi.fn(async () => new Response(payload)) as unknown as typeof fetch;
    await expect(fetchAtomicV11Company('CA000001', collision, { fetchImpl: forbidden }))
      .rejects.toThrow(/unversioned or noncanonical URL/);
    expect(forbidden).not.toHaveBeenCalled();
  });
});
