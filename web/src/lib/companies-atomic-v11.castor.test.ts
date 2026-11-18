import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { estabelecimentoCnpj } from './companies';
import {
  fetchAtomicV11Company, fetchAtomicV11Manifest, parseAtomicV11Manifest,
} from './companies-atomic-v11';

function fixture() {
  const old = JSON.parse(readFileSync(new URL('../../public/manifest.json', import.meta.url), 'utf8'));
  const july = old.snapshots.find((s: { date: string }) => s.date === '2026-07');
  if (!july?.files?.companies) throw new Error('published July fixture missing');
  const august = structuredClone(july);
  august.date = '2026-08';
  august.files.companies = {
    ...august.files.companies,
    schema_version: '1.1.0',
    shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
    shards: august.files.companies.shards.map((s: { shard: string }) => ({
      ...s,
      url: 'https://archive.org/download/ficha-2026-08/wire-1.1/companies-' + s.shard + '.zip',
    })),
  };
  return { current: '2026-08', snapshots: [august] };
}

describe('Castor P0-A: opt-in Protobuf 1.1 reader (offline)', () => {
  it('fetches only the separate index on explicit invocation', async () => {
    const fetchImpl = vi.fn(async () => Response.json(fixture())) as unknown as typeof fetch;
    const result = await fetchAtomicV11Manifest({ fetchImpl });
    expect(result?.current).toBe('2026-08');
    expect(fetchImpl).toHaveBeenCalledOnce();
    const url = String((fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]);
    expect(url).toMatch(/\/manifest-atomic-v1\.1\.json$/);
    expect(url).not.toMatch(/\/manifest\.json$/);
  });

  it('returns null on unpublished 404 and never falls back to legacy index', async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 404 })) as unknown as typeof fetch;
    expect(await fetchAtomicV11Manifest({ fetchImpl })).toBeNull();
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('rejects non-404 HTTP errors', async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 503 })) as unknown as typeof fetch;
    await expect(fetchAtomicV11Manifest({ fetchImpl })).rejects.toThrow(/HTTP 503/);
  });

  it('rejects mixed v1/v1.1 indices before network I/O', async () => {
    const mixed = fixture();
    mixed.snapshots[0]!.files.companies.shard_by = 'cnpj_base_prefix_2';
    expect(() => parseAtomicV11Manifest(mixed)).toThrow(/invalid opt-in|standalone/);
    const fetchImpl = vi.fn(async () => new Response(null, { status: 200 })) as unknown as typeof fetch;
    await expect(fetchAtomicV11Company('00000000', mixed, { fetchImpl }))
      .rejects.toThrow(/invalid opt-in|standalone/);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it.each([
    ['legacy ZIP', 'https://archive.org/download/ficha-2026-08/companies-00.zip'],
    ['wrong month', 'https://archive.org/download/ficha-2026-07/wire-1.1/companies-00.zip'],
    ['other bucket', 'https://archive.org/download/ficha-2026-08/wire-1.1/companies-01.zip'],
    ['other host', 'https://example.org/ficha-2026-08/wire-1.1/companies-00.zip'],
  ])('rejects %s URL', (_label, url) => {
    const index = fixture();
    index.snapshots[0]!.files.companies.shards[0]!.url = url;
    expect(() => parseAtomicV11Manifest(index)).toThrow(/unversioned|noncanonical|invalid opt-in/);
  });

  it('rejects incomplete buckets, absent current and duplicate snapshot dates', () => {
    const missing = fixture();
    missing.snapshots[0]!.files.companies.shards.pop();
    expect(() => parseAtomicV11Manifest(missing)).toThrow(/invalid opt-in/);
    const absent = fixture();
    absent.current = '2026-09';
    expect(() => parseAtomicV11Manifest(absent)).toThrow(/current snapshot is missing/);
    const repeated = fixture();
    repeated.snapshots.push(structuredClone(repeated.snapshots[0]!));
    expect(() => parseAtomicV11Manifest(repeated)).toThrow(/duplicate snapshot/);
  });

  it('reads the real alphanumeric establishment from a versioned ZIP URL', async () => {
    const index = parseAtomicV11Manifest(fixture());
    // Python packer bytes: CNPJ 00000000E08G12, fixture only, no IA access.
    const bytes = Uint8Array.from('a20108100c2a0445303847'.match(/../g)!.map(x => parseInt(x, 16)));
    const payload = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(payload).set(bytes);
    const fetchImpl = vi.fn(async () => new Response(payload)) as unknown as typeof fetch;
    const company = await fetchAtomicV11Company('00000000', index, { fetchImpl });
    expect(company).not.toBeNull();
    expect(estabelecimentoCnpj(company!, company!.estabelecimentos![0]!)).toBe('00000000E08G12');
    const url = String((fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]);
    expect(url).toBe(
      'https://archive.org/download/ficha-2026-08/wire-1.1/companies-00.zip/00/000/000.pb'
    );
  });

  it('refuses unknown month without network I/O', async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 200 })) as unknown as typeof fetch;
    await expect(fetchAtomicV11Company('00000000', parseAtomicV11Manifest(fixture()),
      { month: '2026-09', fetchImpl })).rejects.toThrow(/snapshot 2026-09 not found/);
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
