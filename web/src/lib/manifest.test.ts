import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchManifest, currentSnapshot } from './manifest';
import type { Manifest } from '../schemas/v1/manifest';

const SAMPLE_SNAPSHOT = {
  date: '2026-05',
  schema_version: '1.0.0',
  rfb_layout_date: null,
  generated_at: '2026-05-27T03:00:00Z',
  generator: 'ficha-etl',
  row_counts: {
    cnpjs: 60_000_000,
    cnpj_contatos: 90_000_000,
    raizes: 30_000_000,
    socios: 50_000_000,
  },
  files: {
    cnpjs: {
      url: 'https://archive.org/download/ficha-2026-05/cnpjs.parquet',
      sha1: '1'.repeat(40),
      sha256: 'a'.repeat(64),
      size: 3_000_000_000,
    },
    cnpj_contatos: {
      url: 'https://archive.org/download/ficha-2026-05/cnpj_contatos.parquet',
      sha1: '2'.repeat(40),
      sha256: 'e'.repeat(64),
      size: 200_000_000,
    },
    raizes: {
      url: 'https://archive.org/download/ficha-2026-05/raizes.parquet',
      sha1: '3'.repeat(40),
      sha256: 'b'.repeat(64),
      size: 150_000_000,
    },
    socios: {
      url: 'https://archive.org/download/ficha-2026-05/socios.parquet',
      sha1: '4'.repeat(40),
      sha256: 'c'.repeat(64),
      size: 500_000_000,
    },
    lookups: {
      url: 'https://archive.org/download/ficha-2026-05/lookups.json',
      sha1: '5'.repeat(40),
      sha256: 'd'.repeat(64),
      size: 50_000,
    },
  },
};

const SAMPLE_MANIFEST = {
  current: '2026-05',
  snapshots: [SAMPLE_SNAPSHOT],
};

const LEGACY_2026_04 = {
  ...SAMPLE_SNAPSHOT,
  date: '2026-04',
  generated_at: '2026-04-27T03:00:00Z',
  files: Object.fromEntries(
    Object.entries(SAMPLE_SNAPSHOT.files).map(([name, entry]) => {
      const { sha1: _sha1, ...legacy } = entry;
      return [name, legacy];
    })
  ),
};

describe('fetchManifest', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('parses current entries with sha1 + sha256', async () => {
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify(SAMPLE_MANIFEST), { status: 200 })
    ) as typeof fetch;
    const m = await fetchManifest();
    expect(m).not.toBeNull();
    expect(m!.current).toBe('2026-05');
    expect(m!.snapshots.length).toBe(1);
    const snapshot = m!.snapshots[0];
    expect(snapshot).toBeDefined();
    expect(snapshot!.files.cnpjs).toMatchObject({
      sha1: '1'.repeat(40),
      sha256: 'a'.repeat(64),
    });
  });

  it('temporarily accepts sha256-only legacy snapshots', async () => {
    const legacyManifest = { current: '2026-04', snapshots: [LEGACY_2026_04] };
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify(legacyManifest), { status: 200 })
    ) as typeof fetch;
    const m = await fetchManifest();
    expect(m?.current).toBe('2026-04');
  });

  it('returns null on 404 (manifest não publicado ainda)', async () => {
    globalThis.fetch = vi.fn(async () => new Response('not found', { status: 404 })) as typeof fetch;
    const m = await fetchManifest();
    expect(m).toBeNull();
  });

  it('throws on 500', async () => {
    globalThis.fetch = vi.fn(async () => new Response('server error', { status: 500 })) as typeof fetch;
    await expect(fetchManifest()).rejects.toThrow(/HTTP 500/);
  });

  it('throws on malformed JSON', async () => {
    globalThis.fetch = vi.fn(async () => new Response('{ not json', { status: 200 })) as typeof fetch;
    await expect(fetchManifest()).rejects.toThrow();
  });

  it('throws when schema validation fails', async () => {
    const broken = { current: '2026-05', snapshots: [{ date: 'oops' }] };
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify(broken), { status: 200 })
    ) as typeof fetch;
    await expect(fetchManifest()).rejects.toThrow(/schema validation/);
  });

  it('rethrows network errors', async () => {
    globalThis.fetch = vi.fn(async () => {
      throw new Error('boom');
    }) as typeof fetch;
    await expect(fetchManifest()).rejects.toThrow(/network error/);
  });
});

describe('currentSnapshot', () => {
  it('returns the snapshot matching `current`', () => {
    const m: Manifest = SAMPLE_MANIFEST as Manifest;
    expect(currentSnapshot(m)?.date).toBe('2026-05');
  });

  it('returns null when current points to a non-existent snapshot', () => {
    const m: Manifest = {
      ...SAMPLE_MANIFEST,
      current: '2099-12',
    } as Manifest;
    expect(currentSnapshot(m)).toBeNull();
  });
});
