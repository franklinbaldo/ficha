import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchManifest, currentSnapshot } from './manifest';
import type { Manifest } from '../schemas/v1/manifest';

const SAMPLE_SNAPSHOT = {
  date: '2026-04',
  schema_version: '1.0.0',
  rfb_layout_date: null,
  generated_at: '2026-04-27T03:00:00Z',
  generator: 'ficha-etl',
  row_counts: { cnpjs: 60_000_000, raizes: 30_000_000, socios: 50_000_000 },
  files: {
    cnpjs: {
      url: 'https://archive.org/download/ficha-2026-04/cnpjs.parquet',
      sha256: 'a'.repeat(64),
      size: 3_000_000_000,
    },
    raizes: {
      url: 'https://archive.org/download/ficha-2026-04/raizes.parquet',
      sha256: 'b'.repeat(64),
      size: 150_000_000,
    },
    socios: {
      url: 'https://archive.org/download/ficha-2026-04/socios.parquet',
      sha256: 'c'.repeat(64),
      size: 500_000_000,
    },
    lookups: {
      url: 'https://archive.org/download/ficha-2026-04/lookups.json',
      sha256: 'd'.repeat(64),
      size: 50_000,
    },
  },
};

const SAMPLE_MANIFEST = {
  current: '2026-04',
  snapshots: [SAMPLE_SNAPSHOT],
};

describe('fetchManifest', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('parses a valid manifest', async () => {
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify(SAMPLE_MANIFEST), { status: 200 })
    ) as typeof fetch;
    const m = await fetchManifest();
    expect(m).not.toBeNull();
    expect(m!.current).toBe('2026-04');
    expect(m!.snapshots.length).toBe(1);
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
    const broken = { current: '2026-04', snapshots: [{ date: 'oops' }] };
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
    expect(currentSnapshot(m)?.date).toBe('2026-04');
  });

  it('returns null when current points to a non-existent snapshot', () => {
    const m: Manifest = {
      ...SAMPLE_MANIFEST,
      current: '2099-12',
    } as Manifest;
    expect(currentSnapshot(m)).toBeNull();
  });
});
