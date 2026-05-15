import { describe, expect, it, vi } from 'vitest';
import { ficha } from '../generated/company.pb.js';
import { cnpjpath, companiesZipUrl, fetchCompany } from './companies';

describe('cnpjpath', () => {
  it('zero-pads to 8 digits and slices to XX/XXX/XXX.pb', () => {
    expect(cnpjpath(12345678)).toBe('12/345/678.pb');
    expect(cnpjpath(1)).toBe('00/000/001.pb');
    expect(cnpjpath('99999999')).toBe('99/999/999.pb');
  });
});

describe('companiesZipUrl', () => {
  it('strips trailing slashes and composes the IA path', () => {
    expect(companiesZipUrl('https://archive.org/download', 'ficha-2026-04')).toBe(
      'https://archive.org/download/ficha-2026-04/companies.zip'
    );
    expect(companiesZipUrl('https://archive.org/download/', 'ficha-poc-companies-2026-04')).toBe(
      'https://archive.org/download/ficha-poc-companies-2026-04/companies.zip'
    );
  });
});

function encodeFixture(cnpjBase: number, razao: string): Uint8Array {
  const msg = ficha.v1.Company.create({
    cnpj_base: cnpjBase,
    razao_social: razao,
    estabelecimentos: [
      ficha.v1.Estabelecimento.create({
        cnpj_ordem: 1,
        cnpj_dv: 23,
        tipo: ficha.v1.TipoEstabelecimento.MATRIZ,
        nome_fantasia: 'FANTASIA',
        uf: 'AC',
      }),
    ],
  });
  return ficha.v1.Company.encode(msg).finish();
}

function bytesResponse(bytes: Uint8Array, status = 200): Response {
  // Hand Response an ArrayBuffer (a BodyInit alias) — TS's strict
  // lib.dom typing rejects bare Uint8Array<ArrayBufferLike> as BlobPart
  // even though it works at runtime. Copy the exact byte range into a
  // fresh ArrayBuffer (the source Uint8Array may be a view over a larger
  // backing buffer, so `.buffer` alone would include trailing zeros).
  const ab = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(ab).set(bytes);
  return new Response(ab, { status });
}

// vi.fn typed as `typeof fetch` — protobufjs-style helper takes
// `fetchImpl?: typeof fetch`, which doesn't match `vi.fn`'s inferred type.
type FetchLike = typeof fetch;

describe('fetchCompany', () => {
  it('decodes a 200 response into a Company', async () => {
    const bytes = encodeFixture(12345678, 'EMPRESA TESTE');
    const fetchImpl = vi.fn(async () => bytesResponse(bytes)) as unknown as FetchLike;
    const c = await fetchCompany(12345678, {
      identifier: 'ficha-poc-companies-2026-04',
      fetchImpl,
    });
    expect(c).not.toBeNull();
    expect(c?.cnpj_base).toBe(12345678);
    expect(c?.razao_social).toBe('EMPRESA TESTE');
    expect(c?.estabelecimentos).toHaveLength(1);
    const mock = fetchImpl as unknown as ReturnType<typeof vi.fn>;
    const firstCall = mock.mock.calls[0] ?? [];
    expect(String(firstCall[0])).toContain(
      '/ficha-poc-companies-2026-04/companies.zip/12/345/678.pb'
    );
  });

  it('returns null on 404', async () => {
    const fetchImpl = vi.fn(
      async () => new Response('', { status: 404 })
    ) as unknown as FetchLike;
    const c = await fetchCompany(99999999, {
      identifier: 'ficha-poc-companies-2026-04',
      fetchImpl,
    });
    expect(c).toBeNull();
  });

  it('throws on other non-2xx', async () => {
    const fetchImpl = vi.fn(
      async () => new Response('boom', { status: 503 })
    ) as unknown as FetchLike;
    await expect(
      fetchCompany(1, { identifier: 'x', fetchImpl })
    ).rejects.toThrow(/HTTP 503/);
  });

  it('coerces uint64 cnpj_socio to a plain number', async () => {
    // 14-digit CNPJ — within Number.MAX_SAFE_INTEGER (2^53-1 ≈ 9e15).
    const cnpjSocio = 12345678000123;
    const msg = ficha.v1.Company.create({
      cnpj_base: 1,
      socios: [
        ficha.v1.Socio.create({
          tipo: ficha.v1.TipoSocio.PESSOA_JURIDICA,
          cnpj_socio: cnpjSocio,
        }),
      ],
    });
    const bytes = ficha.v1.Company.encode(msg).finish();
    const fetchImpl = vi.fn(async () => bytesResponse(bytes)) as unknown as FetchLike;
    const c = await fetchCompany(1, { identifier: 'x', fetchImpl });
    const socio = c?.socios?.[0];
    expect(socio).toBeDefined();
    // Runtime type must match the declared `number` typing — not a Long
    // object — otherwise comparisons and JSON shaping break for callers.
    expect(typeof socio!.cnpj_socio).toBe('number');
    expect(socio!.cnpj_socio).toBe(cnpjSocio);
  });
});

