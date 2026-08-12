import { describe, expect, it } from 'vitest';
import { SnapshotEntrySchema } from './manifest';

/**
 * Migração de contrato: SHA-256 → SHA-1 (#183 e a padronização posterior).
 *
 * O escritor passou a emitir `sha1`, mas `2026-04` já está publicado só com
 * `sha256`. Se o leitor exigisse `sha1`, o manifesto em produção viraria
 * inválido e o site quebraria — por isso a migração aceita as duas formas, e
 * estes testes fixam essa janela em vez de deixá-la implícita.
 */
describe('FileEntry — migração de checksum', () => {
  const base = (files: Record<string, unknown>) => ({
    date: '2026-05',
    schema_version: '1.0.0',
    rfb_layout_date: null,
    generated_at: '2026-06-01T00:00:00Z',
    generator: 'ficha-etl',
    row_counts: { cnpjs: 1, raizes: 1, socios: 1 },
    files,
  });
  const url = 'https://archive.org/download/ficha-2026-05/x.parquet';
  const sha1 = 'a'.repeat(40);
  const entry = (extra: Record<string, unknown>) => ({ url, size: 1, ...extra });
  const trio = (extra: Record<string, unknown>) => ({
    cnpjs: entry(extra),
    raizes: entry(extra),
    socios: entry(extra),
    lookups: entry(extra),
  });

  it('aceita o contrato novo — só sha1', () => {
    expect(() => SnapshotEntrySchema.parse(base(trio({ sha1 })))).not.toThrow();
  });

  it('aceita o contrato legado — só sha256 (2026-04 continua válido)', () => {
    expect(() =>
      SnapshotEntrySchema.parse(base(trio({ sha256: 'b'.repeat(64) })))
    ).not.toThrow();
  });

  it('aceita os dois durante a transição', () => {
    expect(() =>
      SnapshotEntrySchema.parse(base(trio({ sha1, sha256: 'b'.repeat(64) })))
    ).not.toThrow();
  });

  it('rejeita arquivo sem checksum nenhum — não é verificável', () => {
    expect(() => SnapshotEntrySchema.parse(base(trio({})))).toThrow();
  });

  it('rejeita sha1 que não é hex de 40', () => {
    expect(() => SnapshotEntrySchema.parse(base(trio({ sha1: 'A'.repeat(40) })))).toThrow();
    expect(() => SnapshotEntrySchema.parse(base(trio({ sha1: 'a'.repeat(39) })))).toThrow();
  });

  it('a exigência de checksum vale também nas entradas com sort', () => {
    const files = {
      ...trio({ sha1 }),
      enderecos: { url, size: 1, sort: ['uf'] },
    };
    expect(() => SnapshotEntrySchema.parse(base(files))).toThrow();
  });

  it('entradas com sort continuam aceitando sha1 + sort', () => {
    const files = {
      ...trio({ sha1 }),
      enderecos: { url, size: 1, sha1, sort: ['uf'] },
    };
    expect(() => SnapshotEntrySchema.parse(base(files))).not.toThrow();
  });
});
