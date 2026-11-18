import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync, symlinkSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = resolve('scripts/check-manifest-byte-identity.mjs');
const tempDirs: string[] = [];
const current = () => ({
  current: '2026-07',
  snapshots: [{
    date: '2026-07',
    files: { cnpjs: {
      url: 'https://archive.org/download/ficha-2026-07/cnpjs.parquet',
      sha256: 'a'.repeat(64), size: 123,
    } },
  }],
});

function fixture(source: string, built = source) {
  const dir = mkdtempSync(join(tmpdir(), 'ficha-lagarto-'));
  tempDirs.push(dir);
  const original = join(dir, 'source.json');
  const output = join(dir, 'built.json');
  writeFileSync(original, source);
  writeFileSync(output, built);
  return { original, output };
}

function run(original: string, output: string) {
  return spawnSync(process.execPath, [cli, original, output], {
    encoding: 'utf8', cwd: resolve('.'),
  });
}

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { force: true, recursive: true });
});

describe('manifesto de dados preservado no build (sem rede)', () => {
  it('aceita a fonte real web/public/manifest.json sem alterar bytes', () => {
    const source = readFileSync(resolve('public/manifest.json'), 'utf8');
    const { original, output } = fixture(source);
    expect(run(original, output).status).toBe(0);
  });

  it('aceita bytes idênticos com um current válido e identidade de cnpjs', () => {
    const { original, output } = fixture(JSON.stringify(current()));
    const result = run(original, output);
    expect(result.status).toBe(0);
    expect(result.stdout).toContain('current=2026-07');
  });

  it('rejeita substituição pelo PWA do SSG', () => {
    const { original, output } = fixture(JSON.stringify(current()), '{"name":"FICHA","display":"standalone"}');
    expect(run(original, output).status).not.toBe(0);
  });

  it('rejeita alteração de bytes apesar de JSON semanticamente igual', () => {
    const { original, output } = fixture(JSON.stringify(current()), JSON.stringify(current(), null, 2));
    expect(run(original, output).stderr).toContain('difere byte a byte');
  });

  it('rejeita current ausente ou que não corresponde a snapshot', () => {
    const invalid = { ...current(), current: '2026-08' };
    const { original, output } = fixture(JSON.stringify(invalid));
    expect(run(original, output).status).not.toBe(0);
  });

  it('rejeita competência duplicada mesmo se o JSON do build for igual', () => {
    const invalid = current();
    invalid.snapshots.push(invalid.snapshots[0]!);
    const { original, output } = fixture(JSON.stringify(invalid));
    expect(run(original, output).stderr).toContain('duplicada');
  });

  it('rejeita objeto PWA já na própria fonte', () => {
    const pwa = '{"background_color":"#fff","display":"standalone","name":"FICHA"}';
    const { original, output } = fixture(pwa);
    expect(run(original, output).status).not.toBe(0);
  });

  it('rejeita arquivo de destino ausente', () => {
    const { original, output } = fixture(JSON.stringify(current()));
    rmSync(output);
    expect(run(original, output).status).not.toBe(0);
  });

  it('rejeita symlink no destino antes de qualquer leitura', () => {
    const { original, output } = fixture(JSON.stringify(current()));
    rmSync(output);
    symlinkSync(original, output);
    expect(run(original, output).stderr).toContain('arquivo regular obrigatório');
  });
});
