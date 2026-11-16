import { readFileSync } from 'node:fs';
import { compile } from 'svelte/compiler';
import { render } from 'svelte/server';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./EmpresaFicha.svelte', import.meta.url), 'utf8');

type EmpresaFixture = {
  cnpj: string | number;
  razao_social: string;
  uf: string;
};

const matriz = '00000000000191';
const filialAlfa = '00000000E08G12';

// Render the real Svelte markup and formatCNPJ, with entirely local fixtures.
// For the expanded-list cases, inject the state normally populated by DuckDB;
// this proves display, not the browser event or any live SQL/Parquet round trip.
async function renderFicha(
  empresa: EmpresaFixture,
  siblings: Array<{ cnpj: string; nome_fantasia: string; uf: string }> = []
): Promise<string> {
  let componentSource = source;
  if (siblings.length > 0) {
    const emptyState = 'let filiais = $state<FilialRow[]>([]);';
    const closedState = 'let showFiliais = $state(false);';
    if (!source.includes(emptyState) || !source.includes(closedState)) {
      throw new Error('Fixture expansion anchors changed in EmpresaFicha.svelte');
    }
    componentSource = componentSource
      .replace(emptyState, `let filiais = $state<FilialRow[]>(${JSON.stringify(siblings)});`)
      .replace(closedState, 'let showFiliais = $state(true);');
  }

  const { js } = compile(componentSource, {
    filename: 'EmpresaFicha.svelte',
    generate: 'server',
  });

  // A data: module cannot resolve bare package specifiers. Use the actual
  // installed Svelte server runtime, rather than a mocked renderer.
  const code = js.code.replace(
    /from ['"](svelte(?:\\/[^'"]+)?)['"]/g,
    (_, specifier: string) => `from ${JSON.stringify(import.meta.resolve(specifier))}`
  );
  const moduleUrl = `data:text/javascript;base64,${Buffer.from(code).toString('base64')}`;
  const component = (await import(/* @vite-ignore */ moduleUrl)).default;
  return render(component, { props: { empresa, db: {} } }).body;
}

describe('EmpresaFicha CNPJ presentation (offline)', () => {
  it('shows an existing numeric matriz without changing its identity', async () => {
    const html = await renderFicha({ cnpj: matriz, razao_social: 'MATRIZ', uf: 'DF' });
    expect(html).toContain('00.000.000/0001-91');
    expect(html).toContain('Filiais / Estabelecimentos');
  });

  it('shows an alphanumeric filial, plus its numeric matriz in the expanded list', async () => {
    const html = await renderFicha(
      { cnpj: filialAlfa, razao_social: 'FILIAL', uf: 'GO' },
      [{ cnpj: matriz, nome_fantasia: 'MATRIZ', uf: 'DF' }]
    );
    expect(html).toContain('00.000.000/E08G-12');
    expect(html).toContain('00.000.000/0001-91');
    expect(html).toContain('MATRIZ');
  });

  it('shows the alphanumeric sibling when the numeric matriz is selected', async () => {
    const html = await renderFicha(
      { cnpj: matriz, razao_social: 'MATRIZ', uf: 'DF' },
      [{ cnpj: filialAlfa, nome_fantasia: 'FILIAL ALFA', uf: 'GO' }]
    );
    expect(html).toContain('00.000.000/0001-91');
    expect(html).toContain('00.000.000/E08G-12');
    expect(html).toContain('FILIAL ALFA');
  });

  it('preserves legacy numeric input, including a leading-zero case', async () => {
    const html = await renderFicha({ cnpj: 11222333000181, razao_social: 'LEGADO', uf: 'SP' });
    expect(html).toContain('11.222.333/0001-81');

    const shortHtml = await renderFicha({ cnpj: '1234567890123', razao_social: 'LEGADO', uf: 'SP' });
    expect(shortHtml).toContain('01.234.567/8901-23');
  });

  it('does not invent a padded identity for a malformed alphanumeric string', async () => {
    const html = await renderFicha({ cnpj: '0000000E08G12', razao_social: 'INVÁLIDO', uf: 'GO' });
    expect(html).toContain('0000000E08G12');
    expect(html).not.toContain('00.000.00E/08G1-2');
  });

  it('keeps sibling SQL parameterized by full literal CNPJ and shared base', () => {
    // Static contract guard only. Query execution requires DuckDB fixture/E2E.
    expect(source).toMatch(/WHERE cnpj_base = \\? AND cnpj != \\?/);
    expect(source).toContain('stmt.query(cnpjBase, cnpjFull)');
    expect(source).toContain('const cnpjFull = canonicalCNPJ(empresa.cnpj)');
  });
});
