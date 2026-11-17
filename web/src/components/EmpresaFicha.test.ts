import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
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
// Synthetic identities: a textual root must not collide with a numeric root.
const matrizRaizAlfa = 'A0000000000191';
const filialRaizAlfa = 'A0000000E08G12';

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
    /from ['"](svelte(?:\/[^'"]+)?)['"]/g,
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
    expect(source).toMatch(/WHERE cnpj_base = \? AND cnpj != \?/);
    expect(source).toContain('stmt.query(cnpjBase, cnpjFull)');
    expect(source).toContain('const cnpjFull = canonicalCNPJ(empresa.cnpj)');
  });
});


describe('P0-E: SQL real para matriz e filial (DuckDB-WASM offline)', () => {
  it('consulta a raiz com parâmetros e mostra os irmãos nos dois sentidos', async () => {
    // Use the SQL actually written in EmpresaFicha, not a rewritten test query.
    const filiaisSource = source.split('async function toggleFiliais()')[1]
      ?.split('// Legacy numeric CNPJs')[0];
    const sql = filiaisSource?.match(/conn\.prepare\(\x60([\s\S]*?)\x60\)/)?.[1];
    expect(sql).toBeTruthy();

    const require = createRequire(import.meta.url);
    const duckdb = require('@duckdb/duckdb-wasm/blocking') as
      typeof import('@duckdb/duckdb-wasm/blocking');
    const db = await duckdb.createDuckDB(
      {
        mvp: {
          mainModule: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm'),
          mainWorker: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-node-mvp.worker.cjs'),
        },
      },
      new duckdb.VoidLogger(),
      duckdb.NODE_RUNTIME,
    );
    await db.instantiate();
    db.open({ path: ':memory:' });
    const conn = db.connect();

    try {
      conn.query(
        'CREATE TABLE cnpjs (' +
          'cnpj VARCHAR, cnpj_base VARCHAR, razao_social VARCHAR, ' +
          'nome_fantasia VARCHAR, uf VARCHAR, municipio_nome VARCHAR, ' +
          'situacao_cadastral VARCHAR)'
      );
      conn.query(
        "INSERT INTO cnpjs VALUES " +
          "('00000000000191', '00000000', 'MATRIZ', 'MATRIZ', 'DF', 'BRASILIA', 'ATIVA')," +
          "('00000000E08G12', '00000000', 'FILIAL', 'FILIAL ALFA', 'GO', 'GOIANIA', 'ATIVA')," +
          "('00000001000191', '00000001', 'OUTRA', 'OUTRA', 'RJ', 'RIO', 'ATIVA')," +
          "('A0000000000191', 'A0000000', 'MATRIZ ALFA', 'MATRIZ ALFA', 'SP', 'SAO PAULO', 'ATIVA')," +
          "('A0000000E08G12', 'A0000000', 'FILIAL ALFA', 'FILIAL ALFA', 'SP', 'CAMPINAS', 'ATIVA')," +
          "('B0000000E08G12', 'B0000000', 'OUTRA RAIZ ALFA', 'OUTRA', 'RJ', 'RIO', 'ATIVA')"
      );

      const consultar = (selecionado: string) => {
        const stmt = conn.prepare(sql!);
        try {
          return stmt.query(selecionado.slice(0, 8), selecionado)
            .toArray()
            .map((row) => row.toJSON() as {
              cnpj: string; nome_fantasia: string; uf: string;
            });
        } finally {
          stmt.close();
        }
      };

      const daFilial = consultar(filialAlfa);
      expect(daFilial.map((r) => r.cnpj)).toEqual([matriz]);
      const htmlFilial = await renderFicha(
        { cnpj: filialAlfa, razao_social: 'FILIAL', uf: 'GO' }, daFilial
      );
      expect(htmlFilial).toContain('00.000.000/E08G-12');
      expect(htmlFilial).toContain('00.000.000/0001-91');

      const daMatriz = consultar(matriz);
      expect(daMatriz.map((r) => r.cnpj)).toEqual([filialAlfa]);
      const htmlMatriz = await renderFicha(
        { cnpj: matriz, razao_social: 'MATRIZ', uf: 'DF' }, daMatriz
      );
      expect(htmlMatriz).toContain('00.000.000/E08G-12');
      expect(htmlMatriz).toContain('FILIAL ALFA');
      expect(htmlMatriz).not.toContain('00000001000191');
      expect(consultar('00000001000191')).toEqual([]);

      // A textual CNPJ root must be compared as text, including the first eight
      // positions. Numeric siblings and another alphabetic root must not leak in.
      const daFilialRaizAlfa = consultar(filialRaizAlfa);
      expect(daFilialRaizAlfa.map((r) => r.cnpj)).toEqual([matrizRaizAlfa]);
      const htmlRaizAlfa = await renderFicha(
        { cnpj: filialRaizAlfa, razao_social: 'FILIAL ALFA', uf: 'SP' },
        daFilialRaizAlfa
      );
      expect(htmlRaizAlfa).toContain('A0.000.000/E08G-12');
      expect(htmlRaizAlfa).toContain('A0.000.000/0001-91');
      expect(htmlRaizAlfa).not.toContain('00.000.000/0001-91');
      expect(consultar(matrizRaizAlfa).map((r) => r.cnpj)).toEqual([filialRaizAlfa]);
      expect(consultar('B0000000E08G12')).toEqual([]);
    } finally {
      conn.close();
    }
  });
});
