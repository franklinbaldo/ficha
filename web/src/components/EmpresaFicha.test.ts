import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { compile } from 'svelte/compiler';
import { render } from 'svelte/server';
import { describe, expect, it, vi } from 'vitest';
import * as ts from 'typescript';

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
  siblings: Array<{ cnpj: string; nome_fantasia: string; uf: string }> = [],
  socios: Array<Record<string, unknown>> = []
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

  if (socios.length > 0) {
    const emptyState = 'let socios = $state<SocioRow[]>([]);';
    const closedState = 'let showSocios = $state(false);';
    if (!source.includes(emptyState) || !source.includes(closedState)) {
      throw new Error('Socios fixture anchors changed in EmpresaFicha.svelte');
    }
    componentSource = componentSource
      .replace(emptyState, `let socios = $state<SocioRow[]>(${JSON.stringify(socios)});`)
      .replace(closedState, 'let showSocios = $state(true);');
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

  it('formats lowercase alphanumeric identities as uppercase without losing a letter', async () => {
    const html = await renderFicha({
      cnpj: 'a0000000e08g12', razao_social: 'FILIAL ALFA', uf: 'GO'
    });
    expect(html).toContain('A0.000.000/E08G-12');
    expect(html).not.toContain('A0.000.000/0000-12');
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
    expect(source).toContain('const cnpjFull = requestedCNPJ;');
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

      // Execute the other two SQL statements taken from this Svelte component.
      // A shared alphanumeric root must not be coerced to a numeric key; contact
      // lookup must additionally isolate the complete 14-character identity.
      const sqlFor = (fn: string, next: string) => {
        const block = source.split(`async function ${fn}()`)[1]
          ?.split(`async function ${next}()`)[0];
        const statement = block?.match(/conn\.prepare\(\x60([\s\S]*?)\x60\)/)?.[1];
        if (!statement) throw new Error(`SQL not found for ${fn}`);
        return statement;
      };

      conn.query(
        'CREATE TABLE socios (' +
          'cnpj_base VARCHAR, nome_socio_razao_social VARCHAR, ' +
          'tipo_descricao VARCHAR, qualificacao_descricao VARCHAR, ' +
          'data_entrada_sociedade VARCHAR, cpf_mascarado VARCHAR, ' +
          'cnpj_socio VARCHAR, pais_nome VARCHAR, faixa_etaria VARCHAR, ' +
          'representante_legal_nome VARCHAR, ' +
          'representante_legal_qualificacao_descricao VARCHAR)'
      );
      conn.query(
        'INSERT INTO socios (cnpj_base, nome_socio_razao_social, tipo_descricao, ' +
          'qualificacao_descricao, cnpj_socio) VALUES ' +
          "('A0000000', 'SOCIA ALFA', 'JURIDICA', 'TITULAR', 'A0000000E08G12')," +
          "('00000000', 'SOCIA NUMERICA', 'JURIDICA', 'TITULAR', '00000000000191')," +
          "('B0000000', 'OUTRA RAIZ', 'JURIDICA', 'TITULAR', 'B0000000E08G12')"
      );
      const sociosStmt = conn.prepare(sqlFor('toggleSocios', 'toggleContatos'));
      try {
        const sociosAlfa = sociosStmt.query(matrizRaizAlfa.slice(0, 8))
          .toArray().map((row) => row.toJSON());
        expect(sociosAlfa.map((row) => row.nome_socio_razao_social)).toEqual(['SOCIA ALFA']);
        expect(sociosAlfa[0]?.cnpj_socio).toBe(filialRaizAlfa);
        const htmlSocios = await renderFicha(
          { cnpj: matrizRaizAlfa, razao_social: 'MATRIZ ALFA', uf: 'SP' },
          [], sociosAlfa
        );
        expect(htmlSocios).toContain('A0.000.000/E08G-12');
        expect(sociosStmt.query(matriz.slice(0, 8)).toArray()
          .map((row) => row.toJSON().nome_socio_razao_social)).toEqual(['SOCIA NUMERICA']);
      } finally {
        sociosStmt.close();
      }

      conn.query(
        'CREATE TABLE cnpj_contatos (cnpj VARCHAR, tipo VARCHAR, valor VARCHAR, posicao INTEGER)'
      );
      conn.query(
        "INSERT INTO cnpj_contatos VALUES " +
          "('A0000000E08G12', 'email', 'filial@exemplo.invalid', 1)," +
          "('A0000000000191', 'email', 'matriz@exemplo.invalid', 1)," +
          "('00000000E08G12', 'email', 'numerica@exemplo.invalid', 1)"
      );
      const contatosStmt = conn.prepare(sqlFor('toggleContatos', 'toggleFiliais'));
      try {
        const contatos = contatosStmt.query(filialRaizAlfa).toArray()
          .map((row) => row.toJSON().valor);
        expect(contatos).toEqual(['filial@exemplo.invalid']);
        expect(contatosStmt.query(matrizRaizAlfa).toArray()
          .map((row) => row.toJSON().valor)).toEqual(['matriz@exemplo.invalid']);
        expect(contatosStmt.query(filialAlfa).toArray()
          .map((row) => row.toJSON().valor)).toEqual(['numerica@exemplo.invalid']);
      } finally {
        contatosStmt.close();
      }
    } finally {
      conn.close();
    }
  });
});

describe('EmpresaFicha request invalidation (static source contract, no browser)', () => {
  it('resets all cached detail panels when the selected CNPJ or DuckDB changes', () => {
    const watcher = source.split('$effect(() => {')[1]?.split('\n  });')[0];
    expect(watcher).toContain('const nextCNPJ = activeCNPJ');
    expect(watcher).toContain('const nextDb = db');
    expect(watcher).toContain('requestEpoch += 1');
    for (const section of ['Socios', 'Contatos', 'Filiais']) {
      const field = section.toLowerCase();
      expect(watcher).toContain(`${field} = [];`);
      expect(watcher).toContain(`show${section} = false;`);
      expect(watcher).toContain(`loading${section} = false;`);
      expect(watcher).toContain(`${field}Error = null;`);
    }
  });

  it('guards async success/error/finally against stale identity and releases resources', () => {
    expect(source).toContain('cnpj === activeCNPJ && requestDb === db && epoch === requestEpoch');
    for (const [fn, next] of [
      ['toggleSocios', 'toggleContatos'],
      ['toggleContatos', 'toggleFiliais'],
      ['toggleFiliais', 'Legacy numeric CNPJs'],
    ] as const) {
      const body = source.split(`async function ${fn}()`)[1]?.split(next)[0];
      expect(body, fn).toBeTruthy();
      expect(body).toContain('const requestedCNPJ = activeCNPJ');
      expect(body).toContain('const epoch = requestEpoch');
      expect(body?.match(/isCurrentRequest\(requestedCNPJ, requestDb, epoch\)/g)?.length).toBe(3);
      expect(body).toMatch(/finally\s*\{\s*await stmt\.close\(\)/);
      expect(body).toMatch(/finally\s*\{\s*await conn\.close\(\)/);
      expect(body).toContain('|| loading');
    }
  });
});
// Source-backed handler with deferred DuckDB stand-in: exercises overlapping
// awaits and cleanup, but does not claim browser hydration or real Parquet.
describe('EmpresaFicha: async filial handler (offline)', () => {
  const tail = source.split('async function toggleFiliais()')[1]
    ?.split('// Legacy numeric CNPJs')[0];
  if (!tail) throw new Error('toggleFiliais source anchor disappeared');
  const executable = ts.transpileModule('async function toggleFiliais()' + tail, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.None },
  }).outputText;
  type FilialState = {
    toggleFiliais(): Promise<void>;
    change(cnpj: string): void;
    snapshot(): { filiais: Array<{ cnpj: string }>; filiaisError: string | null; loadingFiliais: boolean };
  };
  function harness(database: unknown): FilialState {
    // Execute the actual handler, replacing Svelte rune state with local vars.
    const build = new Function('database', [
      "let db = database, activeCNPJ = 'A0000000E08G12', requestEpoch = 1;",
      'let filiais = [], filiaisError = null, loadingFiliais = false, showFiliais = false;',
      'function isCurrentRequest(cnpj, requestDb, epoch) {',
      'return cnpj === activeCNPJ && requestDb === db && epoch === requestEpoch; }',
      executable,
      'return { toggleFiliais,',
      'change(cnpj) { activeCNPJ = cnpj; requestEpoch++; filiais = [];',
      'filiaisError = null; loadingFiliais = false; showFiliais = false; },',
      'snapshot() { return { filiais, filiaisError, loadingFiliais }; } };',
    ].join('\n'));
    return build(database) as FilialState;
  }

  it('discards a delayed result for the previous company, even if it finishes last', async () => {
    const pending: Array<{ full: string; resolve: (result: any) => void }> = [];
    let closedStatements = 0;
    let closedConnections = 0;
    const db = {
      async connect() {
        return {
          async prepare() {
            return {
              async query(_base: string, full: string) {
                return new Promise((resolve) => pending.push({ full, resolve }));
              },
              async close() { closedStatements++; },
            };
          },
          async close() { closedConnections++; },
        };
      },
    };
    const ui = harness(db);
    const first = ui.toggleFiliais();
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    ui.change('B0000000E08G12');
    const second = ui.toggleFiliais();
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    expect(pending.map((request) => request.full))
      .toEqual(['A0000000E08G12', 'B0000000E08G12']);
    const rows = (cnpj: string) => ({ toArray: () => [{ toJSON: () => ({ cnpj }) }] });
    pending[1].resolve(rows('B0000000000191'));
    await second;
    pending[0].resolve(rows('A0000000000191'));
    await first;
    expect(ui.snapshot()).toEqual({
      filiais: [{ cnpj: 'B0000000000191' }],
      filiaisError: null,
      loadingFiliais: false,
    });
    expect(closedStatements).toBe(2);
    expect(closedConnections).toBe(2);
  });

  it('closes an opened connection when prepare fails, without caching success', async () => {
    let closedConnections = 0;
    const ui = harness({
      async connect() {
        return {
          async prepare() { throw new Error('synthetic prepare failure'); },
          async close() { closedConnections++; },
        };
      },
    });
    await ui.toggleFiliais();
    expect(ui.snapshot()).toEqual({
      filiais: [],
      filiaisError: 'synthetic prepare failure',
      loadingFiliais: false,
    });
    expect(closedConnections).toBe(1);
  });
});
