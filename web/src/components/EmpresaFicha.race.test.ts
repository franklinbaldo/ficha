import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import * as ts from 'typescript';

const source = readFileSync(new URL('./EmpresaFicha.svelte', import.meta.url), 'utf8');

// Exercise real handler bodies with deferred DuckDB mocks. The Svelte browser
// lifecycle is not mounted here; this is an async-ordering regression only.
describe('EmpresaFicha: stale socios/contatos (offline)', () => {
  // A stale success AND a stale failure must leave the current company intact.
  // Changing the DuckDB instance with the same CNPJ is also a new dataset.
  const variants = ([
    ['Socios', 'Contatos', 'socios', 'A0000000', 'B0000000'],
    ['Contatos', 'Filiais', 'contatos', 'A0000000E08G12', 'B0000000E08G12'],
  ] as const).flatMap(([section, next, field, a, b]) =>
    (['success', 'error'] as const).flatMap((staleOutcome) =>
      (['cnpj', 'db'] as const).map((changeMode) =>
        [section, next, field, a, b, staleOutcome, changeMode] as const
      )
    )
  );
  it.each(variants)('ignores obsolete %s query (%s / %s / %s / %s / %s / %s)', async (
    section, next, field, a, b, staleOutcome, changeMode
  ) => {
    const handler = source.split(`async function toggle${section}()`)[1]
      ?.split(`async function toggle${next}()`)[0];
    if (!handler) throw new Error(`Missing toggle${section} in Svelte component`);
    const code = ts.transpileModule(`async function toggle${section}()` + handler, {
      compilerOptions: { target: ts.ScriptTarget.ES2022 },
    }).outputText;
    const pending: Array<{
      input: string; resolve: (rows: any) => void; reject: (error: Error) => void;
    }> = [];
    let statementsClosed = 0;
    let connectionsClosed = 0;
    const makeDb = () => ({
      async connect() {
        return {
          async prepare() {
            return {
              query: (input: string) => new Promise((resolve, reject) =>
                pending.push({ input, resolve, reject })),
              async close() { statementsClosed++; },
            };
          },
          async close() { connectionsClosed++; },
        };
      },
    });
    const factory = new Function('initialDb', `
      let db = initialDb, activeCNPJ = 'A0000000E08G12', requestEpoch = 1;
      let ${field} = [], ${field}Error = null;
      let loading${section} = false, show${section} = false;
      const isCurrentRequest = (cnpj, requestDb, epoch) =>
        cnpj === activeCNPJ && requestDb === db && epoch === requestEpoch;
      ${code}
      return {
        toggle: toggle${section},
        change(nextCNPJ, nextDb) {
          activeCNPJ = nextCNPJ;
          db = nextDb;
          requestEpoch++;
          ${field} = []; ${field}Error = null;
          loading${section} = false; show${section} = false;
        },
        snapshot() { return { rows: ${field}, error: ${field}Error, loading: loading${section} }; },
      };
    `);
    const firstDb = makeDb();
    const secondDb = makeDb();
    const ui = factory(firstDb) as {
      toggle(): Promise<void>;
      change(nextCNPJ: string, nextDb: unknown): void;
      snapshot(): { rows: Array<{ marker: string }>; error: string | null; loading: boolean };
    };

    const first = ui.toggle();
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    ui.change(
      changeMode === 'cnpj' ? 'B0000000E08G12' : 'A0000000E08G12',
      changeMode === 'cnpj' ? firstDb : secondDb,
    );
    const second = ui.toggle();
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    expect(pending.map(({ input }) => input)).toEqual([
      a, changeMode === 'cnpj' ? b : a,
    ]);

    const rows = (marker: string) => ({
      toArray: () => [{ toJSON: () => ({ marker }) }],
    });
    pending[1]!.resolve(rows('CURRENT'));
    await second;
    if (staleOutcome === 'success') {
      pending[0]!.resolve(rows('STALE'));
    } else {
      pending[0]!.reject(new Error('stale company failure'));
    }
    await first;
    expect(ui.snapshot()).toEqual({
      rows: [{ marker: 'CURRENT' }], error: null, loading: false,
    });
    expect(statementsClosed).toBe(2);
    expect(connectionsClosed).toBe(2);
  });
});


// Execute the actual $effect body against mutable state. This checks every
// reset branch, but does not mount Svelte or exercise its browser scheduler.
describe('EmpresaFicha: execution of the context invalidation effect (offline)', () => {
  const body = source.split('$effect(() => {')[1]?.split('\n  });')[0];
  if (!body) throw new Error('Missing context invalidation effect');

  function createHarness() {
    const initialDb = {};
    const otherDb = {};
    const factory = new Function('initialDb', [
      "let activeCNPJ = 'A0000000E08G12', db = initialDb;",
      'let previousCNPJ = null, previousDb = null, requestEpoch = 0;',
      'let socios = [], contatos = [], filiais = [];',
      'let sociosError = null, contatosError = null, filiaisError = null;',
      'let loadingSocios = false, loadingContatos = false, loadingFiliais = false;',
      'let showSocios = false, showContatos = false, showFiliais = false;',
      'const runEffect = () => {', body, '};',
      'return {',
      '  runEffect,',
      '  change(cnpj, newDb) { activeCNPJ = cnpj; db = newDb; runEffect(); },',
      '  seed() {',
      "    socios = [{ marker: 'OLD' }];",
      "    contatos = [{ marker: 'OLD' }];",
      "    filiais = [{ marker: 'OLD' }];",
      "    sociosError = 'old error'; contatosError = 'old error'; filiaisError = 'old error';",
      '    loadingSocios = true; loadingContatos = true; loadingFiliais = true;',
      '    showSocios = true; showContatos = true; showFiliais = true;',
      '  },',
      '  snapshot() {',
      '    return {',
      '      requestEpoch, socios, contatos, filiais,',
      '      sociosError, contatosError, filiaisError,',
      '      loadingSocios, loadingContatos, loadingFiliais,',
      '      showSocios, showContatos, showFiliais,',
      '    };',
      '  },',
      '};',
    ].join('\n'));
    const ui = factory(initialDb) as {
      runEffect(): void;
      change(cnpj: string, db: unknown): void;
      seed(): void;
      snapshot(): Record<string, unknown>;
    };
    return { ui, initialDb, otherDb };
  }

  it('preserves expanded data if neither CNPJ nor DuckDB changed', () => {
    const { ui } = createHarness();
    ui.runEffect();
    expect(ui.snapshot().requestEpoch).toBe(1);
    ui.seed();
    const existing = ui.snapshot();
    ui.runEffect();
    expect(ui.snapshot()).toEqual(existing);
  });

  it.each(['cnpj', 'duckdb'] as const)(
    'clears all panels and advances the epoch when %s changes',
    (dimension) => {
      const { ui, initialDb, otherDb } = createHarness();
      ui.runEffect();
      ui.seed();
      ui.change(
        dimension === 'cnpj' ? 'B0000000E08G12' : 'A0000000E08G12',
        dimension === 'duckdb' ? otherDb : initialDb,
      );
      expect(ui.snapshot()).toEqual({
        requestEpoch: 2,
        socios: [], contatos: [], filiais: [],
        sociosError: null, contatosError: null, filiaisError: null,
        loadingSocios: false, loadingContatos: false, loadingFiliais: false,
        showSocios: false, showContatos: false, showFiliais: false,
      });
    },
  );
});
