import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import * as ts from 'typescript';

const source = readFileSync(new URL('./EmpresaFicha.svelte', import.meta.url), 'utf8');

// Exercise real handler bodies with deferred DuckDB mocks. The Svelte browser
// lifecycle is not mounted here; this is an async-ordering regression only.
describe('EmpresaFicha: stale socios/contatos (offline)', () => {
  it.each([
    ['Socios', 'Contatos', 'socios', 'A0000000', 'B0000000'],
    ['Contatos', 'Filiais', 'contatos', 'A0000000E08G12', 'B0000000E08G12'],
  ] as const)('discards an outdated %s query', async (section, next, field, a, b) => {
    const handler = source.split(`async function toggle${section}()`)[1]
      ?.split(`async function toggle${next}()`)[0];
    if (!handler) throw new Error(`Missing toggle${section} in Svelte component`);
    const code = ts.transpileModule(`async function toggle${section}()` + handler, {
      compilerOptions: { target: ts.ScriptTarget.ES2022 },
    }).outputText;
    const pending: Array<{ input: string; resolve: (rows: any) => void }> = [];
    let statementsClosed = 0;
    let connectionsClosed = 0;
    const db = {
      async connect() {
        return {
          async prepare() {
            return {
              query: (input: string) => new Promise((resolve) => pending.push({ input, resolve })),
              async close() { statementsClosed++; },
            };
          },
          async close() { connectionsClosed++; },
        };
      },
    };
    const factory = new Function('db', `
      let activeCNPJ = 'A0000000E08G12', requestEpoch = 1;
      let ${field} = [], ${field}Error = null;
      let loading${section} = false, show${section} = false;
      const isCurrentRequest = (cnpj, requestDb, epoch) =>
        cnpj === activeCNPJ && requestDb === db && epoch === requestEpoch;
      ${code}
      return {
        toggle: toggle${section},
        change() {
          activeCNPJ = 'B0000000E08G12';
          requestEpoch++;
          ${field} = []; ${field}Error = null;
          loading${section} = false; show${section} = false;
        },
        snapshot() { return { rows: ${field}, error: ${field}Error, loading: loading${section} }; },
      };
    `);
    const ui = factory(db) as {
      toggle(): Promise<void>;
      change(): void;
      snapshot(): { rows: Array<{ marker: string }>; error: string | null; loading: boolean };
    };

    const first = ui.toggle();
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    ui.change();
    const second = ui.toggle();
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    expect(pending.map(({ input }) => input)).toEqual([a, b]);

    const rows = (marker: string) => ({
      toArray: () => [{ toJSON: () => ({ marker }) }],
    });
    pending[1]!.resolve(rows('CURRENT'));
    await second;
    pending[0]!.resolve(rows('STALE'));
    await first;
    expect(ui.snapshot()).toEqual({
      rows: [{ marker: 'CURRENT' }], error: null, loading: false,
    });
    expect(statementsClosed).toBe(2);
    expect(connectionsClosed).toBe(2);
  });
});
