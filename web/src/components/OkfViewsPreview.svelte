<script lang="ts">
  import type * as duckdb from '@duckdb/duckdb-wasm';
  import { okfConvenienceViews, previewOkfConvenienceView } from '../lib/okf-views';

  let { db }: { db: duckdb.AsyncDuckDB } = $props();

  let active = $state<string | null>(null);
  let columns = $state<string[]>([]);
  let rows = $state<Record<string, unknown>[]>([]);
  let error = $state<string | null>(null);

  function printable(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  async function preview(name: string) {
    active = name;
    columns = [];
    rows = [];
    error = null;
    try {
      const table = await previewOkfConvenienceView(db, name, 5);
      columns = table.schema.fields.slice(0, 5).map((field) => field.name);
      rows = table.toArray().map((row) =>
        Object.fromEntries(columns.map((column) => [column, row[column]]))
      );
    } catch (e) {
      error = (e as Error).message;
    }
  }
</script>

<section class="okf-views" aria-labelledby="okf-views-title">
  <div class="heading">
    <div>
      <p class="eyebrow">Modelo OKF</p>
      <h2 id="okf-views-title">Views declaradas</h2>
    </div>
    <p>Queries de conveniência versionadas junto ao modelo relacional e executadas aqui pelo DuckDB-WASM.</p>
  </div>

  <div class="view-list">
    {#each okfConvenienceViews as view}
      <article>
        <div>
          <code>{view.name}</code>
          <p>{view.purpose}</p>
          <small>{view.inputs.join(' + ')} → {view.output}</small>
        </div>
        <button type="button" onclick={() => preview(view.name)} disabled={active === view.name && rows.length === 0 && !error}>
          {active === view.name && rows.length === 0 && !error ? 'Consultando…' : 'Ver 5 linhas'}
        </button>
      </article>
    {/each}
  </div>

  {#if error}
    <p class="view-error" role="alert">Não foi possível executar a view: {error}</p>
  {/if}

  {#if rows.length > 0}
    <div class="preview-table">
      <table>
        <thead>
          <tr>{#each columns as column}<th>{column}</th>{/each}</tr>
        </thead>
        <tbody>
          {#each rows as row}
            <tr>{#each columns as column}<td>{printable(row[column])}</td>{/each}</tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</section>

<style>
  .okf-views {
    margin: 2.5rem 0;
    padding-top: 1.25rem;
    border-top: 1px solid #d1d5db;
    text-align: left;
  }

  .heading {
    display: grid;
    grid-template-columns: minmax(180px, 0.7fr) minmax(260px, 1.3fr);
    gap: 1rem 2rem;
    align-items: end;
  }

  .heading h2,
  .heading p { margin: 0; }
  .heading > p { color: #6b7280; line-height: 1.5; }
  .eyebrow { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.12em; color: #6b7280; }

  .view-list { margin-top: 1rem; border-bottom: 1px solid #d1d5db; }
  article {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: center;
    padding: 0.9rem 0;
    border-top: 1px solid #d1d5db;
  }
  article p { margin: 0.25rem 0; color: #374151; }
  article small { color: #6b7280; }
  code { font-weight: 700; }
  button { min-width: auto; padding: 0.5rem 0.75rem; }

  .preview-table { overflow-x: auto; margin-top: 1rem; }
  table { width: 100%; border-collapse: collapse; font-size: 0.78rem; background: white; }
  th, td { padding: 0.45rem 0.6rem; border-bottom: 1px solid #e5e7eb; text-align: left; white-space: nowrap; }
  th { font-weight: 600; }
  .view-error { color: #b91c1c; }

  @media (max-width: 700px) {
    .heading { grid-template-columns: 1fr; }
    article { align-items: flex-start; flex-direction: column; }
  }
</style>
