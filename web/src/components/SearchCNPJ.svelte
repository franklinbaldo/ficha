<script lang="ts">
  import { onMount } from 'svelte';
  import type * as duckdb from '@duckdb/duckdb-wasm';
  import { strip as stripCNPJ } from '../lib/cnpj';
  import { fetchManifest, currentSnapshot } from '../lib/manifest';
  import { createDuckDB, attachCnpjs } from '../lib/analytical';
  import EmpresaFicha from './EmpresaFicha.svelte';

  type EmpresaRow = {
    cnpj: string;
    razao_social: string | null;
    nome_fantasia: string | null;
    uf: string | null;
    cnae_principal_codigo: string | null;
    cnae_principal_descricao: string | null;
    municipio_nome: string | null;
    capital_social: number | null;
  };

  let cnpj = $state('');
  let results = $state<EmpresaRow[]>([]);
  let db = $state<duckdb.AsyncDuckDB | null>(null);
  let loading = $state(false);
  let snapshotDate = $state<string | null>(null);
  let status = $state('Inicializando…');

  async function init() {
    try {
      status = 'Buscando manifest…';
      const manifest = await fetchManifest();
      if (!manifest) {
        status = 'Aguardando primeira publicação. Volte em breve.';
        return;
      }
      const snap = currentSnapshot(manifest);
      if (!snap) {
        status = `Manifest inválido — current=${manifest.current} sem snapshot correspondente.`;
        return;
      }
      snapshotDate = snap.date;

      status = 'Carregando DuckDB-WASM…';
      const duckDB = await createDuckDB();

      status = `Anexando snapshot ${snap.date}…`;
      await attachCnpjs(duckDB, snap.files.cnpjs.url);

      db = duckDB;
      status = `Pronto para consultas — snapshot ${snap.date}`;
    } catch (e) {
      console.error('init error:', e);
      status = 'Erro: ' + (e as Error).message;
    }
  }

  onMount(() => {
    init();
  });

  async function search() {
    if (!db || !cnpj.trim()) return;
    loading = true;
    const clean = stripCNPJ(cnpj);

    try {
      const conn = await db.connect();
      let res;

      if (clean.length === 14) {
        const stmt = await conn.prepare(`
          SELECT
            cnpj,
            razao_social,
            nome_fantasia,
            uf,
            cnae_principal_codigo,
            cnae_principal_descricao,
            municipio_nome,
            capital_social
          FROM cnpjs
          WHERE cnpj = ?
          LIMIT 1
        `);
        res = await stmt.query(clean);
        await stmt.close();
      } else {
        const stmt = await conn.prepare(`
          SELECT
            cnpj,
            razao_social,
            nome_fantasia,
            uf,
            cnae_principal_codigo,
            cnae_principal_descricao,
            municipio_nome,
            capital_social
          FROM cnpjs
          WHERE razao_social ILIKE ?
          LIMIT 20
        `);
        res = await stmt.query(`%${cnpj.trim()}%`);
        await stmt.close();
      }

      results = res.toArray().map((r) => r.toJSON() as EmpresaRow);
      await conn.close();
    } catch (e) {
      console.error('Erro na busca:', e);
    } finally {
      loading = false;
    }
  }
</script>

<div class="container">
  <div class="search-box">
    <div class="input-group">
      <input
        type="text"
        bind:value={cnpj}
        placeholder="CNPJ ou Razão Social..."
        onkeydown={(e) => e.key === 'Enter' && search()}
      />
      <button onclick={search} disabled={loading || !db}>
        {#if loading}
          <span class="spinner"></span>
        {:else}
          Buscar
        {/if}
      </button>
    </div>
    <p class="status {status.startsWith('Erro') ? 'error' : ''}">
      {status}
    </p>
  </div>

  {#if results.length > 0}
    <div class="results-list">
      {#each results as empresa}
        <EmpresaFicha {empresa} />
      {/each}
    </div>
  {:else if !loading && cnpj && db}
    <div class="no-results">
      Nenhum dado encontrado para "{cnpj}".
    </div>
  {/if}
</div>

<style>
  .container {
    max-width: 900px;
    margin: 2rem auto;
    padding: 0 1rem;
    font-family: system-ui, -apple-system, sans-serif;
  }

  .search-box {
    margin-bottom: 2rem;
    text-align: center;
  }

  .input-group {
    display: flex;
    gap: 0.5rem;
    background: white;
    padding: 0.5rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    border: 1px solid #e5e7eb;
  }

  input {
    flex: 1;
    padding: 0.75rem 1rem;
    border: none;
    font-size: 1.125rem;
    outline: none;
    background: transparent;
  }

  button {
    background: #2563eb;
    color: white;
    border: none;
    padding: 0.75rem 2rem;
    border-radius: 8px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    min-width: 120px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  button:hover:not(:disabled) {
    background: #1d4ed8;
    transform: translateY(-1px);
  }

  button:disabled {
    opacity: 0.7;
    cursor: not-allowed;
  }

  .status {
    font-size: 0.875rem;
    margin-top: 0.75rem;
    color: #6b7280;
  }

  .status.error {
    color: #dc2626;
  }

  .results-list {
    display: flex;
    flex-direction: column;
    gap: 1.5rem;
  }

  .no-results {
    text-align: center;
    padding: 3rem;
    background: #f9fafb;
    border-radius: 12px;
    color: #9ca3af;
  }

  .spinner {
    width: 20px;
    height: 20px;
    border: 3px solid rgba(255, 255, 255, 0.3);
    border-radius: 50%;
    border-top-color: white;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>
