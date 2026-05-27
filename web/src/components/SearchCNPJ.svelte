<script lang="ts">
  import { onMount } from 'svelte';
  import type * as duckdb from '@duckdb/duckdb-wasm';
  import { strip as stripCNPJ } from '../lib/cnpj';
  import { fetchManifest, currentSnapshot } from '../lib/manifest';
  import { createDuckDB, attachCnpjs, attachLookups, attachEnderecos, attachPessoas } from '../lib/analytical';
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

  type PessoaRow = {
    cpf_mascarado: string;
    nome_normalizado: string;
    nome_original: string | null;
    papel: string;
    cnpj_base: string;
    qualificacao_codigo: string | null;
    faixa_etaria: string | null;
  };

  type SearchMode = 'empresa' | 'pessoa';

  let cnpj = $state('');
  let searchMode = $state<SearchMode>('empresa');
  let results = $state<EmpresaRow[]>([]);
  let pessoaResults = $state<PessoaRow[]>([]);
  let hasPessoas = $state(false);
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
      await attachLookups(duckDB, snap);
      if (snap.files.enderecos) await attachEnderecos(duckDB, snap.files.enderecos.url);
      if (snap.files.pessoas) {
        await attachPessoas(duckDB, snap.files.pessoas.url);
        hasPessoas = true;
      }

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

    // Strip LIKE wildcard characters from user input — names don't contain % or _,
    // and allowing them would let a single % match the entire table.
    const sanitized = cnpj.trim().replace(/[%_\\]/g, '');

    try {
      const conn = await db.connect();

      if (searchMode === 'pessoa') {
        if (sanitized.length < 3) {
          pessoaResults = [];
          results = [];
          await conn.close();
          return;
        }
        const stmt = await conn.prepare(`
          SELECT
            cpf_mascarado,
            nome_normalizado,
            nome_original,
            papel,
            cnpj_base,
            qualificacao_codigo,
            faixa_etaria
          FROM pessoas
          WHERE nome_normalizado ILIKE ?
          ORDER BY cpf_mascarado, nome_normalizado
          LIMIT 50
        `);
        const res = await stmt.query(`%${sanitized.toUpperCase()}%`);
        await stmt.close();
        pessoaResults = res.toArray().map((r) => r.toJSON() as PessoaRow);
        results = [];
      } else {
        const clean = stripCNPJ(cnpj);
        let res;

        if (clean.length === 14) {
          const stmt = await conn.prepare(`
            SELECT cnpj, razao_social, nome_fantasia, uf,
                   cnae_principal_codigo, cnae_principal_descricao,
                   municipio_nome, capital_social
            FROM cnpjs WHERE cnpj = ? LIMIT 1
          `);
          res = await stmt.query(clean);
          await stmt.close();
        } else {
          if (sanitized.length < 3) {
            results = [];
            pessoaResults = [];
            await conn.close();
            return;
          }
          const stmt = await conn.prepare(`
            SELECT cnpj, razao_social, nome_fantasia, uf,
                   cnae_principal_codigo, cnae_principal_descricao,
                   municipio_nome, capital_social
            FROM cnpjs WHERE razao_social ILIKE ? LIMIT 20
          `);
          res = await stmt.query(`%${sanitized}%`);
          await stmt.close();
        }

        results = res.toArray().map((r) => r.toJSON() as EmpresaRow);
        pessoaResults = [];
      }

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
    {#if hasPessoas}
      <div class="mode-tabs">
        <button
          class="tab {searchMode === 'empresa' ? 'active' : ''}"
          onclick={() => { searchMode = 'empresa'; results = []; pessoaResults = []; }}
        >Empresa</button>
        <button
          class="tab {searchMode === 'pessoa' ? 'active' : ''}"
          onclick={() => { searchMode = 'pessoa'; results = []; pessoaResults = []; }}
        >Pessoa</button>
      </div>
    {/if}
    <div class="input-group">
      <input
        type="text"
        bind:value={cnpj}
        placeholder={searchMode === 'pessoa' ? 'Nome da pessoa…' : 'CNPJ ou Razão Social…'}
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
  {:else if pessoaResults.length > 0}
    <div class="pessoa-results">
      <table>
        <thead>
          <tr>
            <th>CPF mascarado</th>
            <th>Nome</th>
            <th>Papel</th>
            <th>CNPJ base</th>
            <th>Faixa etária</th>
          </tr>
        </thead>
        <tbody>
          {#each pessoaResults as p}
            <tr>
              <td class="mono">{p.cpf_mascarado}</td>
              <td>{p.nome_original ?? p.nome_normalizado}</td>
              <td><span class="badge badge-{p.papel}">{p.papel}</span></td>
              <td class="mono">{p.cnpj_base}</td>
              <td>{p.faixa_etaria ?? '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
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

  .mode-tabs {
    display: flex;
    gap: 0.25rem;
    margin-bottom: 0.75rem;
    justify-content: center;
  }

  .tab {
    background: #f3f4f6;
    color: #374151;
    border: 1px solid #e5e7eb;
    padding: 0.4rem 1.25rem;
    border-radius: 6px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
    min-width: auto;
  }

  .tab:hover:not(:disabled) {
    background: #e5e7eb;
    transform: none;
  }

  .tab.active {
    background: #2563eb;
    color: white;
    border-color: #2563eb;
  }

  .pessoa-results {
    overflow-x: auto;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    font-size: 0.875rem;
  }

  th {
    background: #f9fafb;
    padding: 0.75rem 1rem;
    text-align: left;
    font-weight: 600;
    color: #374151;
    border-bottom: 1px solid #e5e7eb;
  }

  td {
    padding: 0.625rem 1rem;
    border-bottom: 1px solid #f3f4f6;
    color: #111827;
  }

  tr:last-child td {
    border-bottom: none;
  }

  .mono {
    font-family: monospace;
    font-size: 0.8rem;
  }

  .badge {
    display: inline-block;
    padding: 0.125rem 0.5rem;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 600;
  }

  .badge-socio_pf {
    background: #dbeafe;
    color: #1d4ed8;
  }

  .badge-representante {
    background: #fef3c7;
    color: #92400e;
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
