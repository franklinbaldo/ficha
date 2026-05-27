<script lang="ts">
  import type * as duckdb from '@duckdb/duckdb-wasm';

  type SocioRow = {
    nome_socio_razao_social: string;
    tipo_descricao: string;
    qualificacao_descricao: string;
    data_entrada_sociedade: string | null;
    cpf_mascarado: string | null;
    cnpj_socio: string | null;
    pais_nome: string | null;
    faixa_etaria: string | null;
    representante_legal_nome: string | null;
    representante_legal_qualificacao_descricao: string | null;
  };

  let { empresa, db = null } = $props<{ empresa: any; db?: duckdb.AsyncDuckDB | null }>();

  let socios = $state<SocioRow[]>([]);
  let loadingSocios = $state(false);
  let showSocios = $state(false);
  let sociosError = $state<string | null>(null);

  async function toggleSocios() {
    if (showSocios) {
      showSocios = false;
      return;
    }
    showSocios = true;
    if (socios.length > 0) return;
    if (!db) return;

    loadingSocios = true;
    sociosError = null;
    try {
      const conn = await db.connect();
      const cnpjBase = String(empresa.cnpj).padStart(14, '0').substring(0, 8);
      const stmt = await conn.prepare(`
        SELECT nome_socio_razao_social, tipo_descricao, qualificacao_descricao,
               data_entrada_sociedade, cpf_mascarado, cnpj_socio,
               pais_nome, faixa_etaria,
               representante_legal_nome, representante_legal_qualificacao_descricao
        FROM socios
        WHERE cnpj_base = ?
        ORDER BY qualificacao_descricao, nome_socio_razao_social
      `);
      const res = await stmt.query(cnpjBase);
      await stmt.close();
      await conn.close();
      socios = res.toArray().map((r) => r.toJSON() as SocioRow);
    } catch (e) {
      sociosError = (e as Error).message;
    } finally {
      loadingSocios = false;
    }
  }

  function formatCNPJ(cnpj: string | number) {
    let s = String(cnpj).padStart(14, '0');
    return s.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
  }

  function formatCurrency(val: any) {
    if (val === null || val === undefined) return 'R$ 0,00';
    let numVal: number;
    if (typeof val === 'bigint') {
      numVal = Number(val);
    } else if (typeof val === 'string') {
      numVal = Number(val.replace(',', '.'));
    } else {
      numVal = Number(val);
    }
    if (isNaN(numVal)) return 'R$ 0,00';
    return numVal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }
</script>

<div class="ficha-card">
  <div class="ficha-header">
    <div class="ficha-id">
      <span class="label">CNPJ</span>
      <span class="value">{formatCNPJ(empresa.cnpj)}</span>
    </div>
    <div class="ficha-uf">
      {empresa.uf}
    </div>
  </div>

  <div class="ficha-body">
    <div class="field-group full-width">
      <span class="field-label">Razão Social</span>
      <h2 class="field-value main-title">{empresa.razao_social || 'NÃO INFORMADA'}</h2>
    </div>

    <div class="field-group full-width">
      <span class="field-label">Nome Fantasia</span>
      <span class="field-value">{empresa.nome_fantasia || '-'}</span>
    </div>

    <div class="grid-2-cols">
      <div class="field-group">
        <span class="field-label">CNAE Principal</span>
        <span class="field-value">{empresa.cnae_principal_codigo ?? '—'}{empresa.cnae_principal_descricao ? ' — ' + empresa.cnae_principal_descricao : ''}</span>
      </div>
      <div class="field-group">
        <span class="field-label">Capital Social</span>
        <span class="field-value highlight">{formatCurrency(empresa.capital_social)}</span>
      </div>
    </div>

    {#if empresa.municipio_nome}
      <div class="field-group">
        <span class="field-label">Município</span>
        <span class="field-value">{empresa.municipio_nome}</span>
      </div>
    {/if}
  </div>

  {#if db}
    <div class="ficha-footer">
      <button class="socios-btn" onclick={toggleSocios}>
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        {showSocios ? 'Ocultar Sócios' : 'Quadro Societário'}
      </button>
    </div>

    {#if showSocios}
      <div class="socios-panel">
        <h4 class="socios-title">Quadro Societário</h4>
        {#if loadingSocios}
          <div class="loading-state">Consultando sócios…</div>
        {:else if sociosError}
          <div class="error-state">Erro: {sociosError}</div>
        {:else if socios.length === 0}
          <div class="empty-state">Nenhum sócio encontrado.</div>
        {:else}
          <div class="socios-list">
            {#each socios as s}
              <div class="socio-item">
                <div class="socio-header">
                  <span class="socio-nome">{s.nome_socio_razao_social}</span>
                  <span class="badge badge-{s.tipo_descricao}">{s.tipo_descricao.toUpperCase()}</span>
                </div>
                <div class="socio-meta">
                  <span class="meta-item">{s.qualificacao_descricao || '—'}</span>
                  {#if s.data_entrada_sociedade}
                    <span class="meta-sep">·</span>
                    <span class="meta-item">desde {s.data_entrada_sociedade}</span>
                  {/if}
                  {#if s.cpf_mascarado}
                    <span class="meta-sep">·</span>
                    <span class="meta-item mono">{s.cpf_mascarado}</span>
                  {/if}
                  {#if s.cnpj_socio}
                    <span class="meta-sep">·</span>
                    <span class="meta-item mono">{formatCNPJ(s.cnpj_socio)}</span>
                  {/if}
                  {#if s.pais_nome && s.pais_nome !== 'BRASIL' && s.pais_nome !== ''}
                    <span class="meta-sep">·</span>
                    <span class="meta-item">{s.pais_nome}</span>
                  {/if}
                  {#if s.faixa_etaria}
                    <span class="meta-sep">·</span>
                    <span class="meta-item">faixa {s.faixa_etaria}</span>
                  {/if}
                </div>
                {#if s.representante_legal_nome}
                  <div class="rep-legal">
                    <span class="rep-label">Repr. legal:</span>
                    <span class="rep-nome">{s.representante_legal_nome}</span>
                    {#if s.representante_legal_qualificacao_descricao}
                      <span class="rep-qual">({s.representante_legal_qualificacao_descricao})</span>
                    {/if}
                  </div>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/if}
  {/if}
</div>

<style>
  .ficha-card {
    background: var(--color-concreto-bg, #fffdf9);
    border: 1px solid var(--color-concreto-border, #e2dcd0);
    border-radius: var(--radius-0, 0);
    box-shadow: 0 4px 10px rgba(0, 0, 0, 0.05), 0 1px 3px rgba(0, 0, 0, 0.1);
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
    font-family: var(--font-concreto, 'Courier New', Courier, monospace);
    transition: transform 0.2s, box-shadow 0.2s;
  }

  .ficha-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 6px;
    background: repeating-linear-gradient(
      45deg,
      #e0e0e0,
      #e0e0e0 10px,
      #d0d0d0 10px,
      #d0d0d0 20px
    );
    border-bottom: 1px solid #ccc;
  }

  .ficha-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.08), 0 2px 4px rgba(0, 0, 0, 0.12);
  }

  .ficha-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1.5rem 1.5rem 1rem;
    border-bottom: 1px dashed var(--color-concreto-border, #d1c8b4);
    background: var(--color-concreto-bg-alt, #faf8f2);
  }

  .ficha-id {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .label {
    font-size: 0.75rem;
    font-weight: bold;
    color: var(--color-concreto-text-muted, #8c8371);
    text-transform: uppercase;
    letter-spacing: 1px;
  }

  .value {
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--color-concreto-text, #2c2820);
    font-family: var(--font-concreto, 'Courier New', Courier, monospace);
    letter-spacing: -0.5px;
  }

  .ficha-uf {
    background: var(--color-concreto-text, #2c2820);
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: var(--radius-0, 0);
    font-weight: bold;
    font-size: 0.875rem;
    font-family: var(--font-curva, system-ui, sans-serif);
  }

  .ficha-body {
    padding: 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
  }

  .field-group {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .field-label {
    font-size: 0.75rem;
    color: var(--color-concreto-text-muted, #8c8371);
    text-transform: uppercase;
    font-family: var(--font-curva, system-ui, sans-serif);
    letter-spacing: 0.5px;
  }

  .field-value {
    font-size: 1rem;
    color: var(--color-concreto-text-alt, #3d382d);
    font-weight: 600;
    margin: 0;
  }

  .main-title {
    font-size: 1.375rem;
    color: var(--color-concreto-text-strong, #1a1712);
    line-height: 1.3;
  }

  .highlight {
    color: #047857;
  }

  .grid-2-cols {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.5rem;
  }

  .ficha-footer {
    padding: 1rem 1.5rem;
    background: var(--color-concreto-bg-footer, #f5f2eb);
    border-top: 1px solid var(--color-concreto-border, #e2dcd0);
    display: flex;
    justify-content: flex-end;
  }

  .socios-btn {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: white;
    border: 1px solid var(--color-concreto-border, #d1c8b4);
    color: var(--color-concreto-text-muted2, #5c5545);
    padding: 0.5rem 1rem;
    border-radius: var(--radius-0, 0);
    font-size: 0.875rem;
    font-weight: 600;
    cursor: pointer;
    font-family: var(--font-curva, system-ui, sans-serif);
    transition: all 0.2s;
  }

  .socios-btn:hover {
    background: var(--color-concreto-hover, #eeeadd);
    color: var(--color-concreto-text, #2c2820);
  }

  .socios-panel {
    background: var(--color-concreto-text, #2c2820);
    color: #e5e5e5;
    padding: 1.5rem;
    border-top: 2px solid var(--color-concreto-text-strong, #1a1712);
    font-family: var(--font-curva, system-ui, sans-serif);
  }

  .socios-title {
    margin: 0 0 1rem 0;
    font-size: 0.875rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #a3a3a3;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .socios-title::before {
    content: '';
    display: inline-block;
    width: 8px;
    height: 8px;
    background: #fbbf24;
    border-radius: 50%;
  }

  .loading-state,
  .error-state,
  .empty-state {
    color: #a3a3a3;
    font-size: 0.875rem;
    font-style: italic;
  }

  .error-state { color: #f87171; }

  .socios-list {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .socio-item {
    background: #1f1c16;
    border: 1px solid #3d382d;
    padding: 0.875rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .socio-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    flex-wrap: wrap;
  }

  .socio-nome {
    font-weight: 600;
    font-size: 0.9375rem;
    color: #f5f5f5;
  }

  .badge {
    font-size: 0.7rem;
    font-weight: 700;
    padding: 0.1rem 0.45rem;
    border-radius: 3px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .badge-PF { background: #1e3a5f; color: #93c5fd; }
  .badge-PJ { background: #14532d; color: #86efac; }
  .badge-estrangeiro { background: #44260a; color: #fdba74; }

  .socio-meta {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.25rem;
    font-size: 0.8rem;
    color: #a3a3a3;
  }

  .meta-sep { color: #525252; }
  .mono { font-family: 'Courier New', monospace; font-size: 0.75rem; }

  .rep-legal {
    font-size: 0.8rem;
    color: #737373;
    display: flex;
    gap: 0.4rem;
    flex-wrap: wrap;
    margin-top: 0.15rem;
  }

  .rep-label { color: #737373; }
  .rep-nome { color: #d4d4d4; font-weight: 500; }
  .rep-qual { color: #737373; }
</style>
