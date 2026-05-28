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

  type ContatoRow = {
    tipo: string;
    valor: string;
    posicao: number;
  };

  type FilialRow = {
    cnpj: string;
    razao_social: string | null;
    nome_fantasia: string | null;
    uf: string | null;
    municipio_nome: string | null;
    situacao_cadastral: string | null;
  };

  let { empresa, db = null } = $props<{ empresa: any; db?: duckdb.AsyncDuckDB | null }>();

  // Socios
  let socios = $state<SocioRow[]>([]);
  let loadingSocios = $state(false);
  let showSocios = $state(false);
  let sociosError = $state<string | null>(null);

  // Contatos
  let contatos = $state<ContatoRow[]>([]);
  let loadingContatos = $state(false);
  let showContatos = $state(false);
  let contatosError = $state<string | null>(null);

  // Filiais
  let filiais = $state<FilialRow[]>([]);
  let loadingFiliais = $state(false);
  let showFiliais = $state(false);
  let filiaisError = $state<string | null>(null);

  async function toggleSocios() {
    if (showSocios) { showSocios = false; return; }
    showSocios = true;
    if (socios.length > 0 || sociosError) return;
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

  async function toggleContatos() {
    if (showContatos) { showContatos = false; return; }
    showContatos = true;
    if (contatos.length > 0 || contatosError) return;
    if (!db) return;

    loadingContatos = true;
    contatosError = null;
    try {
      const conn = await db.connect();
      const cnpjFull = String(empresa.cnpj).padStart(14, '0');
      const stmt = await conn.prepare(`
        SELECT tipo, valor, posicao
        FROM cnpj_contatos
        WHERE cnpj = ?
        ORDER BY tipo, posicao
      `);
      const res = await stmt.query(cnpjFull);
      await stmt.close();
      await conn.close();
      contatos = res.toArray().map((r) => r.toJSON() as ContatoRow);
    } catch (e) {
      contatosError = (e as Error).message;
    } finally {
      loadingContatos = false;
    }
  }

  async function toggleFiliais() {
    if (showFiliais) { showFiliais = false; return; }
    showFiliais = true;
    if (filiais.length > 0 || filiaisError) return;
    if (!db) return;

    loadingFiliais = true;
    filiaisError = null;
    try {
      const conn = await db.connect();
      const cnpjFull = String(empresa.cnpj).padStart(14, '0');
      const cnpjBase = cnpjFull.substring(0, 8);
      const stmt = await conn.prepare(`
        SELECT cnpj, razao_social, nome_fantasia, uf, municipio_nome, situacao_cadastral
        FROM cnpjs
        WHERE cnpj_base = ? AND cnpj != ?
        ORDER BY cnpj
        LIMIT 50
      `);
      const res = await stmt.query(cnpjBase, cnpjFull);
      await stmt.close();
      await conn.close();
      filiais = res.toArray().map((r) => r.toJSON() as FilialRow);
    } catch (e) {
      filiaisError = (e as Error).message;
    } finally {
      loadingFiliais = false;
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

    {#if empresa.opcao_simples || empresa.opcao_mei}
      <div class="badges-row">
        {#if empresa.opcao_simples}
          <span class="badge badge-simples">Simples Nacional</span>
        {/if}
        {#if empresa.opcao_mei}
          <span class="badge badge-mei">MEI</span>
        {/if}
      </div>
    {/if}
  </div>

  {#if db}
    <div class="ficha-footer">
      <button class="action-btn" onclick={toggleSocios}>
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        {showSocios ? 'Ocultar Sócios' : 'Quadro Societário'}
      </button>
      <button class="action-btn" onclick={toggleContatos}>
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 12a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 1.18h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L7.91 8.91a16 16 0 0 0 6.09 6.09l.91-.91a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>
        {showContatos ? 'Ocultar Contatos' : 'Contatos'}
      </button>
      <button class="action-btn" onclick={toggleFiliais}>
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>
        {showFiliais ? 'Ocultar Filiais' : 'Filiais / Estabelecimentos'}
      </button>
    </div>

    {#if showSocios}
      <div class="detail-panel">
        <h4 class="panel-title">
          <span class="panel-dot"></span>
          Quadro Societário
        </h4>
        {#if loadingSocios}
          <div class="loading-state">Consultando sócios…</div>
        {:else if sociosError}
          <div class="error-state">Erro: {sociosError}</div>
        {:else if socios.length === 0}
          <div class="empty-state">Nenhum sócio encontrado.</div>
        {:else}
          <div class="items-list">
            {#each socios as s}
              <div class="list-item">
                <div class="item-header">
                  <span class="item-nome">{s.nome_socio_razao_social}</span>
                  <span class="badge badge-tipo-{s.tipo_descricao}">{s.tipo_descricao.toUpperCase()}</span>
                </div>
                <div class="item-meta">
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

    {#if showContatos}
      <div class="detail-panel">
        <h4 class="panel-title">
          <span class="panel-dot panel-dot-blue"></span>
          Contatos
        </h4>
        {#if loadingContatos}
          <div class="loading-state">Consultando contatos…</div>
        {:else if contatosError}
          <div class="error-state">Erro: {contatosError}</div>
        {:else if contatos.length === 0}
          <div class="empty-state">Nenhum contato registrado.</div>
        {:else}
          <div class="contatos-grid">
            {#each contatos as c}
              <div class="contato-item">
                <span class="contato-tipo">{c.tipo}</span>
                <span class="contato-valor mono">
                  {#if c.tipo === 'email'}
                    <a href="mailto:{c.valor}" class="contato-link">{c.valor}</a>
                  {:else}
                    {c.valor}
                  {/if}
                </span>
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/if}

    {#if showFiliais}
      <div class="detail-panel">
        <h4 class="panel-title">
          <span class="panel-dot panel-dot-green"></span>
          Filiais / Estabelecimentos
        </h4>
        {#if loadingFiliais}
          <div class="loading-state">Consultando estabelecimentos…</div>
        {:else if filiaisError}
          <div class="error-state">Erro: {filiaisError}</div>
        {:else if filiais.length === 0}
          <div class="empty-state">Nenhum outro estabelecimento encontrado.</div>
        {:else}
          <div class="items-list">
            {#each filiais as f}
              <div class="list-item">
                <div class="item-header">
                  <span class="item-nome mono">{formatCNPJ(f.cnpj)}</span>
                  {#if f.situacao_cadastral}
                    <span class="badge badge-sit-{f.situacao_cadastral.toLowerCase()}">{f.situacao_cadastral}</span>
                  {/if}
                </div>
                <div class="item-meta">
                  {#if f.nome_fantasia}
                    <span class="meta-item">{f.nome_fantasia}</span>
                    <span class="meta-sep">·</span>
                  {/if}
                  {#if f.municipio_nome}
                    <span class="meta-item">{f.municipio_nome}</span>
                  {/if}
                  {#if f.uf}
                    <span class="meta-sep">/</span>
                    <span class="meta-item">{f.uf}</span>
                  {/if}
                </div>
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

  .badges-row {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .badge {
    font-size: 0.7rem;
    font-weight: 700;
    padding: 0.1rem 0.45rem;
    border-radius: 3px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .badge-simples { background: #fef3c7; color: #92400e; border: 1px solid #fde68a; }
  .badge-mei { background: #dbeafe; color: #1e40af; border: 1px solid #bfdbfe; }

  .ficha-footer {
    padding: 0.75rem 1.5rem;
    background: var(--color-concreto-bg-footer, #f5f2eb);
    border-top: 1px solid var(--color-concreto-border, #e2dcd0);
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .action-btn {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    background: white;
    border: 1px solid var(--color-concreto-border, #d1c8b4);
    color: var(--color-concreto-text-muted2, #5c5545);
    padding: 0.4rem 0.75rem;
    border-radius: var(--radius-0, 0);
    font-size: 0.8125rem;
    font-weight: 600;
    cursor: pointer;
    font-family: var(--font-curva, system-ui, sans-serif);
    transition: all 0.2s;
  }

  .action-btn:hover {
    background: var(--color-concreto-hover, #eeeadd);
    color: var(--color-concreto-text, #2c2820);
  }

  .detail-panel {
    background: var(--color-concreto-text, #2c2820);
    color: #e5e5e5;
    padding: 1.25rem 1.5rem;
    border-top: 2px solid var(--color-concreto-text-strong, #1a1712);
    font-family: var(--font-curva, system-ui, sans-serif);
  }

  .panel-title {
    margin: 0 0 1rem 0;
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #a3a3a3;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .panel-dot {
    display: inline-block;
    width: 7px;
    height: 7px;
    background: #fbbf24;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .panel-dot-blue { background: #60a5fa; }
  .panel-dot-green { background: #34d399; }

  .loading-state,
  .error-state,
  .empty-state {
    color: #a3a3a3;
    font-size: 0.875rem;
    font-style: italic;
  }

  .error-state { color: #f87171; }

  .items-list {
    display: flex;
    flex-direction: column;
    gap: 0.625rem;
  }

  .list-item {
    background: #1f1c16;
    border: 1px solid #3d382d;
    padding: 0.75rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .item-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    flex-wrap: wrap;
  }

  .item-nome {
    font-weight: 600;
    font-size: 0.9rem;
    color: #f5f5f5;
  }

  .badge-tipo-PF { background: #1e3a5f; color: #93c5fd; }
  .badge-tipo-PJ { background: #14532d; color: #86efac; }
  .badge-tipo-estrangeiro { background: #44260a; color: #fdba74; }
  .badge-sit-ativa { background: #14532d; color: #86efac; }
  .badge-sit-baixada { background: #3f1f1f; color: #f87171; }
  .badge-sit-suspensa { background: #44260a; color: #fdba74; }
  .badge-sit-inapta { background: #3f1f1f; color: #f87171; }
  .badge-sit-nula { background: #1f1f2e; color: #a5b4fc; }

  .item-meta {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.2rem;
    font-size: 0.8rem;
    color: #a3a3a3;
  }

  .meta-sep { color: #525252; }
  .mono { font-family: 'Courier New', monospace; font-size: 0.75rem; }

  .rep-legal {
    font-size: 0.78rem;
    color: #737373;
    display: flex;
    gap: 0.4rem;
    flex-wrap: wrap;
    margin-top: 0.1rem;
  }

  .rep-label { color: #737373; }
  .rep-nome { color: #d4d4d4; font-weight: 500; }
  .rep-qual { color: #737373; }

  .contatos-grid {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }

  .contato-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    font-size: 0.875rem;
  }

  .contato-tipo {
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #737373;
    min-width: 5rem;
  }

  .contato-valor {
    color: #e5e5e5;
  }

  .contato-link {
    color: #93c5fd;
    text-decoration: none;
  }

  .contato-link:hover {
    text-decoration: underline;
  }
</style>
