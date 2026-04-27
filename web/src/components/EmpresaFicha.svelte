<script lang="ts">
  import { onMount } from 'svelte';

  // Svelte 5 Runes
  let { empresa } = $props<{ empresa: any }>();

  let historico = $state<any[]>([]);
  let loadingHistorico = $state(false);
  let showHistorico = $state(false);

  // Mocking the historical snapshots that would come from Internet Archive JSONs
  async function fetchHistorico() {
    if (historico.length > 0) {
      showHistorico = !showHistorico;
      return;
    }

    loadingHistorico = true;
    showHistorico = true;

    // Simulate network delay
    await new Promise(resolve => setTimeout(resolve, 800));

    // Create a mock previous state to demonstrate diffing
    // We'll change the capital social and maybe razao social slightly
    const prevCapital = Math.max(0, Number(empresa.capital_social || 0) - 50000);
    const mockSnapshot = {
      ...empresa,
      data_snapshot: '2023-01-15',
      capital_social: prevCapital.toString(),
      razao_social: empresa.razao_social.replace(' LTDA', '')
    };

    historico = [mockSnapshot];
    loadingHistorico = false;
  }

  function formatCNPJ(cnpj: string | number) {
    let s = String(cnpj).padStart(14, '0');
    return s.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
  }

  function formatCurrency(val: string | number) {
    let numVal = val;
    if (typeof val === 'string') {
      numVal = Number(val.replace(',', '.'));
    }
    return Number(numVal || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }

  function getDiffClass(currentValue: any, pastValue: any) {
    if (!pastValue) return '';
    return currentValue !== pastValue ? 'changed' : '';
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
        <span class="field-value">{empresa.cnae_principal}</span>
      </div>
      <div class="field-group">
        <span class="field-label">Capital Social</span>
        <span class="field-value highlight">{formatCurrency(empresa.capital_social)}</span>
      </div>
    </div>
  </div>

  <div class="ficha-footer">
    <button class="historico-btn" onclick={fetchHistorico}>
      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l4 2"/></svg>
      {showHistorico ? 'Ocultar Histórico' : 'Rastreamento Histórico'}
    </button>
  </div>

  {#if showHistorico}
    <div class="historico-panel">
      <h4 class="historico-title">Snapshots Anteriores</h4>
      {#if loadingHistorico}
        <div class="loading-state">Consultando Arquivo...</div>
      {:else}
        {#each historico as snap}
          <div class="snapshot-card">
            <div class="snapshot-date">
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="18" x="3" y="4" rx="2" ry="2"/><line x1="16" x2="16" y1="2" y2="6"/><line x1="8" x2="8" y1="2" y2="6"/><line x1="3" x2="21" y1="10" y2="10"/></svg>
              Snapshot: {snap.data_snapshot}
            </div>

            <div class="snapshot-changes">
              {#if snap.razao_social !== empresa.razao_social}
                <div class="change-item">
                  <span class="change-label">Razão Social:</span>
                  <span class="change-old">{snap.razao_social}</span>
                  <span class="change-arrow">→</span>
                  <span class="change-new">{empresa.razao_social}</span>
                </div>
              {/if}

              {#if snap.capital_social !== empresa.capital_social}
                <div class="change-item">
                  <span class="change-label">Capital Social:</span>
                  <span class="change-old">{formatCurrency(snap.capital_social)}</span>
                  <span class="change-arrow">→</span>
                  <span class="change-new">{formatCurrency(empresa.capital_social)}</span>
                </div>
              {/if}

              {#if snap.razao_social === empresa.razao_social && snap.capital_social === empresa.capital_social}
                <div class="no-changes">Sem alterações relevantes neste snapshot.</div>
              {/if}
            </div>
          </div>
        {/each}
      {/if}
    </div>
  {/if}
</div>

<style>
  .ficha-card {
    background: #fffdf9;
    border: 1px solid #e2dcd0;
    border-radius: 8px;
    box-shadow: 0 4px 10px rgba(0, 0, 0, 0.05), 0 1px 3px rgba(0, 0, 0, 0.1);
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
    font-family: 'Courier New', Courier, monospace;
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
    border-bottom: 1px dashed #d1c8b4;
    background: #faf8f2;
  }

  .ficha-id {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .label {
    font-size: 0.75rem;
    font-weight: bold;
    color: #8c8371;
    text-transform: uppercase;
    letter-spacing: 1px;
  }

  .value {
    font-size: 1.25rem;
    font-weight: 700;
    color: #2c2820;
    font-family: 'Courier New', Courier, monospace;
    letter-spacing: -0.5px;
  }

  .ficha-uf {
    background: #2c2820;
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-weight: bold;
    font-size: 0.875rem;
    font-family: system-ui, sans-serif;
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
    color: #8c8371;
    text-transform: uppercase;
    font-family: system-ui, sans-serif;
    letter-spacing: 0.5px;
  }

  .field-value {
    font-size: 1rem;
    color: #3d382d;
    font-weight: 600;
    margin: 0;
  }

  .main-title {
    font-size: 1.375rem;
    color: #1a1712;
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
    background: #f5f2eb;
    border-top: 1px solid #e2dcd0;
    display: flex;
    justify-content: flex-end;
  }

  .historico-btn {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: white;
    border: 1px solid #d1c8b4;
    color: #5c5545;
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-size: 0.875rem;
    font-weight: 600;
    cursor: pointer;
    font-family: system-ui, sans-serif;
    transition: all 0.2s;
  }

  .historico-btn:hover {
    background: #eeeadd;
    color: #2c2820;
  }

  .historico-panel {
    background: #2c2820;
    color: #e5e5e5;
    padding: 1.5rem;
    border-top: 2px solid #1a1712;
    font-family: system-ui, sans-serif;
  }

  .historico-title {
    margin: 0 0 1rem 0;
    font-size: 0.875rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #a3a3a3;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .historico-title::before {
    content: '';
    display: inline-block;
    width: 8px;
    height: 8px;
    background: #fbbf24;
    border-radius: 50%;
  }

  .loading-state {
    color: #a3a3a3;
    font-size: 0.875rem;
    font-style: italic;
  }

  .snapshot-card {
    background: #1f1c16;
    border: 1px solid #3d382d;
    border-radius: 6px;
    padding: 1rem;
    margin-bottom: 1rem;
  }

  .snapshot-card:last-child {
    margin-bottom: 0;
  }

  .snapshot-date {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.75rem;
    color: #fbbf24;
    margin-bottom: 0.75rem;
    font-weight: bold;
  }

  .snapshot-changes {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .change-item {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
    font-size: 0.875rem;
  }

  .change-label {
    color: #a3a3a3;
    width: 100px;
  }

  .change-old {
    color: #ef4444;
    text-decoration: line-through;
  }

  .change-arrow {
    color: #737373;
  }

  .change-new {
    color: #10b981;
    font-weight: 600;
  }

  .no-changes {
    color: #737373;
    font-size: 0.875rem;
    font-style: italic;
  }
</style>