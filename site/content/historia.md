---
type: Page
name: História de empresas
title: História — FICHA
description: Observações históricas em competências arquivadas; comparação remota ainda experimental.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/historia/
layout: product
---

<section class="page-lead">
  <p class="eyebrow">02 / TEMPO</p>
  <h1>O tempo deixa registros.</h1>
  <p class="lede">As competências preservadas mostram observações cadastrais de momentos distintos. A comparação de CNPJ numérico entre competências com Protobuf v1 utiliza arquivos arquivados menores. CNPJs alfanuméricos e meses sem essa camada ainda dependem da rota analítica experimental. Diferenças entre arquivos não determinam a data exata de uma alteração.</p>
  <p class="fineprint notice"><strong>Comparação sujeita a indisponibilidade.</strong> A rota numérica Protobuf não exige Parquet, mas pode falhar por arquivo ausente ou rede. A comparação legada por Parquet continua bloqueada por CORS no teste de 19/11/2026. Isso não demonstra ausência de registros. Os <a href="/ficha/acervo/">arquivos e competências declarados</a> continuam documentados, mesmo quando a consulta no navegador falha. <a href="https://github.com/franklinbaldo/ficha/issues/208">Consulte o diagnóstico</a>.</p>
</section>
<section class="workspace">
  <h2>Competências no acervo</h2>
  <ol id="historia-competencias" class="timeline-list" aria-label="Snapshots publicados"></ol>
  <h2>Comparar duas observações</h2>
  <form id="historia-form" class="query-form">
    <div class="form-field"><label for="historia-cnpj">CNPJ completo</label><input id="historia-cnpj" name="cnpj" type="search" required maxlength="24" placeholder="Digite o CNPJ"></div>
    <div class="form-field"><label for="historia-inicio">Competência inicial</label><select id="historia-inicio" required></select></div>
    <div class="form-field"><label for="historia-fim">Competência final</label><select id="historia-fim" required></select></div>
    <button id="historia-enviar" class="button-primary" type="submit">Comparar</button>
  </form>
  <p class="interface-status" id="historia-estado" role="status" aria-live="polite">Lendo o manifesto…</p>
  <div class="result-panel" id="historia-resultado" aria-label="Comparação cadastral"></div>
</section>
<section class="footnote-section">
  <h2>Limites da comparação</h2>
  <p>Um campo diferente em dois arquivos é evidência de uma diferença observada, não da data precisa da alteração. Uma competência ausente não pode ser preenchida por suposição; a ausência de um CNPJ em um snapshot não prova encerramento.</p>
</section>
