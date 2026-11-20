---
type: Page
name: FICHA
title: FICHA — Consulte empresas e seu histórico cadastral
description: Dados públicos de CNPJ por competência, preservados no Internet Archive.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/
layout: index
---

<section class="hero" aria-labelledby="hero-title">
  <div class="hero-intro">
    <p class="eyebrow">Dados públicos do CNPJ</p>
    <h1 id="hero-title">Consulte empresas e seu <em>histórico cadastral.</em></h1>
    <p class="lede">Abra a ficha de um estabelecimento pelo CNPJ e compare seus registros em diferentes meses.</p>
  </div>
  <div class="hero-layout">
    <div class="hero-search">
      <form id="busca-form" role="search" aria-labelledby="busca-titulo">
        <label id="busca-titulo" for="busca-termo">CNPJ ou razão social</label>
        <div class="search-controls">
          <input id="busca-termo" name="q" type="search" maxlength="200" autocomplete="off" enterkeyhint="search" required placeholder="Ex. 33.000.167/0001-01" aria-describedby="busca-ajuda">
          <button id="busca-enviar" type="submit" aria-label="Pesquisar">Pesquisar</button>
        </div>
        <p id="busca-ajuda" class="search-help">Use um CNPJ numérico completo para abrir a ficha. A busca por nome é experimental.</p>
      </form>
      <p id="consulta-estado" role="status" aria-live="polite" class="interface-status"></p>
      <div class="home-results" aria-labelledby="resultados-titulo">
        <h2 id="resultados-titulo">Resultados da consulta</h2>
        <ul class="companies" id="consulta-lista"></ul>
      </div>
      <div class="example-suggestions" aria-label="CNPJs para experimentar">
        <p class="example-caption">Experimente um CNPJ</p>
        <div class="example-pills" id="example-pills">
          <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=00000000000191">Banco do Brasil <span class="mono">00.000.000/0001-91</span></a>
          <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=33000167000101">Petrobras <span class="mono">33.000.167/0001-01</span></a>
          <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=00360305000104">Caixa <span class="mono">00.360.305/0001-04</span></a>
          <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=34028316000103">Correios <span class="mono">34.028.316/0001-03</span></a>
        </div>
      </div>
      <details class="search-limits">
        <summary>Disponibilidade da consulta</summary>
        <p>Um CNPJ numérico completo abre a ficha por Protobuf. A ficha depende da disponibilidade do acervo. A busca por nome e por CNPJ alfanumérico usa Parquet e pode falhar por CORS. Em caso de erro de rede, a presença do registro continua indeterminada.</p>
        <p>A página História compara CNPJs numéricos entre competências com Protobuf v1. Consulte as <a href="https://github.com/franklinbaldo/ficha/issues/208">evidências de funcionamento</a>.</p>
      </details>
    </div>
    <aside class="hero-bottom" aria-labelledby="referencia-titulo">
      <p class="eyebrow">Referência do catálogo</p>
      <h2 id="referencia-titulo">Competência atual</h2>
      <p id="competencia">__FICHA_CURRENT_COMPETENCE__</p>
      <dl class="reference-data">
        <div><dt>Origem dos dados</dt><dd>Receita Federal</dd></div>
        <div><dt>Preservação dos arquivos</dt><dd>Internet Archive</dd></div>
      </dl>
      <a href="/ficha/manifest.json">Consultar manifesto JSON</a>
    </aside>
  </div>
</section>

<section class="pathways" aria-labelledby="pathways-title">
  <h2 id="pathways-title">Histórico e dados abertos</h2>
  <div class="path-grid">
    <a class="path-card" href="/ficha/historia/"><h3>Comparar registros</h3><p>Compare a ficha de um CNPJ numérico entre duas competências disponíveis.</p></a>
    <a class="path-card" href="/ficha/explorar/"><h3>Explorar o catálogo</h3><p>Compare os tamanhos dos conjuntos de dados declarados ao longo do tempo.</p></a>
    <a class="path-card" href="/ficha/acervo/"><h3>Acessar o acervo</h3><p>Encontre arquivos por competência, com formato, tamanho e hash.</p></a>
  </div>
  <p class="archive-source"><a href="__FICHA_CURRENT_SOURCE_URL__" target="_blank" rel="noopener noreferrer">Arquivo CNPJ em Parquet</a> <span>__FICHA_CURRENT_SOURCE_SIZE__ GB. O link pode iniciar um download.</span></p>
</section>

<noscript><p>Ative o JavaScript para pesquisar. Você também pode abrir o <a href="/ficha/manifest.json">manifesto JSON</a> diretamente.</p></noscript>
