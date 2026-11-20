---
type: Page
name: FICHA
title: FICHA — Cada empresa tem uma história
description: Dados públicos de CNPJ por competência, preservados no Internet Archive.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/
layout: index
---

<section class="hero" aria-labelledby="hero-title">
  <p class="eyebrow">FICHÁRIO PÚBLICO · BRASIL</p>
  <h1 id="hero-title">Cada empresa tem <em>uma história.</em></h1>
  <p class="lede">Encontre uma empresa pelo CNPJ e explore os registros públicos preservados ao longo do tempo.</p>
  <div class="hero-search">
    <form id="busca-form" role="search" aria-labelledby="busca-titulo">
      <label id="busca-titulo" for="busca-termo">Pesquise por CNPJ ou razão social</label>
      <div class="search-controls">
        <input id="busca-termo" name="q" type="search" maxlength="200" autocomplete="off" required placeholder="CNPJ ou nome da empresa">
        <button id="busca-enviar" type="submit" aria-label="Pesquisar">Pesquisar <span aria-hidden="true">↗</span></button>
      </div>
    </form>
    <div class="example-suggestions" aria-label="CNPJs para experimentar">
      <span class="example-caption">Experimente um CNPJ</span>
      <div class="example-pills" id="example-pills">
        <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=00000000000191">Banco do Brasil <span class="mono">00.000.000/0001-91</span></a>
        <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=33000167000101">Petrobras <span class="mono">33.000.167/0001-01</span></a>
        <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=00360305000104">Caixa <span class="mono">00.360.305/0001-04</span></a>
        <a class="example-pill" data-cnpj-example href="/ficha/empresa/?cnpj=34028316000103">Correios <span class="mono">34.028.316/0001-03</span></a>
      </div>
      <p class="example-hint">Exemplos públicos: a ficha abre por Protobuf e depende da disponibilidade do acervo.</p>
    </div>
    <p id="consulta-estado" role="status" aria-live="polite" class="interface-status">Digite um CNPJ completo ou ao menos três letras de uma razão social.</p>
    <div class="home-results" aria-labelledby="resultados-titulo">
      <h2 id="resultados-titulo">Resultados da consulta</h2>
      <ul class="companies" id="consulta-lista"></ul>
    </div>
    <details class="search-limits"><summary>Entenda a disponibilidade e os limites da busca</summary>
      <p class="fineprint notice"><strong>Busca experimental nesta página.</strong> Um CNPJ numérico completo informado acima abre a ficha por Protobuf, sem iniciar consulta Parquet/DuckDB na home. A ficha ainda depende do acervo remoto e pode ficar indisponível. Buscas por nome e CNPJ alfanumérico continuam no caminho analítico Parquet/DuckDB, cujo acesso remoto falhou por CORS em teste de 19/11/2026; erro de rede não significa ausência cadastral. Para consultar diretamente, <a href="/ficha/empresa/">abra a ficha numérica</a>. Essa rota independente já foi exercitada em navegador com registros reais. A busca por nome e a comparação histórica completa ainda não foram resolvidas. Em competências com Protobuf v1, a página História permite comparação experimental de CNPJs numéricos sem Parquet. CNPJs alfanuméricos e meses sem essa camada não são comparáveis nesta interface; não há fallback automático. <a href="https://github.com/franklinbaldo/ficha/issues/208">Acompanhe as evidências</a> ou <a href="/ficha/acervo/">consulte o acervo</a>.</p>
    </details>
  </div>
  <div class="hero-bottom">
    <div class="fact"><span>Competência de referência no manifesto</span><strong id="competencia">__FICHA_CURRENT_COMPETENCE__</strong></div>
    <div class="fact"><span>Origem dos registros</span><strong>Receita Federal · CNPJ</strong></div>
    <a class="text-arrow" href="/ficha/manifest.json">Consultar manifesto público</a>
  </div>
</section>

<section class="pathways" aria-labelledby="pathways-title">
  <div class="section-heading"><p class="eyebrow">PORTAS DE ENTRADA</p><h2 id="pathways-title">Comece pelo que quer descobrir.</h2></div>
  <div class="path-grid">
    <a class="path-card" href="/ficha/empresa/"><span class="path-number">01 / CONSULTA</span><h3>Uma empresa.</h3><p>Explore a ficha cadastral por competência; a leitura remota de registros ainda é experimental.</p><span class="path-arrow" aria-hidden="true">↗</span></a>
    <a class="path-card" href="/ficha/historia/"><span class="path-number">02 / TEMPO</span><h3>Sua história.</h3><p>Veja as competências preservadas; a comparação depende de leitura remota.</p><span class="path-arrow" aria-hidden="true">↗</span></a>
    <a class="path-card" href="/ficha/explorar/"><span class="path-number">03 / EXPLORAR</span><h3>O acervo em números.</h3><p>Explore os conjuntos e seus tamanhos declarados no catálogo.</p><span class="path-arrow" aria-hidden="true">↗</span></a>
    <a class="path-card" href="/ficha/acervo/"><span class="path-number">04 / DADOS ABERTOS</span><h3>Todas as fontes.</h3><p>Acesse snapshots, formatos, tamanhos e hashes declarados.</p><span class="path-arrow" aria-hidden="true">↗</span></a>
  </div>
</section>

<section class="archive-feature" aria-labelledby="arquivo-title">
  <div><p class="eyebrow">PRESERVAÇÃO · NÃO APENAS BUSCA</p><h2 id="arquivo-title">O passado cadastral não deveria desaparecer.</h2><p>O FICHA organiza referências a snapshots do CNPJ por competência. Pesquisadores podem reutilizar os arquivos sem depender da interface.</p><div class="inline-actions"><a class="button-secondary" href="/ficha/acervo/">Explorar dados abertos</a><a class="quiet-link" href="/ficha/sobre/">Como funciona</a></div></div>
  <aside class="archive-aside"><p class="eyebrow">FONTE ARQUIVÍSTICA DO SNAPSHOT</p><p>O arquivo completo de CNPJs em Parquet tem <strong>__FICHA_CURRENT_SOURCE_SIZE__ GB</strong>.</p><a href="__FICHA_CURRENT_SOURCE_URL__" target="_blank" rel="noopener noreferrer">Ver arquivo de origem ↗</a><small>Arquivo grande; o link pode iniciar um download. Não é uma pesquisa no navegador.</small></aside>
</section>
<noscript><p>As consultas interativas requerem JavaScript. O <a href="/ficha/manifest.json">manifesto público</a> e a <a href="/ficha/acervo/">página do acervo</a> continuam acessíveis como documentos.</p></noscript>
