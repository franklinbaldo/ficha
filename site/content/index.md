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
  <p class="lede">Explore o acervo público do CNPJ brasileiro, acompanhe as competências arquivadas e encontre a origem de cada informação. A busca por empresa ainda é experimental.</p>
  <div class="hero-search">
    <form id="busca-form" role="search" aria-labelledby="busca-titulo">
      <label id="busca-titulo" for="busca-termo">Pesquise por CNPJ ou razão social</label>
      <div class="search-controls">
        <input id="busca-termo" name="q" type="search" maxlength="200" autocomplete="off" required placeholder="CNPJ ou nome da empresa">
        <button id="busca-enviar" type="submit" aria-label="Pesquisar">Pesquisar <span aria-hidden="true">↗</span></button>
      </div>
    </form>
    <p id="consulta-estado" role="status" aria-live="polite" class="interface-status">Digite um CNPJ completo ou ao menos três letras de uma razão social.</p>
    <div class="home-results" aria-labelledby="resultados-titulo">
      <h2 id="resultados-titulo">Resultados da consulta</h2>
      <ul class="companies" id="consulta-lista"></ul>
    </div>
    <p class="fineprint notice"><strong>Busca experimental.</strong> Em 19/11/2026, um teste no navegador do próprio site confirmou bloqueio CORS da origem arquivística do Parquet CNPJ. A pesquisa ainda não passou pelos testes de paridade completa com os Parquets remotos, e não há consulta remota funcional comprovada. Uma falha de leitura não significa ausência da empresa; a interface deve informar indisponibilidade sem apresentar resultados simulados. <a href="https://github.com/franklinbaldo/ficha/issues/208#issuecomment-6999541728">Ver evidência e estado da investigação</a> ou <a href="/ficha/acervo/">consultar o acervo</a>.</p>
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
