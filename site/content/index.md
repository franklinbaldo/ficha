---
type: Page
name: FICHA
title: FICHA — dados públicos de empresas
description: Dados públicos de CNPJ por competência, preservados no Internet Archive.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/
layout: index
---

<section class="hero" aria-labelledby="hero-title">
  <p class="eyebrow">Dados abertos · Receita Federal · Brasil</p>
  <h1 id="hero-title">Uma ficha para entender cada época.</h1>
  <p class="lede">O FICHA organiza referências aos dados públicos de empresas brasileiras, competência por competência. Os arquivos e a proveniência estão disponíveis para consulta e reutilização.</p>
  <div class="facts" aria-label="Estado do acervo">
    <div class="fact"><span>Competência apontada pelo manifesto</span><strong id="competencia">__FICHA_CURRENT_COMPETENCE__</strong></div>
    <div class="fact"><span>Origem</span><strong>Cadastro Nacional da Pessoa Jurídica</strong></div>
  </div>
  <div class="actions"><a class="primary-link" href="/ficha/manifest.json">Ver manifesto de dados</a><a class="secondary-link" href="/ficha/sobre/">Entender o projeto</a></div>
  <p>Fonte arquivística do snapshot: <a href="__FICHA_CURRENT_SOURCE_URL__">arquivo completo de CNPJs (Parquet, __FICHA_CURRENT_SOURCE_SIZE__ GB)</a>. O link aponta para um arquivo grande e pode iniciar download; não é pesquisa no navegador.</p>
</section>
<section class="search-section" aria-labelledby="busca-titulo">
  <h2 id="busca-titulo">Pesquisa de CNPJ</h2>
  <p class="notice"><strong>Prévia da busca.</strong> A nova busca por CNPJ numérico, alfanumérico ou razão social ainda não passou pelos testes de paridade completa com os Parquets remotos.</p>
  <form id="busca-form" role="search">
    <label for="busca-termo">CNPJ (numérico ou alfanumérico) ou razão social</label>
    <div class="search-controls">
      <input id="busca-termo" name="q" type="search" maxlength="200" autocomplete="off" required placeholder="CNPJ ou nome da empresa">
      <button id="busca-enviar" type="submit">Pesquisar</button>
    </div>
  </form>
  <p id="consulta-estado" role="status" aria-live="polite">Digite um CNPJ ou uma razão social para iniciar a consulta.</p>
  <section aria-labelledby="consulta-titulo">
    <h3 id="consulta-titulo">Resultados</h3>
    <ul class="companies" id="consulta-lista"></ul>
  </section>
  <noscript><p>A busca precisa de JavaScript. O manifesto público continua disponível pelo link acima.</p></noscript>
</section>
<section class="reading" aria-labelledby="reading-title">
  <h2 id="reading-title">Por onde começar</h2>
  <div class="steps">
    <article><span class="step-number">01</span><h3>Confira a competência</h3><p>O manifesto aponta uma competência de referência e lista os conjuntos publicados. Ela não equivale necessariamente à competência mais recente da Receita.</p></article>
    <article><span class="step-number">02</span><h3>Consulte as referências</h3><p>Cada entrada descreve arquivos, endereços de origem e metadados de integridade. O acesso direto depende da disponibilidade de cada servidor.</p></article>
    <article><span class="step-number">03</span><h3>Veja como o FICHA funciona</h3><p>Conheça o propósito do projeto, as limitações dos dados e o código aberto.</p><p><a href="/ficha/sobre/">Ler sobre o FICHA</a></p></article>
  </div>
</section>
