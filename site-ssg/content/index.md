---
type: Page
name: FICHA
title: FICHA — fichário aberto das empresas brasileiras
description: Dados públicos de CNPJ por competência, preservados no Internet Archive.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/
layout: index
---

<section aria-labelledby="titulo">
  <p class="eyebrow">Receita Federal · série mensal · acesso público</p>
  <h1 id="titulo">Uma empresa brasileira, ficha por ficha.</h1>
  <p>O FICHA consulta referências a competências mensais do cadastro público da Receita Federal.</p>
  <p class="notice"><strong>Prévia técnica não publicada.</strong> A nova busca por CNPJ numérico, alfanumérico ou razão social ainda não passou pelos testes de paridade, Protobuf e navegador. A versão atual do site continua disponível.</p>
  <form id="busca-form" role="search">
    <label for="busca-termo">CNPJ (numérico ou alfanumérico) ou razão social</label>
    <div class="search-controls">
      <input id="busca-termo" name="q" type="search" maxlength="200" autocomplete="off" required placeholder="CNPJ ou nome da empresa">
      <button id="busca-enviar" type="submit">Pesquisar</button>
    </div>
  </form>
  <p id="consulta-estado" role="status" aria-live="polite">Digite um CNPJ ou uma razão social para iniciar a consulta.</p>
  <section aria-labelledby="consulta-titulo">
    <h2 id="consulta-titulo">Resultados</h2>
    <ul class="companies" id="consulta-lista"></ul>
  </section>
  <p><a href="https://franklinbaldo.github.io/ficha/manifest.json">Consultar o manifesto público da versão atual</a></p>
  <noscript><p>A busca precisa de JavaScript. O manifesto público continua disponível pelo link acima.</p></noscript>
</section>
