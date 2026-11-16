---
title: FICHA — ensaio SSG Rust
description: Ensaio isolado do shell estático do Ficha sem consulta real.
language: pt-BR
layout: index
---

<section class="intro" aria-labelledby="page-title">
  <p class="eyebrow">Ficha / protótipo de migração</p>
  <h1 id="page-title">Uma empresa brasileira, ficha por ficha.</h1>
  <p>Esta é apenas uma demonstração do shell estático. Nenhuma consulta a dados cadastrais reais é efetuada.</p>
</section>

<section id="consulta" class="panel" aria-labelledby="search-title">
  <h2 id="search-title">Consulta pública</h2>
  <form id="search-form" novalidate>
    <label for="search-kind">Buscar por</label>
    <select id="search-kind" name="kind">
      <option value="cnpj">Empresa / CNPJ</option>
      <option value="pessoa">Pessoa ou sócio</option>
      <option value="endereco">Endereço</option>
      <option value="cnae">CNAE</option>
    </select>
    <label for="search-term">CNPJ ou termo</label>
    <input id="search-term" name="term" autocomplete="off" required>
    <button type="submit">Consultar</button>
  </form>
  <p id="search-status" role="status" aria-live="polite">Nenhuma consulta realizada.</p>
  <section id="search-result" aria-labelledby="result-title" hidden>
    <h3 id="result-title">Resultado</h3>
    <p id="result-content"></p>
  </section>
  <noscript>A consulta exige JavaScript. Os dados publicados continuam acessíveis pelo manifesto público.</noscript>
</section>

<section class="origin" aria-labelledby="origin-title">
  <h2 id="origin-title">Origem e competência</h2>
  <p>Snapshots da Receita Federal são preservados no Internet Archive.</p>
  <a href="/ficha/manifest.json">Manifesto público de dados</a>
  <p><small>O manifesto pertence ao site de produção, não a esta PoC isolada.</small></p>
</section>
