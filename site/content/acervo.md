---
type: Page
name: Acervo aberto
title: Acervo — FICHA
description: Consulte os snapshots, arquivos, hashes e fontes do catálogo FICHA.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/acervo/
layout: product
---

<section class="page-lead">
  <p class="eyebrow">04 / DADOS ABERTOS</p>
  <h1>Os dados são de todos.</h1>
  <p class="lede">Explore competências e arquivos preservados, com tamanho, identidade declarada e ligação para a fonte. O manifesto é um contrato aberto para pessoas e máquinas.</p>
  <div class="inline-actions"><a class="button-secondary" href="/ficha/manifest.json">Abrir manifesto JSON</a><a class="quiet-link" href="https://github.com/franklinbaldo/ficha">Código-fonte</a></div>
</section>
<section class="workspace">
  <h2>Competências declaradas</h2>
  <p class="fineprint">A competência corrente do manifesto não é necessariamente a última publicada pela Receita Federal.</p>
  <ul id="acervo-competencias" class="snapshot-list"></ul>
  <div class="form-field compact-select"><label for="acervo-competencia">Inspecionar competência</label><select id="acervo-competencia"></select></div>
  <p id="acervo-estado" role="status" aria-live="polite" class="interface-status">Carregando o catálogo…</p>
  <div id="acervo-resultado" aria-label="Arquivos declarados no manifesto"></div>
</section>
<section class="footnote-section">
  <h2>Custódia e disponibilidade são coisas diferentes.</h2>
  <p>O Internet Archive preserva os artefatos. O manifesto registra referências, tamanhos e hashes, mas não garante que cada arquivo esteja acessível em tempo real. O FICHA separa o que foi declarado da consulta efetivamente realizada.</p>
</section>
