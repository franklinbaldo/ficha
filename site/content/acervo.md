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
  <p>O Internet Archive é o destino de guarda indicado para os arquivos do FICHA. O manifesto registra referências, tamanhos e hashes declarados, mas isso, por si só, não certifica os bytes de cada ZIP, a versão de todos os registros Protobuf nem o acesso aos arquivos em tempo real. Nas competências Protobuf atuais, a versão do wire ainda não é atestada pelo catálogo (<a href="https://github.com/franklinbaldo/ficha/issues/383">limite documentado</a>). A consulta e a custódia de um arquivo específico exigem conferência própria.</p>
</section>
