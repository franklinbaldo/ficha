---
type: Page
name: Ficha da empresa
title: Ficha da empresa — FICHA
description: Ficha cadastral em competência arquivada; consulta remota por CNPJ ainda experimental.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/empresa/
layout: product
---

<section class="page-lead">
  <p class="eyebrow">01 / FICHA CADASTRAL</p>
  <h1>Uma empresa, uma ficha.</h1>
  <p class="lede">Uma ficha reúne informações públicas de uma competência arquivada quando a origem pode ser lida no navegador. A consulta numérica por CNPJ completo usa o membro Protobuf da competência, sem baixar o Parquet inteiro. A consulta remota ainda é experimental: erro de leitura não significa que a empresa esteja ausente do acervo.</p>
  <p class="fineprint notice"><strong>Consulta sujeita a indisponibilidade.</strong> A ficha por CNPJ numérico foi exercitada em teste de navegador com dois registros reais do acervo Protobuf, antes da publicação desta versão. Isso não garante acesso contínuo nem equivale a repetir a consulta no site publicado. Uma falha de leitura, inclusive HTTP 404, não comprova ausência cadastral. A busca por nome ainda usa Parquet sujeito a CORS. Já a comparação de CNPJ numérico nas competências Protobuf v1 usa registros menores; CNPJs alfanuméricos e competências sem essa camada continuam experimentais. Se precisar, <a href="/ficha/acervo/">consulte os arquivos do acervo</a>. <a href="https://github.com/franklinbaldo/ficha/issues/208">Veja os testes e limites</a>.</p>
</section>
<section class="workspace" aria-labelledby="consulta-empresa">
  <h2 id="consulta-empresa">Consultar CNPJ</h2>
  <form id="empresa-form" class="query-form" role="search">
    <div class="form-field"><label for="empresa-cnpj">CNPJ completo numérico (14 dígitos)</label><input type="search" id="empresa-cnpj" name="cnpj" maxlength="24" inputmode="numeric" autocomplete="off" required placeholder="00.000.000/0001-00"></div>
    <div class="form-field"><label for="empresa-competencia">Competência dos dados</label><select id="empresa-competencia" name="competencia" required></select></div>
    <button class="button-primary" id="empresa-enviar" type="submit" aria-label="Abrir ficha">Abrir ficha</button>
  </form>
  <p class="fineprint">CNPJ alfanumérico ainda não é atendido pelo leitor Protobuf numérico v1 desta ficha. A consulta histórica permanece experimental.</p>
  <p class="interface-status" id="empresa-estado" role="status" aria-live="polite">Preparando o catálogo de competências…</p>
  <div id="empresa-resultado" class="result-panel" aria-label="Ficha da empresa"></div>
  <noscript><p>Para consultar a ficha no navegador é necessário JavaScript. O <a href="/ficha/acervo/">catálogo de dados</a> permanece disponível.</p></noscript>
</section>
<section class="footnote-section">
  <h2>O que é uma ficha?</h2>
  <p>Um recorte do cadastro público da Receita Federal em uma competência. A ficha não comprova a situação atual da empresa e não substitui certidões ou documentos oficiais.</p>
  <p><a href="/ficha/historia/">Comparar duas competências</a> · <a href="/ficha/acervo/">Conferir as fontes</a></p>
</section>
