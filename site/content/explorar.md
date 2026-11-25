---
type: Page
name: Explorar dados
title: Explorar — FICHA
description: Explore a estrutura e o volume dos conjuntos de dados públicos publicados.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/explorar/
layout: product
---

<section class="page-lead">
  <p class="eyebrow">03 / EXPLORAR</p>
  <h1>Veja o tamanho do acervo.</h1>
  <p class="lede">Conheça os conjuntos de dados de cada competência e compare sua dimensão. Esta visualização usa exclusivamente metadados reais do manifesto publicado.</p>
  <p class="fineprint">As barras de arquivos usam o maior tamanho declarado da competência como referência. Filtrar a lista não muda a escala; arquivos sem tamanho informado não recebem barra.</p>
</section>
<section class="workspace">
  <h2>Conjuntos por competência</h2>
  <div class="filter-row">
    <div class="form-field"><label for="explorar-competencia">Competência</label><select id="explorar-competencia" disabled></select></div>
    <div class="form-field"><label for="explorar-filtro">Filtrar por conjunto</label><input id="explorar-filtro" disabled type="search" placeholder="Ex.: cnpjs, socios, enderecos"></div>
  </div>
  <p id="explorar-contador" class="big-stat"></p>
  <p id="explorar-estado" class="interface-status" role="status" aria-live="polite">Lendo o manifesto…</p>
  <div id="explorar-grafico" class="asset-chart" aria-label="Gráfico de tamanho de arquivos declarados"></div>
  <div class="series-section">
    <h2>Linhas declaradas em cada competência</h2>
    <p class="fineprint">Estes números são contagens de linhas informadas pelo ETL no manifesto. Não representam necessariamente empresas ativas, aberturas ou baixas e não são uma verificação independente.</p>
    <div class="form-field compact-select"><label for="explorar-serie">Conjunto de registros</label><select id="explorar-serie" disabled></select></div>
    <div id="explorar-serie-grafico" aria-label="Série histórica de linhas declaradas"></div>
  </div>
</section>
<section class="footnote-section" aria-labelledby="regional-titulo">
  <p class="eyebrow">PRÓXIMA CAMADA / BRASIL</p>
  <h2 id="regional-titulo">Indicadores regionais precisam de agregados.</h2>
  <p>Mapas por UF, municípios, CNAE e evolução histórica exigem tabelas agregadas, contratos de proveniência e consulta testada. Eles não são deduzidos do tamanho dos arquivos e não serão exibidos com valores fictícios.</p>
  <p><a href="/ficha/acervo/">Conferir o catálogo completo</a></p>
</section>
