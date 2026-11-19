---
type: Documentation
title: Site público do FICHA (SSG Rust)
description: Produto público e acervo com dados por competência, preservados no Internet Archive
---

# Site público FICHA · SSG Rust

A interface greenfield do FICHA traduz a [visão de produto](../docs/rfc/ficha-greenfield-visao-produto-2026-11.md) em cinco jornadas: **início/busca**, **empresa**, **história**, **explorar** e **acervo**.

## Organização

- `content/`: home, sobre, empresa, história, explorar e acervo.
- `templates/`: HTML semântico; `product.html` carrega JavaScript apenas nas páginas interativas.
- `static/css/site.css`: identidade editorial responsiva.
- `static/js/catalog.mjs`: catálogo e proveniência do manifesto.
- `static/js/atomic-record.mjs` e `atomic-browser.mjs`: ficha por CNPJ numérico v1, validação da raiz/membro Protobuf e lookup RFB de competência; o decoder canônico é empacotado pelo build SSG.
- `static/js/record.mjs`: acesso legado por Parquet/DuckDB, restrito às rotas experimentais que o usam.
- `static/js/history.mjs` e `page-helpers.mjs`: comparação histórica numérica Protobuf v1 e controles partilhados, sem fallback implícito.
- `static/js/pages.mjs`: UI das páginas novas, sem resultados simulados.
- `static/js/search-app.mjs`: busca experimental da home, com links de resultado para a ficha.
- `scripts/finalize.mjs`: copia o manifesto original byte a byte e os módulos para o diretório de publicação.
- `tests/`: testes de catálogo, consulta, regressões e páginas.

## Estado funcional e limites

A navegação, o catálogo e os gráficos de **tamanho de arquivos declarados** funcionam a partir do manifesto do próprio site. A ficha numérica completa agora tenta ler **somente um membro Protobuf v1**, com orçamento e estados explícitos de ausência/indisponibilidade. A comparação histórica por CNPJ numérico usa exclusivamente Protobuf v1, enquanto a busca por nome na home continua usando o transporte legado Parquet/DuckDB, dependente de CORS/Range. A História recusa CNPJ alfanumérico antes de qualquer leitura remota; não existe fallback implícito. O [E2E Chromium #44586268099](https://github.com/franklinbaldo/ficha/actions/runs/44586268099), executado no HEAD da [PR #367](https://github.com/franklinbaldo/ficha/pull/367), consultou Nubank e Petrobras em PB público e testou os caminhos negativos; o [deploy Pages #44586905819](https://github.com/franklinbaldo/ficha/actions/runs/44586905819) atestou publicação/HTTP após o merge, **não** uma repetição do E2E na URL publicada. Esses testes tampouco atestam todos os shards nem disponibilidade contínua. A [issue #208](https://github.com/franklinbaldo/ficha/issues/208) mantém esse escopo e documenta a falha CORS do transporte legado. Sem acesso, a UI informa indisponibilidade, não dados fictícios.

Os gráficos em Explorar descrevem arquivos, **não** quantidade de empresas, movimento econômico ou mapas por UF. Indicadores regionais demandam agregados com proveniência e contrato próprios. Não existe fallback opaco para uma API de terceiros.

O manifesto permanece canônico em `web/public/manifest.json` por enquanto. O finalizador confere `current`, integridade declarada, identidade exata dos bytes e substitui somente o PWA do SSG reconhecido. Esta implementação não altera ETL, uploads, snapshots, cron ou infraestrutura.

## Validações

```sh
node --test site/scripts/finalize.test.mjs
node --test site/tests/*.test.mjs
# Workflow deploy.yml: build SSG + HTTP smoke do site publicado
```

O smoke HTTP comprova páginas e arquivos publicados, **não** a consulta real. O pipeline de captura inclui agora um E2E específico com dois CNPJs numéricos publicados e casos negativos, distinto do smoke. A pesquisa alfanumérica v1.1 e a busca textual permanecem fora da fatia Protobuf v1; validar separadamente antes de declarar completude de #208.

**Motto:** menos páginas que explicam o FICHA; mais páginas nas quais se utiliza o FICHA.

## Recuperação de uma regressão do site publicado

Esta seção documenta o *rollback* mencionado na [ADR 0029](../docs/adr/0029-ssg-rust-public-site.md). É um procedimento de **código do site**, não de dados: não reverte snapshots, não altera `web/public/manifest.json` e não autoriza ETL, upload ao Internet Archive ou promoção de competência.

1. **Diferenciar falha de publicação de falha de consulta.** Conferir o `build → deploy → verify` da execução de [Deploy site](../.github/workflows/deploy.yml) para o SHA de `main` e abrir a home, `/ficha/sobre/`, `/ficha/css/site.css`, `/ficha/js/boot.mjs` e `/ficha/manifest.json`. Um `verify` verde comprova apenas o smoke HTTP e a identidade byte a byte do manifesto. Não comprova DuckDB-WASM, CORS/Range nem consulta efetiva; essa investigação está na [#208](https://github.com/franklinbaldo/ficha/issues/208).
2. **Preparar um revert mínimo em branch nova a partir da `main` atual.** Identificar o commit de `site/**` que introduziu a regressão e revertê-lo via PR, ou reaplicar os arquivos de uma revisão SSG comprovadamente boa, **sem reset/force-push da `main`**. Evitar incluir `etl/**`, manifesto ou mudanças de contrato de dados na reversão. Conferir o diff e o SHA exato antes de integrar.
3. **Validar o mesmo contrato que será publicado.** Rodar os testes Node em `site/scripts/finalize.test.mjs` e `site/tests/*.test.mjs`; executar `ssg check --config config.toml`, `ssg build --config config.toml` e `node scripts/finalize.mjs` a partir de `site/`. Conferir o CI do SHA da PR. O `verify` atual **exige** `#busca-form` na home e `mountSearch` em `boot.mjs`: voltar cegamente ao Marco 0 sem formulário falhará no smoke. Uma decisão de produto de remover a busca exige mudar também os testes/expectativas de modo explícito, com aprovação própria, não enfraquecê-los só para obter CI verde.
4. **Publicar pelo fluxo normal.** Com a PR validada e integrada, o `push` em `main` que altera `site/**` dispara `Deploy site`, produz `site/dist/ficha` e executa `verify` após deploy. Reabrir as URLs públicas e conferir o SHA da execução. Um build local, workflow disparado ou página HTTP 200 isolada não bastam para declarar recuperação.

Não tentar “voltar ao Astro” como atalho: a ADR 0029 o substituiu para o Pages, e uma reversão de stack exige decisão de produto distinta. Se o defeito for exclusivamente a leitura dos Parquets remotos, o rollback do shell não restaura por si só o transporte CORS+Range.
