---
type: Documentation
title: Site público do FICHA (SSG Rust)
description: Publicação estática com busca client-side desacoplada e manifesto RFB intacto
---

# Site público FICHA (SSG Rust)

Site estático construído com **SSG Rust v0.0.75**, HTML nativo, CSS e JavaScript modular para busca client-side ([Direção Ficha #269](https://github.com/franklinbaldo/ficha/issues/269)). A [#297](https://github.com/franklinbaldo/ficha/pull/297) consolidou os módulos da antiga árvore shadow `site-ssg/` em `site/`, eliminando a duplicação.

**Estado de prova (18/11/2026):** o [workflow pós-merge #44347752600](https://github.com/franklinbaldo/ficha/actions/runs/44347752600), já com o ajuste do smoke na [#298](https://github.com/franklinbaldo/ficha/pull/298), concluiu build, deploy e verificação HTTP com sucesso. Isso atesta o shell publicado, JS encontrado e identidade do manifesto conforme o smoke; **não atesta consulta real** com DuckDB-WASM/Worker, CORS+Range ou Parquets arquivados. A lacuna funcional permanece na [#208](https://github.com/franklinbaldo/ficha/issues/208). Não confundir formulário com busca utilizável.

## Estrutura

- `content/`: Páginas Markdown (`index.md` com a home e busca, `sobre.md`).
- `templates/`: Templates HTML Tera (`index.html`, `page.html`, etc.).
- `static/css/`: Folhas de estilo nativas (`site.css`).
- `static/js/`: Módulos ES de busca e dados (`boot.mjs`, `search-app.mjs`, `duckdb-runtime.mjs`, `query-core.mjs`, `dataset.mjs`).
- `scripts/`: Finalizador de build (`finalize.mjs`), cópia de assets, remoção de PWA e validações.
- `tests/`: Bateria de testes de integridade e regressão do site e módulos de dados.

O workflow `.github/workflows/deploy.yml` valida os testes em PRs que alteram `site/**`; após merge na `main`, publica automaticamente no GitHub Pages e executa HTTP smoke de integridade.

## Recuperação de uma regressão do site publicado

Esta seção documenta o *rollback* mencionado na [ADR 0029](../docs/adr/0029-ssg-rust-public-site.md). É um procedimento de **código do site**, não de dados: não reverte snapshots, não altera `web/public/manifest.json` e não autoriza ETL, upload ao Internet Archive ou promoção de competência.

1. **Diferenciar falha de publicação de falha de consulta.** Conferir o `build → deploy → verify` da execução de [Deploy site](../.github/workflows/deploy.yml) para o SHA de `main` e abrir a home, `/ficha/sobre/`, `/ficha/css/site.css`, `/ficha/js/boot.mjs` e `/ficha/manifest.json`. Um `verify` verde comprova apenas o smoke HTTP e a identidade byte a byte do manifesto. Não comprova DuckDB-WASM, CORS/Range nem consulta efetiva; essa investigação está na [#208](https://github.com/franklinbaldo/ficha/issues/208).
2. **Preparar um revert mínimo em branch nova a partir da `main` atual.** Identificar o commit de `site/**` que introduziu a regressão e revertê-lo via PR, ou reaplicar os arquivos de uma revisão SSG comprovadamente boa, **sem reset/force-push da `main`**. Evitar incluir `etl/**`, manifesto ou mudanças de contrato de dados na reversão. Conferir o diff e o SHA exato antes de integrar.
3. **Validar o mesmo contrato que será publicado.** Rodar os testes Node em `site/scripts/finalize.test.mjs` e `site/tests/*.test.mjs`; executar `ssg check --config config.toml`, `ssg build --config config.toml` e `node scripts/finalize.mjs` a partir de `site/`. Conferir o CI do SHA da PR. O `verify` atual **exige** `#busca-form` na home e `mountSearch` em `boot.mjs`: voltar cegamente ao Marco 0 sem formulário falhará no smoke. Uma decisão de produto de remover a busca exige mudar também os testes/expectativas de modo explícito, com aprovação própria, não enfraquecê-los só para obter CI verde.
4. **Publicar pelo fluxo normal.** Com a PR validada e integrada, o `push` em `main` que altera `site/**` dispara `Deploy site`, produz `site/dist/ficha` e executa `verify` após deploy. Reabrir as URLs públicas e conferir o SHA da execução. Um build local, workflow disparado ou página HTTP 200 isolada não bastam para declarar recuperação.

Não tentar “voltar ao Astro” como atalho: a ADR 0029 o substituiu para o Pages, e uma reversão de stack exige decisão de produto distinta. Se o defeito for exclusivamente a leitura dos Parquets remotos, o rollback do shell não restaura por si só o transporte CORS+Range.
