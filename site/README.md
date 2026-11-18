---
type: Documentation
title: Site público mínimo do FICHA (SSG Rust)
description: Primeira publicação estática, sem busca remota e com manifesto RFB intacto
---

# Site público mínimo

Primeira fatia do FICHA: home, Sobre, competência indicada pelo manifesto e links de dados. SSG Rust v0.0.75, sem Astro, Svelte, DuckDB, JavaScript no navegador ou consulta simulada. [Direção Ficha #269](https://github.com/franklinbaldo/ficha/issues/269).

O workflow `.github/workflows/deploy.yml` testa SSG check/build em PRs que alteram `site/**`; após merge na `main`, publica Pages automaticamente e verifica HTTP, links, CSS e a identidade **byte a byte** do `web/public/manifest.json`. O PWA `manifest.json` gerado pelo SSG deve coincidir com a estrutura conhecida antes de ser removido. A competência aparece no HTML a partir do próprio manifesto versionado.

Rollback: revert do merge desta fatia. O workflow anterior Astro pode ser restaurado pelo mesmo revert. Nenhuma rotina de ETL, upload ao Internet Archive, publicação de dados ou cron foi alterada.

A busca real e seus gates (CORS/Range, Protobuf, DuckDB-WASM, alfa, corrida A→B) seguem em [#208](https://github.com/franklinbaldo/ficha/issues/208) e [#276](https://github.com/franklinbaldo/ficha/pull/276). Não são anunciados como funcionais nesta versão.
