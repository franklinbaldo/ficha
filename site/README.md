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

