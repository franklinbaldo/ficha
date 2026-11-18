---
type: Documentation
title: Site público do FICHA (SSG Rust)
description: Publicação estática com busca client-side desacoplada e manifesto RFB intacto
---

# Site público FICHA (SSG Rust)

Site estático construído com **SSG Rust v0.0.75**, HTML nativo, CSS e JavaScript modular para busca client-side ([Direção Ficha #269](https://github.com/franklinbaldo/ficha/issues/269)).

## Estrutura

- `content/`: Páginas Markdown (`index.md` com a home e busca, `sobre.md`).
- `templates/`: Templates HTML Tera (`index.html`, `page.html`, etc.).
- `static/css/`: Folhas de estilo nativas (`site.css`).
- `static/js/`: Módulos ES de busca e dados (`boot.mjs`, `search-app.mjs`, `duckdb-runtime.mjs`, `query-core.mjs`, `dataset.mjs`).
- `scripts/`: Finalizador de build (`finalize.mjs`), cópia de assets, remoção de PWA e validações.
- `tests/`: Bateria de testes de integridade e regressão do site e módulos de dados.

O workflow `.github/workflows/deploy.yml` valida os testes em PRs que alteram `site/**`; após merge na `main`, publica automaticamente no GitHub Pages e executa HTTP smoke de integridade.
