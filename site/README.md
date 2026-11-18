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
- `static/js/record.mjs`: consulta de CNPJ por competência, com DuckDB sob demanda e encerramento de recursos.
- `static/js/pages.mjs`: UI das páginas novas, sem resultados simulados.
- `static/js/search-app.mjs`: busca experimental da home, com links de resultado para a ficha.
- `scripts/finalize.mjs`: copia o manifesto original byte a byte e os módulos para o diretório de publicação.
- `tests/`: testes de catálogo, consulta, regressões e páginas.

## Estado funcional e limites

A navegação, o catálogo e os gráficos de **tamanho de arquivos declarados** funcionam a partir do manifesto do próprio site. A ficha e a comparação temporal executam consulta real **somente quando** os Parquets puderem ser acessados via CORS/Range. A [issue #208](https://github.com/franklinbaldo/ficha/issues/208) documenta uma falha desse transporte. Sem esse acesso, a UI mostra falha e ligação para a origem, não dados fictícios.

Os gráficos em Explorar descrevem arquivos, **não** quantidade de empresas, movimento econômico ou mapas por UF. Indicadores regionais demandam agregados com proveniência e contrato próprios. Não existe fallback opaco para uma API de terceiros.

O manifesto permanece canônico em `web/public/manifest.json` por enquanto. O finalizador confere `current`, integridade declarada, identidade exata dos bytes e substitui somente o PWA do SSG reconhecido. Esta implementação não altera ETL, uploads, snapshots, cron ou infraestrutura.

## Validações

```sh
node --test site/scripts/finalize.test.mjs
node --test site/tests/*.test.mjs
# Workflow deploy.yml: build SSG + HTTP smoke do site publicado
```

O smoke HTTP comprova páginas e arquivos publicados, **não** a consulta real. O teste end-to-end real de CNPJ numérico/alfanumérico segue como condição de promoção funcional em #208.

**Motto:** menos páginas que explicam o FICHA; mais páginas nas quais se utiliza o FICHA.
