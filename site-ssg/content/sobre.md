---
type: Page
name: Sobre o FICHA
title: Sobre — FICHA
description: Como o FICHA organiza acesso a dados públicos de CNPJ.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/sobre/
layout: page
---

# Sobre o FICHA

**FICHA** significa *Fichário de Identificação de CNPJ Hospedado no Archive*. O projeto facilita a consulta aos dados públicos da Receita Federal e a preservação dos conjuntos por competência.

## Duas camadas de dados

A camada atômica armazena registros por raiz de CNPJ em arquivos Protobuf/ZIP quando declarados pelo snapshot. A camada analítica utiliza arquivos Parquet e DuckDB-WASM para cruzamentos no navegador. A disponibilidade e a versão de cada artefato dependem do manifesto publicado; sua declaração não é prova de acesso remoto.

## Fontes, competência e limites

Os dados vêm das bases públicas da Receita Federal e são preservados no Internet Archive. A interface não modifica as informações de origem e cada snapshot representa um estado no tempo. Consulte o [manifesto público](https://franklinbaldo.github.io/ficha/manifest.json) para ver competência, arquivos e verificações disponíveis.

## Código

O projeto é aberto e está disponível em [github.com/franklinbaldo/ficha](https://github.com/franklinbaldo/ficha).
