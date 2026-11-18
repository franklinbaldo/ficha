---
type: Page
name: Sobre o FICHA
title: Sobre — FICHA
description: Fontes, competências e limites do fichário público.
language: pt-BR
permalink: https://franklinbaldo.github.io/ficha/sobre/
layout: page
---

# Sobre o FICHA

O **FICHA**, Fichário de Identificação de CNPJ Hospedado no Archive, organiza referências aos dados públicos do Cadastro Nacional da Pessoa Jurídica (CNPJ) da Receita Federal, preservados por competência.

## Duas camadas de dados

A camada atômica armazena registros por raiz de CNPJ em arquivos Protobuf/ZIP quando declarados pelo snapshot. A camada analítica utiliza arquivos Parquet e DuckDB-WASM para cruzamentos no navegador. A disponibilidade e a versão de cada artefato dependem do manifesto publicado; sua declaração não é prova de acesso remoto.

## O que já está disponível

A [página inicial](/ficha/) apresenta a competência apontada pelo manifesto público e a interface de busca. O [manifesto de dados](/ficha/manifest.json) contém referências aos arquivos e seus metadados. A origem arquivística e a disponibilidade de acesso podem variar com o tempo.

## Código e documentação

O código e as decisões técnicas estão em [franklinbaldo/ficha](https://github.com/franklinbaldo/ficha). A publicação do site é independente das rotinas de ETL, upload ao Internet Archive ou promoção de competências.
