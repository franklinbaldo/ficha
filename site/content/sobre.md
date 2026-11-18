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

## O que já está disponível

A [página inicial](/ficha/) apresenta a competência apontada pelo manifesto público. O [manifesto de dados](/ficha/manifest.json) contém referências aos arquivos e seus metadados. A origem arquivística e a disponibilidade de acesso podem variar com o tempo.

## O que esta versão ainda não faz

A interface de pesquisa por CNPJ numérico, alfanumérico ou razão social **não está habilitada**. O projeto estuda a consulta no navegador, inclusive os formatos Parquet e Protobuf, mas a existência desses arquivos não demonstra que um navegador consiga lê-los diretamente de seus endereços arquivísticos. Não alteramos nem produzimos os dados de origem nesta publicação.

## Código e documentação

O código e as decisões técnicas estão em [franklinbaldo/ficha](https://github.com/franklinbaldo/ficha). A publicação do site é independente das rotinas de ETL, upload ao Internet Archive ou promoção de competências.
