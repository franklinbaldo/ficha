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

**Cada empresa tem uma história.** O FICHA, *Fichário de Identificação de CNPJ Hospedado no Archive*, é um projeto de acesso a dados públicos do CNPJ da Receita Federal, preservados por competência.

O que interessa não é apenas descobrir como uma empresa aparece em uma lista. É saber **de quando é o registro**, de onde veio e quais observações sobrevivem no acervo histórico.

## As duas portas de entrada

O [fichário](/ficha/empresa/) permite consultar uma empresa em determinada competência. A [história](/ficha/historia/) permite selecionar snapshots existentes e comparar diferenças observadas. Uma diferença entre arquivos não prova quando ocorreu um evento cadastral.

O [acervo](/ficha/acervo/) apresenta os arquivos, tamanhos e hashes declarados no manifesto. [Explorar](/ficha/explorar/) exibe uma visualização dos tamanhos dos conjuntos realmente publicados, sem confundi-los com estatísticas populacionais sobre empresas.

## O que o sistema armazena

A camada atômica organiza registros por raiz de CNPJ em Protobuf/ZIP nos snapshots que declaram esses artefatos. A camada analítica utiliza Parquet e DuckDB-WASM para consulta no navegador. O projeto preserva arquivos no Internet Archive e publica o contrato de dados pelo [manifesto](/ficha/manifest.json).

A disponibilidade de um artefato no manifesto **não é prova de acesso remoto**. Nem o êxito do deploy significa que uma consulta real funcionará: o Internet Archive pode não permitir as leituras CORS/HTTP Range exigidas pelo navegador. Nessa situação o FICHA deve exibir indisponibilidade, nunca registros inventados.

## Origem, atualidade e limitações

Cada ficha deve identificar a competência utilizada. A competência de referência do manifesto não implica que seja a última competência divulgada pela Receita. Dados de competências distintas não autorizam presumir continuidade entre períodos não arquivados, nem substituem certidões atuais.

## Código aberto

O projeto e as decisões técnicas estão em [franklinbaldo/ficha](https://github.com/franklinbaldo/ficha). Este site usa SSG Rust, HTML e JavaScript modular, com preservação dos dados separada da apresentação.
