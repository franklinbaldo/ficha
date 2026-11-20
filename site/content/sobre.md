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

O [fichário](/ficha/empresa/) oferece uma interface experimental de consulta por CNPJ e competência. A [história](/ficha/historia/) apresenta os snapshots existentes e uma comparação condicionada à leitura dos dados remotos. Uma diferença entre arquivos não prova quando ocorreu um evento cadastral.

O [acervo](/ficha/acervo/) apresenta os arquivos, tamanhos e hashes declarados no manifesto. [Explorar](/ficha/explorar/) exibe uma visualização dos tamanhos dos conjuntos realmente publicados, sem confundi-los com estatísticas populacionais sobre empresas.

## O que o sistema armazena

A camada atômica organiza registros por raiz de CNPJ em Protobuf/ZIP nos snapshots que declaram esses artefatos. A camada analítica utiliza Parquet e DuckDB-WASM para consulta no navegador. O projeto preserva arquivos no Internet Archive e publica o contrato de dados pelo [manifesto](/ficha/manifest.json).

A disponibilidade de um artefato no manifesto **não é prova de acesso remoto**. Em [teste com Chromium 152 na home publicada, em 19/11/2026](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-6999541728), a tentativa de ler um byte do `cnpjs.parquet` canônico no Internet Archive foi bloqueada por CORS (ausência de `Access-Control-Allow-Origin`). Esse teste mede o transporte no navegador, **não** a integridade do arquivo inteiro, nem uma consulta SQL de CNPJ; um HTTP Range 206 fora do navegador não remove a restrição. A busca por nome e CNPJ alfanumérico continua experimental via Parquet/DuckDB. A busca por CNPJ numérico completo com dígitos verificadores válidos segue outra rota, em Protobuf v1, sem Parquet: [um ensaio com Chromium no site publicado em 20/11/2026](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-7030287292) concluiu a consulta da Petrobras em 2026-07. Esse ensaio pontual não demonstra disponibilidade contínua, acesso a outras competências nem integridade de todos os ZIPs. Em caso de falha, a interface deve informar indisponibilidade, nunca inventar registros. O [catálogo](/ficha/acervo/) e o [manifesto](/ficha/manifest.json) permanecem como portas de consulta documental.

## Origem, atualidade e limitações

Cada ficha deve identificar a competência utilizada. A competência de referência do manifesto não implica que seja a última competência divulgada pela Receita. Dados de competências distintas não autorizam presumir continuidade entre períodos não arquivados, nem substituem certidões atuais.

## Código aberto

O projeto e as decisões técnicas estão em [franklinbaldo/ficha](https://github.com/franklinbaldo/ficha). Este site usa SSG Rust, HTML e JavaScript modular, com preservação dos dados separada da apresentação.
