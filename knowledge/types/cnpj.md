---
type: TypeSpec
title: Cnpj
description: Uma linha publicada em cnpjs.parquet; representa um estabelecimento de CNPJ com dados da empresa denormalizados.
---

# Cnpj

Contrato relacional de uma linha de `cnpjs.parquet`.

O documento descreve o tipo; os milhões de registros permanecem no Parquet publicado. A declaração física adjacente `cnpj.schema.sql` é consumida pelo `okf-parser` (RFC 0006) para gerar as projeções DuckDB, JSON Schema, Zod e Pydantic.

Relaciona-se a uma [Raiz](raiz.md) por `cnpj_base`.
