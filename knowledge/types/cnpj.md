---
type: TypeSpec
title: Cnpj
parquet: cnpjs
manifest_file: cnpjs
description: Uma linha publicada em cnpjs.parquet; representa um estabelecimento de CNPJ com dados da empresa denormalizados.
---

# Cnpj

Contrato relacional de uma linha de `cnpjs.parquet`.

Os milhões de registros permanecem no Parquet publicado. Os documentos `type: Cnpj` do bundle são linhas representativas do mesmo shape e permitem ao `okf-parser` gerar JSON Schema, Zod e outras projeções sem criar uma segunda materialização dos dados.

Relaciona-se a uma [Raiz](raiz.md) por `cnpj_base`.
