---
type: View
name: empresas_ativas
inputs:
  - Cnpj
output: Cnpj
purpose: Filtrar estabelecimentos cuja situação cadastral publicada é ativa.
sql: |
  SELECT *
  FROM cnpjs
  WHERE situacao_cadastral = '02'
---

# Empresas ativas

View de conveniência. Não é uma interpretação canônica nem uma nova fonte de dados; é uma query nomeada sobre `cnpjs.parquet` e pode ser executada diretamente em DuckDB/DuckDB-WASM.

Usa o tipo [Cnpj](../types/cnpj.md).
