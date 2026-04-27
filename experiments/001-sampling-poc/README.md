# 001 — sampling-poc

**Status:** concluído (movido de `etl/sampling_poc.py` na reorganização inicial).

**Hipótese:** dá pra gerar tanto a camada analítica (Parquet) quanto a atômica (ZIP de JSONs por CNPJ) numa única passada usando Ibis + DuckDB + PyArrow.

**Método:** lê CSVs mockados (`data_sample/`) com layout RFB simulado, normaliza com Ibis, escreve Parquet via PyArrow e gera ZIP com um JSON por CNPJ.

**Resultado:** funciona em escala de amostra. Schemas Zod em `web/src/schemas/` foram derivados dessa estrutura.

**Limitações conhecidas:** caminhos hardcoded (`/workspace/...`), sem tratamento de erro, dados são mock.
