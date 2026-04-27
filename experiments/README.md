# Experiments

Provas de conceito, benchmarks e estudos descartáveis. Cada experimento vive em sua própria pasta numerada (`NNN-nome-curto/`) e tem um `README.md` com hipótese, método e resultado.

Versionados em `main` para preservar histórico — não são deletados depois de concluídos. Quando algo daqui virar produção, **migra** pra `etl/` ou `web/`, não move.

## Índice

| # | Experimento | Status | Conclusão |
|---|---|---|---|
| 001 | sampling-poc | concluído | PoC inicial: gera Parquet de amostra + ZIP de fichas JSON usando Ibis + DuckDB. Base do schema atual. |
| 002 | poc-acre | concluído | Recorte estadual (AC) com dados mockados. Validou abordagem por UF. |

## Próximos experimentos planejados

- `003-particionamento-uf` — medir tempo de range request DuckDB-WASM em Parquet particionado vs único
- `004-indice-nomes` — Parquet auxiliar ordenado por razão social pra autocomplete
- `005-tamanho-row-group` — impacto do `row_group_size` em latência de primeira query
