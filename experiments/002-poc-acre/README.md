# 002 — poc-acre

**Status:** concluído (movido de `etl/poc_acre/` na reorganização inicial).

**Hipótese:** processar um recorte estadual (Acre — menor UF em volume) é viável end-to-end e valida o particionamento por UF.

**Método:** mock de CSVs RFB filtrados por UF=AC, ETL com DuckDB direto, gera `cnpjs_AC.parquet` + `cnpjs_AC.zip` (fichas JSON).

**Resultado:** abordagem por UF é prática. Particionamento real fica como experimento futuro (003).

**Limitações:** hardcoded paths, mock data, output não documentado quanto a tamanho/perf.
