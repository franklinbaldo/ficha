# ADR 0011 — Não usar partitioning Hive-style nos Parquets

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

Considerei particionar os Parquets ao estilo Hive (`cnpjs/UF=AC/`, `cnpjs/raiz_prefix=01/`, etc.) para reduzir bytes baixados em queries filtradas.

## Decisão

**Não usar partitioning.** Em vez disso, confiar em:

1. **Sort interno** dos Parquets pela chave de acesso primária ([ADR 0008](0008-three-parquet-architecture.md))
2. **Bloom filters** em colunas frequentemente buscadas
3. **Row group statistics** (min/max) que o DuckDB usa automaticamente para pruning
4. **Múltiplos Parquets especializados** por padrão de acesso

## Por quê

DuckDB-WASM lê Parquet via HTTP range requests. Para uma query, ele:

1. Lê o footer (KB)
2. Aplica predicate pushdown via stats por row group
3. Aplica bloom filters quando disponíveis
4. Baixa só as colunas selecionadas dos row groups que sobraram

**Para lookup por CNPJ:** Parquet sorted-by-cnpj + bloom em cnpj equivale a um índice B-tree externo. Pruning eliminate ~99% dos row groups.

**Para busca por nome / agregações:** o Parquet `raizes.parquet` é pequeno (~150MB) o bastante pra full-scan ser aceitável. Particionamento por UF não ajudaria (queries não têm UF como predicado).

**Para "empresas no setor X":** se virar caso de uso, vira `cnae.parquet` em Tier 2 — não partição.

## Consequências

- ✅ ETL produz `N` arquivos finais (3-4), não `N × |partições|` (60+).
- ✅ Manifest simples — uma URL por Parquet.
- ✅ Internet Archive item simples — uploads diretos.
- ✅ DuckDB-WASM não precisa de path expansion (`*.parquet`) que tem limitações via HTTP.
- ⚠️ Queries com filter de baixa cardinalidade (UF, CNAE) leem mais bytes que um schema particionado. Aceitável dado que `raizes.parquet` é pequeno.

## Sinal pra reconsiderar

Se medirmos algum dos seguintes, abrir nova ADR:

- Latência de query mediana > 2s para acesso comum
- Row groups com pruning < 20% efetivo (ou seja, a maioria dos row groups precisa ser lida)
- `cnpjs.parquet` passar de 10GB

Até lá, partitioning fica como hipótese descartada com fundamento.

## Relação com outros ADRs

- Sucede a discussão original em [ADR 0008](0008-three-parquet-architecture.md) sobre layout dos Parquets.
- Não conflita com [ADR 0003](0003-schema-versioning.md) (versionamento de schema é ortogonal a layout físico).
