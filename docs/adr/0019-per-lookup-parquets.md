# ADR 0019 — Per-lookup parquets alongside lookups.json (W10)

**Status:** Accepted
**Data:** 2026-05-15

## Contexto

Conforme descrito no plano de performance (§10 / W10), existe a necessidade de realizar consultas analíticas e filtros sobre as tabelas de domínio (lookups) da RFB, como buscar CNAEs por prefixo de descrição ou buscar municípios.

Atualmente, o arquivo `lookups.json` é utilizado para renderização síncrona na interface de usuário. Embora ideal para a lâmina, o JSON não tem suporte nativo a índices colunares, o que não é ideal para SQL composability (filtros e agregações por descrição via DuckDB-WASM).

## Decisão

Publicar seis novos arquivos Parquet para cada tipo de lookup sob o prefixo `<snapshot>/lookups/<kind>.parquet`: `cnaes`, `motivos`, `municipios`, `naturezas`, `paises` e `qualificacoes`.

Esses Parquets vão **coexistir** com o `lookups.json` atual, atendendo casos de uso diferentes: JSON para renderização síncrona na UI, Parquet para consultas analíticas via DuckDB-WASM.

- **Schema:** `(codigo VARCHAR, descricao VARCHAR, descricao_normalizada VARCHAR)`.
- **Sort:** Ordenado por `codigo`.
- **Bloom filters:** Nas colunas `codigo` e `descricao_normalizada`.
- **Frontend:** O wiring desses Parquets no client será gerido através de `attachLookups(db, manifest)`, registrando-os diretamente no DuckDB.

## Por quê

- **SQL Composability:** Ter os lookups como Parquets separados permite que joins e agregações complexas funcionem nativamente (ex: `JOIN lookup_cnaes ON cnae_codigo`).
- **Pruning Eficiente:** O layout ordenado junto com os bloom filters em `codigo` e `descricao_normalizada` garante leituras de poucos kilobytes e range requests otimizados.
- **Dual Format:** Manter o `lookups.json` preserva o carregamento instantâneo da interface sem forçar downloads do DuckDB para mostrar uma descrição básica.

## Consequências

- ✅ Joins analíticos com domínios agora funcionam de forma fluida nas queries do frontend.
- ✅ Autocomplete ou busca textual direta em CNAEs ou municípios fica muito mais rápida.
- ⚠️ ETL precisa gerar 6 novos arquivos, aumentando levemente a contagem de itens no snapshot.
- ⚠️ O client agora requer a execução de `attachLookups` para atrelar as views/tabelas virtuais.

## Referências

- PR #34
- Plano de Performance `docs/perf-plan-2026-05.md` (§10 / W10)
