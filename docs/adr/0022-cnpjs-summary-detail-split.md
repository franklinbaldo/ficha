# ADR 0022 — `cnpjs_summary.parquet` para lista de busca

**Status:** Proposed
**Data:** 2026-07-14
**Contexto:** docs/perf-plan-2026-05.md §4.2 (W4.2)

---

## Contexto

`cnpjs.parquet` carrega 40+ colunas; a lista de resultados de busca
(`SearchCNPJ.svelte`) usa ~10 delas (cnpj, razão social, UF, CNAE, capital,
nome fantasia, situação). Hoje a busca por nome roda `ILIKE '%...%'` direto
sobre `cnpjs.parquet` (ver `docs/vision-blockers-2026-07.md`, Dimensão 4), o
que derrota o bloom filter e baixa row groups inteiros mesmo para poucos
resultados.

## Decisão (proposta, não implementada)

Emitir `cnpjs_summary.parquet`: projeção de ~10 colunas de `cnpjs.parquet`,
sorted por `cnpj`, com bloom em `cnpj` e `razao_social_normalizada`.

- `EmpresaFicha.svelte` (detalhe) continua usando `cnpjs.parquet` completo.
- `SearchCNPJ.svelte` (lista) passaria a usar `cnpjs_summary.parquet`.

Estimativa do perf-plan: ~5× menor que `cnpjs.parquet`, cold-cache search
~5× mais rápida.

## Por que ainda não implementado

Sequenciamento do perf-plan (M0 → M2): a fase 3 do ETL precisava primeiro
completar um run E2E sem OOM antes de multiplicar outputs. Esse teste E2E
ainda está pendente (ver `docs/vision-blockers-2026-07.md`, Dimensão 1) — não
há código de `write_cnpjs_summary_parquet` em `etl/src/ficha_etl/transform.py`
nem entrada correspondente no schema de manifest (`manifest.py`).

## Consequências (se implementado)

- ✅ ~5× menos bytes por busca por nome no cold cache.
- ⚠️ Novo write no phase 3; schema bump; precisa coordenar com qualquer
  particionamento futuro de `cnpjs.parquet` (W1.3) se ambos landarem no mesmo
  release.
- ⚠️ `SearchCNPJ.svelte` precisa trocar a fonte da query de lista sem quebrar
  o caminho de detalhe (`EmpresaFicha.svelte`), que continua em
  `cnpjs.parquet`.

## Próximos passos

1. Implementar `write_cnpjs_summary_parquet` no ETL.
2. Adicionar entrada `cnpjs_summary` ao manifest schema.
3. Trocar a fonte de dados da lista em `SearchCNPJ.svelte` para
   `cnpjs_summary.parquet`, mantendo `cnpjs.parquet` para o detalhe.
