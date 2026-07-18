# Benchmark: migração do agregado `raizes` (`LIST(DISTINCT)`) para Ibis (2026-07-18)

**Contexto:** companheiro do [benchmark do `cnpjs`](ibis-cnpjs-benchmark-2026-07-18.md).
O [ADR 0017](adr/0017-ibis-shared-analytical-layer.md) marca o `raizes` como o
alvo **arriscado** de uma migração para Ibis, porque a agregação que monta
`ufs_atuacao`/`cnaes_principais_distintos` (uma lista distinta por `cnpj_base`) é
onde o OOM real de produção morava: `docs/perf-plan-2026-05.md` §1.1 documenta
que `LIST(DISTINCT …)` estourou 5.5 GiB com ~50M grupos (PR #24), porque o
hash-aggregate do DuckDB **não spilla** o estado de hash-set por grupo do
DISTINCT-dentro-do-LIST. Produção trocou por um **pre-dedup de dois passos**
(`SELECT DISTINCT` → `GROUP BY list()`), dois agregados comuns que o DuckDB
*consegue* spillar.

Este benchmark responde: **dá para migrar o `raizes` para Ibis sem reintroduzir
o OOM?**

**Harness:** [`etl/scripts/diagnostics/ibis_raizes_agg_benchmark.py`](../etl/scripts/diagnostics/ibis_raizes_agg_benchmark.py).
Gera `empresa` (um por grupo) + `estabelecimento` (vários por grupo, espalhados
por UFs/CNAEs), e constrói `(cnpj_base, ufs_atuacao, cnaes_principais_distintos)`
de quatro formas, todas dirigidas por `empresa` (a forma real de produção), sob
`threads=1` + `memory_limit` apertado. Verifica que as quatro produzem listas
**set-equivalentes** por grupo (checksum com `list_sort`).

## O achado central: o que o Ibis compila

```
t.uf.collect(distinct=True)          ->  ARRAY_AGG(DISTINCT uf) FILTER (WHERE uf IS NOT NULL)
                                          [mesma família DISTINCT-no-agregado que causou o OOM]
t.select(...).distinct()
   .group_by(...).agg(uf.collect())  ->  ARRAY_AGG(uf) sobre (SELECT DISTINCT ...)
                                          [idêntico à forma de dois passos de produção]
```

Ou seja: o Ibis **consegue** expressar a forma segura de produção (via
`.distinct().collect()`), mas a forma idiomática (`collect(distinct=True)`)
compila para a família DISTINCT-no-agregado.

## Resultados

DuckDB 1.5.2, Ibis 12.0.0, 16 GB RAM / 4 cores, `threads=1`. 5M grupos / 25M
linhas, `memory_limit=4 GB`. Estável em 2 execuções.

| Caminho | Forma compilada | Resultado | Pico de spill |
|---|---|---|---|
| `sql_naive` | `LIST(DISTINCT …)` (uma passada, ufs+cnaes, + join) | **OOM (falha)** | 5.3–5.5 GB |
| `sql_predup` | pre-dedup de dois passos (produção) | ok, 35s | 1.6 GB |
| `ibis_idiomatic` | `ARRAY_AGG(DISTINCT …) FILTER(…)` | ok, 29s | 0 B |
| `ibis_predup` | dois passos via Ibis | ok, 36s | 1.6 GB |

`ibis_predup` bate **exatamente** com o `sql_predup` de produção (**1.01× do
spill**) — como esperado, é a mesma forma compilada.

### Ressalva importante sobre o `sql_naive` OOM e o `ibis_idiomatic` 0-spill

Uma sonda isolada (só o agregado de UF, sem join, sem cnaes) mostrou que **as
três formas de agregado — inclusive `LIST(DISTINCT NULLIF(uf,''))` — rodam com
0 spill a 4 GB**. O OOM do `sql_naive` só aparece na query *completa* do raizes:
duas listas-distintas (ufs + cnaes) **mais** o join com `empresa`, tudo
pipelinado numa passada, mantém o estado de hash-set não-spillável dos dois
agregados em memória ao mesmo tempo. O pre-dedup quebra isso em pedaços que o
DuckDB materializa e spilla.

Consequência: o bom desempenho do `ibis_idiomatic` (0 spill) é **dependente da
forma do plano** que o Ibis gera para o join, não uma propriedade robusta do
`ARRAY_AGG(DISTINCT)`. Ele pertence à **mesma família** que estourou como
`sql_naive`. **Não tratar `collect(distinct=True)` como caminho de migração
seguro** só porque foi rápido aqui — pode regredir em outra escala ou versão do
DuckDB.

## Recomendação

- **`raizes` PODE ser migrado para Ibis** — pela forma explícita de dois passos
  (`.distinct()` → `.group_by().agg(col.collect())`), que compila para e
  benchmarka **idêntico** à produção (1.6 GB, 1.01×). Esse é o caminho seguro.
- **Não migrar via `collect(distinct=True)`.** Compila para a família
  DISTINCT-no-agregado que reproduz o OOM histórico; seu bom número aqui é
  dependente do plano, sem garantia de memória.
- O OOM histórico **se reproduz** a 1/10 da escala na forma de query realista —
  o risco que o ADR 0017 nomeou é real, e o pre-dedup de produção segue
  necessário e correto.
- **Confirmação definitiva:** rodar o harness em CI na escala de produção
  (`FICHA_BENCH_EMPRESA_ROWS=67000000`, `FICHA_BENCH_MEMORY_GB=9`), comparando
  `ibis_predup` vs `sql_predup`, antes de trocar o código de produção.

## Fecho: o que trava (e destrava) a migração completa para Ibis

Com este benchmark, o quadro fica completo:

| Alvo | Estado | Caminho |
|---|---|---|
| lookups | ✅ migrado | `write_lookup_parquets` |
| socios | ✅ migrado | `_socios_select_sql` (bit-a-bit) |
| cnpjs | evidência: baixo risco | join direto em Ibis (ver benchmark cnpjs) |
| raizes | evidência: viável **com regra** | dois passos via `.distinct().collect()`, nunca `collect(distinct=True)` |

A "última peça" não é um bloqueio, é uma **regra**: ao expressar listas-distintas
por grupo em Ibis, usar o pre-dedup de dois passos, não o distinct-collect
idiomático. Com isso, toda a camada analítica do ETL é migrável para Ibis sem
reintroduzir o OOM.

## Reproduzir

```bash
cd etl
uv run python scripts/diagnostics/ibis_raizes_agg_benchmark.py                 # smoke local
FICHA_BENCH_EMPRESA_ROWS=5000000 FICHA_BENCH_MEMORY_GB=4 \                      # reproduz o OOM
    uv run python scripts/diagnostics/ibis_raizes_agg_benchmark.py
FICHA_BENCH_EMPRESA_ROWS=67000000 FICHA_BENCH_MEMORY_GB=9 \                     # escala de produção (CI)
    uv run python scripts/diagnostics/ibis_raizes_agg_benchmark.py
```
