# ADR 0006 — Validação pragmática (não Great Expectations)

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

ETL ingere dump da RFB mensal. Layouts mudam silenciosamente, contagens variam, campos podem virar nulos. Precisamos detectar problemas antes de publicar um snapshot ruim.

Great Expectations (GX) é o framework canônico de Data Quality em Python. Avaliado.

## Decisão

**Não adotar GX no v0.1.** Usar asserts SQL simples no `etl/src/ficha_etl/transform.py` direto sobre o DuckDB que já está em uso.

```python
def validate_parquet(path: Path) -> None:
    rows = duckdb.sql(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    assert rows > 50_000_000, f"Parquet com poucas linhas: {rows}"
    nulls = duckdb.sql(
        f"SELECT COUNT(*) FROM '{path}' WHERE cnpj IS NULL"
    ).fetchone()[0]
    assert nulls == 0, f"{nulls} CNPJs nulos"
    # ... 5-10 checagens críticas
```

## Por que não GX

| Critério | GX | Asserts SQL |
|---|---|---|
| Deps adicionais | ~50 pacotes | 0 (DuckDB já está aqui) |
| Tempo de execução | minutos | ms |
| Curva de aprendizado | Datasource → Asset → BatchRequest → Checkpoint → Validator | `assert` |
| Vale para 5-10 regras simples | over-engineering | adequado |

## Quando reconsiderar

Se o número de regras passar de **~15-20**, ou se quisermos **trend analysis** (comparar snapshot atual vs histórico), avaliar:

1. **Pandera** — schema-as-class Python, ~1 dep, integra polars/pandas. Primeira escolha.
2. **dbt-style SQL tests** — `tests/uniqueness_cnpj.sql` retorna 0 linhas se OK. Zero deps novas.
3. **soda-core** — DSL YAML mais leve que GX.
4. **GX** — só se vendermos para um time de dados que já usa GX.

## Drift detection futura

O `manifest.json` (ver [ADR 0003](0003-schema-versioning.md)) carregará estatísticas básicas (`row_count`, `null_pct_per_column`, `min_max_capital_social`). Diff entre manifests sucessivos detecta drift sem framework algum.

## Consequências

- ✅ ETL inicial enxuto, fácil de debugar.
- ✅ Dep tree do `etl/` controlado.
- ⚠️ Se DQ virar prioridade alta, refatorar para Pandera vai custar 1 dia.
- ⚠️ Sem Data Docs HTML — aceitável (logs estruturados resolvem).
