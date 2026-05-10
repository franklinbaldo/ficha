# W11 — `cnpj_cnaes.parquet` association table

## Context

Today secondary CNAEs ship as a `VARCHAR[]` array column on
`cnpjs.parquet` (`cnae_secundario_codigos`). That preserves
registration order but makes position-aware queries expensive —
"which companies have CNAE X as their *primary* activity?" or
"rank-2 CNAEs of restaurants" requires `unnest` + filter over 60M
rows.

W11 externalizes the many-to-many into a dedicated parquet, same
architectural pattern as W7 (enderecos) and W8 (pessoas) in the
plan. The denormalized array on `cnpjs.parquet` stays — it's still
the cheap path for the lâmina renderer. `cnpj_cnaes.parquet` is
the inverse/analytical index.

## Scope — ETL side

In `etl/src/ficha_etl/transform.py`, add a new function
`write_cnpj_cnaes_parquet(con, output_path)` and wire it into
`transform_snapshot` after `write_cnpjs_parquet`.

### Schema

Columns: `cnpj, cnpj_base, cnae_codigo, posicao`
- `cnpj` = `cnpj_basico || cnpj_ordem || cnpj_dv` (full 14-char)
- `cnpj_base` = `cnpj_basico`
- `cnae_codigo` = the CNAE code (string)
- `posicao` = `0` for the primary (`cnae_fiscal_principal`),
  `1, 2, …` for each secondary in registration order from
  `cnae_fiscal_secundaria`

Sort by `(cnae_codigo, posicao, cnpj_base)`. Bloom filters on
`cnae_codigo` and `cnpj_base`. NB: do NOT add a separate
`is_principal` boolean — the `posicao = 0` filter already
benefits from row-group min/max pruning given the sort, and a
bloom on a 2-value column is meaningless.

### SQL shape

```sql
COPY (
  SELECT
    cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
    cnpj_basico AS cnpj_base,
    cnae_fiscal_principal AS cnae_codigo,
    0::INTEGER AS posicao
  FROM estabelecimento
  WHERE cnae_fiscal_principal IS NOT NULL
    AND cnae_fiscal_principal <> ''
  UNION ALL
  SELECT
    cnpj_basico || cnpj_ordem || cnpj_dv,
    cnpj_basico,
    trim(s.value) AS cnae_codigo,
    s.idx::INTEGER AS posicao
  FROM estabelecimento,
       LATERAL (
         SELECT idx, unnest AS value
         FROM (
           SELECT generate_subscripts(arr, 1) AS idx, unnest(arr) AS unnest
           FROM (SELECT str_split(cnae_fiscal_secundaria, ',') AS arr) t
         )
       ) s
  WHERE cnae_fiscal_secundaria IS NOT NULL
    AND cnae_fiscal_secundaria <> ''
  ORDER BY cnae_codigo, posicao, cnpj_base
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
```

DuckDB syntax for `unnest WITH ORDINALITY` may need tweaking;
the shape is what matters. If `generate_subscripts(arr, 1)` doesn't
work, use `unnest(arr, recursive=>false)` plus a row_number window,
or `LATERAL(SELECT row_number() OVER ()…)`.

### Tests

In `etl/tests/test_transform.py`, add a focused test:

```python
def test_write_cnpj_cnaes_parquet_position_ordering(tmp_path, ...):
    # Setup estabelecimento with one row whose cnae_fiscal_secundaria
    # is "5611201,4711301,9311500" (3 secondaries in known order).
    # Assert that cnpj_cnaes parquet has 4 rows for that cnpj
    # (1 primary + 3 secondaries) with posicao 0,1,2,3 in order.
```

Also extend `test_transform_snapshot_writes_lookups_and_3_parquets`
to assert `cnpj_cnaes.parquet` exists and the ACME matriz row
yields the expected `(cnae_codigo, posicao)` pairs.

## Scope — manifest

Update `etl/src/ficha_etl/manifest.py` (or wherever
`manifest.json` is built) to include the new parquet:

```json
"cnpj_cnaes": {
  "url": ".../cnpj_cnaes.parquet",
  "sort": ["cnae_codigo", "posicao", "cnpj_base"]
}
```

## Scope — frontend (optional, can defer)

Don't add a frontend query in this PR unless it's trivial. The
parquet is queryable via `db.registerFileURL` + `attachX` pattern
once W10 lands. Adding the wiring without a UI use case is
premature.

## Acceptance criteria

- `uv run pytest tests/` — all tests pass + new test for position
  ordering.
- `uv run --directory etl ruff check src tests && uv run --directory etl ruff format --check src tests` — clean.
- The output parquet has the expected schema (4 columns), expected
  row count (≈ sum of distinct CNAE-codes per estabelecimento, or
  ~3× total estabelecimento rows on average).
- `WHERE posicao = 0` works correctly (returns primaries only).

## Plan reference

`docs/perf-plan-2026-05.md` §11 / W11 / Phase 7 PR 7a / M4.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2`.
- Open PR against `claude/ficha-perf-plan-v2` titled
  `feat: W11 cnpj_cnaes.parquet position-aware association (Phase 7 PR 7a / M4)`.
- PR body should reference this prompt file and `docs/perf-plan-2026-05.md` §11.

## Out of scope

Do **not**:
- Touch `cnpjs.parquet`'s denormalized arrays (`cnae_secundario_codigos`,
  `cnae_secundario_descricoes`) — they stay.
- Implement W12 / W7 / W8 in this PR.
- Add the frontend wiring beyond what the manifest update requires.
- Add new dependencies.
