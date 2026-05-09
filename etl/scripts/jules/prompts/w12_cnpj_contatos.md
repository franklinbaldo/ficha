# W12 — `cnpj_contatos.parquet` reverse contact lookup

## Context

Today contact data ships as 7 wide columns on `cnpjs.parquet`:
`ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
correio_eletronico` (`transform.py:459–460`). They're naturally a
multi-valued contact list collapsed wide. Externalize into a
parquet so reverse lookups become possible:

- "who owns this phone number?" (single bloom hit)
- "all CNPJs at @prefeitura.sp.gov.br" (bloom on email domain)
- "shared phone number across multiple CNPJs" (`GROUP BY valor
  HAVING count(*) > 1` — fraud signal)

The wide columns on `cnpjs.parquet` stay; this is the inverse
index, additive.

## Scope — ETL side

In `etl/src/ficha_etl/transform.py`, add
`write_cnpj_contatos_parquet(con, output_path)` and wire it into
`transform_snapshot` after `write_cnpjs_parquet`.

### Schema

Columns: `cnpj, cnpj_base, tipo, valor, posicao`
- `tipo ∈ {'telefone', 'fax', 'email'}`
- For phones: `valor = ddd_N || telefone_N`, `posicao ∈ {1, 2}`
- For fax: `valor = ddd_fax || fax`, `posicao = 0`
- For email: `valor = correio_eletronico`, `posicao = 0`

Sort by `(tipo, valor, cnpj)`. Bloom on `valor` and on
`split_part(valor, '@', 2)` (email domain). The latter enables
the public-sector mapping use case.

NB: do NOT include rows where the source field is NULL or empty.
The defensive filter goes in the WHERE clause, not in a separate
step.

### SQL shape (5 UNION ALL arms)

```sql
COPY (
  -- telefone_1
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
         cnpj_basico AS cnpj_base,
         'telefone' AS tipo,
         ddd_1 || telefone_1 AS valor,
         1::INTEGER AS posicao
  FROM estabelecimento
  WHERE telefone_1 IS NOT NULL AND telefone_1 <> ''
    AND ddd_1 IS NOT NULL AND ddd_1 <> ''
  UNION ALL
  -- telefone_2
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
         cnpj_basico,
         'telefone',
         ddd_2 || telefone_2,
         2::INTEGER
  FROM estabelecimento
  WHERE telefone_2 IS NOT NULL AND telefone_2 <> ''
    AND ddd_2 IS NOT NULL AND ddd_2 <> ''
  UNION ALL
  -- fax
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
         cnpj_basico,
         'fax',
         ddd_fax || fax,
         0::INTEGER
  FROM estabelecimento
  WHERE fax IS NOT NULL AND fax <> ''
    AND ddd_fax IS NOT NULL AND ddd_fax <> ''
  UNION ALL
  -- email
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
         cnpj_basico,
         'email',
         correio_eletronico,
         0::INTEGER
  FROM estabelecimento
  WHERE correio_eletronico IS NOT NULL AND correio_eletronico <> ''
  ORDER BY tipo, valor, cnpj
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
```

### Tests

In `etl/tests/test_transform.py`:

```python
def test_write_cnpj_contatos_parquet_shape(tmp_path, ...):
    # Setup estabelecimento with one row that has phone, fax, email.
    # Assert the parquet has 3 rows (or fewer if some fields empty),
    # one per tipo, with the expected (valor, posicao).
```

Extend `test_transform_snapshot_writes_lookups_and_3_parquets`
with one assertion on cnpj_contatos.parquet existence + ACME
expected rows.

## Scope — manifest

Add `cnpj_contatos` entry to `manifest.json` build with the new
URL.

## Scope — privacy posture

Update `etl/scripts/jules/prompts/` adjacent docs OR add a
docstring in the new function explaining: phones and emails are
PII, but RFB publishes them publicly already. This parquet is
just a re-shape of public data with no enrichment, no new
exposure. The function docstring is a fine place for this; no
ADR required if the prose is clear.

## Acceptance criteria

- `uv run pytest tests/` — passes including the new test.
- `uv run --directory etl ruff check src tests` — clean.
- Output parquet schema is `(cnpj VARCHAR, cnpj_base VARCHAR,
  tipo VARCHAR, valor VARCHAR, posicao INTEGER)`.
- Empty/NULL source fields are excluded (no rows where `valor`
  is empty).

## Plan reference

`docs/perf-plan-2026-05.md` §12 / W12 / Phase 7 PR 7b / M4.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2`.
- Open PR against `claude/ficha-perf-plan-v2` titled
  `feat: W12 cnpj_contatos.parquet reverse contact lookup (Phase 7 PR 7b / M4)`.
- PR body should reference this prompt and the plan §12.

## Out of scope

Do **not**:
- Drop the wide columns from `cnpjs.parquet`.
- Implement W7/W8/W11 in this PR.
- Normalize phone numbers further than `ddd || telefone` concatenation
  (no formatting, no country code prefix).
- Add new dependencies.
