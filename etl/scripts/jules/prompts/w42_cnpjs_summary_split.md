# W4.2 — Summary + detail parquet split for `cnpjs.parquet`

## Context

Currently `cnpjs.parquet` carries 40+ columns per estabelecimento.
The frontend's autocomplete / search list (`SearchCNPJ.svelte`)
displays only ~10 of those columns. On cold-cache, every search
downloads enough row-groups to retrieve the full wide-row payload
even though only a few columns are visible.

W4.2 splits the data into:
- **`cnpjs_summary.parquet`** — narrow, sorted, bloomed for the
  exact-cnpj lookup path that W4.1 (already merged in commit
  3d2c4c1) created.
- **`cnpjs.parquet`** — keeps the full schema for the lâmina detail
  view (`EmpresaFicha.svelte`).

## Scope — ETL

In `etl/src/ficha_etl/transform.py`:

1. Add `write_cnpjs_summary_parquet(con, output_path)` that
   projects ~10 columns from `cnpjs.parquet`-equivalent JOIN:
   ```
   cnpj, cnpj_base, razao_social, razao_social_normalizada,
   nome_fantasia, uf, municipio_codigo, municipio_nome,
   cnae_principal_codigo, cnae_principal_descricao, capital_social
   ```
   Sort by `cnpj`. Bloom on `cnpj` and `razao_social_normalizada`.
   ROW_GROUP_SIZE 200000.

2. Wire it into `transform_snapshot` after `write_cnpjs_parquet`.
   Reuse the same JOINs the cnpjs writer uses; don't introduce a
   separate scan.

3. Update `etl/src/ficha_etl/manifest.py` to include
   `cnpjs_summary.parquet` in the manifest entry.

4. Update `etl/src/ficha_etl/mirror.py` if it has a
   filename-→-purpose mapping for the IA upload step.

## Scope — Frontend

In `web/src/lib/analytical.ts`:

1. Add `attachCnpjsSummary(db, url)` mirror of `attachCnpjs` but
   registers a separate URL and creates `VIEW cnpjs_summary`.

2. In `SearchCNPJ.svelte`, switch the search query to use
   `cnpjs_summary` for both branches (length-14 exact and ILIKE
   name search). The lâmina detail view (`EmpresaFicha.svelte`)
   keeps using `cnpjs` (full schema).

3. Update the snapshot manifest type
   (`web/src/schemas/v1/manifest.ts`) to include the new
   `cnpjs_summary` entry.

## Scope — Tests

- ETL: extend `test_transform_snapshot_writes_lookups_and_4_parquets`
  → make it a 5-parquet test. Assert summary has expected columns
  and matches cnpjs row count (or contains a known row).
- Frontend: extend the existing search test to verify
  `cnpjs_summary` is queried (not `cnpjs`) for the search path.

## Acceptance criteria

- `uv run pytest etl/tests/` — all passes
- `bun run build && bunx astro check` (in `web/`) — clean
- New parquet ~5× smaller than `cnpjs.parquet` (verify in test
  via row-bytes ratio)
- Manifest schema bumped (or backwards-compatible additive entry —
  prefer additive)

## Plan reference

`docs/perf-plan-2026-05.md` §4.2 / W4.2 / Phase 5 PR 5b / M2.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2`.
- Open PR against `claude/ficha-perf-plan-v2` titled
  `perf: W4.2 cnpjs_summary.parquet split (Phase 5 PR 5b)`.

## Out of scope

- Don't change `cnpjs.parquet` schema (keep it as-is for the lâmina).
- Don't touch the ranking / scoring of search results (UI layer).
- Don't add new dependencies.
