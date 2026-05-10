# W10 — Per-lookup parquets + frontend `attachLookups`

## Context

Today FICHA ships lookup tables (cnaes, motivos, municipios,
naturezas, paises, qualificacoes) in a single `lookups.json` blob,
loaded synchronously by the frontend for codigo→descricao render glue.

That works for *display* but blocks SQL queries that filter or
aggregate *by description*: filtering "all CNPJs in cities matching
'Bras%'" requires a JS-side name→codigo step today, which the
frontend doesn't have.

W10 adds a per-lookup parquet for each kind so DuckDB-WASM can JOIN
directly. `lookups.json` stays — it's still the right format for
synchronous render glue (DuckDB-WASM cold-start is hundreds of ms;
results need the codigo→descricao map immediately). Both can
coexist; the parquet is for SQL composition, the JSON for instant
render.

## Scope — ETL side

In `etl/src/ficha_etl/transform.py`:

1. Add `write_lookup_parquets(con, output_dir)` after
   `write_lookups_json`. Iterate `_LOOKUP_KINDS`:
   ```python
   for kind in _LOOKUP_KINDS:
       (output_dir / "lookups").mkdir(exist_ok=True)
       con.execute(
           "COPY (SELECT codigo, descricao, "
           "UPPER(strip_accents(descricao)) AS descricao_normalizada "
           f"FROM lookup_{kind}) "
           "TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)",
           [str(output_dir / "lookups" / f"{kind}.parquet")],
       )
   ```
   Sort by `codigo`; one row group per file is fine (largest is ~5500 rows).

2. Wire `write_lookup_parquets` into `transform_snapshot` after
   `write_lookups_json` is called.

3. Update the manifest builder (`etl/src/ficha_etl/manifest.py` or
   wherever `manifest.json` is assembled) to include the lookups
   directory. Manifest entry shape (analogous to existing parquet entries):
   ```json
   "lookups": {
     "cnaes": { "url": "…/lookups/cnaes.parquet" },
     "motivos": { "url": "…/lookups/motivos.parquet" },
     …
   }
   ```

4. Update the upload step (`etl/src/ficha_etl/upload.py`) to push
   the new files to IA. The existing infrastructure should handle
   directory uploads — verify and adjust.

5. Tests in `etl/tests/test_transform.py`:
   - Extend `test_transform_snapshot_writes_lookups_and_3_parquets`
     to also assert each `lookups/<kind>.parquet` exists, has the
     expected schema (codigo, descricao, descricao_normalizada),
     and contains the expected row count from
     `LOOKUP_FIXTURES[kind]`.

## Scope — frontend side

In `web/src/lib/analytical.ts`:

1. Add `attachLookups(db, manifest)` that registers each lookup
   parquet as a DuckDB-WASM file URL and creates a view per kind:
   ```typescript
   for (const [kind, info] of Object.entries(manifest.lookups)) {
     await db.registerFileURL(`${kind}.parquet`, info.url, ...);
     await conn.query(`CREATE OR REPLACE VIEW lookup_${kind} AS SELECT * FROM '${kind}.parquet'`);
   }
   ```
2. Don't touch `attachCnpjs` — that path stays.
3. Don't drop `lookups.json` — both keep working.

In `web/src/components/SearchCNPJ.svelte` or wherever the snapshot
manifest is loaded after `attachCnpjs`, also call `attachLookups`.

## Acceptance criteria

- ETL: `uv run pytest etl/tests/` passes; lookups/*.parquet files
  appear in test output_dir; each has 3 columns, correct row count.
- ETL lint: `uv run --directory etl ruff check src tests` clean.
- Frontend: `bun run build && bunx astro check` clean.
- Smoke: at runtime, query
  `SELECT codigo, descricao FROM lookup_municipios WHERE descricao_normalizada LIKE 'BRAS%'`
  in DuckDB-WASM works and returns expected rows.

## Plan reference

`docs/perf-plan-2026-05.md` §10 / W10 / Phase 6 / M4.

This is the *first* M4 entry — validates the `attachLookups`
pattern that later parquets (W11 cnpj_cnaes, W12 cnpj_contatos, W7
enderecos, W8 pessoas) reuse.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2` (the active perf-plan
  working branch in PR #31). Why: §10 references the existing plan
  document and the schemas currently tracked there.
- Open PR against `claude/ficha-perf-plan-v2` titled
  `feat: W10 per-lookup parquets + attachLookups (Phase 6 / M4)`.
  The maintainer will integrate it into the working branch first;
  M4 work consolidates there before reaching main.
- PR body should reference this prompt file and the plan §10.

## Out of scope

Do **not**:
- Drop `lookups.json` or change its schema.
- Drop the denormalized `*_descricao` columns from
  `cnpjs.parquet` / `raizes.parquet` / `socios.parquet`. Per
  plan §10.4 the per-lookup parquet is *additive*, not a stepping
  stone to denormalization removal.
- Implement W11 / W12 / W7 / W8 in this PR — those are separate
  workstreams.
- Add a build step that renders descriptions client-side for
  components that already get them denormalized.
