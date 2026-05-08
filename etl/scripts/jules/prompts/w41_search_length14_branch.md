# W4.1 — Branch search by CNPJ length-14 in `SearchCNPJ.svelte`

## Context

`web/src/components/SearchCNPJ.svelte:80–82` has a single search query
that ORs `cnpj LIKE '%…%'` with `razao_social ILIKE '%…%'`:

```sql
SELECT cnpj, razao_social, nome_fantasia, uf, …
FROM cnpjs
WHERE cnpj LIKE ?
   OR razao_social ILIKE ?
LIMIT 20
```

Both predicates have leading `%`, so neither hits the bloom filter
on `cnpj` (per ADR 0008). Every search downloads every row group of
`cnpjs.parquet` (~1 GB compressed). On a typed-prefix workflow this
is ~1 GB per query — wasteful and slow on cold cache.

The fix: branch on whether the user's input parses as a complete
14-digit CNPJ. If yes, do an *exact-match* query that hits the bloom
filter; if no, fall back to the current ILIKE. Result: 10× lower
bytes on the dominant typed-CNPJ path.

## Scope

In `web/src/components/SearchCNPJ.svelte`, modify the `search()`
function (lines ~60–93 currently). The existing `stripCNPJ()`
helper already strips non-digits; reuse it.

Logic:
1. `const clean = stripCNPJ(cnpj);`
2. If `clean.length === 14`:
   ```sql
   SELECT … FROM cnpjs WHERE cnpj = ? LIMIT 1
   ```
   Pass `clean` as the bind. This hits the bloom filter on `cnpj`
   and downloads ~1 row group (~MB).
3. Otherwise:
   ```sql
   SELECT … FROM cnpjs WHERE razao_social ILIKE ? LIMIT 20
   ```
   Pass `%${cnpj.trim()}%` as the bind. Same as today's name-search
   path, just isolated.
4. Keep prepared statements + parameter binding (no SQL injection
   risk — already the existing pattern).
5. Preserve the existing `EmpresaRow` type usage and result mapping.

## Acceptance criteria

- Type a complete CNPJ (14 digits, with or without punctuation) →
  the search query is `WHERE cnpj = ?`, exact match.
- Type a partial CNPJ or any non-digit query → the search query is
  `WHERE razao_social ILIKE ?`.
- `bun run build` succeeds (run from `web/`).
- `bunx astro check` passes.
- Manual smoke: open the dev server, search a known CNPJ, see one
  result; search a name fragment, see up to 20 results.

## Plan reference

`docs/perf-plan-2026-05.md` §4.1 / W4.1 / Phase 5 PR 5a / M2.

Independent of M0 — frontend-only change. Safe to merge any time.

## Branch + PR

- Start from `main`.
- Open PR against `main` titled
  `perf(web): W4.1 length-14 branch in SearchCNPJ (Phase 5 PR 5a)`.
- PR body should reference this prompt file and `docs/perf-plan-2026-05.md`
  §4.1.

## Out of scope

Do **not**:
- Touch `web/src/lib/analytical.ts` (the `attachCnpjs` glue is fine
  as-is; this is a query-shape change only).
- Add a separate `cnpjs_summary.parquet` route (that's PR 5b — a
  different workstream).
- Add new dependencies, refactor the component beyond the search()
  function, or change styling.
- Touch any ETL code.
