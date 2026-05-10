# Hardening — ADRs for the four new analytical parquets

## Context

Working branch `claude/ficha-perf-plan-v2` shipped four new
parquets that are not yet documented in
`docs/adr/`:

- `lookups/<kind>.parquet` (W10) — per-lookup parquets +
  `attachLookups()` (PR #34)
- `cnpj_cnaes.parquet` (W11) — position-aware CNAE association (PR #36)
- `cnpj_contatos.parquet` (W12) — reverse contact lookup (PR #37)
- `cnpjs_summary.parquet` (W4.2 — under review separately)

The performance plan (`docs/perf-plan-2026-05.md`) describes the
*why* but ADRs are missing. ADRs make the architecture decisions
discoverable and durable beyond a single PR thread.

## Scope

Write 4 new ADRs in `docs/adr/`:

### `0019-per-lookup-parquets.md`

**Title:** Per-lookup parquets alongside lookups.json (W10).
**Status:** Accepted.
Decision summary:
- Six parquets under `<snapshot>/lookups/<kind>.parquet` for
  cnaes / motivos / municipios / naturezas / paises /
  qualificacoes.
- Schema: `(codigo VARCHAR, descricao VARCHAR,
  descricao_normalizada VARCHAR)`. Sort by `codigo`. Bloom on
  `codigo` and `descricao_normalizada`.
- Coexists with `lookups.json`: JSON for synchronous render,
  parquet for SQL composability (filter/aggregate by description).
- Frontend wiring via `attachLookups(db, manifest)`.

### `0020-cnpj-cnaes-association.md`

**Title:** `cnpj_cnaes.parquet` — position-aware CNAE association
(W11).
**Status:** Accepted.
Decision summary:
- Schema: `(cnpj, cnpj_base, cnae_codigo, posicao)`.
- `posicao = 0` is primary CNAE, `posicao >= 1` is secondary in
  registration order.
- Sorted by `(cnae_codigo, posicao, cnpj_base)`. Bloom on
  `cnae_codigo` and `cnpj_base`.
- The denormalized arrays in `cnpjs.parquet`
  (`cnae_secundario_codigos`, `cnae_secundario_descricoes`)
  STAY — they're cheap for the lâmina view.
- This parquet is the inverse index for reverse-CNAE queries.

### `0021-cnpj-contatos-reverse-lookup.md`

**Title:** `cnpj_contatos.parquet` — reverse contact lookup
(W12).
**Status:** Accepted.
Decision summary:
- Schema: `(cnpj, cnpj_base, tipo, valor, posicao)` with
  `tipo ∈ {'telefone', 'fax', 'email'}`.
- Sorted by `(tipo, valor, cnpj)`. Bloom on `valor` and on
  `split_part(valor, '@', 2)` (email domain).
- **Privacy posture:** phones and emails are PII. RFB publishes
  them publicly; this parquet is a pure re-shape. No enrichment.
  Document the trade-off and reference the source of public data.
- Wide columns on `cnpjs.parquet` STAY (they feed the lâmina
  view).

### `0022-cnpjs-summary-detail-split.md` (only if W4.2 has merged)

**Title:** `cnpjs_summary.parquet` for autocomplete / search (W4.2).
**Status:** Accepted.
Decision summary:
- ~10-column projection of cnpjs.parquet sorted by cnpj with
  bloom on cnpj and razao_social_normalizada.
- Detail view (`EmpresaFicha.svelte`) uses cnpjs.parquet; search
  list (`SearchCNPJ.svelte`) uses cnpjs_summary.parquet.
- Cuts cold-cache search bytes ~5×.

If W4.2's PR hasn't landed yet, write this ADR anyway with status
**Proposed** and a note about where the implementation lives.

## ADR template

Use the same shape as existing ADRs (e.g.,
`docs/adr/0008-three-parquet-architecture.md`). Each ADR has:
- Status (Accepted / Proposed / Superseded)
- Date
- Contexto
- Decisão
- Por quê
- Consequências
- (optional) Referências

Write in the same style — Portuguese with English technical
terms where appropriate, terse and concrete.

## Update `docs/adr/README.md`

Add the four new entries to the index, in numerical order. If
the README has a `## Por área` section, slot the new ADRs into
the relevant categories (e.g., "Schema / Parquet shape").

## Acceptance criteria

- 4 new ADR files in `docs/adr/0019…0022.md` (skip 0022 if W4.2
  not merged yet).
- `docs/adr/README.md` updated with index entries.
- No code changes — pure documentation PR.
- Each ADR cross-references the originating PR(s) and the perf
  plan §.

## Plan reference

Hardening / documentation — supports M4 work shipped in PRs
#33, #34, #36, #37.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2`.
- Open PR against `claude/ficha-perf-plan-v2` titled
  `docs(adr): 0019–0022 for new analytical parquets (hardening)`.

## Out of scope

- Don't write retroactive ADRs for things shipped before this batch.
- Don't restructure the ADR directory or change the numbering
  scheme.
- Don't refactor any code.
