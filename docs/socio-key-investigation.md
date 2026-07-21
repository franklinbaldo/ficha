# Socio row-identity investigation

This is an investigation, not a canonical contract. [Issue #97](https://github.com/franklinbaldo/ficha/issues/97)
slice 5 asked whether `socio` -- unlike `empresa`/`simples` (`cnpj_basico`
alone) or `estabelecimento` (`cnpj_basico`+`cnpj_ordem`+`cnpj_dv`) -- has any
defensible key at all: the RFB layout (`registry.SOCIO_COLUMNS`) records one
company-partner relationship per row and publishes no row id. **No
`SOCIO_CANONICAL` contract exists and no canonical socio writer was built**
-- both remain future work, gated on this recommendation.

This document has gone through two revisions. The first tested one flat
composite key uniformly across every row -- correct cardinality, wrong
characterization: it treated `socio`'s 12,832 blank-`cnpj_cpf_socio` rows as
a generic key-integrity gap, when they are in fact the entire
foreign-partner category, and it could not account for RFB masking
`cnpj_cpf_socio` for individuals. The second introduced the category-aware
model below but proved "exact duplicate" using a hash comparison, which is
evidence, not proof, of row equality, and used tests built from empty
strings where the real pipeline produces SQL NULL. This revision fixes both:
"conflicting vs exact" is now a real per-column comparison, restricted to
where it is cheap and meaningful (company-scoped relationship candidates);
name normalization strips accents; representative-field variation detection
is NULL-preserving; and `faixa_etaria`'s exclusion is argued on semantic
(temporal-stability) grounds, backed by the measured fact that including it
changes nothing once the recommended relationship key is already applied.

## Method

`identificador_socio` splits every row into exactly three categories with
structurally different partner-identity content (confirmed against the
complete real 2026-04 snapshot, see below):

- **`"1"` Pessoa Juridica** -- `cnpj_cpf_socio` is always a complete,
  unmasked 14-digit CNPJ.
- **`"2"` Pessoa Fisica** -- `cnpj_cpf_socio` is always masked down to its
  middle six digits (e.g. `"***816343**"`).
- **`"3"` Socio Estrangeiro** -- `cnpj_cpf_socio` is always blank; this is
  not a data-quality gap, it is the entire foreign-partner category.

[`socio_key_audit.py`](https://github.com/franklinbaldo/ficha/blob/main/etl/src/ficha_etl/socio_key_audit.py)
(evidence-only) reads all ten real parts through the production
registry-backed reader, projects every raw column plus a normalized-name
diagnostic column into per-part checkpoints, then measures, **per
category**, two layers of candidate keys:

- **identity-level** -- "who is this partner," independent of which
  company:
  - PJ: `pj:cnpj` (`cnpj_cpf_socio` alone) and `pj:cnpj_nome` (+ normalized
    name, as a diagnostic -- PJ's CNPJ is already unmasked and complete, so
    name is not expected to add anything);
  - PF: `pf:cpf` (masked CPF alone), `pf:cpf_nome` (+ normalized name),
    `pf:cpf_nome_faixa` (+ `faixa_etaria`, age bracket);
  - Foreign: `foreign:nome` (normalized name alone), `foreign:nome_pais`
    (+ `pais`, country code).
- **relationship-level** -- `cnpj_basico` (company) + that category's
  recommended partner identity, narrowed in stages by `qualificacao_socio`
  then `data_entrada_sociedade`: `<prefix>:company_partner` ->
  `<prefix>:company_partner_qualificacao` -> `<prefix>:relationship`. For
  Pessoa Fisica, `<prefix>:relationship_with_faixa` additionally measures
  the effect of `faixa_etaria` on top of the recommended relationship key
  (see Recommendation for why it is measured but not adopted).

For each candidate: blank/null key-integrity failures (only `cnpj_basico`
and, where relevant to that category, `cnpj_cpf_socio` gate row validity),
distinct valid key count, duplicate key count, excess rows, cross-part
duplicates. **Conflicting-vs-exact** ("how many duplicate keys have more
than one distinct full row, meaning something else genuinely differs, not
just the same fact republished") is measured only for **relationship-level**
candidates, via a real per-column comparison of every raw column -- not a
hash. Identity-level candidates report this as `null`/not computed: they
are company-UNSCOPED by construction, so their "duplicate keys" set can
cover nearly the entire category (masked CPF alone has only ~1,000,000
possible values across 26.8M real Pessoa Fisica rows), which makes a
full-row comparison there both too expensive and not a meaningful
measurement -- an apparent overlap at that level is usually just the same
partner appearing in different companies, not a conflict to resolve.
Category-specific diagnostics (below) measure what genuinely varies at the
identity level instead.

Category-specific diagnostics, run separately from the candidate-key
measurements above:

- **PJ**: CNPJ format/length validity (14 digits, all-numeric); same CNPJ
  with different normalized corporate names (a consistency check, not an
  identity signal); whether adding name resolves any collision a valid
  CNPJ doesn't.
- **PF**: same masked CPF with different normalized names; same masked CPF
  + name with different `faixa_etaria`.
- **All categories**: whether `representante_legal` /
  `nome_representante_legal` / `qualificacao_representante_legal` vary
  independently within an otherwise-identical relationship group. This
  check is **NULL-preserving**: a group where one occurrence has a legal
  representative on file and another has none (real SQL NULL, not an empty
  string) counts as variation, not as "consistent" -- plain
  `COUNT(DISTINCT ...)` alone silently ignores NULL rows and would miss
  exactly that case. These fields are never folded into a candidate key
  unless this measurement shows they define a genuinely separate
  relationship; where a conflict remains unresolved (see Socio Estrangeiro
  below), it is reported and preserved, not silently absorbed into a wider
  key.

Normalized partner name strips accents (`strip_accents()`, a DuckDB core
string function, no extension required), uppercases, collapses internal
whitespace, and trims. RFB free text is not consistently accented or cased
across records -- the same real name can appear as `"JOSÉ"` in one row and
`"JOSE"` in another -- so a literal comparison would wrongly treat these as
different people.

## Real run (2026-04, complete ten-part dataset)

This is the **second real run** against the same underlying source data,
executed entirely through CI in an **aggregation-only** mode that never
touches the raw ZIPs:

1. The original per-part checkpoints (all ten `columns/part-N.parquet`,
   with their `evidence/part-N.key-audit.manifest.json` checksums) were
   produced by workflow run
   [`29789142307`](https://github.com/franklinbaldo/ficha/actions/runs/29789142307)
   (commit `29fa59921c492021ec8cb52b22d8ae2c8e7c5805`), uploaded as artifact
   `socio-key-audit-2026-04-29789142307`.
2. `socio_key_audit.py` gained `run_aggregation_only()`/`--aggregate-only`
   and a new `reanalyze` job in `socio-key-audit.yml`: given a
   `reanalyze_source_run_id` input, it downloads that prior run's
   checkpoint artifact via `actions/download-artifact@v4` (cross-run,
   same repo), **independently re-verifies each of the ten parts' SHA-256
   against its manifest** (a corrupted or tampered restored checkpoint
   fails the run instead of silently producing a report from wrong data),
   and then runs only the global cross-part aggregation -- no ZIP
   download, no CSV extraction, no archive.org network access at all.
3. This document's numbers come from dispatching that job with
   `reanalyze_source_run_id=29789142307`: workflow run
   [`29823339857`](https://github.com/franklinbaldo/ficha/actions/runs/29823339857),
   job `reanalyze`, produced the durable artifact
   `socio-key-audit-reanalysis-2026-04-29823339857`, containing
   `global.socio-key-audit.json` with `"mode": "aggregation-only"`,
   `"source_commit": "502fe11e72cabc9cf270dda9c5413f7b74188f14"` (this
   revision's code, including the real-comparison/NULL-preservation/accent
   fixes), and `"verified_checkpoint_checksums"` for all ten parts.

DuckDB 1.5.2 (same version as the original run), `threads=1`,
`memory_limit=8.3 GiB`. Total job duration: 6m11s end to end (download the
already-verified artifact, re-verify ten checksums, run the full
category-aware aggregation, upload the new evidence artifact) -- versus the
original run's ~7m21s that also had to download and extract ten real ZIPs,
demonstrating the aggregation-only path avoids that cost as intended.

27,494,723 total rows, split by `identificador_socio` into exactly three
categories that sum to the total with no stragglers:

| Category | `identificador_socio` | Rows | Share |
|---|---|---:|---:|
| Pessoa Juridica | `"1"` | 692,074 | 2.5% |
| Pessoa Fisica | `"2"` | 26,789,817 | 97.4% |
| Socio Estrangeiro | `"3"` | 12,832 | 0.05% |

### Pessoa Juridica (692,074 rows)

| Candidate | Distinct | Duplicate | Excess | Cross-part | Conflicting |
|---|---:|---:|---:|---:|---:|
| `pj:cnpj` (identity) | 298,584 | 98,274 | 393,490 | 89,152 | not computed |
| `pj:cnpj_nome` (identity) | 298,584 | 98,274 | 393,490 | 89,152 | not computed |
| `pj:company_partner` (relationship) | 692,074 | **0** | 0 | 0 | **0** |
| `pj:company_partner_qualificacao` | 692,074 | 0 | 0 | 0 | 0 |
| `pj:relationship` | 692,074 | 0 | 0 | 0 | 0 |

The bare CNPJ identity candidate shows heavy duplication (a single partner
CNPJ legitimately holds stakes in many different companies -- that is
expected, not a data-quality problem), and adding normalized name changes
**nothing**: `pj:cnpj_nome`'s distinct/duplicate/excess/cross-part numbers
are identical to `pj:cnpj`'s. Diagnostics confirm why:
`cnpj_format_valid_count` is 692,074/692,074 (100%, all 14 digits,
all-numeric, zero malformed), and `same_cnpj_different_normalized_name_count`
is **0** -- not one PJ CNPJ is ever republished under a different corporate
name. `name_resolves_collision_beyond_valid_cnpj` is `False`. Once scoped by
company (`pj:company_partner` = `cnpj_basico` + bare CNPJ), duplication
disappears entirely: **zero** duplicate keys among all 692,074 rows, at
every relationship width tested, verified by real per-column comparison
(there is nothing to compare, since there are no duplicate keys at all).
`representante_independence` found zero duplicate relationship groups to
even check.

### Pessoa Fisica (26,789,817 rows)

| Candidate | Distinct | Duplicate | Excess | Cross-part | Conflicting |
|---|---:|---:|---:|---:|---:|
| `pf:cpf` (identity) | 999,820 | 998,765 | 25,789,997 | 997,626 | not computed |
| `pf:cpf_nome` (identity) | 17,762,739 | 4,859,470 | 9,027,078 | 4,302,837 | not computed |
| `pf:cpf_nome_faixa` (identity) | 17,762,816 | 4,859,428 | 9,027,001 | 4,302,802 | not computed |
| `pf:company_partner` (relationship) | 26,789,811 | **6** | 6 | 0 | **6** |
| `pf:company_partner_qualificacao` | 26,789,811 | 6 | 6 | 0 | 6 |
| `pf:relationship` | 26,789,817 | **0** | 0 | 0 | **0** |
| `pf:relationship_with_faixa` | 26,789,817 | **0** | 0 | 0 | **0** |

Masked CPF alone is confirmed almost useless as an identity signal on its
own: only 999,820 distinct masked values exist across 26.8M individual
rows -- almost exactly the ~1,000,000 combinations the masking format
(`***XXXXXX**`) can produce, so the vast majority of masked codes are
shared by unrelated people purely by pigeonhole. Diagnostics confirm this
directly: `same_masked_cpf_different_normalized_name_count` is **998,460**
out of 999,820 masked CPF values (99.9%) -- almost every masked code covers
multiple different names. Adding normalized name resolves most of that:
distinct identity count jumps to 17,762,739 (still short of 26.8M, since
common names really do repeat across different real people).

The relationship-level picture is the real finding: once scoped by company
(`pf:company_partner` = `cnpj_basico` + masked CPF + normalized name),
duplication collapses from millions to **6** residual keys out of
26,789,817 rows (0.00002%), independently verified as genuine conflicts by
real full-row comparison (not a hash). Adding `qualificacao_socio` +
`data_entrada_sociedade` (`pf:relationship`) resolves all 6 -- **zero
duplicates, zero conflicts** at the recommended relationship candidate.

**`faixa_etaria` (age bracket) is measured but deliberately excluded from
the recommendation.** `pf:relationship_with_faixa` -- the recommended
relationship key plus `faixa_etaria` -- produces the exact same numbers as
`pf:relationship` without it: zero duplicates either way, because
`pf:relationship` already reaches zero, so there is nothing left for
`faixa_etaria` to resolve at that level. The identity-level diagnostic
(`same_masked_cpf_and_name_different_faixa_etaria_count` = 77) shows it
resolves a small number of cases at the unscoped identity level, but that
is not the basis for excluding it -- **the real reason is semantic, not
just measured impact**: age bracket is a temporally unstable attribute (a
real person's `faixa_etaria` changes as they age across snapshots),
structurally unlike `qualificacao_socio`/`data_entrada_sociedade`, which
are facts fixed at the moment a partner relationship began and do not
drift between snapshots of the same relationship. Including a
temporally-drifting attribute in an identity key risks the same
relationship being seen as "changing identity" across snapshots for a
reason that has nothing to do with the relationship itself.
`representante_independence` found zero duplicate groups to check (there
are none, since `pf:relationship` already reaches zero duplicates).

### Socio Estrangeiro (12,832 rows)

| Candidate | Distinct | Duplicate | Excess | Cross-part | Conflicting |
|---|---:|---:|---:|---:|---:|
| `foreign:nome` (identity) | 11,919 | 657 | 913 | 523 | not computed |
| `foreign:nome_pais` (identity) | 11,968 | 617 | 864 | 487 | not computed |
| `foreign:company_partner` (relationship) | 12,805 | 27 | 27 | 0 | **5** |
| `foreign:company_partner_qualificacao` | 12,806 | 26 | 26 | 0 | **4** |
| `foreign:relationship` | 12,807 | **25** | 25 | 0 | **3** |

This category has **no reliable natural identifier at all** -- confirmed,
not just suspected. Normalized name alone already has real collisions (657
duplicate name groups); adding `pais` only marginally helps (617 duplicate
groups, a ~6% reduction). Even the widest relationship candidate
(`cnpj_basico` + name + country + role + entry date) leaves **25 residual
duplicate keys**, of which **3 are genuinely conflicting** (real full-row
comparison, not a hash) -- 22 are byte-identical republished rows, 3 are
not.

`representante_independence` found 25 duplicate relationship groups and
**3 with representante variation**, the same 3 that show up as
conflicting above -- confirmed as a real, NULL-preserving comparison (a
group where one occurrence has a legal representative on file and another
has none would count as variation too, though in this snapshot the 3
variations happen to be between two different non-NULL values). **This
finding is reported, not acted on**: `representante_legal` is deliberately
**not** promoted into `foreign:relationship`'s identity to "resolve" these
3 cases. Doing so would silently convert a measured, open data-quality
question ("are these the same partner or two different ones sharing every
other measured field?") into an assumed answer baked into the key, without
independent evidence that `representante_legal` is itself a reliable
disambiguator for this category. The 3 conflicting relationship groups
remain conflicting in this document's recommended candidate. With no
CPF/CNPJ analog available for foreign partners, `foreign:relationship` is
the best available candidate but explicitly **not** a fully defensible
key.

## Recommendation

**Category-specific relationship-level keys, not one flat composite:**

- **Pessoa Juridica**: `(cnpj_basico, cnpj_cpf_socio)` -- bare, unmasked
  CNPJ. Zero duplicates, zero conflicts (real comparison), at every width
  tested. Name adds nothing measurable and should not be part of the key.
- **Pessoa Fisica**: `(cnpj_basico, cnpj_cpf_socio, nome_socio_razao_social
  [normalized, accent-stripped], qualificacao_socio,
  data_entrada_sociedade)`. Masked CPF alone is not usable; normalized name
  is required to make the identity meaningful; `qualificacao_socio` and
  `data_entrada_sociedade` resolve the last 6 residual company-scoped
  collisions to zero (verified by real comparison). `faixa_etaria` is
  excluded on temporal-stability grounds (see above) -- and, measured at
  the relationship level, changes nothing anyway, since the recommended
  key already reaches zero duplicates without it.
- **Socio Estrangeiro**: `(cnpj_basico, nome_socio_razao_social
  [normalized, accent-stripped], pais, qualificacao_socio,
  data_entrada_sociedade)` is the best available candidate, but **not a
  defensible key on its own** -- 25 residual duplicates remain (3
  genuinely conflicting, verified by real comparison), and this category
  structurally has no unmasked identifier equivalent to CNPJ/CPF to fall
  back on. `representante_legal` is measured as a diagnostic
  (`representante_independence`) but deliberately **not** promoted into
  the key -- see above. Any canonical contract for this category needs an
  explicit, documented policy for these residuals (e.g. deterministic
  collapse for the 22 exact duplicates, and treating the 3 conflicting
  cases as an accepted, documented data-quality gap rather than silently
  resolving them).

A **synthetic row identity is not necessary** for Pessoa Juridica or Pessoa
Fisica on correctness grounds -- both achieve perfect uniqueness at the
relationship level with natural columns, verified by real full-row
comparison. It remains an open question for Socio Estrangeiro, where no
natural composite reaches zero conflicts.

This recommendation is **not** a `SOCIO_CANONICAL` declaration. Before any
canonical contract is written, at minimum:

- confirm this pattern holds on at least one more historical snapshot
  (this is one month's evidence, now cross-checked across two independent
  runs of the same snapshot -- see Real run above -- but still one month);
- decide a policy for Socio Estrangeiro's 25 residual duplicates (22 exact,
  3 conflicting, preserved not resolved);
- decide whether a single `SOCIO_CANONICAL` contract can express
  category-specific keys (e.g. a nullable/coalesced identity column plus a
  `identificador_socio`-conditioned uniqueness policy), or whether `socio`
  needs to be split into per-category physical outputs;
- decide whether a derived/synthetic identity column is worth adding for
  downstream ergonomics even though it is not required for correctness in
  two of the three categories.

## Limitations

- One snapshot (2026-04) only. Cardinality/conflict rates could differ in
  other months, though the underlying mechanism (CPF masking, foreign
  partners lacking a CPF/CNPJ, multiple roles/re-entries) is structural,
  not month-specific, so similar rates are plausible.
- "Zero cross-part duplicates" at the relationship level is a measured
  fact for this snapshot, not a guarantee.
- Socio Estrangeiro's 3 conflicting residual duplicates are flagged, not
  resolved -- this document does not attempt to determine whether they
  represent genuinely different people or a data-quality artifact.
- This says nothing about lookups (slice 6) or about `socio`'s eventual
  physical writer design -- those are writer-implementation questions for
  whenever slice 5's recommendation is acted on, out of scope for this
  investigation.
- Aggregation-only mode re-verifies each restored checkpoint's checksum
  against its own manifest, but does not re-verify the manifest itself
  against, say, an independent third copy of the raw ZIP -- it inherits
  the original run's (29789142307) source-ZIP checksums as its root of
  trust. A full `audit` dispatch that re-downloads and re-extracts the raw
  ZIPs from scratch remains the strongest independent check and has not
  been re-run since the original investigation.

## Deliberate boundary

This workflow only measures. It does not write a canonical Parquet, does
not declare `SOCIO_CANONICAL`, does not publish to Internet Archive, and
does not feed the monthly pipeline. The next gates, per
[issue #97](https://github.com/franklinbaldo/ficha/issues/97), are the
lookups (slice 6) and, eventually, acting on this recommendation to give
`socio` an actual canonical contract and writer.
