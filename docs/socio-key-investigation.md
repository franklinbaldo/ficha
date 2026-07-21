# Socio row-identity investigation

This is an investigation, not a canonical contract. [Issue #97](https://github.com/franklinbaldo/ficha/issues/97)
slice 5 asked whether `socio` -- unlike `empresa`/`simples` (`cnpj_basico`
alone) or `estabelecimento` (`cnpj_basico`+`cnpj_ordem`+`cnpj_dv`) -- has any
defensible key at all: the RFB layout (`registry.SOCIO_COLUMNS`) records one
company-partner relationship per row and publishes no row id. **No
`SOCIO_CANONICAL` contract exists and no canonical socio writer was built**
-- both remain future work, gated on this recommendation.

This document supersedes an earlier version of the same investigation that
tested one flat composite key uniformly across every row. That first pass
measured cardinality correctly but mischaracterized what it was measuring:
it treated `socio`'s 12,832 blank-`cnpj_cpf_socio` rows as a generic
key-integrity gap, when they are in fact the entire foreign-partner
category (`identificador_socio="3"`), which structurally has no CPF/CNPJ
field at all. It also could not account for RFB masking `cnpj_cpf_socio`
down to its middle six digits for individuals, since a flat candidate
applied the same columns to every row regardless of partner type. Both
issues are fixed by the category-specific model below.

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
  `<prefix>:company_partner_qualificacao` -> `<prefix>:relationship`.

For each candidate: blank/null key-integrity failures (only `cnpj_basico`
and, where relevant to that category, `cnpj_cpf_socio` gate row validity),
distinct valid key count, duplicate key count, excess rows, cross-part
duplicates, and how many duplicate keys are **exact** (every other raw
column also matches) versus **conflicting** (something else genuinely
differs).

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
  independently within an otherwise-identical relationship group -- these
  are never folded into a candidate key unless this measurement shows they
  define a genuinely separate relationship.

Normalized partner name (`_nome_socio_norm`) is deliberately conservative:
uppercase, collapse internal whitespace, trim.

## Real run (2026-04, complete ten-part dataset)

This category-aware analysis reuses the exact same per-part checkpoints as
the original flat investigation -- source commit
`29fa59921c492021ec8cb52b22d8ae2c8e7c5805`, workflow run
[`29789142307`](https://github.com/franklinbaldo/ficha/actions/runs/29789142307),
artifact `socio-key-audit-2026-04-29789142307`. All ten `columns/part-N.parquet`
files were re-downloaded from that already-completed GH Actions artifact and
their checksums re-verified against the original run's manifests before this
analysis touched them; no raw `SociosN.zip` was re-downloaded from the
Internet Archive mirror.

The category-aware queries themselves were run **locally** against those
verified checkpoints (DuckDB 1.5.2, same version as the original CI run),
not via a fresh GH Actions dispatch: `socio-key-audit.yml`'s checkpoint
cache is keyed in part on a hash of `socio_key_audit.py` itself, so any
code change -- including the category-aware rewrite -- invalidates that
cache and would force a full re-download of all ten real ZIPs from
archive.org on the next dispatch. Re-deriving the same measurements locally
from the already-verified checkpoints avoids that unnecessary re-download
while still measuring the real, complete 27,494,723-row snapshot.

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
| `pj:cnpj` (identity) | 298,584 | 98,274 | 393,490 | 89,152 | 98,274 |
| `pj:cnpj_nome` (identity) | 298,584 | 98,274 | 393,490 | 89,152 | 98,274 |
| `pj:company_partner` (relationship) | 692,074 | **0** | 0 | 0 | 0 |
| `pj:company_partner_qualificacao` | 692,074 | 0 | 0 | 0 | 0 |
| `pj:relationship` | 692,074 | 0 | 0 | 0 | 0 |

The bare CNPJ identity candidate shows heavy duplication (a single partner
CNPJ legitimately holds stakes in many different companies -- that is
expected, not a data-quality problem), and adding normalized name changes
**nothing**: `pj:cnpj_nome`'s numbers are byte-identical to `pj:cnpj`'s.
Diagnostics confirm why: `cnpj_format_valid_count` is 692,074/692,074
(100%, all 14 digits, all-numeric, zero malformed), and
`same_cnpj_different_normalized_name_count` is **0** -- not one PJ CNPJ is
ever republished under a different corporate name.
`name_resolves_collision_beyond_valid_cnpj` is `False`. Once scoped by
company (`pj:company_partner` = `cnpj_basico` + bare CNPJ), duplication
disappears entirely: **zero** duplicate keys among all 692,074 rows, at
every width tested. `representante_independence` found zero duplicate
relationship groups to even check (there are none).

### Pessoa Fisica (26,789,817 rows)

| Candidate | Distinct | Duplicate | Excess | Cross-part | Conflicting |
|---|---:|---:|---:|---:|---:|
| `pf:cpf` (identity) | 999,820 | 998,765 | 25,789,997 | 997,626 | 998,765 |
| `pf:cpf_nome` (identity) | 17,762,739 | 4,859,470 | 9,027,078 | 4,302,837 | 4,859,470 |
| `pf:cpf_nome_faixa` (identity) | 17,762,816 | 4,859,428 | 9,027,001 | 4,302,802 | 4,859,428 |
| `pf:company_partner` (relationship) | 26,789,811 | **6** | 6 | 0 | 6 |
| `pf:company_partner_qualificacao` | 26,789,811 | 6 | 6 | 0 | 6 |
| `pf:relationship` | 26,789,817 | **0** | 0 | 0 | 0 |

Masked CPF alone is confirmed almost useless as an identity signal on its
own: only 999,820 distinct masked values exist across 26.8M individual
rows -- almost exactly the ~1,000,000 combinations the masking format
(`***XXXXXX**`) can produce, so the vast majority of masked codes are
shared by unrelated people purely by pigeonhole. Diagnostics confirm this
directly: `same_masked_cpf_different_normalized_name_count` is **998,460**
out of 999,820 masked CPF values (99.9%) -- almost every masked code covers
multiple different names. Adding normalized name resolves most of that:
distinct identity count jumps to 17,762,739 (still short of 26.8M, since
common names really do repeat across different real people). Adding
`faixa_etaria` on top barely moves the needle --
`same_masked_cpf_and_name_different_faixa_etaria_count` is only **77** --
confirming age bracket is a weak tertiary signal, not a primary one.

The relationship-level picture is the real finding: once scoped by company
(`pf:company_partner` = `cnpj_basico` + masked CPF + normalized name),
duplication collapses from millions to **6** residual keys out of
26,789,817 rows (0.00002%), and adding `qualificacao_socio` +
`data_entrada_sociedade` (`pf:relationship`) resolves all 6 --
**zero duplicates, zero conflicts** at the widest relationship candidate.
`representante_independence` again found zero duplicate groups to check.

### Socio Estrangeiro (12,832 rows)

| Candidate | Distinct | Duplicate | Excess | Cross-part | Conflicting |
|---|---:|---:|---:|---:|---:|
| `foreign:nome` (identity) | 11,919 | 657 | 913 | 523 | 638 |
| `foreign:nome_pais` (identity) | 11,968 | 617 | 864 | 487 | 598 |
| `foreign:company_partner` (relationship) | 12,805 | 27 | 27 | 0 | 5 |
| `foreign:company_partner_qualificacao` | 12,806 | 26 | 26 | 0 | 4 |
| `foreign:relationship` | 12,807 | **25** | 25 | 0 | **3** |

This category has **no reliable natural identifier at all** -- confirmed,
not just suspected. Normalized name alone already has real collisions (657
duplicate name groups, most genuinely conflicting -- different foreign
partners sharing a common name); adding `pais` only marginally helps (617
duplicate groups, a ~6% reduction). Even the widest relationship candidate
(`cnpj_basico` + name + country + role + entry date) leaves **25 residual
duplicate keys**, of which **3 are genuinely conflicting** (not just
byte-identical republished rows). `representante_independence` found 25
duplicate relationship groups and **3 with representante variation** --
the same 3 that show up as conflicting above, suggesting those specific
cases may in fact be two different foreign partners who happen to share
every other measured field, distinguishable only by their legal
representative. With no CPF/CNPJ analog available for this category, this
document treats `foreign:relationship` as the best available candidate but
explicitly **not** a fully defensible key.

## Recommendation

**Category-specific relationship-level keys, not one flat composite:**

- **Pessoa Juridica**: `(cnpj_basico, cnpj_cpf_socio)` -- bare, unmasked
  CNPJ. Zero duplicates, zero conflicts, at every width tested. Name adds
  nothing measurable and should not be part of the key.
- **Pessoa Fisica**: `(cnpj_basico, cnpj_cpf_socio, nome_socio_razao_social
  [normalized], qualificacao_socio, data_entrada_sociedade)`. Masked CPF
  alone is not usable; normalized name is required to make the identity
  meaningful; `qualificacao_socio` and `data_entrada_sociedade` resolve the
  last 6 residual company-scoped collisions to zero. `faixa_etaria` is not
  worth including -- it resolves only 77 cases system-wide and is already
  implicitly covered by name-based disambiguation.
- **Socio Estrangeiro**: `(cnpj_basico, nome_socio_razao_social
  [normalized], pais, qualificacao_socio, data_entrada_sociedade)` is the
  best available candidate, but **not a defensible key on its own** -- 25
  residual duplicates remain (3 genuinely conflicting), and this category
  structurally has no unmasked identifier equivalent to CNPJ/CPF to fall
  back on. Any canonical contract for this category needs an explicit,
  documented policy for these residuals (e.g. deterministic collapse for
  the 22 exact duplicates, and either accepting the 3 conflicting cases as
  an inherent data-quality gap or investigating `representante_legal`
  further as a possible fourth disambiguator specific to this category).

A **synthetic row identity is not necessary** for Pessoa Juridica or Pessoa
Fisica on correctness grounds -- both achieve perfect or near-perfect
uniqueness at the relationship level with natural columns. It remains an
open question for Socio Estrangeiro, where no natural composite reaches
zero conflicts.

This recommendation is **not** a `SOCIO_CANONICAL` declaration. Before any
canonical contract is written, at minimum:

- confirm this pattern holds on at least one more historical snapshot
  (this is one month's evidence);
- decide a policy for Socio Estrangeiro's 25 residual duplicates (22 exact,
  3 conflicting);
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
- These specific numbers come from a local re-analysis of the already
  verified per-part checkpoints, not from a fresh `socio-key-audit.yml`
  `audit` dispatch (which would currently force a full raw-ZIP re-download
  due to the checkpoint cache key including a hash of `socio_key_audit.py`
  itself). A future dispatch against this code, accepting that re-download
  cost, would independently confirm these figures end-to-end through CI.

## Deliberate boundary

This workflow only measures. It does not write a canonical Parquet, does
not declare `SOCIO_CANONICAL`, does not publish to Internet Archive, and
does not feed the monthly pipeline. The next gates, per
[issue #97](https://github.com/franklinbaldo/ficha/issues/97), are the
lookups (slice 6) and, eventually, acting on this recommendation to give
`socio` an actual canonical contract and writer.
