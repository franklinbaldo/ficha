# Socio row-identity investigation

This is an investigation, not a canonical contract. [Issue #97](https://github.com/franklinbaldo/ficha/issues/97)
slice 5 asked whether `socio` -- unlike `empresa`/`simples` (`cnpj_basico`
alone) or `estabelecimento` (`cnpj_basico`+`cnpj_ordem`+`cnpj_dv`) -- has any
defensible key at all: the RFB layout (`registry.SOCIO_COLUMNS`) records one
company-partner relationship per row and publishes no row id. This document
reports the measured cardinality/conflict rate of five candidate composite
keys against the complete real `2026-04` snapshot (all ten `SociosN.zip`
parts) and ends with an explicit recommendation. **No `SOCIO_CANONICAL`
contract exists and no canonical socio writer was built** -- both remain
future work, gated on this recommendation.

## Method

[`socio_key_audit.py`](https://github.com/franklinbaldo/ficha/blob/main/etl/src/ficha_etl/socio_key_audit.py)
(new, evidence-only) reads all ten real parts through the production
registry-backed reader, projects every raw column (not a fixed key -- which
columns matter was the open question) into per-part checkpoints, then
measures five candidate keys, narrowest to widest:

1. **`cnpj_basico_socio`** -- `(cnpj_basico, cnpj_cpf_socio)`: "which
   company, which partner." The minimal natural pairing.
2. **`+identificador_socio`** -- partner type (legal entity / individual /
   foreign, per RFB's layout).
3. **`+qualificacao_socio`** -- the partner's role/qualification code.
4. **`+data_entrada_sociedade`** -- entry date, distinguishing a partner who
   left and re-entered under the same role.
5. **`full_row_minus_names`** -- every raw column except the two free-text
   name fields (`nome_socio_razao_social`, `nome_representante_legal`): all
   nine of `cnpj_basico`, `identificador_socio`, `cnpj_cpf_socio`,
   `qualificacao_socio`, `data_entrada_sociedade`, `pais`,
   `representante_legal`, `qualificacao_representante_legal`,
   `faixa_etaria`. The largest defensible NATURAL composite before
   resorting to a synthetic row identity.

For each candidate: blank/null key-integrity failures (only `cnpj_basico`
and `cnpj_cpf_socio` gate row validity -- other columns, like `pais`, are
legitimately blank for most real rows and are reported as a diagnostic, not
an exclusion; see the module docstring for why treating every column as a
validity gate was a real bug caught before this run), distinct valid key
count, duplicate key count, excess rows, cross-part duplicates, and --
critically -- how many duplicate keys are **exact** (every other raw
column, including the two name fields, also matches -- the same fact just
republished) versus **conflicting** (something else genuinely differs). A
candidate with a high conflict rate does not actually identify a
real-world fact; a candidate with zero conflicts and a near-zero duplicate
rate is a real, defensible key.

## Real run (2026-04, complete ten-part dataset)

- Source commit: `29fa59921c492021ec8cb52b22d8ae2c8e7c5805`
- Workflow run: [`29789142307`](https://github.com/franklinbaldo/ficha/actions/runs/29789142307)
- Artifact: `socio-key-audit-2026-04-29789142307` (id `8479703886`),
  526,889,583 bytes, expires 2026-08-20 (30-day retention) -- this document
  is the durable record once it expires
- All ten parts confirmed present, `total_rows_scanned` (27,494,723) equals
  the exact sum of every part's `rows_raw`; every per-part checkpoint
  checksum in `checkpoint_checksums` was independently recomputed and
  matched exactly
- DuckDB 1.5.2, `threads=1`, `memory_limit=8.3 GiB` (auto-detected)
- Total job duration: `2026-07-21T00:03:22Z` to `2026-07-21T00:10:43Z` --
  about **7m21s** end to end (download all ten ZIPs, extract, run five
  candidate-key analyses per part plus the global cross-part aggregation)

### Results by candidate

27,494,723 total rows. `cnpj_basico` is populated in every row;
`cnpj_cpf_socio` is blank in 12,832 rows (0.047%) -- a genuine key-integrity
gap excluded from every candidate below, not counted as an ordinary
duplicate.

| Candidate | Distinct valid keys | Duplicate keys | Excess rows | Cross-part | **Conflicting** |
|---|---:|---:|---:|---:|---:|
| `cnpj_basico_socio` | 27,481,625 | 266 | 266 | 0 | **266** |
| `+identificador_socio` | 27,481,625 | 266 | 266 | 0 | **266** |
| `+qualificacao_socio` | 27,481,685 | 206 | 206 | 0 | **206** |
| `+data_entrada_sociedade` | 27,481,824 | 67 | 67 | 0 | **67** |
| `full_row_minus_names` | 27,481,865 | 26 | 26 | 0 | **0** |

Every excess row equals its candidate's duplicate-key count -- every
duplicate key in this real snapshot has exactly two occurrences, never
three or more. **Zero cross-part duplicates were found for any candidate**:
whatever repetition exists is entirely within one physical `SociosN.zip`
part in this snapshot (an observed fact for 2026-04, not a structural
guarantee for every future snapshot).

The narrow candidate's 266 duplicate keys are **100% conflicting** -- not
one is a simple republished row. Each additional column absorbs some of
those conflicts by turning what looked like "the same key, different
payload" into "actually a different key":

- `+identificador_socio` absorbs **0** -- it never actually differs among
  any of the 266 pairs;
- `+qualificacao_socio` absorbs **60** (22.6% of the 266) -- confirmed by
  inspecting a real example: `cnpj_basico=00084380`,
  `cnpj_cpf_socio=***816343**` appears twice in part 0 with identical
  `identificador_socio` and `data_entrada_sociedade` but
  `qualificacao_socio` 49 in one row and 22 in the other -- the same
  partner holding two distinct roles in the same company, both entered on
  the same date;
- `+data_entrada_sociedade` absorbs **139** more (52.3%) -- a partner who
  left and re-entered, or a corrected date, under the same role;
- the four remaining columns (`pais`, `representante_legal`,
  `qualificacao_representante_legal`, `faixa_etaria`) jointly absorb the
  last **41** (15.4%);
- **26 pairs (9.8% of the original 266) remain irreducible even at the
  full natural composite** -- and every one of those 26 is an EXACT
  duplicate (byte-identical across all nine non-name columns), not a
  conflict. `pais` is blank in 27,402,219 of 27,481,865 valid rows
  (99.71%) -- confirming it is populated only for foreign partners, exactly
  as expected, and is correctly NOT treated as a validity gate for the
  other 99.71%.

## Recommendation

**A natural composite key is defensible: all nine non-name raw columns
(`full_row_minus_names`).**

- Zero conflicts: no pair sharing that full combination has genuinely
  different information anywhere else in the row.
- The residual duplicate rate is 26 rows out of 27,481,865 valid keys
  (0.0000946%) -- and every one of those 26 is a byte-identical republished
  row, not an unresolved semantic ambiguity. A deterministic full-row
  tiebreak (the same technique `transform._dedupe_cnpj_basico_table`
  already uses for `empresa`/`simples`) would collapse them with no
  correctness question attached, unlike empresa/simples's still-open
  conflicting-duplicate semantics (issue #76) -- socio's residual
  duplicates are NOT conflicting, so there is no analogous open question
  here.
- A **synthetic row identity is not necessary** on correctness grounds:
  the natural composite already achieves near-perfect uniqueness. It may
  still be worth considering later purely for ERGONOMICS (a nine-column
  composite is unwieldy for downstream joins/APIs), but that is a
  usability decision separate from "is there a defensible key," which this
  run answers yes to.
- The narrower `(cnpj_basico, cnpj_cpf_socio, identificador_socio,
  qualificacao_socio, data_entrada_sociedade)` candidate is close (67
  conflicting keys, 0.000244%) but is NOT recommended as the primary key:
  it leaves 67 real, measured semantic conflicts unresolved, all of which
  the full composite resolves cleanly. If a narrower key is wanted later
  for practical reasons, this snapshot's evidence should inform exactly
  which columns are safe to drop and which are not (`identificador_socio`
  contributed nothing measurable; the other three each resolved real
  cases).

This recommendation is **not** a `SOCIO_CANONICAL` declaration. Before any
canonical contract is written, at minimum:

- confirm this pattern holds on at least one more historical snapshot
  (this is one month's evidence);
- decide how to handle the 12,832 blank-`cnpj_cpf_socio` rows (key-integrity
  failure, matching `estabelecimento`'s "fail" pattern, vs. some other
  policy) and the 26 exact duplicates (deterministic collapse, matching
  `empresa`/`simples`'s `deterministic-collapse` pattern);
- decide whether a nine-column `primary_key` tuple is acceptable in
  `registry.ParquetSpec` as-is, or whether a derived/synthetic identity
  column is worth adding for ergonomics even though it is not required for
  correctness.

## Limitations

- One snapshot (2026-04) only. Cardinality/conflict rates could differ in
  other months, though the mechanism (multiple roles, re-entries, data
  corrections) is structural, not month-specific, so similar rates are
  plausible.
- "Zero cross-part duplicates" is a measured fact for this snapshot, not a
  guarantee -- `socio_key_audit.py`'s cross-part detection exists
  specifically because that failure mode is invisible to any single-part
  view (the same class of gap issue #100 found for estabelecimento).
- This says nothing about lookups (slice 6) or about `socio`'s eventual
  physical writer design (single-part vs. dataset-level, bucketing) --
  those are writer-implementation questions for whenever slice 5's
  recommendation is acted on, out of scope for this investigation.

## Deliberate boundary

This workflow only measures. It does not write a canonical Parquet, does
not declare `SOCIO_CANONICAL`, does not publish to Internet Archive, and
does not feed the monthly pipeline. The next gates, per
[issue #97](https://github.com/franklinbaldo/ficha/issues/97), are the
lookups (slice 6) and, eventually, acting on this recommendation to give
`socio` an actual canonical contract and writer.
