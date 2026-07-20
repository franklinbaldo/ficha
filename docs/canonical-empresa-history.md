# Historical canonical empresa dataset

This is the dataset-level counterpart to
[docs/canonical-estabelecimento-history.md](canonical-estabelecimento-history.md)
for `empresa` (RFC 0001 Fase 3, [issue #97](https://github.com/franklinbaldo/ficha/issues/97)
slice 3). It runs the complete ten-part physical empresa dataset
(`Empresas0.zip`..`Empresas9.zip`) of one historical `ficha-YYYY-MM` Internet
Archive item through `canonical_shadow.write_canonical_dataset` in one call,
still outside the monthly pipeline and public products.

Unlike estabelecimento's per-part runner, this cannot process one physical
ZIP at a time: empresa's `duplicate_policy="deterministic-collapse"`
requires every occurrence of a `cnpj_basico` to be visible in the same
deduplication scope to be collapsed correctly (see
[PR #104](https://github.com/franklinbaldo/ficha/pull/104) and
`canonical_shadow.py`'s module docstring for why a per-part writer would
silently under-deduplicate a multi-part `deterministic-collapse` table).

## Manual workflow

Use **Canonical Shadow — historical empresa dataset**
(`canonical-empresa-history.yml`). Inputs are:

- `month`: historical IA item suffix (`YYYY-MM`);
- `sample_size`: deterministic reversible sample size (default `1000`).

There is no `part` input (all ten parts are always processed together) and
no `force` input -- this orchestrator does not implement checkpoint reuse
(see below). Pull requests never touch the network: the PR smoke job runs
the same orchestration against ten tiny generated ZIP fixtures, including a
deliberate cross-part conflicting duplicate, and a separate fully-mocked
(`httpx.MockTransport`) check that an incomplete part set fails closed.

## Disk lifecycle and no checkpoint reuse

Each of the ten ZIPs is downloaded (or read from a local override),
checksummed, extracted to its one CSV, and then **deleted immediately** --
all ten several-hundred-MB ZIPs are never resident on disk at once. The ten
*extracted* CSVs do need to coexist for the single
`write_canonical_dataset` call (that is the entire point of dataset-level
deduplication: there is no way to avoid it while still seeing every part in
one scope); they and the DuckDB work directory are removed after the writer
returns, success or failure.

This orchestrator is **deliberately not restartable/checkpointed**, unlike
`canonical_history.py`'s per-part reuse. That per-part mechanism has known
gaps tracked in [issue #103](https://github.com/franklinbaldo/ficha/issues/103)
(acquisition mode and retained evidence checksums do not fully participate
in checkpoint identity yet); reproducing the same design here across ten
parts without first closing #103 would just add a second copy of the same
trap. Every dispatch of this workflow performs a full fresh run.

## Real run (2026-04, complete ten-part dataset)

The gate described below was exercised for real on 2026-07-20 via
[PR #105](https://github.com/franklinbaldo/ficha/pull/105) (an operational
PR that implements this orchestrator and dispatches the real workflow).
GitHub Actions artifacts expire, so the load-bearing numbers are recorded
here instead of only living in the run.

- Source commit: `ccbf62cdbd32ad9e14800bcc251e3b711acb4688`
- Workflow run: [`29777100790`](https://github.com/franklinbaldo/ficha/actions/runs/29777100790)
- Target: `2026-04`, all ten `EmpresasN.zip` parts, sample size 1,000
- Artifact: `canonical-empresa-history-2026-04-29777100790` (id `8475572635`),
  1,073,107,992 bytes, expires 2026-08-19 (30-day retention) -- this document
  and the checksums below are the durable record once it expires
- Canonical output: `canonical/empresa.parquet`, 1,089,233,701 bytes,
  `sha256:b535c0701ccec05e156bd9a8637e36faffca15886db6c4f4671f8218e6a27ce2`
- Evidence: `evidence/empresa.quality.json`
  (`sha256:12f5003ddf282ebdf27bdea143c0df111f2f262b95c81f398f52601da9eea902`),
  `evidence/empresa.metrics.json`
  (`sha256:695eb5a9e7acf161273d0d63c3a77a13e9ceb689ecd18e2e312af2f02e89feff`)

### Sources

| Part | Acquisition | Compressed (ZIP) | Extracted (CSV) |
|---|---|---:|---:|
| Empresas0.zip | downloaded | 518,166,309 B | 2,123,246,136 B |
| Empresas1.zip | downloaded | 77,878,173 B | 325,857,654 B |
| Empresas2.zip | downloaded | 79,108,033 B | 344,202,384 B |
| Empresas3.zip | downloaded | 85,116,282 B | 349,174,838 B |
| Empresas4.zip | downloaded | 90,265,314 B | 353,836,656 B |
| Empresas5.zip | downloaded | 97,381,750 B | 352,355,032 B |
| Empresas6.zip | downloaded | 94,295,167 B | 357,942,576 B |
| Empresas7.zip | downloaded | 98,819,050 B | 356,079,788 B |
| Empresas8.zip | downloaded | 98,917,626 B | 356,585,249 B |
| Empresas9.zip | downloaded | 94,482,846 B | 362,199,393 B |
| **Total** | | **1,334,430,550 B** (~1,272.6 MiB) | **5,281,479,706 B** (~5,036.8 MiB) |

Every source entry's URL matches the expected mirror pattern
(`https://archive.org/download/ficha-2026-04/raw/<name>`), and every ZIP and
extracted-CSV checksum in the manifest was independently recomputed against
the downloaded artifact and matched exactly (`sha256`, per-part, in
`evidence/empresa.history.json`).

### Quality and cardinality

| Metric | Value |
|---|---:|
| Rows raw / canonical | 67,640,878 / 67,640,878 |
| Required-key failures (`cnpj_basico` null/blank) | 0 |
| Duplicate key count | **0** |
| Excess duplicate row count | **0** |
| Conflicting key count | **0** |
| Invalid casts (`capital_social`) | 0 |
| Sample size / seed | 1,000 / 42 |
| Sample fingerprint | `80ffcb35e1791fd1b5a81a109ba2f139d377e2f90deae7ced37851effea9ce30` |
| Sample mismatches | 0 |
| Schema match | yes |

`rows_canonical == rows_raw - duplicate_key_rows` holds exactly
(67,640,878 = 67,640,878 - 0). `files_read` in the resource envelope is
`10`, confirming the dataset-level scope actually processed all ten parts,
not the single-part runner's implicit `1`.

**No duplicate or conflicting `cnpj_basico` was found anywhere across the
2026-04 snapshot's ten empresa parts.** This is a measured result for this
specific snapshot, not a general claim that empresa never has duplicates:
PR #71's real production incident (a +532,104-row mismatch, cited as the
original evidence for empresa's `duplicate_policy="deterministic-collapse"`)
remains the reason this writer implements deterministic collapse and
bounded conflict evidence at all -- see the *Limitations* section below.
Independently confirmed directly against the Parquet with DuckDB:
`COUNT(*) == COUNT(DISTINCT cnpj_basico) == 67,640,878`, zero null/blank
`cnpj_basico`, the schema matches `registry.EMPRESA_CANONICAL` column-for-
column (including `capital_social DECIMAL(18,2)`), rows are emitted in
non-decreasing `cnpj_basico` order end to end (the explicit `ORDER BY`
`write_canonical_dataset` adds -- see PR #104), and `_source_file` contains
exactly the ten expected part names and nothing else.

### Resource envelope

The `wall_seconds`/RSS/disk figures below come from the ONE metrics stage
this orchestrator instruments -- `canonical_empresa_dataset`, i.e. the
canonical writer's own `write_canonical_dataset` call. They do **not**
include this orchestrator's own preflight/download/extract time, which
happens entirely outside that stage. (A follow-up commit after this run,
`8c5c00a4`, added an explicit `resource_summary.scope: "canonical-writer-
stage"` field to the manifest for exactly this reason -- a purely
label/clarity change, not a rerun-worthy one; this run's manifest predates
that field, but `wall_seconds` here is the identical writer-stage-only
measurement either way.)

| Metric (canonical-writer stage only) | Value |
|---|---:|
| Wall time | 322.3 s (~5m22s) |
| RSS peak / delta | 8,985.5 / 8,897.3 MiB |
| DuckDB temp peak | 2,023.2 MiB |
| Workdir peak | 5,527.9 MiB |
| Filesystem used peak | 44,275.1 MiB / 29.97% |
| Files read | 10 |
| DuckDB version | 1.5.2 |
| Execution profile | `threads=1`, `memory_limit=8.3 GiB` (auto-detected) |

Separately, the GitHub Actions **`historical` job as a whole** ran from
`2026-07-20T20:40:31Z` to `2026-07-20T21:00:04Z` -- about **19m33s**
end to end, covering checkout/setup, disk-space preparation, preflight
HEAD checks, sequentially downloading and extracting all ten ZIPs
(~1.27 GiB compressed → ~5.04 GiB extracted), the 322.3s writer stage
above, evidence/summary writing and artifact upload. This total is
reported here, not in the manifest, precisely so it is never confused with
the writer-stage-only `wall_seconds` field.

## Limitations

- **Issue #76**: empresa's `duplicate_policy="deterministic-collapse"`
  applies the same deterministic full-row tiebreak
  `transform._dedupe_cnpj_basico_table` already uses in production to any
  conflicting duplicate it finds. That survivor is *reproducible* (same
  input always produces the same output, verified above and by PR #104's
  order-independence tests) but is **not proven semantically correct** --
  no domain rule establishes that the deterministically-chosen row is the
  "right" one when two conflicting empresa records share a `cnpj_basico`.
  This run measured zero such conflicts for 2026-04, so the question did
  not arise in practice here, but the policy -- and its unresolved
  semantic-correctness question -- remains in effect for any future
  snapshot that does have conflicts.
- This is fixture-free, real-data evidence for exactly one snapshot
  (2026-04). It says nothing about `simples`, `socio`, or lookups (slices 4-6
  of issue #97), about whether 2026-04 is representative of other months, or
  about the monthly product pipeline (this writer does not feed it).
- Not restartable -- see *Disk lifecycle and no checkpoint reuse* above.

## Deliberate boundary

This workflow processes the complete ten-part empresa dataset and publishes
only a temporary GitHub Actions artifact. It does not upload canonical data
to Internet Archive, feed a product, or decide final physical layout
(bucketing remains deferred -- see `write_canonical_dataset`'s docstring
for the resource-failure protocol that would trigger that decision; this
run did not hit any resource limit, so bucketing was not needed). The next
gates, per [issue #97](https://github.com/franklinbaldo/ficha/issues/97),
are `simples` (slice 4), `socio` (slice 5, pending a key/cardinality
investigation), and the lookups (slice 6) -- only after all entities have
canonical coverage does triangular raw/canonical/product validation become
meaningful.
