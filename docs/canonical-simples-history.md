# Historical canonical simples dataset

This is the single-file counterpart to
[docs/canonical-empresa-history.md](canonical-empresa-history.md) for
`simples` (RFC 0001 Fase 3, [issue #97](https://github.com/franklinbaldo/ficha/issues/97)
slice 4). Unlike empresa (ten physical `EmpresasN.zip` parts), `simples` is
published as ONE physical file (`Simples.zip`), so it runs through the same
single-part entry point estabelecimento uses --
`canonical_shadow.write_canonical_part`/`run_canonical_shadow_part` -- via
`canonical_history_simples.py`, still outside the monthly pipeline and
public products.

The only writer-level difference from estabelecimento is that simples
declares `duplicate_policy="deterministic-collapse"` (the production loader
already runs `_dedupe_cnpj_basico_table` for `simples`, the same call as for
`empresa` -- see `registry.SIMPLES_CANONICAL`'s comment), which
`write_canonical_part` handles by both collapsing duplicates AND emitting
rows in explicit primary-key order -- the same ordering guarantee empresa's
dataset writer has, fixed in [PR #107](https://github.com/franklinbaldo/ficha/pull/107)
so it applies to any single-file `deterministic-collapse` table, not just
multi-part ones.

## Manual workflow

Use **Canonical Shadow — historical simples dataset**
(`canonical-simples-history.yml`). Inputs are:

- `month`: historical IA item suffix (`YYYY-MM`);
- `sample_size`: deterministic reversible sample size (default `1000`).

Pull requests never touch the network: the PR smoke job runs the same
orchestration against one tiny generated `Simples.zip` fixture (an exact
duplicate, a genuinely conflicting duplicate, and a malformed date), plus a
separate check -- also fully offline -- that a missing, invalid, or
misnamed local override fails closed.

## Disk lifecycle and no checkpoint reuse

The one ZIP is downloaded (or read from a local override), checksummed,
extracted to its one CSV, and deleted immediately -- it is never retained
once its CSV is safely extracted and checksummed, on either the success or
the failure path. The extracted CSV and the DuckDB work directory are
removed after the writer returns, success or failure.

This orchestrator is **deliberately not restartable/checkpointed**, unlike
`canonical_history.py`'s per-part estabelecimento reuse. That mechanism has
known gaps tracked in [issue #103](https://github.com/franklinbaldo/ficha/issues/103);
reproducing it here without first closing #103 would just add a second copy
of the same trap. Every dispatch of this workflow performs a full fresh run.

## Real run (2026-04, complete Simples.zip)

The gate described below was exercised for real on 2026-07-20 via
[PR #107](https://github.com/franklinbaldo/ficha/pull/107) (an operational
PR that implements this orchestrator and dispatches the real workflow).
GitHub Actions artifacts expire, so the load-bearing numbers are recorded
here instead of only living in the run.

- Source commit: `8a468328e83c7bc03f2caeef8fc2742eba282d4d`
- Workflow run: [`29783648145`](https://github.com/franklinbaldo/ficha/actions/runs/29783648145)
- Target: `2026-04`, `Simples.zip`, sample size 1,000
- Artifact: `canonical-simples-history-2026-04-29783648145` (id `8477660005`),
  170,156,670 bytes, expires 2026-08-19 (30-day retention) -- this document
  and the checksums below are the durable record once it expires
- Source ZIP: 291,329,242 bytes,
  `sha256:9edefa550a98e949bfcb4aec7f5656cc3c10d2ca9a78ddb09c07e239b2d2aacc`
  (independently re-downloaded and recomputed to confirm this matches the
  manifest exactly)
- Extracted CSV: 3,030,113,835 bytes,
  `sha256:494360af6b9ed69b986584dd584e5555beba3f671ccaac4fe8f0a8cca5bbdd0f`
- Canonical output: `canonical/simples.parquet`, 174,496,651 bytes,
  `sha256:00ea6925caed7dcce0c8333ac632ffcb876c19f39461f8141686b43a3af22773`
- Evidence: `evidence/simples.quality.json`
  (`sha256:4762667552422fe84ee43b87fd48ef637a18b40c3f820d47a6dae3797eb52534`),
  `evidence/simples.metrics.json`
  (`sha256:52997a27e6fd39a8834f6057b482f5fea5d70a5f4f1879e7486ebc8d2c0e00a6`)

### Quality and cardinality

| Metric | Value |
|---|---:|
| Rows raw / canonical | 48,097,045 / 48,097,045 |
| Required-key failures (`cnpj_basico` null/blank) | 0 |
| Duplicate key count | **0** |
| Excess duplicate row count | **0** |
| Conflicting key count | **0** |
| Sample size / seed | 1,000 / 42 |
| Sample fingerprint | `6e0ec52a7b331b32514cd5b657b7aba9490d2a0293fbf4a758d47d84000f120f` |
| Sample mismatches | 0 |
| Schema match | yes |

`rows_canonical == rows_raw - duplicate_key_rows` holds exactly
(48,097,045 = 48,097,045 - 0). `files_read` in the resource envelope is `1`.
**No duplicate or conflicting `cnpj_basico` was found anywhere in the
2026-04 `Simples.zip`** -- a measured result for this specific snapshot, not
a general claim (see *Limitations* below for why the registry still
declares `duplicates-expected`/`deterministic-collapse` regardless).
Independently confirmed directly against the Parquet with DuckDB:
`COUNT(*) == COUNT(DISTINCT cnpj_basico) == 48,097,045`, zero null/blank
`cnpj_basico`, the schema matches `registry.SIMPLES_CANONICAL`
column-for-column (four typed `DATE` columns, two plain `VARCHAR`), rows
are emitted in non-decreasing `cnpj_basico` order end to end, and
`_source_file` contains only `"Simples.zip"`.

### Invalid date casts -- a real, measured, non-trivial rate

| Column | Invalid (nonblank, unparseable) | % of 48,097,045 rows |
|---|---:|---:|
| `data_opcao_simples` | 0 | 0.0% |
| `data_exclusao_simples` | 24,706,477 | 51.4% |
| `data_opcao_mei` | 12,857,692 | 26.7% |
| `data_exclusao_mei` | 29,646,508 | 61.6% |

These are not assumed to be zero, and they are not: the `null-and-count`
policy means these values are written as `NULL` in the canonical output
(never dropped or quarantined) and counted here, exactly as documented for
estabelecimento's much smaller invalid-date rate. The NULL count for each
of these three columns in the canonical Parquet matches its invalid-cast
count exactly -- there are no genuinely BLANK raw values in these fields at
all, only nonblank ones that either parse or don't.

Independently re-downloading the real `Simples.zip` and inspecting raw rows
confirms why: RFB does not leave these date fields blank when they don't
apply -- it writes the literal 8-character sentinel `"00000000"` (e.g. a
company still active in Simples has `data_exclusao_simples="00000000"`
rather than an empty field; a company that never opted into MEI has both
MEI dates as `"00000000"`). `try_strptime('00000000', '%Y%m%d')` correctly
fails to parse month/day `00` as a real date, so the current registry
contract counts this as an "invalid cast," not a "not applicable" case.
This is expected RFB convention, not corrupted or malformed data -- but the
registry does not currently distinguish "genuinely malformed" from "RFB's
own N/A sentinel," so the raw count is reported here without adjustment.
Teaching the cast to recognize `"00000000"` as an explicit null sentinel
(rather than counting it as an invalid cast) is a reasonable future
registry refinement, out of scope for this slice.

### Resource envelope

The `wall_seconds`/RSS/disk figures below come from the ONE metrics stage
this orchestrator instruments -- `canonical_simples_part`, i.e. the
canonical writer's own `write_canonical_part` call (`resource_summary.scope
== "canonical-writer-stage"` in the manifest). They do **not** include this
orchestrator's own preflight/download/extract time.

| Metric (canonical-writer stage only) | Value |
|---|---:|
| Wall time | 102.0 s |
| RSS peak / delta | 4,117.0 / 4,033.1 MiB |
| DuckDB temp peak | 0.0 MiB |
| Workdir peak | 480.1 MiB |
| Filesystem used peak | 36,612.7 MiB / 24.79% |
| Files read | 1 |
| DuckDB version | 1.5.2 |
| Execution profile | `threads=1`, `memory_limit=8.3 GiB` (auto-detected) |

Separately, the GitHub Actions **`historical` job as a whole** ran from
`2026-07-20T22:21:51Z` to `2026-07-20T22:25:21Z` -- about **3m30s** end to
end, covering checkout/setup, disk-space preparation, the preflight HEAD
check, downloading and extracting the one ZIP (~278 MiB compressed →
~2.82 GiB extracted), the 102.0s writer stage above, evidence/summary
writing and artifact upload. This total is reported here, not in the
manifest, precisely so it is never confused with the writer-stage-only
`wall_seconds` field. Simples' single file makes this run roughly 5.6x
faster end to end than empresa's ten-part run (~3m30s vs. ~19m33s).

## Limitations

- **Issue #76**: `simples`' `duplicate_policy="deterministic-collapse"`
  applies the same deterministic full-row tiebreak
  `transform._dedupe_cnpj_basico_table` uses in production to any
  conflicting duplicate it finds. That survivor is *reproducible* but not
  *proven semantically correct* -- no domain rule establishes that the
  deterministically-chosen row is the "right" one when two conflicting
  simples records share a `cnpj_basico`. This run measured zero such
  conflicts for 2026-04, so the question did not arise in practice here,
  but the policy -- and its unresolved semantic-correctness question --
  remains in effect for any future snapshot that does have conflicts.
- The `"00000000"` date sentinel (see above) means the invalid-cast rate for
  three of the four date columns is large and expected, not a data-quality
  alarm -- but the registry does not yet distinguish it from a genuinely
  malformed value.
- This is fixture-free, real-data evidence for exactly one snapshot
  (2026-04). It says nothing about `socio` or lookups (slices 5-6 of issue
  #97), about whether 2026-04 is representative of other months, or about
  the monthly product pipeline (this writer does not feed it).
- Not restartable -- see *Disk lifecycle and no checkpoint reuse* above.

## Deliberate boundary

This workflow processes the complete `Simples.zip` and publishes only a
temporary GitHub Actions artifact. It does not upload canonical data to
Internet Archive or feed a product. The next gates, per
[issue #97](https://github.com/franklinbaldo/ficha/issues/97), are `socio`
(slice 5, pending a key/cardinality investigation) and the lookups
(slice 6) -- only after all entities have canonical coverage does
triangular raw/canonical/product validation become meaningful.
