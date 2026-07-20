# Canonical estabelecimento shadow part

This is the first executable slice of RFC 0001 Phase 2. It converts one
**already-extracted** Receita Federal `estabelecimento` CSV into one typed
canonical Parquet part. It is intentionally outside the monthly production
pipeline and does not feed public products.

## Run

```bash
cd etl
uv run python -m ficha_etl.canonical_shadow \
  --csv /path/to/Estabelecimentos0.csv \
  --source-file Estabelecimentos0.zip \
  --snapshot 2026-07 \
  --output /path/to/canonical/estabelecimento/part-0.parquet
```

Optional flags select the work directory, quality-report path, metrics path,
sample size and whether to retain the file-backed DuckDB work state.

## Publication gates

The final Parquet replaces the target only after all of these pass:

- every full-CNPJ key component is present and nonblank;
- the full-CNPJ key is unique within the part;
- raw and canonical row counts match;
- Parquet names and DuckDB types match the registry exactly;
- a deterministic `reservoir ... REPEATABLE(42)` sample roundtrips every raw
  field, all three typed dates and both lineage fields.

Nonblank date values that cast to `NULL` are counted under the registry's
`null-and-count` policy. They do not disappear silently.

## Evidence

A run writes two sidecar files unless their paths are overridden:

- `part-0.quality.json`: raw/canonical counts, key failures, duplicate excess,
  invalid casts by column, sample fingerprint/mismatches, schema result and the
  experimental physical profile;
- `part-0.metrics.json`: the shared RFC metrics envelope, including effective
  DuckDB pragmas, code/runtime versions, wall time, RSS and disk peaks.

A failed load-bearing gate preserves quality/resource evidence and does not
replace an existing Parquet target.

## Deliberate boundary

This slice does not fetch or extract ZIPs, merge the ten establishment parts,
publish artifacts, implement checksum-backed resume, or compare a canonical
product with the current public product. Those are responsibilities of the
historical shadow-workflow slice.

`ZSTD` and row groups of `200,000` are explicit experimental writer defaults,
aligned with current writers. The registry continues to leave codec, row-group
size and bucketing undecided until a historical shadow run produces evidence.
