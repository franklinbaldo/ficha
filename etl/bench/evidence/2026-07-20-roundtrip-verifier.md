# Roundtrip verifier decision evidence — 2026-07-20

This closes the last evidence gap left by the Fase 0 benchmark work. It compares
the historical multi-query verifier with the current single-query
`assert_roundtrip` implementation without changing production behavior.

## Provenance

- Source commit: `3648ca5801ecf4071114fafb24b0a7c08ca8ee27`
- Evidence run: `29709299668`
- Profile: 500,000 empresas, 10 estabelecimento chunks, 5 repetitions
- Source rows validated: 666,666
- Sample: 1,000 rows, reservoir `REPEATABLE(42)`
- Sample fingerprint: `d92ff5b347204796ffd480d0a13d6cd98e421411596894ec387b52561cb3a4b8`
- Harness: `2026-07-profile-v3`
- Runtime: Python 3.11.15, DuckDB 1.5.2, Linux x86_64, 4 CPUs
- Effective DuckDB profile: `threads=1`, file-backed databases,
  `memory_limit=8.3 GiB`, `preserve_insertion_order=false`

Each variant/repetition ran in a fresh child process against its own copy of the
same closed DuckDB database. Execution order alternated strictly between
current→legacy and legacy→current.

## Conservative comparison

The historical implementation used `ORDER BY random() LIMIT 1000` and then one
Parquet point query per sampled CNPJ. This benchmark deliberately gives the
legacy side the current deterministic reservoir sampler, so both variants
inspect the exact same rows and seed. The result isolates the structural change
under review—1,000 point queries versus one sampled join—and does **not** credit
the current verifier for also removing the old full-table random sort.

## Result

| Variant | Wall median | Wall spread | CPU median | CPU spread |
|---|---:|---:|---:|---:|
| Historical multi-query | 14.8044 s | 2.8281 s | 15.1652 s | 2.8338 s |
| Current single-query | 0.1492 s | 0.0037 s | 0.1496 s | 0.0039 s |

The current verifier is **99.2× faster by wall-clock median**, a 98.99% reduction.
The 14.6551 s median delta is far larger than the maximum observed spread, so the
result is not noise-dominated.

## Correctness and resource envelope

Across all five repetitions, both variants:

- accepted the valid Parquet;
- rejected a same-row-count Parquet whose `razao_social` was deliberately
  corrupted on every row;
- used the identical sample fingerprint shown above.

Neither variant spilled to `duckdb_tmp`, changed the copied database, or exceeded
the same 51,654,656-byte state peak. `ru_maxrss` established no new high-water
mark after the post-open baseline (1,216.7 MiB) in either variant; the resulting
zero RSS delta has the documented cumulative-process meaning and is not a claim
of zero memory use.

## Decision

Keep the current single-query `assert_roundtrip` implementation. The historical
multi-query shape is decisively rejected under the production profile even after
removing its old `ORDER BY random()` disadvantage.

With this result, every Fase 0 performance question recorded in the benchmark
README has decision-grade evidence. The benchmark-measurement phase is closed;
subsequent work can move to the first RFC delivery: canonical registry,
canonical estabelecimento, historical shadow workflow, and triangular
raw/canonical/product validation.
