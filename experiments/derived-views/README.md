# Derived views experiment

Goal: test small DuckDB views directly over the Parquets already published by FICHA, without introducing a second family of datasets.

The experiment treats published snapshot artifacts as inputs and derived SQL as task-specific convenience. A view is not canonical and materialization is optional.

## First pass

1. Resolve the current snapshot from `web/public/manifest.json`.
2. Inspect real Parquet schemas with DuckDB/httpfs.
3. Define 2–3 narrowly named views for concrete tasks.
4. Record `EXPLAIN`, row counts and elapsed time.
5. Materialize one view with `COPY ... TO ... (FORMAT PARQUET)` only to compare cost and size.
6. Repeat against the previous snapshot when input schemas are compatible.

## Promotion rule

A SQL definition leaves `experiments/` only after it has a concrete consumer, documented inputs/outputs, invariants, and evidence from a real snapshot.
