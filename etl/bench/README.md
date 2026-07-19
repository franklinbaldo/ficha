# ETL transform benchmark

Scaled, repeatable timing for the `ficha_etl.transform` stages, so each
performance change is defended by a *measured* number instead of intuition.
It runs the real stage functions on synthetic RFB-shaped data big enough to be
representative but small enough to iterate on a laptop.

## Run

```bash
# whole-pipeline stage timings (data cached under bench/.work by scale/chunks)
uv run --all-extras python bench/benchmark.py --scale 500000 --chunks 8
uv run --all-extras python bench/benchmark.py --scale 2000000 --chunks 16 --json out.json
```

`--scale` = number of empresas (unique `cnpj_basico`); establishments come out
~1.33× that (one matriz each, plus a filial for every third base). Numbers are
wall-clock seconds on **this** machine — compare a stage against itself across a
code change, not against another machine. Run twice, trust the second (warm)
run. `bench/.work/` is gitignored.

## Method notes

- The stage order mirrors `transform_snapshot`: load lookups → load
  empresa/simples/socio → load estabelecimento → contatos/cnaes/enderecos (scan
  the in-memory table) → drop table → chunked cnpjs (reloads CSV per chunk) →
  roundtrip verify (reloads CSV).
- For a head-to-head between two implementations of the *same* stage, prefer an
  **in-process interleaved A/B** (see `ab_contatos_cnaes.py`) over comparing two
  git branches: running both variants in one process on one loaded table, with
  iterations interleaved, cancels the thermal/background drift that otherwise
  shows up as a ~10-15% offset between separate runs.

## Findings so far

| Change | Stage | Result |
|---|---|---|
| Single-query `reservoir REPEATABLE` verify | `verify_roundtrip` | **23.4s → 1.1s** at 300k (21×); the old `ORDER BY random()` + 1000 point-lookups dominated the whole run |
| One-scan contatos (`LATERAL VALUES`) | `write_cnpj_contatos` | **2.04× slower** (0.835s → 1.700s at 1M). Output identical, but rejected — DuckDB parallelizes the 4 independent scans better than one correlated fan-out. |
| One-scan cnaes (`list_concat`+`UNNEST`) | `write_cnpj_cnaes` | **1.16× slower** (0.743s → 0.862s). Rejected for the same reason. |

The contatos/cnaes result is why the "collapse to one scan" idea was **not**
merged: the scans are over an already-loaded in-memory columnar table (cheap and
parallel), so eliminating them by doing more per-row work is a net loss. Keep the
UNION-ALL versions. `ab_contatos_cnaes.py` is retained as the evidence and as a
template for future same-stage A/Bs.
