# ficha-py

Python analytical layer for [FICHA](https://github.com/franklinbaldo/ficha):
Ibis expressions over the CNPJ parquets published on the Internet Archive.

One mental model for the ETL, for Colab notebooks, and (eventually) for the
frontend. See [ADR 0017](../docs/adr/0017-ibis-shared-analytical-layer.md).

## Install

```bash
pip install ficha-py
```

## Quick start (notebook)

```python
import ficha_py
from ficha_py import _

# Stream parquets directly from the Internet Archive (no download).
con = ficha_py.connect_ia(month="2026-04")

# Five companies in São Paulo
ficha_py.cnpjs(con).filter(_.uf == "SP").limit(5).execute()

# Sócios of a given raiz (cnpj_base)
ficha_py.socios_de(con, "00000001").execute()
```

## Quick start (local snapshot)

If you've downloaded a snapshot directory containing `cnpjs.parquet`,
`raizes.parquet`, `socios.parquet`:

```python
import ficha_py
con = ficha_py.connect_local("/path/to/snapshot")
ficha_py.cnpjs(con).count().execute()
```

## What's exported

| Symbol | What |
|---|---|
| `connect_local(parquet_dir)` | DuckDB backend over a local snapshot dir |
| `connect_ia(month)` | DuckDB backend reading IA parquets via httpfs |
| `cnpjs(con)` | Ibis Table — one row per estabelecimento |
| `raizes(con)` | Ibis Table — one row per raiz (cnpj_base) |
| `socios(con)` | Ibis Table — sócios PF + PJ |
| `socios_de(con, cnpj_base)` | Filtered sócios for a single raiz |
| `_` | Re-export of `ibis._` (column reference shorthand) |

The same expressions run unchanged against `connect_local` or `connect_ia`.

## Why Ibis?

Three FICHA surfaces query the same parquets:
ETL, notebooks, frontend. Ibis lets us define the analytical vocabulary
once and execute it against any DuckDB-compatible backend (embedded,
DuckDB+httpfs, DuckDB-WASM in the browser). See ADR 0017 for the full
rationale.
