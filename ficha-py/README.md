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

If you've downloaded a full snapshot directory (the 7 main parquets plus
`lookups/<kind>.parquet` for each lookup — same set
`ficha_etl.manifest.build_snapshot_entry` requires):

```python
import ficha_py
con = ficha_py.connect_local("/path/to/snapshot")
ficha_py.cnpjs(con).count().execute()
```

## Notebooks

See `../notebooks/` for runnable Colab-style examples: getting started,
aggregation by UF, and 1-hop graph traversal via `pessoas.parquet`.

## What's exported

| Symbol | What |
|---|---|
| `connect_local(parquet_dir)` | DuckDB backend over a local snapshot dir |
| `connect_ia(month)` | DuckDB backend reading IA parquets via httpfs |
| `cnpjs(con)` | Ibis Table — one row per estabelecimento |
| `raizes(con)` | Ibis Table — one row per raiz (cnpj_base) |
| `socios(con)` | Ibis Table — sócios PF + PJ |
| `enderecos(con)` | Ibis Table — reverse lookup por endereço/município (ADR 0023) |
| `pessoas(con)` | Ibis Table — reverse lookup PF por CPF mascarado + nome (ADR 0024) |
| `cnpj_cnaes(con)` | Ibis Table — associação CNPJ↔CNAE posicional (ADR 0020) |
| `cnpj_contatos(con)` | Ibis Table — reverse lookup de telefone/fax/email (ADR 0021) |
| `lookup(con, kind)` | Ibis Table bruta `(codigo, descricao)` para um dos 6 lookups |
| `lookup_normalized(con, kind)` | idem, + `descricao_normalizada` (ADR 0019) |
| `socios_de(con, cnpj_base)` | Filtered sócios for a single raiz |
| `filiais_de(con, cnpj_base)` | Filtered estabelecimentos (matriz + filiais) for a single raiz |
| `LOOKUP_KINDS` | Tuple with the 6 valid lookup kinds |
| `_` | Re-export of `ibis._` (column reference shorthand) |

The same expressions run unchanged against `connect_local` or `connect_ia`.

## Used by the ETL

`ficha_etl.transform.write_lookup_parquets` imports
`ficha_py.views.lookup_normalized` to build the exact same expression it
writes to `lookups/<kind>.parquet` — one definition, shared by the ETL and
by any notebook. The heavier joins (`cnpjs`/`raizes`) stay in raw SQL in the
ETL; see the "Estado da implementação" section of
[ADR 0017](../docs/adr/0017-ibis-shared-analytical-layer.md) for why.

## Why Ibis?

Three FICHA surfaces query the same parquets:
ETL, notebooks, frontend. Ibis lets us define the analytical vocabulary
once and execute it against any DuckDB-compatible backend (embedded,
DuckDB+httpfs, DuckDB-WASM in the browser). See ADR 0017 for the full
rationale.
