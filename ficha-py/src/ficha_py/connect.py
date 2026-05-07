"""Connection helpers.

Two flavors:

- `connect_local(parquet_dir)`: read parquets from a local directory.
  Used by the ETL after it writes them, by tests with fixture parquets,
  and by users who downloaded a snapshot.

- `connect_ia(month)`: read parquets straight from the Internet Archive
  via DuckDB's httpfs extension. The standard Colab/notebook entry point.
"""

from __future__ import annotations

from pathlib import Path

import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend

_IA_BASE = "https://archive.org/download"
_ITEM_PREFIX = "ficha"
_PARQUETS = ("cnpjs", "raizes", "socios")


def _is_valid_month(month: str) -> bool:
    if len(month) != 7 or month[4] != "-":
        return False
    y, m = month[:4], month[5:]
    return y.isdigit() and m.isdigit() and 1 <= int(m) <= 12


def _ia_item_url(month: str) -> str:
    if not _is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return f"{_IA_BASE}/{_ITEM_PREFIX}-{month}"


def connect_local(parquet_dir: str | Path) -> DuckDBBackend:
    """Open an in-memory DuckDB and register cnpjs/raizes/socios from `parquet_dir`.

    Expected files: `cnpjs.parquet`, `raizes.parquet`, `socios.parquet`.
    """
    parquet_dir = Path(parquet_dir)
    con: DuckDBBackend = ibis.duckdb.connect()
    for name in _PARQUETS:
        path = parquet_dir / f"{name}.parquet"
        if path.exists():
            con.read_parquet(str(path), table_name=name)
    return con


def connect_ia(month: str) -> DuckDBBackend:
    """Open an in-memory DuckDB with httpfs and point cnpjs/raizes/socios at IA.

    DuckDB's httpfs extension fetches parquet metadata via HTTP range
    requests; only the row groups touched by a query are downloaded.
    Suitable for Colab where users want to run a single filter against
    multi-GB parquets without downloading them whole.
    """
    base = _ia_item_url(month)
    con: DuckDBBackend = ibis.duckdb.connect()
    con.raw_sql("INSTALL httpfs; LOAD httpfs;")
    for name in _PARQUETS:
        url = f"{base}/{name}.parquet"
        con.read_parquet(url, table_name=name)
    return con
