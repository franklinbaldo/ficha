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

from .tables import LOOKUP_KINDS

_IA_BASE = "https://archive.org/download"
_ITEM_PREFIX = "ficha"
_PARQUETS = (
    "cnpjs",
    "raizes",
    "socios",
    "enderecos",
    "pessoas",
    "cnpj_cnaes",
    "cnpj_contatos",
)


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
    """Open an in-memory DuckDB and register the full snapshot from `parquet_dir`.

    Expected: the 7 main parquets (`cnpjs`, `raizes`, `socios`, `enderecos`,
    `pessoas`, `cnpj_cnaes`, `cnpj_contatos`) plus `lookups/<kind>.parquet`
    for each of `LOOKUP_KINDS`, registered as `lookup_<kind>`. All must be
    present -- a typo or interrupted copy that's missing one would otherwise
    leave the connection in a half-broken state where `cnpjs(con)` works but
    `socios(con)` fails with an opaque table-not-found error inside unrelated
    query code. Fail fast here instead. Mirrors the same set of files
    `ficha_etl.manifest.build_snapshot_entry` requires for a valid snapshot.
    """
    parquet_dir = Path(parquet_dir)
    expected = [(name, parquet_dir / f"{name}.parquet") for name in _PARQUETS]
    expected += [
        (f"lookup_{kind}", parquet_dir / "lookups" / f"{kind}.parquet") for kind in LOOKUP_KINDS
    ]
    missing = [path for _name, path in expected if not path.exists()]
    if missing:
        names = ", ".join(str(p) for p in missing)
        raise FileNotFoundError(
            f"snapshot dir {parquet_dir} is missing required parquet(s): {names}"
        )
    con: DuckDBBackend = ibis.duckdb.connect()
    for name, path in expected:
        con.read_parquet(str(path), table_name=name)
    return con


def connect_ia(month: str) -> DuckDBBackend:
    """Open an in-memory DuckDB with httpfs and point the full snapshot at IA.

    DuckDB's httpfs extension fetches parquet metadata via HTTP range
    requests; only the row groups touched by a query are downloaded.
    Suitable for Colab where users want to run a single filter against
    multi-GB parquets without downloading them whole.
    """
    base = _ia_item_url(month)
    con: DuckDBBackend = ibis.duckdb.connect()
    # Try LOAD first (works on pre-provisioned runtimes where INSTALL is
    # blocked: read-only extension dir, restricted egress, or environments
    # that ship httpfs in the base image). Only fall back to INSTALL if
    # the extension genuinely isn't on the path.
    try:
        con.raw_sql("LOAD httpfs;")
    except Exception:
        con.raw_sql("INSTALL httpfs; LOAD httpfs;")
    for name in _PARQUETS:
        con.read_parquet(f"{base}/{name}.parquet", table_name=name)
    for kind in LOOKUP_KINDS:
        con.read_parquet(f"{base}/lookups/{kind}.parquet", table_name=f"lookup_{kind}")
    return con
