"""Composable analytical views.

These are *expressions*, not query results: callers can chain `.filter()`,
`.select()`, `.limit()`, `.execute()` to materialize. The same expression
runs against `connect_local` or `connect_ia` without modification.
"""

from __future__ import annotations

from ibis import _
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Table

from .tables import socios as _socios_table


def socios_de(con: DuckDBBackend, cnpj_base: str) -> Table:
    """Sócios (PF + PJ) of a given raiz, by `cnpj_base`.

    Returns an Ibis Table expression filtered to the requested raiz.
    Add `.execute()` to materialize a pandas DataFrame.
    """
    if not cnpj_base or len(cnpj_base) != 8 or not cnpj_base.isdigit():
        raise ValueError(f"cnpj_base must be 8 digits, got {cnpj_base!r}")
    return _socios_table(con).filter(_.cnpj_base == cnpj_base)
