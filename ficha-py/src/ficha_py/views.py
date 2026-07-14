"""Composable analytical views.

These are *expressions*, not query results: callers can chain `.filter()`,
`.select()`, `.limit()`, `.execute()` to materialize. The same expression
runs against `connect_local` or `connect_ia` without modification.
"""

from __future__ import annotations

import ibis
from ibis import _
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Table

from .tables import cnpjs as _cnpjs_table
from .tables import lookup as _lookup_table
from .tables import socios as _socios_table


def socios_de(con: DuckDBBackend, cnpj_base: str) -> Table:
    """Sócios (PF + PJ) of a given raiz, by `cnpj_base`.

    Returns an Ibis Table expression filtered to the requested raiz.
    Add `.execute()` to materialize a pandas DataFrame.
    """
    if not cnpj_base or len(cnpj_base) != 8 or not cnpj_base.isdigit():
        raise ValueError(f"cnpj_base must be 8 digits, got {cnpj_base!r}")
    return _socios_table(con).filter(_.cnpj_base == cnpj_base)


def filiais_de(con: DuckDBBackend, cnpj_base: str) -> Table:
    """Estabelecimentos (matriz + filiais) de uma raiz, by `cnpj_base`.

    Returns an Ibis Table expression filtered to the requested raiz.
    Add `.execute()` to materialize a pandas DataFrame.
    """
    if not cnpj_base or len(cnpj_base) != 8 or not cnpj_base.isdigit():
        raise ValueError(f"cnpj_base must be 8 digits, got {cnpj_base!r}")
    return _cnpjs_table(con).filter(_.cnpj_base == cnpj_base)


@ibis.udf.scalar.builtin
def strip_accents(s: str) -> str:
    """Stub para a função nativa `strip_accents` do DuckDB (sem extensão)."""


def lookup_normalized(con: DuckDBBackend, kind: str) -> Table:
    """Lookup `(codigo, descricao, descricao_normalizada)`, sorted by codigo.

    Mesma expressão usada por `ficha_etl.transform.write_lookup_parquets`
    (ADR 0019) — importada pelo ETL em vez de duplicada em SQL string, para
    que ETL e notebooks compartilhem a mesma definição de "lookup
    normalizado" (ADR 0017).
    """
    t = _lookup_table(con, kind)
    return t.select(
        "codigo",
        "descricao",
        descricao_normalizada=strip_accents(t.descricao).upper(),
    ).order_by("codigo")
