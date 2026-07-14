"""FICHA — Python analytical layer.

Ibis expressions over the CNPJ parquets published on the Internet Archive
(see ADR 0017). One mental model for ETL, notebooks, and (eventually)
the frontend.

Quick start:

    >>> import ficha_py
    >>> con = ficha_py.connect_ia(month="2026-04")
    >>> ficha_py.cnpjs(con).filter(ficha_py._.uf == "SP").limit(5).execute()
"""

from __future__ import annotations

from ibis import _

from .connect import connect_ia, connect_local
from .tables import (
    LOOKUP_KINDS,
    cnpj_cnaes,
    cnpj_contatos,
    cnpjs,
    enderecos,
    lookup,
    pessoas,
    raizes,
    socios,
)
from .views import filiais_de, lookup_normalized, socios_de

__all__ = [
    "_",
    "connect_ia",
    "connect_local",
    "LOOKUP_KINDS",
    "cnpjs",
    "raizes",
    "socios",
    "enderecos",
    "pessoas",
    "cnpj_cnaes",
    "cnpj_contatos",
    "lookup",
    "socios_de",
    "filiais_de",
    "lookup_normalized",
]

__version__ = "0.2.0"
