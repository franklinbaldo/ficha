"""Table references.

Thin wrappers over `con.table(...)` so callers don't have to remember
parquet file names. The schemas are documented in `web/src/schemas/v1/`
(Zod) and pinned by the parquet footer's `ficha.schema_version`.
"""

from __future__ import annotations

from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Table


def cnpjs(con: DuckDBBackend) -> Table:
    """One row per estabelecimento, denormalized with empresa + simples + lookups inline.

    Sort: cnpj. Bloom filter: cnpj. Best for direct lookup by CNPJ.
    See ADR 0008.
    """
    return con.table("cnpjs")


def raizes(con: DuckDBBackend) -> Table:
    """One row per raiz (cnpj_base), with aggregates (qtd_estab, array of UFs).

    Sort: razao_social_normalizada. Best for autocomplete and aggregation.
    """
    return con.table("raizes")


def socios(con: DuckDBBackend) -> Table:
    """Sócios PF and PJ mixed, with `tipo` flag.

    Sort: cnpj_base. Bloom filter: cnpj_base, cpf_socio_mascarado.
    Best for "sócios de X" and reverse "X é sócio de Y" queries.
    """
    return con.table("socios")
