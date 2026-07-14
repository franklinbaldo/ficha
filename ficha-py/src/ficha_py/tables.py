"""Table references.

Thin wrappers over `con.table(...)` so callers don't have to remember
parquet file names. The schemas are documented in `web/src/schemas/v1/`
(Zod) and pinned by the parquet footer's `ficha.schema_version`.
"""

from __future__ import annotations

from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Table

# Mirrors ficha_etl.transform._LOOKUP_KINDS (tabelas (codigo, descricao)
# pequenas, encoding ISO-8859-1). Mantido independente ali propositalmente —
# ficha-py não depende de ficha-etl, só do shape publicado no snapshot.
LOOKUP_KINDS: tuple[str, ...] = (
    "cnaes",
    "motivos",
    "municipios",
    "naturezas",
    "paises",
    "qualificacoes",
)


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


def enderecos(con: DuckDBBackend) -> Table:
    """Reverse lookup por endereço/município.

    Sort: (uf, municipio_codigo, logradouro_normalizado, numero).
    See ADR 0023.
    """
    return con.table("enderecos")


def pessoas(con: DuckDBBackend) -> Table:
    """Reverse lookup PF: em quais empresas uma pessoa aparece (sócio ou representante).

    Sort: (cpf_mascarado, nome_normalizado). Chave composta — ver ADR 0024.
    """
    return con.table("pessoas")


def cnpj_cnaes(con: DuckDBBackend) -> Table:
    """Associação CNPJ↔CNAE posicional (posicao=0 é o principal).

    Sort: (cnae_codigo, posicao, cnpj_base). See ADR 0020.
    """
    return con.table("cnpj_cnaes")


def cnpj_contatos(con: DuckDBBackend) -> Table:
    """Reverse lookup de contatos (telefone/fax/email) por CNPJ.

    Sort: (tipo, valor, cnpj). See ADR 0021.
    """
    return con.table("cnpj_contatos")


def lookup(con: DuckDBBackend, kind: str) -> Table:
    """Tabela de referência bruta `(codigo, descricao)` para um dos seis kinds.

    `kind` deve estar em `LOOKUP_KINDS`. Nome da tabela segue a convenção
    `lookup_<kind>` já usada pelo ETL (`ficha_etl.transform._LOOKUP_KINDS`).
    """
    if kind not in LOOKUP_KINDS:
        raise ValueError(f"kind must be one of {LOOKUP_KINDS}, got {kind!r}")
    return con.table(f"lookup_{kind}")
