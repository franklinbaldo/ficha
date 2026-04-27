"""URLs, file inventory, and metadata constants for the RFB CNPJ dump.

See ADR 0010 for source-of-truth and override strategy.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Literal

DEFAULT_RFB_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
)

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def base_url() -> str:
    """RFB base URL, overridable via env var for tests/mirrors."""
    return os.environ.get("FICHA_RFB_BASE_URL", DEFAULT_RFB_BASE_URL).rstrip("/")


# RFB splits the three big tables into 10 ZIPs each (suffixes 0..9).
_BIG_TABLES = ("Empresas", "Estabelecimentos", "Socios")
_SINGLE_TABLES = (
    "Simples",
    "Cnaes",
    "Motivos",
    "Municipios",
    "Naturezas",
    "Paises",
    "Qualificacoes",
)


FileKind = Literal[
    "empresas",
    "estabelecimentos",
    "socios",
    "simples",
    "cnaes",
    "motivos",
    "municipios",
    "naturezas",
    "paises",
    "qualificacoes",
]


@dataclass(frozen=True)
class RemoteFile:
    """A single ZIP file in the RFB monthly dump."""

    name: str  # e.g., "Empresas3.zip"
    url: str
    kind: FileKind


def files_for_month(month: str, base: str | None = None) -> list[RemoteFile]:
    """Returns the full list of ZIPs published by RFB for a given snapshot.

    `month` must be in YYYY-MM format. Validation is the caller's job; we
    raise ValueError for obvious malformed input as a safety net.
    """
    if not _MONTH_RE.fullmatch(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    root = (base or base_url()).rstrip("/")
    out: list[RemoteFile] = []
    for table in _BIG_TABLES:
        for n in range(10):
            name = f"{table}{n}.zip"
            out.append(
                RemoteFile(
                    name=name,
                    url=f"{root}/{month}/{name}",
                    kind=table.lower(),  # type: ignore[arg-type]
                )
            )
    for table in _SINGLE_TABLES:
        name = f"{table}.zip"
        out.append(
            RemoteFile(
                name=name,
                url=f"{root}/{month}/{name}",
                kind=table.lower(),  # type: ignore[arg-type]
            )
        )
    return out


def is_valid_month(month: str) -> bool:
    return bool(_MONTH_RE.fullmatch(month))
