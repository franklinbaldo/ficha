"""Inventário canônico dos arquivos publicados pela RFB num release CNPJ.

Este módulo só fala de **nomes e tipos**. URLs concretas vivem nos módulos
de transporte (`upstream.py` para Nextcloud RFB, `mirror.py` para IA).

Ver ADR 0008 (3 Parquets), ADR 0010 (origem RFB), ADR 0012 (IA mirror).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

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
    """A single ZIP file expected in any monthly RFB release."""

    name: str  # e.g., "Empresas3.zip"
    url: str
    kind: FileKind


@dataclass(frozen=True)
class FileSpec:
    """Nome + kind dum arquivo, sem URL."""

    name: str
    kind: FileKind


def canonical_inventory() -> list[FileSpec]:
    """Lista os 37 arquivos esperados num release RFB completo."""
    out: list[FileSpec] = []
    for table in _BIG_TABLES:
        for n in range(10):
            out.append(FileSpec(name=f"{table}{n}.zip", kind=table.lower()))  # type: ignore[arg-type]
    for table in _SINGLE_TABLES:
        out.append(FileSpec(name=f"{table}.zip", kind=table.lower()))  # type: ignore[arg-type]
    return out


_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def is_valid_month(month: str) -> bool:
    return bool(_MONTH_RE.fullmatch(month))
