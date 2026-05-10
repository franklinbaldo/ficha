"""URLs do mirror FICHA no Internet Archive.

Cada snapshot mensal vive num item `ficha-YYYY-MM` no IA. Estrutura interna:

    ficha-2026-03/
      raw/
        Empresas0.zip ... Empresas9.zip
        Estabelecimentos0.zip ... Estabelecimentos9.zip
        Socios0.zip ... Socios9.zip
        Simples.zip Cnaes.zip Motivos.zip Municipios.zip
        Naturezas.zip Paises.zip Qualificacoes.zip
      cnpjs.parquet
      raizes.parquet
      socios.parquet
      cnpj_contatos.parquet
      lookups.json

Ver ADR 0012.
"""

from __future__ import annotations

import os

from .sources import RemoteFile, canonical_inventory, is_valid_month

DEFAULT_IA_BASE_URL = "https://archive.org/download"
DEFAULT_IA_HEALTH_URL = "https://archive.org/"
ITEM_PREFIX = "ficha"


def base_url() -> str:
    """IA download base, overridable via env var (testes / mirror alternativo)."""
    return os.environ.get("FICHA_IA_BASE_URL", DEFAULT_IA_BASE_URL).rstrip("/")


def health_url() -> str:
    """Endpoint pra checar se o IA está respondendo (frente do site, sempre 200)."""
    return os.environ.get("FICHA_IA_HEALTH_URL", DEFAULT_IA_HEALTH_URL)


def item_id(month: str) -> str:
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return f"{ITEM_PREFIX}-{month}"


def item_root(month: str) -> str:
    """URL base do item IA do mês."""
    return f"{base_url()}/{item_id(month)}"


def raw_file_url(month: str, filename: str) -> str:
    """URL do ZIP cru (mirror RFB) dentro do item."""
    return f"{item_root(month)}/raw/{filename}"


def parquet_url(month: str, name: str) -> str:
    """URL de um Parquet transformado dentro do item.

    `name` deve ser um dos: cnpjs, cnpj_contatos, raizes, socios.
    """
    return f"{item_root(month)}/{name}.parquet"


def lookups_url(month: str) -> str:
    return f"{item_root(month)}/lookups.json"


def lookup_parquet_url(month: str, kind: str) -> str:
    """URL de um parquet de lookup transformado dentro do item."""
    return f"{item_root(month)}/lookups/{kind}.parquet"


def raw_files_for_month(month: str) -> list[RemoteFile]:
    """Mesma lista de 37 arquivos do `canonical_inventory`, mas com URLs IA."""
    return [
        RemoteFile(name=spec.name, url=raw_file_url(month, spec.name), kind=spec.kind)
        for spec in canonical_inventory()
    ]
