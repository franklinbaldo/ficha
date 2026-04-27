"""Acesso direto ao Nextcloud-flat da RFB.

Modelo (ADR 0014, supersedes ADR 0013):

A RFB publica os dumps abertos do CNPJ num diretório **flat**, sem mês
no path:

    https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip
    https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos0.zip
    ...

Apenas o snapshot **atual** é servido — o conteúdo é sobrescrito a cada
release. Histórico é responsabilidade do mirror IA (ADR 0012).

Confirmado em 2026 via portal oficial dados.gov.br
(`/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`).
"""

from __future__ import annotations

import os

from .sources import RemoteFile, canonical_inventory

DEFAULT_RFB_BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ"


def base_url() -> str:
    """RFB base URL, overridable via env var (testes / mirror alternativo)."""
    return os.environ.get("FICHA_RFB_BASE_URL", DEFAULT_RFB_BASE_URL).rstrip("/")


def file_url(filename: str) -> str:
    """URL direta de um ZIP do release atual."""
    return f"{base_url()}/{filename}"


def current_files() -> list[RemoteFile]:
    """Lista de 37 arquivos do snapshot atual, com URLs RFB diretas."""
    return [
        RemoteFile(name=spec.name, url=file_url(spec.name), kind=spec.kind)
        for spec in canonical_inventory()
    ]
