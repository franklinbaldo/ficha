"""Cliente WebDAV pro Nextcloud share da RFB.

Modelo (ADR 0015):

    https://arquivos.receitafederal.gov.br/public.php/webdav/
    │
    ├── 2023-05/
    │   ├── Empresas0.zip ... Empresas9.zip
    │   ├── Estabelecimentos0.zip ... Estabelecimentos9.zip
    │   ├── Socios0.zip ... Socios9.zip
    │   └── Simples.zip Cnaes.zip Motivos.zip Municipios.zip
    │       Naturezas.zip Paises.zip Qualificacoes.zip
    │
    ├── 2023-06/ ... 2026-04/   (mesma estrutura, 35 meses)
    │
    └── cnpj.tar.gz  (legacy bundle, ignorado)

Auth: Basic com `username = TOKEN`, password vazio.

`upstream` é o ÚNICO ponto de contato com o RFB. Tudo o mais (transform,
frontend) consome do mirror IA (ver ADR 0012).
"""

from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Final

import httpx

from .sources import RemoteFile, canonical_inventory, is_valid_month

log = logging.getLogger(__name__)

DEFAULT_BASE_URL: Final = "https://arquivos.receitafederal.gov.br"
WEBDAV_PATH: Final = "/public.php/webdav"

# Token público observado estável desde 2023-05. Atualizar via PR se rotacionar.
KNOWN_TOKENS: Final[tuple[str, ...]] = ("YggdBLfdninEJX9",)

ENV_VAR: Final = "CNPJ_SHARE_TOKEN"
ENV_BASE_URL: Final = "FICHA_RFB_BASE_URL"

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)

# Apenas pasta `YYYY-MM/` é considerada snapshot válido.
_MONTH_DIR_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

_DAV_NS = "{DAV:}"


class NoTokenError(RuntimeError):
    """Levantado quando nenhum token disponível responde."""


@dataclass(frozen=True)
class FileEntry:
    """Metadado de um arquivo dentro de uma pasta mensal."""

    name: str
    size: int
    etag: str
    content_type: str


def base_url() -> str:
    """Origem do Nextcloud, overridable via env var."""
    return os.environ.get(ENV_BASE_URL, DEFAULT_BASE_URL).rstrip("/")


def webdav_url(*parts: str) -> str:
    """Junta `parts` ao path WebDAV. Sem encoding — usar nomes ASCII."""
    if not parts:
        return f"{base_url()}{WEBDAV_PATH}/"
    suffix = "/".join(p.strip("/") for p in parts)
    return f"{base_url()}{WEBDAV_PATH}/{suffix}"


def discover_token(*, client: httpx.Client | None = None) -> str:
    """Resolve um token funcional. Tenta env → KNOWN_TOKENS, valida via PROPFIND."""
    own_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        env_tok = os.environ.get(ENV_VAR, "").strip()
        if env_tok and _token_works(env_tok, client):
            log.info("token from env (%s)", ENV_VAR)
            return env_tok
        for tok in KNOWN_TOKENS:
            if _token_works(tok, client):
                log.info("token from KNOWN_TOKENS")
                return tok
        raise NoTokenError(
            f"no working share token found (tried env {ENV_VAR} + "
            f"{len(KNOWN_TOKENS)} known tokens)"
        )
    finally:
        if own_client:
            client.close()


def list_snapshots(token: str, *, client: httpx.Client | None = None) -> list[str]:
    """PROPFIND root, devolve lista de pastas YYYY-MM ordenadas."""
    own_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        body = _propfind(client, token, webdav_url())
        months: list[str] = []
        for href in _hrefs(body):
            name = href.rstrip("/").rsplit("/", 1)[-1]
            if _MONTH_DIR_RE.fullmatch(name):
                months.append(name)
        return sorted(set(months))
    finally:
        if own_client:
            client.close()


def list_files(
    token: str, month: str, *, client: httpx.Client | None = None
) -> list[FileEntry]:
    """PROPFIND da pasta do mês, devolve metadados dos arquivos."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    own_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        url = webdav_url(month) + "/"
        body = _propfind(client, token, url)
        return _parse_files(body, month)
    finally:
        if own_client:
            client.close()


def file_url(token: str, month: str, filename: str) -> str:
    """URL absoluta de download de um arquivo dentro de um mês."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return webdav_url(month, filename)


def files_for_month(token: str, month: str) -> list[RemoteFile]:
    """Lista canônica (37 specs) com URLs WebDAV pro mês alvo."""
    return [
        RemoteFile(name=spec.name, url=file_url(token, month, spec.name), kind=spec.kind)
        for spec in canonical_inventory()
    ]


def _token_works(token: str, client: httpx.Client) -> bool:
    """PROPFIND Depth: 0 na raiz — espera 207 Multi-Status."""
    try:
        r = client.request(
            "PROPFIND",
            webdav_url(),
            auth=httpx.BasicAuth(token, ""),
            headers={"Depth": "0", "Content-Type": "text/xml"},
        )
    except httpx.HTTPError as exc:
        log.debug("token probe http error: %s", exc)
        return False
    return r.status_code in (200, 207)


def _propfind(client: httpx.Client, token: str, url: str) -> bytes:
    r = client.request(
        "PROPFIND",
        url,
        auth=httpx.BasicAuth(token, ""),
        headers={"Depth": "1", "Content-Type": "text/xml"},
    )
    r.raise_for_status()
    return r.content


def _hrefs(body: bytes) -> list[str]:
    """Extrai todos os <d:href> do XML PROPFIND."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        log.warning("propfind XML parse failed: %s", exc)
        return []
    return [el.text or "" for el in root.iter(f"{_DAV_NS}href")]


def _parse_files(body: bytes, month: str) -> list[FileEntry]:
    """Parseia respostas <d:response> em FileEntry. Pula a própria pasta."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        log.warning("propfind XML parse failed: %s", exc)
        return []
    out: list[FileEntry] = []
    for resp in root.iter(f"{_DAV_NS}response"):
        href_el = resp.find(f"{_DAV_NS}href")
        if href_el is None or not href_el.text:
            continue
        href = href_el.text.rstrip("/")
        name = href.rsplit("/", 1)[-1]
        if not name or name == month:
            continue  # entrada da própria pasta
        # Procura o propstat com 200 OK
        prop = None
        for ps in resp.iter(f"{_DAV_NS}propstat"):
            status_el = ps.find(f"{_DAV_NS}status")
            if status_el is not None and "200" in (status_el.text or ""):
                prop = ps.find(f"{_DAV_NS}prop")
                break
        if prop is None:
            continue
        size = _int_text(prop.find(f"{_DAV_NS}getcontentlength")) or 0
        ctype = _text(prop.find(f"{_DAV_NS}getcontenttype")) or ""
        etag = _text(prop.find(f"{_DAV_NS}getetag")) or ""
        # Pula coleções (subpastas) — não esperadas em mês, mas defensivo
        rt = prop.find(f"{_DAV_NS}resourcetype")
        if rt is not None and rt.find(f"{_DAV_NS}collection") is not None:
            continue
        out.append(FileEntry(name=name, size=size, etag=etag, content_type=ctype))
    out.sort(key=lambda f: f.name)
    return out


def _text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return (el.text or "").strip().strip('"')


def _int_text(el: ET.Element | None) -> int | None:
    txt = _text(el)
    if not txt:
        return None
    try:
        return int(txt)
    except ValueError:
        return None
