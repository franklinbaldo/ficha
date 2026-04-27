"""Discovery do token Nextcloud da RFB e construção de URLs upstream.

Ver ADR 0013 — three-layer fallback: env → known tokens → scrape.

Este módulo é o ÚNICO ponto de contato com o Nextcloud da RFB. Tudo o mais
(transform, frontend) consome do mirror IA (ADR 0012).
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Final

import httpx

from .sources import RemoteFile, canonical_inventory

log = logging.getLogger(__name__)

NEXTCLOUD_BASE: Final = "https://arquivos.receitafederal.gov.br"

# Página oficial onde a RFB linka o share atual.
RFB_LANDING_PAGE: Final = (
    "https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/"
    "cadastros/consultas/dados-publicos-cnpj"
)

# Tokens observados em uso (ver ADR 0013). Atualizar via PR quando rotacionarem.
KNOWN_TOKENS: Final[tuple[str, ...]] = (
    "gn672Ad4CF8N6TK",
    "YggdBLfdninEJX9",
)

ENV_VAR: Final = "CNPJ_SHARE_TOKEN"

_TOKEN_RE = re.compile(r"arquivos\.receitafederal\.gov\.br/s/([A-Za-z0-9]{10,32})")
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)


class NoTokenFoundError(RuntimeError):
    """Levantado quando todas as 3 estratégias de discovery falham."""


@dataclass(frozen=True)
class TokenDiscovery:
    token: str
    source: str  # "env" | "known" | "scrape"


def discover_token(*, client: httpx.Client | None = None) -> TokenDiscovery:
    """Tenta env → known → scrape e devolve o primeiro token que responde 200."""
    own_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        env_tok = os.environ.get(ENV_VAR, "").strip()
        if env_tok:
            log.info("token from env (%s); not validating", ENV_VAR)
            return TokenDiscovery(token=env_tok, source="env")

        for tok in KNOWN_TOKENS:
            if _is_token_live(tok, client):
                log.info("token from KNOWN_TOKENS: %s", tok)
                return TokenDiscovery(token=tok, source="known")

        scraped = _scrape_token(client)
        if scraped and _is_token_live(scraped, client):
            log.info("token from scrape: %s", scraped)
            return TokenDiscovery(token=scraped, source="scrape")

        raise NoTokenFoundError(
            "no working share token found "
            f"(tried env {ENV_VAR}, {len(KNOWN_TOKENS)} known tokens, "
            "and scrape of gov.br landing page)"
        )
    finally:
        if own_client:
            client.close()


def share_root_url(token: str) -> str:
    return f"{NEXTCLOUD_BASE}/s/{token}"


def file_url(token: str, filename: str) -> str:
    """URL Nextcloud de download de um arquivo específico do share."""
    return f"{NEXTCLOUD_BASE}/s/{token}/download?path=%2F&files={filename}"


def files_in_share(token: str) -> list[RemoteFile]:
    """Inventário canônico em URLs Nextcloud — mesma lista que `canonical_inventory`."""
    return [
        RemoteFile(name=spec.name, url=file_url(token, spec.name), kind=spec.kind)
        for spec in canonical_inventory()
    ]


def _is_token_live(token: str, client: httpx.Client) -> bool:
    try:
        r = client.head(share_root_url(token))
        return 200 <= r.status_code < 400
    except httpx.HTTPError as exc:
        log.debug("token %s probe failed: %s", token, exc)
        return False


def _scrape_token(client: httpx.Client) -> str | None:
    try:
        r = client.get(RFB_LANDING_PAGE)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        log.warning("scrape of %s failed: %s", RFB_LANDING_PAGE, exc)
        return None
    matches = _TOKEN_RE.findall(r.text)
    if not matches:
        log.warning("no token pattern matched in landing page")
        return None
    # Tokens podem aparecer várias vezes; pega o mais frequente como heurística.
    from collections import Counter
    most_common, _ = Counter(matches).most_common(1)[0]
    return most_common


