"""Smoke check do ETL: valida que upstream + mirror estão acessíveis.

Modelo (ADR 0012):
    RFB Nextcloud  →  ficha-YYYY-MM @ Internet Archive  →  frontend

Smoke verifica os dois alvos em separado:

1. **Upstream RFB**: consigo descobrir um token Nextcloud válido? (ADR 0013)
2. **Mirror IA**: consigo alcançar archive.org?

Cada lado é reportado independentemente. Falha total = ambos quebrados.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from . import mirror, upstream

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)


@dataclass
class SmokeReport:
    upstream_ok: bool
    upstream_detail: str
    mirror_ok: bool
    mirror_detail: str

    @property
    def all_ok(self) -> bool:
        return self.upstream_ok and self.mirror_ok

    @property
    def blocking_failure(self) -> bool:
        """Apenas mirror caído é bloqueante. Upstream sem token é warning
        — significa que operador precisa atualizar KNOWN_TOKENS manualmente,
        mas não bloqueia PRs do código."""
        return not self.mirror_ok


def run_smoke() -> SmokeReport:
    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        upstream_ok, upstream_detail = _check_upstream(client)
        mirror_ok, mirror_detail = _check_mirror(client)
    return SmokeReport(
        upstream_ok=upstream_ok,
        upstream_detail=upstream_detail,
        mirror_ok=mirror_ok,
        mirror_detail=mirror_detail,
    )


def _check_upstream(client: httpx.Client) -> tuple[bool, str]:
    try:
        result = upstream.discover_token(client=client)
    except upstream.NoTokenFoundError as exc:
        return False, str(exc)
    return True, f"token={result.token} source={result.source}"


def _check_mirror(client: httpx.Client) -> tuple[bool, str]:
    """HEAD na front page do IA — endpoint mais estável que `/download`."""
    url = mirror.health_url()
    try:
        r = client.head(url)
    except httpx.HTTPError as exc:
        return False, f"{url} → {exc}"
    if 200 <= r.status_code < 400:
        return True, f"{url} → HTTP {r.status_code}"
    return False, f"{url} → HTTP {r.status_code}"
