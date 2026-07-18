"""Smoke check do ETL.

Modelo (ADR 0012 + 0015):

    RFB Nextcloud (WebDAV)  →  ficha-YYYY-MM @ Internet Archive  →  frontend

Smoke verifica os dois alvos:

1. **Upstream**: PROPFIND root do Nextcloud com token discovered.
2. **Mirror**: HEAD em archive.org/.

Mirror caído = bloqueante. Upstream caído = warning não-bloqueante.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

from . import mirror, upstream

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)

# O Internet Archive emite 5xx/429 transitórios (derive assíncrono, rate-limit —
# ver comentários em manifest.py e vision-blockers-2026-07). Um único HEAD 503
# não deve alarmar como outage bloqueante: tenta algumas vezes antes de falhar.
_MIRROR_ATTEMPTS = 3
_MIRROR_BACKOFF_S = 2.0  # linear: espera 2s, depois 4s entre tentativas


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
        """Apenas mirror caído é bloqueante. Upstream caído = warning."""
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
        token = upstream.discover_token(client=client)
    except upstream.NoTokenError as exc:
        return False, str(exc)
    try:
        snapshots = upstream.list_snapshots(token, client=client)
    except httpx.HTTPError as exc:
        return False, f"PROPFIND failed with token={token}: {exc}"
    if not snapshots:
        return False, f"token={token} responded but listed 0 snapshots"
    return True, (
        f"token={token}  snapshots={len(snapshots)}  oldest={snapshots[0]}  newest={snapshots[-1]}"
    )


def _mirror_head_once(client: httpx.Client, url: str) -> tuple[bool, str, bool]:
    """Um HEAD. Retorna (ok, detail, retryable).

    retryable=True quando o erro é transitório (falha de transporte, 5xx ou 429)
    e vale a pena tentar de novo; 4xx (exceto 429) é definitivo.
    """
    try:
        r = client.head(url)
    except httpx.HTTPError as exc:
        return False, f"{url} → {exc}", True
    if 200 <= r.status_code < 400:
        return True, f"{url} → HTTP {r.status_code}", False
    retryable = r.status_code >= 500 or r.status_code == 429
    return False, f"{url} → HTTP {r.status_code}", retryable


def _check_mirror(client: httpx.Client) -> tuple[bool, str]:
    """HEAD em archive.org/ com retry para erros transitórios.

    Mirror caído é bloqueante (SmokeReport.blocking_failure), então um 503
    transitório do IA não pode virar falha de smoke por si só — senão o check
    semanal fica vermelho por um soluço de derive/rate-limit. Falha apenas se
    todas as tentativas falharem ou o erro for definitivo (4xx que não 429).
    """
    url = mirror.health_url()
    detail = ""
    for attempt in range(1, _MIRROR_ATTEMPTS + 1):
        ok, detail, retryable = _mirror_head_once(client, url)
        if ok:
            return True, detail + (f" (após {attempt} tentativas)" if attempt > 1 else "")
        if not retryable:
            return False, detail
        if attempt < _MIRROR_ATTEMPTS:
            log.warning("mirror smoke: %s — tentativa %d/%d", detail, attempt, _MIRROR_ATTEMPTS)
            time.sleep(_MIRROR_BACKOFF_S * attempt)
    return False, f"{detail} (após {_MIRROR_ATTEMPTS} tentativas)"
