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
        f"token={token}  snapshots={len(snapshots)}  "
        f"oldest={snapshots[0]}  newest={snapshots[-1]}"
    )


def _check_mirror(client: httpx.Client) -> tuple[bool, str]:
    url = mirror.health_url()
    try:
        r = client.head(url)
    except httpx.HTTPError as exc:
        return False, f"{url} → {exc}"
    if 200 <= r.status_code < 400:
        return True, f"{url} → HTTP {r.status_code}"
    return False, f"{url} → HTTP {r.status_code}"
