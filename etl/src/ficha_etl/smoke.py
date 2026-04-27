"""HEAD-only smoke check para detectar mudanças na URL/disponibilidade do RFB
sem baixar bytes.

Uso típico em CI scheduled — falha cedo se o RFB mudar layout/URL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from .sources import RemoteFile, base_url

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)


@dataclass
class SmokeResult:
    file: RemoteFile
    status: int | None
    size: int | None
    error: str | None

    @property
    def ok(self) -> bool:
        return self.status is not None and 200 <= self.status < 300


def smoke_check(files: list[RemoteFile]) -> list[SmokeResult]:
    """Roda HEAD em cada URL. Não levanta — devolve resultado por arquivo."""
    results: list[SmokeResult] = []
    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        for f in files:
            results.append(_head(f, client))
    return results


def diagnose_root(month: str) -> tuple[str, int | None, str | None]:
    """Faz HEAD na URL raiz dos dumps + na pasta do mês alvo.

    Retorna (url_da_pasta_do_mes, status_code, error_msg). Útil para
    distinguir 'RFB inteiro fora do ar' de 'mês ainda não publicado'.
    """
    month_url = f"{base_url()}/{month}/"
    try:
        with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
            r = client.head(month_url)
            return month_url, r.status_code, None
    except httpx.HTTPError as exc:
        return month_url, None, str(exc)


def _head(file: RemoteFile, client: httpx.Client) -> SmokeResult:
    try:
        r = client.head(file.url)
        cl = r.headers.get("content-length")
        size = int(cl) if cl and cl.isdigit() else None
        return SmokeResult(file=file, status=r.status_code, size=size, error=None)
    except httpx.HTTPError as exc:
        return SmokeResult(file=file, status=None, size=None, error=str(exc))
