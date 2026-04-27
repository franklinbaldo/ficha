"""HEAD-only smoke check para detectar mudanças na URL/disponibilidade do RFB
sem baixar bytes.

Uso típico em CI scheduled — falha cedo se o RFB mudar layout/URL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import httpx

from .sources import RemoteFile, base_url

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, (idx % 12) + 1


def find_latest_available_month(*, max_lookback: int = 6) -> str | None:
    """Probe parent directories backwards until one returns 200.

    Used in CI smoke pra que mês ainda não publicado pelo RFB não falhe a build.
    Retorna 'YYYY-MM' do mês mais recente disponível ou None se nenhum dos
    `max_lookback` últimos meses respondeu.
    """
    today = date.today()
    candidates: list[str] = []
    for offset in range(0, max_lookback + 1):
        y, m = _shift_month(today.year, today.month, -offset)
        candidates.append(f"{y:04d}-{m:02d}")

    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        for month in candidates:
            url = f"{base_url()}/{month}/"
            try:
                r = client.head(url)
            except httpx.HTTPError as exc:
                log.warning("probe %s failed: %s", month, exc)
                continue
            if 200 <= r.status_code < 300:
                log.info("latest available month: %s", month)
                return month
            log.debug("probe %s → HTTP %d", month, r.status_code)
    return None


# kept for backward compat — unused once we fix the date math, but harmless.
def _last_month_iso(today: date | None = None) -> str:
    today = today or date.today()
    last = today.replace(day=1) - timedelta(days=1)
    return last.strftime("%Y-%m")


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
