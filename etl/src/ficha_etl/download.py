"""Download de ZIPs do dump RFB.

Estratégia:
- HTTP streaming pra evitar ler tudo em memória (ZIPs vão de 100MB a 1GB+).
- Resume via Range requests quando arquivo parcial existir.
- Retry com backoff exponencial em erros de rede.
- Verifica tamanho final via Content-Length quando disponível.

Não extrai os ZIPs — extração é responsabilidade do `transform.py`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx

from .sources import RemoteFile

log = logging.getLogger(__name__)

# Conservador pra não estressar o RFB. Eles podem rate-limitar.
_HTTP_TIMEOUT = httpx.Timeout(connect=30.0, read=300.0, write=60.0, pool=30.0)
_CHUNK_BYTES = 1024 * 1024  # 1 MiB


@dataclass
class DownloadResult:
    file: RemoteFile
    path: Path
    size_bytes: int
    resumed: bool


def download_one(
    file: RemoteFile,
    target_dir: Path,
    *,
    client: httpx.Client | None = None,
    max_attempts: int = 4,
) -> DownloadResult:
    """Baixa um RemoteFile pra `target_dir`. Resume parciais. Retry exponencial."""
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / file.name

    own_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        return _download_with_retry(file, target, client, max_attempts)
    finally:
        if own_client:
            client.close()


def download_all(
    files: Iterable[RemoteFile],
    target_dir: Path,
    *,
    max_attempts: int = 4,
    extra_headers: dict[str, str] | None = None,
) -> list[DownloadResult]:
    """Baixa todos os arquivos sequencialmente, reusando o cliente HTTP.

    `extra_headers` é aplicado a todas as requests do cliente (ex.: Basic auth).
    """
    results: list[DownloadResult] = []
    with httpx.Client(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
        headers=extra_headers or {},
    ) as client:
        for f in files:
            results.append(download_one(f, target_dir, client=client, max_attempts=max_attempts))
    return results


def _download_with_retry(
    file: RemoteFile,
    target: Path,
    client: httpx.Client,
    max_attempts: int,
) -> DownloadResult:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _download_streaming(file, target, client)
        except (httpx.HTTPError, OSError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            backoff = min(60.0, 2.0**attempt)
            log.warning(
                "download failed (attempt %d/%d) for %s: %s — retry in %.1fs",
                attempt,
                max_attempts,
                file.name,
                exc,
                backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(
        f"download of {file.name} failed after {max_attempts} attempts"
    ) from last_exc


def _download_streaming(
    file: RemoteFile,
    target: Path,
    client: httpx.Client,
) -> DownloadResult:
    existing = target.stat().st_size if target.exists() else 0
    headers: dict[str, str] = {}
    resumed = False
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"
        resumed = True
        log.info("resuming %s from byte %d", file.name, existing)

    mode = "ab" if resumed else "wb"
    with client.stream("GET", file.url, headers=headers) as response:
        if resumed and response.status_code == 200:
            # Server ignored Range — start over.
            log.info("server ignored Range for %s; starting from scratch", file.name)
            existing = 0
            resumed = False
            mode = "wb"
        elif resumed and response.status_code != 206:
            response.raise_for_status()
        else:
            response.raise_for_status()

        with target.open(mode) as fh:
            for chunk in response.iter_bytes(_CHUNK_BYTES):
                fh.write(chunk)

    size = target.stat().st_size
    expected = _expected_size(response, existing)
    if expected is not None and size != expected:
        raise RuntimeError(f"{file.name}: downloaded {size} bytes, expected {expected}")

    log.info("downloaded %s (%s bytes)", file.name, f"{size:,}")
    return DownloadResult(file=file, path=target, size_bytes=size, resumed=resumed)


def _expected_size(response: httpx.Response, prior_bytes: int) -> int | None:
    """Best-effort total size. Returns None when the server didn't advertise it."""
    if response.status_code == 206:
        # Content-Range: bytes start-end/total
        cr = response.headers.get("content-range", "")
        if "/" in cr:
            tail = cr.rsplit("/", 1)[1]
            if tail.isdigit():
                return int(tail)
        return None
    cl = response.headers.get("content-length")
    if cl and cl.isdigit():
        return prior_bytes + int(cl)
    return None
