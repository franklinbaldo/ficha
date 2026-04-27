"""Fetcher de ZIPs do RFB com chain de fallback.

A ordem default tenta as fontes mais baratas/locais primeiro:

1. **LocalCacheFetcher** — `.cache/raw/{month}/{filename}` (ex.: cache de
   GitHub Actions ou rodadas anteriores).
2. **IAMirrorFetcher** — `https://archive.org/download/ficha-{month}/raw/...`
   Existe se o ETL desta data já fez mirror.
3. **UpstreamFetcher** — Nextcloud WebDAV da RFB. Última instância (origem).

Cada fetcher devolve um `Path` local (download persiste no `cache_dir`).
`ChainedFetcher.get()` para no primeiro que retorna não-None.

Ver ADR 0012 (IA como source-of-truth) e ADR 0015 (RFB Nextcloud).
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import httpx

from . import download as download_mod
from . import mirror, upstream
from .sources import FileKind, RemoteFile

log = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = Path(".cache/raw")
_HTTP_TIMEOUT = httpx.Timeout(connect=30.0, read=300.0, write=60.0, pool=30.0)

# Mapa de nome → FileKind para construção de RemoteFile (kind é só typing).
_KIND_MAP: dict[str, FileKind] = {
    "Empresas": "empresas",
    "Estabelecimentos": "estabelecimentos",
    "Socios": "socios",
    "Simples": "simples",
    "Cnaes": "cnaes",
    "Motivos": "motivos",
    "Municipios": "municipios",
    "Naturezas": "naturezas",
    "Paises": "paises",
    "Qualificacoes": "qualificacoes",
}


def _kind_for_filename(filename: str) -> FileKind:
    base = filename.removesuffix(".zip").rstrip("0123456789")
    return _KIND_MAP.get(base, "empresas")


class Fetcher(Protocol):
    """Tenta obter um arquivo. Devolve Path local ou None se a fonte não tem."""

    name: str

    def get(self, filename: str) -> Path | None: ...


@dataclass
class LocalCacheFetcher:
    """Lê de `cache_dir/{month}/{filename}`. Não faz requisições."""

    cache_dir: Path
    month: str
    name: str = "local"

    def get(self, filename: str) -> Path | None:
        path = self.cache_dir / self.month / filename
        if path.exists() and path.stat().st_size > 0:
            log.info("[%s] hit: %s", self.name, path)
            return path
        return None


@dataclass
class IAMirrorFetcher:
    """Tenta baixar de `archive.org/download/ficha-{month}/raw/{file}`.

    Checa existência do item IA uma única vez (HEAD no item root) e cacheia
    o resultado — evita 37 HEADs individuais quando o item ainda não existe.
    """

    month: str
    cache_dir: Path
    name: str = "ia"
    _item_exists: bool | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _check_item(self, client: httpx.Client) -> bool:
        with self._lock:
            if self._item_exists is not None:
                return self._item_exists
            item_url = mirror.item_root(self.month)
            status_label = "error"
            try:
                resp = client.head(item_url)
                status_label = str(resp.status_code)
                self._item_exists = resp.status_code == 200
            except httpx.HTTPError:
                self._item_exists = False
            if not self._item_exists:
                log.info(
                    "[%s] item %s not found (HTTP %s) — skipping mirror for all files",
                    self.name,
                    mirror.item_id(self.month),
                    status_label,
                )
            return self._item_exists

    def get(self, filename: str) -> Path | None:
        url = mirror.raw_file_url(self.month, filename)
        with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
            if not self._check_item(client):
                return None
            try:
                head = client.head(url)
            except httpx.HTTPError as exc:
                log.info("[%s] HEAD failed for %s: %s", self.name, url, exc)
                return None
            if head.status_code != 200:
                log.info("[%s] HEAD %s → HTTP %d (skipping)", self.name, url, head.status_code)
                return None
            target_dir = self.cache_dir / self.month
            target_dir.mkdir(parents=True, exist_ok=True)
            rfile = RemoteFile(name=filename, url=url, kind=_kind_for_filename(filename))
            try:
                result = download_mod.download_one(rfile, target_dir, client=client)
            except RuntimeError as exc:
                log.warning("[%s] download failed: %s", self.name, exc)
                return None
            log.info(
                "[%s] downloaded: %s (%s bytes)", self.name, result.path, f"{result.size_bytes:,}"
            )
            return result.path


@dataclass
class UpstreamFetcher:
    """Baixa do Nextcloud WebDAV da RFB com Basic auth."""

    token: str
    month: str
    cache_dir: Path
    name: str = "rfb"

    def get(self, filename: str) -> Path | None:
        url = upstream.file_url(self.token, self.month, filename)
        auth = httpx.BasicAuth(self.token, "")
        with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True, auth=auth) as client:
            target_dir = self.cache_dir / self.month
            target_dir.mkdir(parents=True, exist_ok=True)
            rfile = RemoteFile(name=filename, url=url, kind=_kind_for_filename(filename))
            try:
                result = download_mod.download_one(rfile, target_dir, client=client)
            except RuntimeError as exc:
                log.warning("[%s] download failed: %s", self.name, exc)
                return None
            log.info(
                "[%s] downloaded: %s (%s bytes)", self.name, result.path, f"{result.size_bytes:,}"
            )
            return result.path


@dataclass
class ChainedFetcher:
    """Tenta cada fetcher em ordem; primeiro hit ganha."""

    fetchers: list[Fetcher] = field(default_factory=list)

    def get(self, filename: str) -> Path:
        for f in self.fetchers:
            path = f.get(filename)
            if path is not None:
                return path
        sources = ", ".join(f.name for f in self.fetchers)
        raise FileNotFoundError(f"{filename!r} not found in any source ({sources})")

    def get_all_parallel(
        self,
        filenames: list[str],
        *,
        workers: int = 4,
    ) -> dict[str, Path]:
        """Baixa todos os arquivos em paralelo com ThreadPoolExecutor.

        Args:
            filenames: lista de nomes de arquivo a baixar.
            workers: número de downloads simultâneos (default: 4).

        Returns:
            dict {filename: local_path} na ordem original.

        Raises:
            FileNotFoundError: se qualquer arquivo não for encontrado.
        """
        total = len(filenames)
        completed = 0
        lock = threading.Lock()
        results: dict[str, Path] = {}

        log.info("parallel download: %d files, %d workers", total, workers)

        def _fetch(name: str) -> tuple[str, Path]:
            path = self.get(name)
            nonlocal completed
            with lock:
                completed += 1
                log.info("[%d/%d] fetched %s", completed, total, name)
            return name, path

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch, name): name for name in filenames}
            for future in as_completed(futures):
                name, path = future.result()  # propaga exceção se houver
                results[name] = path

        return results


def default_chain(
    month: str,
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    include_upstream: bool = True,
) -> ChainedFetcher:
    """Constrói chain padrão: local → IA → upstream RFB.

    `include_upstream=False` para casos onde queremos falhar rápido se o
    arquivo não está em local nem IA (ex.: CI que não deve bater no RFB).
    """
    chain: list[Fetcher] = [
        LocalCacheFetcher(cache_dir=cache_dir, month=month),
        IAMirrorFetcher(month=month, cache_dir=cache_dir),
    ]
    if include_upstream:
        try:
            token = upstream.discover_token()
            chain.append(UpstreamFetcher(token=token, month=month, cache_dir=cache_dir))
        except upstream.NoTokenError as exc:
            log.warning("upstream RFB unavailable, omitted from chain: %s", exc)
    return ChainedFetcher(fetchers=chain)
