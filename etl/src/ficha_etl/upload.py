"""Upload dos outputs do pipeline FICHA para o Internet Archive.

Cada snapshot mensal vive num item `ficha-YYYY-MM`. Estrutura interna:

    ficha-YYYY-MM/
      raw/            ← 37 ZIPs crus espelhados da RFB
      cnpjs.parquet
      raizes.parquet
      socios.parquet
      lookups.json

Ver ADR 0012 e mirror.py (URLs de leitura).
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import internetarchive as ia

from .mirror import item_id
from .sources import canonical_inventory, is_valid_month
from .transform import _LOOKUP_KINDS
from . import upstream

log = logging.getLogger(__name__)

# Timeout generoso: ZIPs grandes podem levar minutos pra transferir
_STREAM_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=600.0, pool=30.0)
_IA_S3_BASE = "https://s3.us.archive.org"
_CHUNK = 1024 * 1024  # 1 MiB

_RETRIES = 5
_RETRY_SLEEP = 30  # segundos entre tentativas

# IA S3 occasionally returns 5xx (transient backend error) or 409 (bucket-create
# race when multiple workers fire `auto-make-bucket` simultaneously). Both are
# retriable — we re-issue the whole RFB GET + IA PUT pair.
_TRANSIENT_STATUS = frozenset({409, 500, 502, 503, 504})


class _IAS3Error(RuntimeError):
    """PUT to IA S3 returned a non-2xx response. Carries status for retry logic."""

    def __init__(self, status: int, url: str, body: str = "") -> None:
        self.status = status
        self.url = url
        # ASCII-only message — runners can default stderr to ascii encoding
        # (LANG=C), and an em-dash in an error message is enough to trigger
        # UnicodeEncodeError when the exception is printed.
        snippet = body.strip()[:200].encode("ascii", "replace").decode("ascii")
        suffix = f" :: {snippet}" if snippet else ""
        super().__init__(f"IA S3 PUT failed: HTTP {status} - {url}{suffix}")


_IA_METADATA_BASE: dict[str, object] = {
    "mediatype": "data",
    "subject": ["CNPJ", "Receita Federal", "dados abertos", "Brasil"],
    "creator": "franklinbaldo",
    "licenseurl": "https://creativecommons.org/publicdomain/zero/1.0/",
    "language": "por",
}


def _ia_s3_put(
    identifier: str,
    remote_name: str,
    body_iter,
    *,
    content_length: str,
    access_key: str,
    secret_key: str,
    is_first: bool = False,
) -> None:
    """PUT streaming para o S3 do Internet Archive.

    `body_iter` é consumido em chunks de 1 MiB — nenhum byte é bufferizado em disco.
    `content_length` é repassado no header para que o IA não use chunked encoding.
    `is_first=True` envia os metadados do item junto com o primeiro arquivo.
    """
    headers: dict[str, str] = {
        "Authorization": f"LOW {access_key}:{secret_key}",
        "Content-Length": content_length,
        "x-archive-size-hint": content_length,
        "x-archive-queue-derive": "0",
        "x-archive-auto-make-bucket": "1",
    }
    if is_first:
        # Metadados do item — enviados uma vez só no primeiro PUT.
        # NB: IA S3 metadata vai em headers HTTP, que precisam ser ASCII.
        # Em-dash / acentos aqui crasham com UnicodeEncodeError dentro do
        # httpx ANTES do PUT sair (ver PR #24, run 25502969568).
        headers.update(
            {
                "x-archive-meta-mediatype": "data",
                "x-archive-meta-title": f"FICHA CNPJ - {identifier}",
                "x-archive-meta-subject": "CNPJ;Receita Federal;dados abertos;Brasil",
                "x-archive-meta-creator": "franklinbaldo",
                "x-archive-meta-licenseurl": ("https://creativecommons.org/publicdomain/zero/1.0/"),
            }
        )
    url = f"{_IA_S3_BASE}/{identifier}/{remote_name}"
    # Defense: every header value MUST be ASCII (IA S3 spec + httpx).
    for k, v in headers.items():
        try:
            v.encode("ascii")
        except UnicodeEncodeError as exc:
            raise _IAS3Error(0, url, f"non-ASCII header {k!r}: {v!r} ({exc.reason})") from exc
    with httpx.Client(timeout=_STREAM_TIMEOUT) as client:
        resp = client.put(url, content=body_iter, headers=headers)
    if resp.status_code not in (200, 201):
        raise _IAS3Error(resp.status_code, url, resp.text)


def _stream_one_zip(
    spec,
    *,
    rfb_token: str,
    month: str,
    identifier: str,
    access_key: str,
    secret_key: str,
    is_first: bool,
) -> str:
    """Faz GET streaming da RFB e PUT direto ao IA S3. Zero bytes em disco."""
    rfb_url = upstream.file_url(rfb_token, month, spec.name)
    remote_name = f"raw/{spec.name}"

    with httpx.Client(timeout=_STREAM_TIMEOUT, follow_redirects=True) as dl:
        with dl.stream("GET", rfb_url, auth=(rfb_token, "")) as rfb_resp:
            rfb_resp.raise_for_status()
            content_length = rfb_resp.headers.get("content-length", "")
            if not content_length:
                raise RuntimeError(
                    f"RFB não retornou Content-Length para {spec.name} "
                    "— streaming sem tamanho não é suportado"
                )
            log.info(
                "streaming %s → IA (%.1f MB)",
                spec.name,
                int(content_length) / 1024 / 1024,
            )
            _ia_s3_put(
                identifier,
                remote_name,
                rfb_resp.iter_bytes(_CHUNK),
                content_length=content_length,
                access_key=access_key,
                secret_key=secret_key,
                is_first=is_first,
            )
    return spec.name


def _stream_one_zip_with_retry(
    spec,
    *,
    rfb_token: str,
    month: str,
    identifier: str,
    access_key: str,
    secret_key: str,
    is_first: bool,
) -> str:
    """`_stream_one_zip` with exponential backoff on transient IA S3 errors.

    The PUT body is a one-shot iterator off the RFB GET stream, so retrying
    means re-issuing the whole GET+PUT pair. RFB GETs are cheap relative to
    IA PUTs, so this is fine.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _RETRIES + 1):
        try:
            return _stream_one_zip(
                spec,
                rfb_token=rfb_token,
                month=month,
                identifier=identifier,
                access_key=access_key,
                secret_key=secret_key,
                is_first=is_first,
            )
        except _IAS3Error as exc:
            last_exc = exc
            if exc.status not in _TRANSIENT_STATUS or attempt == _RETRIES:
                raise
            sleep_s = min(_RETRY_SLEEP * (2 ** (attempt - 1)), 300)
            log.warning(
                "%s: HTTP %d (attempt %d/%d) — retrying in %ds",
                spec.name,
                exc.status,
                attempt,
                _RETRIES,
                sleep_s,
            )
            time.sleep(sleep_s)
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt == _RETRIES:
                raise
            sleep_s = min(_RETRY_SLEEP * (2 ** (attempt - 1)), 300)
            log.warning(
                "%s: %s (attempt %d/%d) — retrying in %ds",
                spec.name,
                type(exc).__name__,
                attempt,
                _RETRIES,
                sleep_s,
            )
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


_IA_METADATA_URL = "https://archive.org/metadata/{identifier}"


def _existing_raw_files_on_ia(identifier: str) -> set[str]:
    """Returns the set of `raw/*` file names already on ia:{identifier}.

    Empty set when the item doesn't exist yet, or when the metadata API
    fails (502/timeout) -- in that case we fall back to streaming
    everything, which is correct (PUTs are idempotent overwrites at IA).
    """
    url = _IA_METADATA_URL.format(identifier=identifier)
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
        if resp.status_code != 200:
            log.info(
                "ia:%s metadata returned HTTP %d -- assuming new item", identifier, resp.status_code
            )
            return set()
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("ia:%s metadata fetch failed (%s) -- will stream all", identifier, exc)
        return set()

    files = data.get("files") or []
    raw = {
        f["name"]
        for f in files
        if isinstance(f, dict)
        and f.get("name", "").startswith("raw/")
        and int(f.get("size", 0) or 0) > 0
    }
    return raw


def stream_raw_zips_to_ia(
    month: str,
    *,
    access_key: str,
    secret_key: str,
    workers: int = 4,
) -> None:
    """Espelha os 37 ZIPs da RFB para ficha-YYYY-MM/raw/ SEM tocar disco.

    Abre GET streaming para cada ZIP no WebDAV da RFB e faz PUT direto ao
    endpoint S3 do Internet Archive. A memória usada por worker é de apenas
    1 MiB (tamanho do chunk) — ideal para runners com disco limitado.

    Args:
        month: snapshot no formato YYYY-MM.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
        workers: downloads/uploads simultâneos (default: 4).
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    identifier = item_id(month)
    all_specs = list(canonical_inventory())

    # Skip ZIPs already present on IA. Idempotent re-runs (e.g. retry after
    # transform failure, or backfill resuming a partial month) avoid
    # ~30 min and ~7 GB of pointless re-streaming. RFB historical files
    # are immutable per ADR 0015, so name-based existence is sufficient.
    # IMPORTANT: this runs BEFORE upstream.discover_token() so a recovery
    # run with all files already on IA succeeds even if RFB is down or
    # has rotated its share token.
    existing = _existing_raw_files_on_ia(identifier)
    specs = [s for s in all_specs if f"raw/{s.name}" not in existing]
    skipped = len(all_specs) - len(specs)
    if skipped:
        log.info(
            "skipping %d/%d ZIPs already on ia:%s/raw/",
            skipped,
            len(all_specs),
            identifier,
        )
    if not specs:
        log.info("all %d ZIPs already on ia:%s — stream is a no-op", len(all_specs), identifier)
        return

    # Only contact RFB if we actually need to stream from it.
    try:
        rfb_token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        raise RuntimeError(f"sem token RFB para streaming: {exc}") from exc

    total = len(specs)
    done = 0
    lock = threading.Lock()
    first_lock = threading.Lock()
    # `is_first` carries item metadata (title, subject, license, etc.).
    # We always send it on the first task of every run -- if a prior
    # run created the item but its `is_first` worker died (e.g. em-dash
    # crash, see PR #24), the item could exist with files but no
    # metadata. Re-sending the metadata headers on a PUT is idempotent
    # at IA's end (overwrite with same values). Cost: 5 extra header
    # keys on exactly one PUT per run.
    first_sent = False

    log.info(
        "streaming %d ZIPs RFB → ia:%s/raw/ (%d workers, zero disk)",
        total,
        identifier,
        workers,
    )

    def _task(spec) -> str:
        nonlocal done, first_sent
        with first_lock:
            is_first = not first_sent
            if is_first:
                first_sent = True
        name = _stream_one_zip_with_retry(
            spec,
            rfb_token=rfb_token,
            month=month,
            identifier=identifier,
            access_key=access_key,
            secret_key=secret_key,
            is_first=is_first,
        )
        with lock:
            done += 1
            log.info("[%d/%d] streamed %s → IA", done, total, name)
        return name

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_task, spec): spec for spec in specs}
        for future in as_completed(futures):
            future.result()  # propaga exceção se houver

    log.info("all %d ZIPs streamed to ia:%s OK", total, identifier)


def _check_responses(responses: list, label: str) -> None:
    """Levanta RuntimeError se algum response indicar falha."""
    for r in responses:
        if r.status_code not in (200, 201):
            raise RuntimeError(f"IA upload failed [{label}]: HTTP {r.status_code} — {r.url}")


def upload_outputs(
    month: str,
    output_dir: Path,
    *,
    access_key: str,
    secret_key: str,
) -> None:
    """Faz upload dos 3 parquets + lookups.json para o item IA do mês.

    Args:
        month: snapshot no formato YYYY-MM.
        output_dir: diretório local com os 4 arquivos produzidos pelo transform.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    outputs = {
        "cnpjs.parquet": output_dir / "cnpjs.parquet",
        "raizes.parquet": output_dir / "raizes.parquet",
        "socios.parquet": output_dir / "socios.parquet",
        "lookups.json": output_dir / "lookups.json",
    }

    for kind in _LOOKUP_KINDS:
        outputs[f"lookups/{kind}.parquet"] = output_dir / "lookups" / f"{kind}.parquet"

    missing = [name for name, path in outputs.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"outputs missing before upload: {missing}")

    identifier = item_id(month)
    metadata = {
        **_IA_METADATA_BASE,
        "title": f"FICHA CNPJ {month}",
        "description": (
            f"Dados do CNPJ (Cadastro Nacional de Pessoas Jurídicas) da Receita Federal "
            f"do Brasil — snapshot {month}. "
            f"Processado pelo projeto FICHA (https://github.com/franklinbaldo/ficha)."
        ),
    }

    log.info("uploading %d output files to ia:%s", len(outputs), identifier)
    responses = ia.upload(
        identifier,
        files={name: str(path) for name, path in outputs.items()},
        metadata=metadata,
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "outputs")
    log.info("uploaded outputs to ia:%s OK", identifier)


def upload_companies_zip(
    month: str,
    companies_zip_path: Path,
    *,
    access_key: str,
    secret_key: str,
    identifier_override: str | None = None,
) -> None:
    """Faz upload de companies.zip para o item IA do mês.

    Args:
        month: snapshot no formato YYYY-MM.
        companies_zip_path: caminho local do companies.zip produzido por pack_from_parquets.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
        identifier_override: identificador IA alternativo (ex.: item POC).
            Default usa item_id(month).
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    if not companies_zip_path.exists():
        raise FileNotFoundError(f"companies.zip not found: {companies_zip_path}")

    identifier = identifier_override or item_id(month)
    log.info("uploading companies.zip to ia:%s", identifier)
    responses = ia.upload(
        identifier,
        files={"companies.zip": str(companies_zip_path)},
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "companies.zip")
    log.info("uploaded ia:%s/companies.zip OK", identifier)


def upload_raw_zips(
    month: str,
    cache_dir: Path,
    *,
    access_key: str,
    secret_key: str,
) -> None:
    """Espelha os ZIPs crus da RFB para ficha-YYYY-MM/raw/.

    Pula ZIPs ausentes no cache (warning) mas falha se nenhum for encontrado.

    Args:
        month: snapshot no formato YYYY-MM.
        cache_dir: raiz do cache local; ZIPs esperados em cache_dir/month/*.zip.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    raw_dir = cache_dir / month
    files: dict[str, str] = {}
    for spec in canonical_inventory():
        local = raw_dir / spec.name
        if local.exists() and local.stat().st_size > 0:
            # Remote name com prefixo raw/ cria "pasta" no item IA.
            files[f"raw/{spec.name}"] = str(local)
        else:
            log.warning("raw ZIP not in cache, skipping mirror: %s", local)

    if not files:
        raise FileNotFoundError(f"no raw ZIPs found in {raw_dir} — run download first")

    identifier = item_id(month)
    log.info("uploading %d raw ZIPs to ia:%s/raw/", len(files), identifier)
    responses = ia.upload(
        identifier,
        files=files,
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "raw-zips")
    log.info("uploaded %d raw ZIPs to ia:%s OK", len(files), identifier)
