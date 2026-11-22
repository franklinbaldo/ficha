"""Upload dos outputs do pipeline FICHA para o Internet Archive.

Cada snapshot mensal vive num item `ficha-YYYY-MM`. Estrutura interna:

    ficha-YYYY-MM/
      raw/            ← 37 ZIPs crus espelhados da RFB
      cnpjs.parquet
      cnpj_contatos.parquet
      cnpj_cnaes.parquet
      raizes.parquet
      socios.parquet
      enderecos.parquet
      pessoas.parquet
      lookups.json
      lookups/
        cnaes.parquet
        motivos.parquet
        municipios.parquet
        naturezas.parquet
        paises.parquet
        qualificacoes.parquet

Ver ADR 0012 e mirror.py (URLs de leitura).
"""

from __future__ import annotations

import hashlib
import logging
import tempfile
import time
from enum import Enum
from pathlib import Path

import httpx
import internetarchive as ia

from .mirror import item_id
from .sources import canonical_inventory, is_valid_month
from .transform import _LOOKUP_KINDS
from . import raw_preflight, upstream

log = logging.getLogger(__name__)


class RawStreamOutcome(str, Enum):
    """Transport activity, never authenticated remote custody."""

    ALL_NAMES_PRESENT_UNPROVEN = "all_names_present_unproven"
    SENT_UNVERIFIED = "sent_unverified"


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
        with dl.stream(
            "GET", rfb_url, auth=(rfb_token, ""), headers={"Accept-Encoding": "identity"}
        ) as rfb_resp:
            rfb_resp.raise_for_status()
            # raise_for_status() accepts 2xx including 206 Partial Content. No
            # byte range was requested: uploading a partial ZIP would silently
            # publish a truncated object with a matching Content-Length.
            if rfb_resp.status_code != 200:
                raise RuntimeError(
                    f"RFB unexpected HTTP status for {spec.name}: {rfb_resp.status_code}"
                )
            # httpx.iter_bytes() transparently decompresses encoded HTTP bodies.
            # A ZIP mirror must upload the identical octets whose length RFB reports.
            content_encoding = rfb_resp.headers.get("content-encoding", "identity").strip().lower()
            if content_encoding != "identity":
                raise RuntimeError(
                    f"RFB unexpected Content-Encoding for {spec.name}: {content_encoding!r}"
                )
            content_length = rfb_resp.headers.get("content-length", "")
            if not content_length:
                raise RuntimeError(
                    f"RFB não retornou Content-Length para {spec.name} "
                    "— streaming sem tamanho não é suportado"
                )
            # An empty HTTP 200 body is not an RFB ZIP, even with a valid
            # Content-Length header. Require a positive ASCII decimal size
            # before opening the first IA PUT.
            if (
                not content_length.isascii()
                or not content_length.isdecimal()
                or int(content_length) == 0
            ):
                raise RuntimeError(
                    f"RFB invalid Content-Length for {spec.name}: {content_length!r}"
                )
            log.info(
                "streaming %s → IA (%.1f MB)",
                spec.name,
                int(content_length) / 1024 / 1024,
            )
            _ia_s3_put(
                identifier,
                remote_name,
                rfb_resp.iter_raw(_CHUNK),
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


def _catalog_raw_size(value: object, name: str) -> int:
    """Parse IA catalog byte sizes without bool/float/Unicode coercion.

    This is a catalog observation, NOT an authentication of the RFB bytes.
    """
    if type(value) is int:
        size = value
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        try:
            size = int(value)
        except ValueError as exc:
            raise RuntimeError(f"IA raw metadata: invalid size for {name}") from exc
    else:
        raise RuntimeError(f"IA raw metadata: invalid size for {name}")
    if size < 0:
        raise RuntimeError(f"IA raw metadata: negative size for {name}")
    return size


def _existing_raw_files_on_ia(identifier: str) -> set[str]:
    """Return raw names with unambiguous positive sizes in the IA catalog.

    Name and size are NOT proof that the ZIP equals the RFB source. Reject
    malformed or duplicate entries before starting any upstream GET / IA PUT.
    Only a confirmed 404 permits treating the IA item as absent.
    """
    url = _IA_METADATA_URL.format(identifier=identifier)
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
        if resp.status_code in (401, 403):
            raise RuntimeError(
                f"IA raw metadata: access denied HTTP {resp.status_code} for {identifier}"
            )
        if resp.status_code == 429:
            raise RuntimeError(f"IA raw metadata: rate limited HTTP 429 for {identifier}")
        if resp.status_code == 404:
            return set()
        if resp.status_code != 200:
            raise RuntimeError(
                f"IA raw metadata: unavailable HTTP {resp.status_code} for {identifier}"
            )
    except httpx.HTTPError as exc:
        raise RuntimeError(f"IA raw metadata: fetch failed for {identifier}") from exc

    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"IA raw metadata: invalid JSON for {identifier}") from exc

    if not isinstance(data, dict) or not isinstance(data.get("files"), list):
        raise RuntimeError(f"IA raw metadata: invalid file catalog for {identifier}")
    raw: set[str] = set()
    seen: set[str] = set()
    for entry in data["files"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise RuntimeError(f"IA raw metadata: malformed file entry for {identifier}")
        name = entry["name"]
        if not name.startswith("raw/"):
            continue
        if name in seen:
            raise RuntimeError(f"IA raw metadata: duplicate file entry {name}")
        seen.add(name)
        if _catalog_raw_size(entry.get("size"), name) > 0:
            raw.add(name)
    return raw


def stream_raw_zips_to_ia(
    month: str,
    *,
    access_key: str,
    secret_key: str,
    workers: int = 4,
    spool_dir: Path | None = None,
) -> RawStreamOutcome:
    """Admite todos os 37 ZIPs antes de qualquer IA PUT neste caminho.

    Exige scratch explícito e disponível; sem ele a rotina recusa publicação.
    O lote é preparado sequencialmente em disco e validado por ZIP/CRC e
    limites globais. O estágio temporário não é spool imutável nem journal.
    Retentativas ambíguas permanecem desabilitadas neste caminho.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    identifier = item_id(month)
    all_specs = list(canonical_inventory())

    # Check existing raw objects before contacting RFB (ADR 0015).
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
        return RawStreamOutcome.ALL_NAMES_PRESENT_UNPROVEN

    # Without an operator-selected disk there is no safe capacity assumption.
    # The old zero-disk producer cannot guarantee 37/37 before the first PUT.
    if spool_dir is None or not spool_dir.is_dir():
        raise RuntimeError("raw preflight requires an existing explicit spool_dir")
    if workers < 1:
        raise ValueError("workers must be positive")

    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        raise RuntimeError(f"sem token RFB para preflight: {exc}") from exc

    # No IA PUT can occur until stage_all validates *every* canonical
    # source, including the last one, against global compressed/expanded caps.
    # This first slice deliberately does not retry ambiguous PUTs or certify
    # the scratch inode as immutable; those require journal/readback follow-ups.
    with tempfile.TemporaryDirectory(prefix="ficha-raw-preflight-", dir=spool_dir) as temp:
        staged = raw_preflight.stage_all(all_specs, month, token, Path(temp))
        for index, spec in enumerate(specs):
            path, size, pinned_sha = staged[spec.name]
            # Verify and transmit via the SAME still-open descriptor. The
            # pathname may be replaced after this check: reopening it would
            # silently send different bytes. This does not seal against an
            # existing writable FD or provide durable UNKNOWN recovery.
            with path.open("rb") as handle:
                current_sha = hashlib.file_digest(handle, "sha256").hexdigest()
                if current_sha != pinned_sha:
                    raise RuntimeError(f"raw preflight: staged bytes changed: {spec.name}")
                handle.seek(0)
                _ia_s3_put(
                    identifier,
                    f"raw/{spec.name}",
                    raw_preflight.file_chunks(handle),
                    content_length=str(size),
                    access_key=access_key,
                    secret_key=secret_key,
                    is_first=index == 0,
                )
    log.info("raw preflight admitted %d sources; sent %d missing names", len(staged), len(specs))
    return RawStreamOutcome.SENT_UNVERIFIED


def _check_responses(responses: list, label: str, expected: int | None = None) -> None:
    """A quantidade de respostas também integra o recibo de upload."""
    if not responses or (expected is not None and len(responses) != expected):
        raise RuntimeError(
            f"IA upload incomplete [{label}]: expected {expected}, got {len(responses) if responses else 0}"
        )
    for r in responses:
        if r.status_code not in (200, 201):
            raise RuntimeError(f"IA upload failed [{label}]: HTTP {r.status_code} — {r.url}")


def upload_outputs(
    month: str,
    output_dir: Path,
    *,
    access_key: str,
    secret_key: str,
    frozen: dict | None = None,
) -> None:
    """Faz upload de todos os parquets + lookups.json para o item IA do mês.

    Args:
        month: snapshot no formato YYYY-MM.
        output_dir: diretório local com os arquivos produzidos pelo transform.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    outputs = {
        "cnpjs.parquet": output_dir / "cnpjs.parquet",
        "cnpj_contatos.parquet": output_dir / "cnpj_contatos.parquet",
        "cnpj_cnaes.parquet": output_dir / "cnpj_cnaes.parquet",
        "raizes.parquet": output_dir / "raizes.parquet",
        "socios.parquet": output_dir / "socios.parquet",
        "enderecos.parquet": output_dir / "enderecos.parquet",
        "pessoas.parquet": output_dir / "pessoas.parquet",
        "lookups.json": output_dir / "lookups.json",
    }

    for kind in _LOOKUP_KINDS:
        outputs[f"lookups/{kind}.parquet"] = output_dir / "lookups" / f"{kind}.parquet"

    if frozen is None:
        missing = [name for name, path in outputs.items() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"outputs missing before upload: {missing}")
    elif set(frozen) != set(outputs):
        raise ValueError("frozen output set differs from expected upload files")

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
        files=frozen if frozen is not None else {name: str(path) for name, path in outputs.items()},
        metadata=metadata,
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "outputs", len(outputs) if frozen is not None else None)
    log.info("uploaded outputs to ia:%s OK", identifier)


def upload_companies_zip(
    month: str,
    companies_zip_path: Path,
    *,
    access_key: str,
    secret_key: str,
    identifier_override: str | None = None,
    frozen=None,
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
    if frozen is None and not companies_zip_path.exists():
        raise FileNotFoundError(f"companies.zip not found: {companies_zip_path}")

    identifier = identifier_override or item_id(month)
    log.info("uploading companies.zip to ia:%s", identifier)
    responses = ia.upload(
        identifier,
        files={"companies.zip": frozen if frozen is not None else str(companies_zip_path)},
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "companies.zip", 1 if frozen is not None else None)
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
