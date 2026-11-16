"""Validate a metadata-only RFB origin receipt without reading ZIP contents.

A PROPFIND ETag is an opaque version validator, *not* a byte digest. This
receipt can prove that the RFB advertised files at observation time, but
cannot prove their contents, ZIP integrity, or identity with IA raw/*.
"""

from __future__ import annotations

import re
from datetime import datetime
from email.utils import parsedate_to_datetime

from .sources import canonical_inventory, is_valid_month

SCHEMA_VERSION = "rfb-propfind-metadata-v1"
_SOURCE_ROOT = "https://arquivos.receitafederal.gov.br/public.php/webdav"
_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "month",
        "source",
        "identity_level",
        "content_hash_verified",
        "etag_semantics",
        "total_size_bytes",
        "files",
    }
)
_SOURCE_KEYS = frozenset(
    {"url", "method", "depth", "http_status", "server_date", "observed_at_utc"}
)
_ENTRY_KEYS = frozenset({"name", "size_bytes", "etag_raw", "content_type", "ia_raw_name_expected"})
_ETAG = re.compile(r'(?:W/)?"[^"\r\n]+"\Z')


def validate_origin_receipt(receipt: object) -> None:
    """Fail closed for missing/extra inventory, contradictory or forged claims.

    This validates the *shape and internal coherence* of metadata assertions,
    not the trustworthiness of the server or any ZIP's bytes.
    """
    if not isinstance(receipt, dict):
        raise ValueError("receipt must be a JSON object")
    if set(receipt) != _RECEIPT_KEYS:
        raise ValueError("unexpected origin receipt fields")
    if receipt.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported origin receipt schema")

    month = receipt.get("month")
    if not isinstance(month, str) or not is_valid_month(month):
        raise ValueError("invalid month")
    source = receipt.get("source")
    if not isinstance(source, dict) or set(source) != _SOURCE_KEYS:
        raise ValueError("source metadata missing or has extra claims")
    expected_url = f"{_SOURCE_ROOT}/{month}/"
    if (
        source.get("url") != expected_url
        or source.get("method") != "PROPFIND"
        or source.get("depth") != 1
        or type(source.get("http_status")) is not int
        or source["http_status"] != 207
    ):
        raise ValueError("source is not a successful canonical RFB WebDAV listing")

    observed = source.get("observed_at_utc")
    try:
        instant = datetime.fromisoformat(observed)
    except (TypeError, ValueError) as exc:
        raise ValueError("observation time missing or invalid") from exc
    if instant.utcoffset() is None or instant.utcoffset().total_seconds() != 0:
        raise ValueError("observation time must be UTC")
    try:
        server_date = parsedate_to_datetime(source.get("server_date", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError("HTTP server date missing or invalid") from exc
    if server_date.utcoffset() is None:
        raise ValueError("HTTP server date must contain timezone")

    if receipt.get("identity_level") != "metadata-only":
        raise ValueError("origin identity must stay metadata-only")
    if receipt.get("content_hash_verified") is not False:
        raise ValueError("metadata-only receipt cannot attest verified bytes")
    if receipt.get("etag_semantics") != "opaque-validator-not-content-hash":
        raise ValueError("ETag must not be presented as a cryptographic hash")

    entries = receipt.get("files")
    canonical = {spec.name for spec in canonical_inventory()}
    if not isinstance(entries, list) or len(entries) != len(canonical):
        raise ValueError("must contain all 37 canonical ZIP entries")
    observed_names: set[str] = set()
    total = 0
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != _ENTRY_KEYS:
            raise ValueError("unexpected ZIP metadata fields")
        name = entry["name"]
        if not isinstance(name, str) or name not in canonical or name in observed_names:
            raise ValueError(f"unexpected or duplicated ZIP: {name!r}")
        observed_names.add(name)
        size = entry["size_bytes"]
        if type(size) is not int or size <= 0:
            raise ValueError(f"{name}: invalid byte size")
        if entry["content_type"] != "application/zip":
            raise ValueError(f"{name}: unexpected MIME type")
        if entry["ia_raw_name_expected"] != f"raw/{name}":
            raise ValueError(f"{name}: IA path is only an expected one-to-one mapping")
        if not isinstance(entry["etag_raw"], str) or not _ETAG.fullmatch(entry["etag_raw"]):
            raise ValueError(f"{name}: missing or invalid opaque ETag")
        total += size
    if observed_names != canonical:
        raise ValueError("inventory names do not match canonical 37")
    if type(receipt.get("total_size_bytes")) is not int or receipt["total_size_bytes"] != total:
        raise ValueError("total byte count contradicts individual metadata")
