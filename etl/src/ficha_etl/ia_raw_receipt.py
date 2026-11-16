"""Passive RFB -> Internet Archive raw ZIP metadata reconciliation (Ficha #247 P1-C).

The RFB WebDAV ETag is opaque, not a cryptographic content hash. IA reports a
SHA-1 for its stored object, but without an independently measured source SHA-1
we can establish only correspondence of names and advertised sizes, NOT byte
identity, ZIP integrity or permission to skip/rewrite a remote object.

This module is pure: no network, archive GET, uploads or production decisions.
The RFB origin receipt itself must be independently validated at its source.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .sources import canonical_inventory, is_valid_month

_SHA1 = re.compile(r"[0-9a-fA-F]{40}\Z")
_ORIGIN_SCHEMA = "rfb-propfind-metadata-v1"


@dataclass(frozen=True)
class RawFileCheck:
    name: str
    expected_size: int
    remote_size: int | None
    remote_sha1: str | None
    status: str  # size_and_sha1_observed | size_mismatch | missing | incomplete_metadata


@dataclass(frozen=True)
class RawMetadataReport:
    month: str
    files: tuple[RawFileCheck, ...]
    unexpected_raw_names: tuple[str, ...]

    @property
    def metadata_consistent(self) -> bool:
        """Only metadata correspondence; NEVER proof of content identity."""
        return not self.unexpected_raw_names and all(
            f.status == "size_and_sha1_observed" for f in self.files
        )

    @property
    def content_identity_verified(self) -> bool:
        """No source content digest or bytes are inspected by this module."""
        return False


def _positive_size(value: object) -> int | None:
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        parsed = int(value)
        return parsed if parsed > 0 else None
    return None


def reconcile_raw_metadata(origin: object, archive: object) -> RawMetadataReport:
    """Compare 37 RFB PROPFIND assertions with one IA item metadata response.

    Duplicate names, incomplete inventory, wrong item and malformed top-level
    records fail closed. Missing or insufficient *file* metadata is reported as
    unverified, never interpreted as an identical object.
    """
    if not isinstance(origin, dict) or origin.get("schema_version") != _ORIGIN_SCHEMA:
        raise ValueError("invalid RFB origin receipt schema")
    month = origin.get("month")
    if not isinstance(month, str) or not is_valid_month(month):
        raise ValueError("invalid month")
    source = origin.get("source")
    if not isinstance(source, dict) or (
        source.get("method") != "PROPFIND"
        or source.get("depth") != 1
        or source.get("http_status") != 207
        or source.get("url") != f"https://arquivos.receitafederal.gov.br/public.php/webdav/{month}/"
    ):
        raise ValueError("RFB origin metadata is not a canonical PROPFIND observation")
    if (
        origin.get("identity_level") != "metadata-only"
        or origin.get("content_hash_verified") is not False
        or origin.get("etag_semantics") != "opaque-validator-not-content-hash"
    ):
        raise ValueError("RFB metadata must not claim cryptographic identity")

    expected_names = {spec.name for spec in canonical_inventory()}
    origin_files = origin.get("files")
    if not isinstance(origin_files, list) or len(origin_files) != len(expected_names):
        raise ValueError("incomplete RFB inventory")
    sizes: dict[str, int] = {}
    for entry in origin_files:
        if not isinstance(entry, dict):
            raise ValueError("invalid RFB entry")
        name = entry.get("name")
        if (
            not isinstance(name, str)
            or name not in expected_names
            or name in sizes
            or entry.get("ia_raw_name_expected") != f"raw/{name}"
            or entry.get("content_type") != "application/zip"
            or not isinstance(entry.get("etag_raw"), str)
            or not entry["etag_raw"]
        ):
            raise ValueError(f"invalid/duplicate RFB entry: {name!r}")
        size = _positive_size(entry.get("size_bytes"))
        if size is None or type(entry.get("size_bytes")) is not int:
            raise ValueError(f"invalid RFB size for {name}")
        sizes[name] = size
    if sizes.keys() != expected_names:
        raise ValueError("RFB inventory names differ from canonical 37")
    if type(origin.get("total_size_bytes")) is not int or origin["total_size_bytes"] != sum(
        sizes.values()
    ):
        raise ValueError("RFB metadata total disagrees with entries")

    if not isinstance(archive, dict):
        raise ValueError("IA metadata is unavailable")
    ia_item = archive.get("metadata")
    if not isinstance(ia_item, dict) or ia_item.get("identifier") != f"ficha-{month}":
        raise ValueError("wrong IA item identifier")
    ia_files = archive.get("files")
    if not isinstance(ia_files, list):
        raise ValueError("IA has no usable files listing")
    raw: dict[str, dict] = {}
    for item in ia_files:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ValueError("malformed IA files listing")
        name = item["name"]
        if name.startswith("raw/"):
            if name in raw:
                raise ValueError(f"duplicate IA raw entry: {name}")
            raw[name] = item

    checks: list[RawFileCheck] = []
    for name in sorted(expected_names):
        remote_name = f"raw/{name}"
        entry = raw.get(remote_name)
        expected_size = sizes[name]
        if entry is None:
            checks.append(RawFileCheck(remote_name, expected_size, None, None, "missing"))
            continue
        remote_size = _positive_size(entry.get("size"))
        sha1 = entry.get("sha1")
        valid_sha1 = isinstance(sha1, str) and _SHA1.fullmatch(sha1) is not None
        status = (
            "incomplete_metadata"
            if remote_size is None or not valid_sha1
            else "size_mismatch"
            if remote_size != expected_size
            else "size_and_sha1_observed"
        )
        checks.append(
            RawFileCheck(
                remote_name,
                expected_size,
                remote_size,
                sha1.lower() if valid_sha1 else None,
                status,
            )
        )

    unexpected = tuple(sorted(set(raw) - {f"raw/{name}" for name in expected_names}))
    return RawMetadataReport(month, tuple(checks), unexpected)
