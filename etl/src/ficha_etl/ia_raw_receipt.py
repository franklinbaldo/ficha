"""Passive RFB -> Internet Archive raw ZIP metadata reconciliation (Ficha #247 P1-C).

The RFB WebDAV ETag is opaque, not a cryptographic content hash. IA reports a
SHA-1 for its stored object, but without an independently measured source SHA-1
we can establish only correspondence of names and advertised sizes, NOT byte
identity, ZIP integrity or permission to skip/rewrite a remote object.

This module is pure: no network, archive GET, uploads or production decisions.
The canonical RFB origin receipt is validated before any IA comparison.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .rfb_origin_receipt import validate_origin_receipt
from .sources import canonical_inventory

_SHA1 = re.compile(r"[0-9a-fA-F]{40}\Z")


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
    # P1-B owns validation of the complete RFB receipt. Never accept a
    # partial/forged metadata receipt as evidence of IA correspondence.
    validate_origin_receipt(origin)
    if not isinstance(origin, dict):
        raise ValueError("invalid RFB origin receipt")
    month = origin["month"]
    sizes = {entry["name"]: entry["size_bytes"] for entry in origin["files"]}
    expected_names = {spec.name for spec in canonical_inventory()}

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


@dataclass(frozen=True)
class RawHeadObservation:
    """Size-level observation of one IA HEAD response, never a byte digest."""

    name: str
    catalog_size: int | None
    head_size: int | None
    status: str

    @property
    def head_size_consistent(self) -> bool:
        return self.status == "head_size_matches_catalog"

    @property
    def content_identity_verified(self) -> bool:
        return False


def check_raw_head(
    report: RawMetadataReport,
    name: str,
    *,
    http_status: object,
    content_length: object,
    content_encoding: str | None = None,
) -> RawHeadObservation:
    """Compare supplied HEAD transport headers with one listed IA object.

    The caller must associate the HEAD response with the correct object URL.
    Neither HTTP 200 nor matching sizes attest remote ZIP content or RFB bytes.
    This function performs no network request and authorizes no upload/skip.
    """
    entry = next((file for file in report.files if file.name == name), None)
    if entry is None:
        raise ValueError("HEAD target is not in the canonical IA inventory")
    head_size = _positive_size(content_length)
    encoding = content_encoding.strip().lower() if isinstance(content_encoding, str) else None
    if type(http_status) is not int or http_status != 200:
        status = "http_not_ok"
    elif content_encoding is not None and encoding != "identity":
        status = "unsupported_encoding"
    elif head_size is None:
        status = "missing_or_invalid_length"
    elif not report.metadata_consistent or entry.status != "size_and_sha1_observed":
        status = "catalog_incomplete"
    elif head_size != entry.remote_size:
        status = "head_size_mismatch"
    else:
        status = "head_size_matches_catalog"
    return RawHeadObservation(name, entry.remote_size, head_size, status)
