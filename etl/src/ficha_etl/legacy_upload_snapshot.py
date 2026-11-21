"""Immutable, descriptor-bound upload handles for the legacy CLI.

All local failures must happen before the first raw RFB -> IA PUT.
This does not reconcile the independent acquisition of the ten RFB ZIPs.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO

from .transform import _LOOKUP_KINDS

_BASE_FILES = {
    "cnpjs": "cnpjs.parquet",
    "cnpj_contatos": "cnpj_contatos.parquet",
    "cnpj_cnaes": "cnpj_cnaes.parquet",
    "raizes": "raizes.parquet",
    "socios": "socios.parquet",
    "enderecos": "enderecos.parquet",
    "pessoas": "pessoas.parquet",
    "lookups": "lookups.json",
    "companies_zip": "companies.zip",
}
_CHUNK = 1024 * 1024


def _expected_files(entry: dict) -> dict[str, tuple[int, str, str]]:
    files = entry.get("files")
    lookups = entry.get("lookups")
    if not isinstance(files, dict) or not isinstance(lookups, dict):
        raise ValueError("legacy manifest missing files/lookups")
    if set(files) != set(_BASE_FILES) or set(lookups) != set(_LOOKUP_KINDS):
        raise ValueError("legacy manifest has missing or extra upload files")

    entries = {name: files[key] for key, name in _BASE_FILES.items()}
    entries.update({f"lookups/{kind}.parquet": lookups[kind] for kind in _LOOKUP_KINDS})
    expected = {}
    for name, metadata in entries.items():
        if not isinstance(metadata, dict):
            raise ValueError(f"{name}: invalid manifest identity")
        size, sha1, sha256 = (metadata.get("size"), metadata.get("sha1"), metadata.get("sha256"))
        if (
            type(size) is not int
            or size <= 0
            or not isinstance(sha1, str)
            or len(sha1) != 40
            or not isinstance(sha256, str)
            or len(sha256) != 64
            or any(c not in "0123456789abcdef" for c in sha1.lower() + sha256.lower())
        ):
            raise ValueError(f"{name}: invalid manifest size or hashes")
        expected[name] = (size, sha1.lower(), sha256.lower())
    return expected


def freeze_uploads(stack: ExitStack, output_dir: Path, entry: dict) -> dict[str, BinaryIO]:
    """Copy *all* upload candidates before any remote action, then verify copied bytes.

    Handles are owned by caller's ExitStack, kept open through synchronous IA upload.
    Copy errors, missing paths or byte mismatches abort before the first PUT.
    """
    expected = _expected_files(entry)
    total = sum(size for size, _, _ in expected.values())
    handles: dict[str, BinaryIO] = {}

    # Capacity check targets the filesystem of an actual anonymous scratch handle.
    probe = stack.enter_context(tempfile.TemporaryFile(mode="w+b"))
    if hasattr(os, "fstatvfs"):
        vfs = os.fstatvfs(probe.fileno())
        available = vfs.f_bavail * vfs.f_frsize
        margin = max(64 * 1024 * 1024, (total + 9) // 10)
        if available < total + margin or vfs.f_favail < len(expected):
            raise OSError(
                f"TMPDIR insufficient for legacy snapshots: need={total + margin}, "
                f"available={available}, free_inodes={vfs.f_favail}"
            )

    for name, (size, sha1, sha256) in expected.items():
        frozen = stack.enter_context(tempfile.TemporaryFile(mode="w+b"))
        digest1 = hashlib.sha1(usedforsecurity=False)
        digest256 = hashlib.sha256()
        actual = 0
        with (output_dir / name).open("rb") as original:
            for chunk in iter(lambda: original.read(_CHUNK), b""):
                actual += len(chunk)
                if actual > size:
                    raise ValueError(f"{name}: source exceeds manifest size")
                frozen.write(chunk)
                digest1.update(chunk)
                digest256.update(chunk)

        if actual != size or digest1.hexdigest() != sha1 or digest256.hexdigest() != sha256:
            raise ValueError(f"{name}: copied bytes differ from manifest")

        # Re-read the anonymous handle rather than trusting the mutable pathname.
        frozen.seek(0)
        copy_sha1 = hashlib.sha1(usedforsecurity=False)
        copy_sha256 = hashlib.sha256()
        copied = 0
        for chunk in iter(lambda: frozen.read(_CHUNK), b""):
            copied += len(chunk)
            copy_sha1.update(chunk)
            copy_sha256.update(chunk)
        if copied != size or copy_sha1.hexdigest() != sha1 or copy_sha256.hexdigest() != sha256:
            raise ValueError(f"{name}: frozen handle differs from manifest")
        frozen.seek(0)
        handles[name] = frozen
    return handles
