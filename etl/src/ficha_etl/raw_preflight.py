"""Preflight físico offline-first para os 37 ZIPs raw da RFB.

Confere unicidade e inventário independente, tamanho e CRC antes do egress
S3. Ainda não certifica spool imutável, journal durável ou readback IA.
"""

from __future__ import annotations

import hashlib
import os
import zipfile
from pathlib import Path

import httpx

from . import upstream
from .sources import canonical_inventory

_CHUNK = 1024 * 1024
_MAX_RAW_COMPRESSED_BYTES = 12 * 1024**3
_MAX_RAW_UNCOMPRESSED_BYTES = 160 * 1024**3
_MAX_RAW_MEMBERS = 2048


def _raw_preflight_specs(specs: list) -> None:
    """Require the independent, exactly-once 37-file RFB inventory."""
    expected = [s.name for s in canonical_inventory()]
    names = [s.name for s in specs]
    if (
        len(expected) != 37
        or len(set(expected)) != 37
        or names != expected
        or any(name != Path(name).name or not name.endswith(".zip") for name in names)
    ):
        raise RuntimeError("raw preflight: invalid canonical inventory")


def _raw_zip_crc_and_budget(path: Path, *, expanded: int, members: int) -> tuple[int, int]:
    """Bound declared expansion globally, then read every member to check CRC."""
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if not entries or len(entries) + members > _MAX_RAW_MEMBERS:
                raise RuntimeError("raw preflight: invalid ZIP member count")
            names = [entry.filename for entry in entries]
            if len(names) != len(set(names)):
                raise RuntimeError("raw preflight: duplicate ZIP member")
            for entry in entries:
                if entry.flag_bits & 1:
                    raise RuntimeError("raw preflight: encrypted ZIP member")
                if entry.file_size < 0 or expanded + entry.file_size > _MAX_RAW_UNCOMPRESSED_BYTES:
                    raise RuntimeError("raw preflight: global uncompressed limit")
                expanded += entry.file_size
            bad = archive.testzip()
            if bad is not None:
                raise RuntimeError(f"raw preflight: ZIP CRC failed: {bad}")
            return expanded, members + len(entries)
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, EOFError) as exc:
        raise RuntimeError(f"raw preflight: invalid ZIP {path.name}") from exc


def stage_all(
    specs: list, month: str, token: str, target: Path
) -> dict[str, tuple[Path, int, str]]:
    """Stage all 37 RFB octet streams, even when some names exist in IA.

    Never begin an IA PUT in this function. Scratch storage is temporary and
    NOT an immutable spool or durable retry journal.
    """
    _raw_preflight_specs(specs)
    admitted: dict[str, tuple[Path, int, str]] = {}
    total_compressed = 0
    expanded = 0
    members = 0
    with httpx.Client(
        timeout=httpx.Timeout(connect=30, read=600, write=600, pool=30), follow_redirects=False
    ) as client:
        for spec in specs:
            url = upstream.file_url(token, month, spec.name)
            target_file = target / spec.name
            with client.stream(
                "GET", url, auth=(token, ""), headers={"Accept-Encoding": "identity"}
            ) as response:
                # No implicit HTTP redirects: only the requested RFB source is admitted.
                # Even a valid ZIP on another host (or downgraded to HTTP) is untrusted.
                if response.is_redirect:
                    raise RuntimeError(f"raw preflight: redirected RFB source {spec.name}")
                response.raise_for_status()
                if response.status_code != 200:
                    raise RuntimeError(f"raw preflight: non-200 RFB source {spec.name}")
                encoding = response.headers.get("content-encoding", "identity").strip().lower()
                if encoding != "identity":
                    raise RuntimeError(f"raw preflight: encoded RFB source {spec.name}")
                length_text = response.headers.get("content-length", "")
                if (
                    not length_text.isascii()
                    or not length_text.isdecimal()
                    or int(length_text) == 0
                ):
                    raise RuntimeError(f"raw preflight: invalid RFB length {spec.name}")
                expected = int(length_text)
                if total_compressed + expected > _MAX_RAW_COMPRESSED_BYTES:
                    raise RuntimeError("raw preflight: global compressed limit")
                digest = hashlib.sha256()
                size = 0
                with target_file.open("xb") as output:
                    for chunk in response.iter_raw(_CHUNK):
                        size += len(chunk)
                        if size > expected or total_compressed + size > _MAX_RAW_COMPRESSED_BYTES:
                            raise RuntimeError(f"raw preflight: RFB length/limit {spec.name}")
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                if size != expected:
                    raise RuntimeError(f"raw preflight: RFB length mismatch {spec.name}")
                total_compressed += size
            expanded, members = _raw_zip_crc_and_budget(
                target_file, expanded=expanded, members=members
            )
            admitted[spec.name] = (target_file, size, digest.hexdigest())
    if set(admitted) != {spec.name for spec in specs} or len(admitted) != 37:
        raise RuntimeError("raw preflight: incomplete batch")
    return admitted


def file_chunks(path: Path):
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            yield chunk
