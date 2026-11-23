"""Preflight físico offline-first para os 37 ZIPs raw da RFB.

Confere unicidade e inventário independente, tamanho e CRC antes do egress
S3. Ainda não certifica spool imutável, journal durável ou readback IA.
"""

from __future__ import annotations

import hashlib
import os
import struct
import zipfile
import zlib
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


def _check_member_exact(
    archive: zipfile.ZipFile, entry: zipfile.ZipInfo, budget: int, next_offset: int
) -> int:
    """Count actual inflated bytes, independently of ZIP header size claims.

    zipfile.testzip() can accept a crafted DEFLATE/STORED stream whose local
    and central headers both understate file_size and CRC. It only reads the
    declared prefix. This reader consumes the *entire* compressed member and
    checks EOF, actual length and CRC, with bounded decompression memory.
    """
    if entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
        raise RuntimeError("raw preflight: unsupported ZIP compression")
    handle = archive.fp
    if handle is None:
        raise RuntimeError("raw preflight: unreadable ZIP")
    handle.seek(entry.header_offset)
    header = handle.read(30)
    if len(header) != 30:
        raise RuntimeError("raw preflight: truncated ZIP local header")
    (
        signature,
        _,
        flags,
        method,
        _,
        _,
        local_crc,
        local_compressed,
        local_uncompressed,
        name_len,
        extra_len,
    ) = struct.unpack("<IHHHHHIIIHH", header)
    if signature != 0x04034B50 or method != entry.compress_type or flags != entry.flag_bits:
        raise RuntimeError("raw preflight: ZIP local header mismatch")
    name = handle.read(name_len)
    if name.decode("utf-8" if flags & 0x800 else "cp437") != entry.filename:
        raise RuntimeError("raw preflight: ZIP member name mismatch")
    local_extra = handle.read(extra_len)
    if len(local_extra) != extra_len:
        raise RuntimeError("raw preflight: truncated ZIP local extra")
    # Parse *all* local extras: bit 3 changes which size fields are
    # authoritative, but never excuses broken TLV framing or a missing
    # ZIP64 record for explicit 0xffffffff sentinels.
    cursor = 0
    zip64_extra = None
    while cursor < len(local_extra):
        if cursor + 4 > len(local_extra):
            raise RuntimeError("raw preflight: malformed ZIP local extra")
        kind, width = struct.unpack_from("<HH", local_extra, cursor)
        cursor += 4
        if cursor + width > len(local_extra):
            raise RuntimeError("raw preflight: malformed ZIP local extra")
        if kind == 1:
            if zip64_extra is not None:
                raise RuntimeError("raw preflight: duplicate ZIP64 local extra")
            zip64_extra = local_extra[cursor : cursor + width]
        cursor += width
    if 0xFFFFFFFF in (local_compressed, local_uncompressed):
        needed = 8 * (
            (local_compressed == 0xFFFFFFFF) + (local_uncompressed == 0xFFFFFFFF)
        )
        if zip64_extra is None or len(zip64_extra) < needed:
            raise RuntimeError("raw preflight: missing ZIP64 local sizes")
        cursor = 0
        if local_uncompressed == 0xFFFFFFFF:
            local_uncompressed = struct.unpack_from("<Q", zip64_extra, cursor)[0]
            cursor += 8
        if local_compressed == 0xFFFFFFFF:
            local_compressed = struct.unpack_from("<Q", zip64_extra, cursor)[0]
    # Without bit 3, the local CRC and decoded sizes must match central.
    # With bit 3, the data descriptor remains authoritative.
    if not flags & 8:
        if (local_crc, local_compressed, local_uncompressed) != (
            entry.CRC,
            entry.compress_size,
            entry.file_size,
        ):
            raise RuntimeError("raw preflight: ZIP local CRC/size mismatch")
    data_end = handle.tell() + entry.compress_size
    if data_end > next_offset:
        raise RuntimeError("raw preflight: ZIP payload overlaps next entry")
    # Size understatements can hide a tail even if both compressed AND
    # uncompressed sizes in the ZIP headers are forged consistently.
    gap = next_offset - data_end
    if flags & 8:
        # Data descriptors may be signed/unsigned and 32/64-bit.
        if gap not in (12, 16, 20, 24):
            raise RuntimeError("raw preflight: invalid ZIP data descriptor")
        handle.seek(data_end)
        descriptor = handle.read(gap)
        # Descriptor length disambiguates the optional signature. An unsigned
        # descriptor's CRC-32 may itself equal the signature 0x08074b50.
        if gap in (16, 24):
            if not descriptor.startswith(b"PK\x07\x08"):
                raise RuntimeError("raw preflight: ZIP descriptor signature mismatch")
            descriptor = descriptor[4:]
        if len(descriptor) == 12:
            descriptor_values = struct.unpack("<III", descriptor)
        elif len(descriptor) == 20:
            descriptor_values = struct.unpack("<IQQ", descriptor)
        else:
            raise RuntimeError("raw preflight: invalid ZIP data descriptor")
        if descriptor_values != (entry.CRC, entry.compress_size, entry.file_size):
            raise RuntimeError("raw preflight: ZIP descriptor mismatch")
    elif gap:
        raise RuntimeError("raw preflight: unclaimed ZIP payload bytes")
    handle.seek(data_end - entry.compress_size)

    remaining = entry.compress_size
    expanded = 0
    crc = 0
    inflater = zlib.decompressobj(-15) if method == zipfile.ZIP_DEFLATED else None
    while remaining:
        raw = handle.read(min(_CHUNK, remaining))
        if not raw:
            raise RuntimeError("raw preflight: truncated ZIP payload")
        remaining -= len(raw)
        if inflater is None:
            expanded += len(raw)
            if expanded > budget:
                raise RuntimeError("raw preflight: global uncompressed limit")
            crc = zlib.crc32(raw, crc)
        else:
            while raw:
                available = min(_CHUNK, budget - expanded + 1)
                if available < 1:
                    raise RuntimeError("raw preflight: global uncompressed limit")
                chunk = inflater.decompress(raw, available)
                expanded += len(chunk)
                if expanded > budget:
                    raise RuntimeError("raw preflight: global uncompressed limit")
                crc = zlib.crc32(chunk, crc)
                tail = inflater.unconsumed_tail
                if inflater.eof:
                    if tail or inflater.unused_data or remaining:
                        raise RuntimeError("raw preflight: trailing ZIP deflate bytes")
                    break
                if tail == raw:
                    raise RuntimeError("raw preflight: stalled ZIP deflate stream")
                raw = tail
    if inflater is not None and not inflater.eof:
        raise RuntimeError("raw preflight: truncated ZIP deflate stream")
    if expanded != entry.file_size:
        raise RuntimeError("raw preflight: ZIP uncompressed size mismatch")
    if crc != entry.CRC:
        raise RuntimeError("raw preflight: ZIP CRC failed")
    return expanded


def _raw_zip_crc_and_budget(path: Path, *, expanded: int, members: int) -> tuple[int, int]:
    """Check all compressed bytes and actual expansion before any IA PUT."""
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if not entries or len(entries) + members > _MAX_RAW_MEMBERS:
                raise RuntimeError("raw preflight: invalid ZIP member count")
            names = [entry.filename for entry in entries]
            if len(names) != len(set(names)):
                raise RuntimeError("raw preflight: duplicate ZIP member")
            positions = sorted(entry.header_offset for entry in entries)
            if len(positions) != len(set(positions)):
                raise RuntimeError("raw preflight: duplicate ZIP local header")
            next_offsets = {
                position: (positions[i + 1] if i + 1 < len(positions) else archive.start_dir)
                for i, position in enumerate(positions)
            }
            for entry in entries:
                if entry.flag_bits & 1:
                    raise RuntimeError("raw preflight: encrypted ZIP member")
                if entry.file_size < 0 or expanded + entry.file_size > _MAX_RAW_UNCOMPRESSED_BYTES:
                    raise RuntimeError("raw preflight: global uncompressed limit")
                expanded += _check_member_exact(
                    archive,
                    entry,
                    _MAX_RAW_UNCOMPRESSED_BYTES - expanded,
                    next_offsets[entry.header_offset],
                )
            return expanded, members + len(entries)
    except (
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        OSError,
        EOFError,
        UnicodeError,
        zlib.error,
        struct.error,
    ) as exc:
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
    seen_hashes: set[str] = set()
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
                # A full-object HTTP 200 cannot carry partial-content semantics.
                # A matching ZIP CRC only authenticates received bytes, not remote completeness.
                if "content-range" in response.headers:
                    raise RuntimeError(f"raw preflight: unexpected RFB Content-Range {spec.name}")
                encoding = response.headers.get("content-encoding", "identity").strip().lower()
                if encoding != "identity":
                    raise RuntimeError(f"raw preflight: encoded RFB source {spec.name}")
                length_text = response.headers.get("content-length", "")
                if not length_text.isascii() or not length_text.isdecimal():
                    raise RuntimeError(f"raw preflight: invalid RFB length {spec.name}")
                # Bound digit count before int(): Python rejects enormous decimals.
                if len(length_text) > len(str(_MAX_RAW_COMPRESSED_BYTES)):
                    raise RuntimeError(f"raw preflight: invalid RFB length {spec.name}")
                expected = int(length_text)
                if expected == 0:
                    raise RuntimeError(f"raw preflight: invalid RFB length {spec.name}")
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
            pinned_sha = digest.hexdigest()
            if pinned_sha in seen_hashes:
                raise RuntimeError(f"raw preflight: duplicate source SHA-256: {spec.name}")
            seen_hashes.add(pinned_sha)
            admitted[spec.name] = (target_file, size, pinned_sha)
    if set(admitted) != {spec.name for spec in specs} or len(admitted) != 37:
        raise RuntimeError("raw preflight: incomplete batch")
    return admitted


def file_chunks(handle):
    """Read a previously verified, still-open descriptor without reopening its path.

    The caller owns the handle's lifetime. This prevents namespace replacement
    between the final digest check and a synchronous PUT; it does NOT seal the
    inode against a writer that already holds a writable descriptor.
    """
    while chunk := handle.read(_CHUNK):
        yield chunk
