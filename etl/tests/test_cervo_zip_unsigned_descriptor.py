"""ZIP descriptor framing controls; synthetic bytes, no RFB/IA network."""

from __future__ import annotations

import io
import struct
import subprocess
import sys
import zipfile
import zlib
from pathlib import Path

import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("unseekable")


def _zip_with_descriptor(payload: bytes, *, method: int, unsigned: bool) -> bytes:
    sink = _NonSeekable()
    with zipfile.ZipFile(sink, mode="w", compression=method) as archive:
        archive.writestr("K3241.K03200Y2.D60808.EMPRECSV", payload)
    raw = sink.getvalue()
    if not unsigned:
        return raw

    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry = archive.infolist()[0]
        offset = entry.header_offset
        local = struct.unpack_from("<IHHHHHIIIHH", raw, offset)
        name_length, extra_length = local[-2:]
        start = offset + 30 + name_length + extra_length + entry.compress_size
        if raw[start : start + 4] != b"PK\x07\x08":
            raise AssertionError("signed descriptor expected")
    unsigned_raw = bytearray(raw[:start] + raw[start + 4 :])
    eocd = unsigned_raw.rfind(b"PK\x05\x06")
    if eocd < 0:
        raise AssertionError("EOCD missing")
    central_offset = struct.unpack_from("<I", unsigned_raw, eocd + 16)[0]
    struct.pack_into("<I", unsigned_raw, eocd + 16, central_offset - 4)
    return bytes(unsigned_raw)


def _descriptor_crc_offset(raw: bytes) -> int:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry = archive.infolist()[0]
        local = struct.unpack_from("<IHHHHHIIIHH", raw, entry.header_offset)
        start = entry.header_offset + 30 + local[-2] + local[-1] + entry.compress_size
        gap = archive.start_dir - start
        return start + (4 if gap in (16, 24) else 0)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
@pytest.mark.parametrize("unsigned", [False, True])
def test_valid_descriptors(tmp_path: Path, method: int, unsigned: bool):
    raw = _zip_with_descriptor(b"a;b\n", method=method, unsigned=unsigned)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if archive.testzip() is not None:
            raise AssertionError("valid control rejected by stdlib")
    source = tmp_path / "test.zip"
    source.write_bytes(raw)
    if _raw_zip_crc_and_budget(source, expanded=0, members=0) != (4, 1):
        raise AssertionError("valid descriptor rejected by raw preflight")


def test_unsigned_crc_equal_signature_is_valid(tmp_path: Path):
    # CRC=0x08074b50 equals optional descriptor signature bytes PK\x07\x08.
    payload = b"\xac\x0a\x7a\xd5"
    if zlib.crc32(payload) != 0x08074B50:
        raise AssertionError("incorrect collision fixture")
    raw = _zip_with_descriptor(payload, method=zipfile.ZIP_STORED, unsigned=True)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        readback = archive.read(archive.infolist()[0])
        if archive.testzip() is not None or readback != payload:
            raise AssertionError("invalid fixture")
    source = tmp_path / "test.zip"
    source.write_bytes(raw)
    if _raw_zip_crc_and_budget(source, expanded=0, members=0) != (4, 1):
        raise AssertionError("unsigned descriptor CRC collision falsely rejected")


@pytest.mark.parametrize("unsigned", [False, True])
def test_corrupted_descriptor_crc_is_rejected(tmp_path: Path, unsigned: bool):
    raw = bytearray(_zip_with_descriptor(b"a;b\n", method=zipfile.ZIP_DEFLATED, unsigned=unsigned))
    raw[_descriptor_crc_offset(raw)] ^= 1
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if archive.testzip() is not None:
            raise AssertionError("stdlib unexpectedly rejected negative control")
    source = tmp_path / "test.zip"
    source.write_bytes(raw)
    with pytest.raises(RuntimeError, match="ZIP descriptor mismatch"):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)


def test_optimized_python_replays_descriptor_controls():
    tests = (
        "test_valid_descriptors",
        "test_unsigned_crc_equal_signature_is_valid",
        "test_corrupted_descriptor_crc_is_rejected",
    )
    selectors = [f"{__file__}::{name}" for name in tests]
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
