"""Synthetic bit-3 ZIP local-extra TLV controls without ZIP64 sentinels.

A complete central directory and matching CRC do not validate local extra
field framing. No network, production ETL, or Internet Archive calls.
"""

from __future__ import annotations

import io
import struct
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force ZIP data descriptor")


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"
_GOOD_EXTRAS = (b"", b"\x34\x12\x01\x00x")
_BAD_EXTRAS = (
    b"\x01",  # orphan byte
    b"\x01\x00\x10",  # partial TLV header
    b"\x01\x00\x10\x00\x00\x00",  # data shorter than declared width
)
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)


def _fixture(method: int, extra: bytes) -> bytes:
    sink = _NonSeekable()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    flags = struct.unpack_from("<H", raw, 6)[0]
    compressed, expanded = struct.unpack_from("<II", raw, 18)
    name_len, previous_extra_len = struct.unpack_from("<HH", raw, 26)
    if not flags & 8 or (compressed, expanded) != (0, 0) or previous_extra_len:
        raise AssertionError("expected bit-3 local header without ZIP64 sentinels")
    start = 30 + name_len
    raw[start:start] = extra
    struct.pack_into("<H", raw, 28, len(extra))
    end = raw.rfind(b"PK\x05\x06")
    if end < 0:
        raise AssertionError("missing end of central directory")
    central = struct.unpack_from("<I", raw, end + 16)[0]
    struct.pack_into("<I", raw, end + 16, central + len(extra))
    return bytes(raw)


def _complete_tlvs(extra: bytes) -> bool:
    cursor = 0
    while cursor < len(extra):
        if len(extra) - cursor < 4:
            return False
        _, width = struct.unpack_from("<HH", extra, cursor)
        cursor += 4
        if width > len(extra) - cursor:
            return False
        cursor += width
    return True


def _check_stdlib(raw: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry = archive.infolist()[0]
        if not entry.flag_bits & 8 or archive.testzip() is not None:
            raise AssertionError("invalid standard-library control")
        if archive.read(entry) != _PAYLOAD:
            raise AssertionError("payload changed")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("extra", _GOOD_EXTRAS)
def test_valid_bit3_extra_without_sentinels(tmp_path: Path, method: int, extra: bytes):
    raw = _fixture(method, extra)
    if not _complete_tlvs(extra):
        raise AssertionError("positive control is malformed")
    _check_stdlib(raw)
    source = tmp_path / "valid.zip"
    source.write_bytes(raw)
    if _raw_zip_crc_and_budget(source, expanded=0, members=0) != (len(_PAYLOAD), 1):
        raise AssertionError("valid ZIP incorrectly rejected")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("extra", _BAD_EXTRAS)
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="Known gap: bit-3 local TLV framing is not checked without ZIP64 (#433)",
)
def test_malformed_bit3_extra_without_sentinels(
    tmp_path: Path, method: int, extra: bytes
):
    raw = _fixture(method, extra)
    if _complete_tlvs(extra):
        raise AssertionError("negative control is structurally valid")
    _check_stdlib(raw)
    source = tmp_path / "invalid.zip"
    source.write_bytes(raw)
    with pytest.raises(RuntimeError, match="malformed ZIP local extra"):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)


def test_optimized_python_replays_bit3_no_zip64_matrix():
    selectors = [
        f"{__file__}::test_valid_bit3_extra_without_sentinels",
        f"{__file__}::test_malformed_bit3_extra_without_sentinels",
    ]
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
