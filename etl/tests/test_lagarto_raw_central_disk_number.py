"""Synthetic central disk-number regressions against the real Ficha preflight.

Common ZIP permits disk 0 or a ZIP64 disk sentinel whose extra contains zero.
A nonzero central per-entry disk number must not be silently accepted.
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

_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_DATA = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)


class _NonSeekable(io.BytesIO):
    def seek(self, *_args: object, **_kwargs: object) -> None:
        raise OSError("force bit 3")


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise AssertionError(reason)


def _make(method: int, bit3: bool, disk16: int) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as writer:
        writer.writestr(_NAME, _DATA)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    _require(cd > 0 and eocd > cd, "original headers")
    _require(struct.unpack_from("<H", raw, cd + 34)[0] == 0, "initial disk")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3, "bit 3")
    struct.pack_into("<H", raw, cd + 34, disk16)
    if disk16 == 0xFFFF:
        name_length, extra_length = struct.unpack_from("<HH", raw, cd + 28)
        _require(extra_length == 0, "initial central extra")
        extra = struct.pack("<HHI", 1, 4, 0)
        raw[cd + 46 + name_length : cd + 46 + name_length] = extra
        struct.pack_into("<H", raw, cd + 30, len(extra))
        eocd += len(extra)
        size = struct.unpack_from("<I", raw, eocd + 12)[0]
        struct.pack_into("<I", raw, eocd + 12, size + len(extra))
    return bytes(raw)


def _check_fixture(raw: bytes, bit3: bool, disk16: int) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as reader:
        (entry,) = reader.infolist()
        _require(entry.filename == _NAME, "member name")
        _require(bool(entry.flag_bits & 8) == bit3, "member bit 3")
        _require(entry.file_size == len(_DATA), "member size")
        _require(reader.read(entry) == _DATA, "payload")
        _require(reader.testzip() is None, "CRC")
        cd = reader.start_dir
        _require(struct.unpack_from("<H", raw, cd + 34)[0] == disk16, "central disk")
        expected_extra = struct.pack("<HHI", 1, 4, 0) if disk16 == 0xFFFF else b""
        _require(entry.extra == expected_extra, "central ZIP64 disk extra")


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "central-disk.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("disk16", [0, 0xFFFF], ids=["plain", "zip64-zero"])
def test_central_disk_zero_allowed(
    tmp_path: Path, method: int, bit3: bool, disk16: int
) -> None:
    raw = _make(method, bit3, disk16)
    _check_fixture(raw, bit3, disk16)
    _require(_preflight(tmp_path, raw) == (len(_DATA), 1), "valid disk rejected")


@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="current preflight admits nonzero central member disk number",
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("disk16", [1, 2, 65534])
def test_central_nonzero_disk_rejected(
    tmp_path: Path, method: int, bit3: bool, disk16: int
) -> None:
    raw = _make(method, bit3, disk16)
    _check_fixture(raw, bit3, disk16)
    try:
        result = _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require("disk" in str(exc).lower(), "wrong rejection gate")
    else:
        _require(result == (len(_DATA), 1), "unexpected admission result")
        pytest.fail("nonzero central member disk admitted")


def test_optimized_disk_number_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_central_disk_zero_allowed",
            f"{__file__}::test_central_nonzero_disk_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
