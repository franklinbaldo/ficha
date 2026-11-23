"""Common ZIP: bit-3 local ZIP64 must be empty, even with 32-bit sentinels.

Synthetic fixtures only. Test real Ficha raw preflight, not RFB provenance.
The existing #497 tests cover nonempty local ZIP64 *without* sentinels;
these exercise the independent case of explicit local sentinels under bit 3.
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
_PAYLOAD = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_SENTINELS = ("compressed", "uncompressed", "both")


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force bit-3 descriptor")


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def _make(method: int, sentinel: str) -> bytes:
    sink = _NonSeekable()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    _require(cd > 0 and eocd > cd, "central header and EOCD")
    _require(struct.unpack_from("<III", raw, 14) == (0, 0, 0), "bit3 local zeros")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "bit3 flag")
    packed, unpacked = struct.unpack_from("<II", raw, cd + 20)
    _require(unpacked == len(_PAYLOAD), "central size")
    if sentinel == "zero":
        extra_payload, local_packed, local_unpacked = b"", 0, 0
    elif sentinel == "compressed":
        extra_payload, local_packed, local_unpacked = (
            struct.pack("<Q", packed),
            0xFFFFFFFF,
            0,
        )
    elif sentinel == "uncompressed":
        extra_payload, local_packed, local_unpacked = (
            struct.pack("<Q", unpacked),
            0,
            0xFFFFFFFF,
        )
    elif sentinel == "both":
        extra_payload, local_packed, local_unpacked = (
            struct.pack("<QQ", unpacked, packed),
            0xFFFFFFFF,
            0xFFFFFFFF,
        )
    else:
        raise ValueError(sentinel)
    extra = struct.pack("<HH", 0x0001, len(extra_payload)) + extra_payload
    name_length, extra_length = struct.unpack_from("<HH", raw, 26)
    _require(extra_length == 0, "local extra originally empty")
    raw[30 + name_length : 30 + name_length] = extra
    # Header size fields begin at +18; extra length is at +28.
    struct.pack_into("<II", raw, 18, local_packed, local_unpacked)
    struct.pack_into("<H", raw, 28, len(extra))
    eocd += len(extra)
    old_cd_offset = struct.unpack_from("<I", raw, eocd + 16)[0]
    _require(old_cd_offset == cd, "central-directory offset original")
    struct.pack_into("<I", raw, eocd + 16, old_cd_offset + len(extra))
    return bytes(raw)


def _check_fixture(raw: bytes, sentinel: str) -> None:
    name_length, extra_length = struct.unpack_from("<HH", raw, 26)
    kind, width = struct.unpack_from("<HH", raw, 30 + name_length)
    _require(kind == 1, "ZIP64 local extra tag")
    _require(width == extra_length - 4, "ZIP64 local payload exact length")
    expected_width = {"zero": 0, "compressed": 8, "uncompressed": 8, "both": 16}[sentinel]
    _require(width == expected_width, "ZIP64 local sentinel payload width")
    compressed32, uncompressed32 = struct.unpack_from("<II", raw, 18)
    _require(
        (compressed32, uncompressed32)
        == {
            "zero": (0, 0),
            "compressed": (0xFFFFFFFF, 0),
            "uncompressed": (0, 0xFFFFFFFF),
            "both": (0xFFFFFFFF, 0xFFFFFFFF),
        }[sentinel],
        "local 32-bit sentinel fields",
    )
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        (entry,) = archive.infolist()
        _require(bool(entry.flag_bits & 8), "bit3 remains set")
        _require(entry.file_size == len(_PAYLOAD), "central file size")
        _require(archive.read(entry) == _PAYLOAD, "read payload")
        _require(archive.testzip() is None, "CRC")
        _require(entry.extra == b"", "central extra not modified")
        _require(
            raw[archive.start_dir - 16 : archive.start_dir - 12] == b"PK\x07\x08",
            "descriptor",
        )


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "bit3-sentinel.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
def test_bit3_empty_local_zip64_is_allowed(tmp_path: Path, method: int) -> None:
    raw = _make(method, "zero")
    _check_fixture(raw, "zero")
    _require(_preflight(tmp_path, raw) == (len(_PAYLOAD), 1), "empty local ZIP64 rejected")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("sentinel", _SENTINELS)
def test_bit3_sentinel_local_zip64_must_be_rejected(
    tmp_path: Path, method: int, sentinel: str
) -> None:
    raw = _make(method, sentinel)
    _check_fixture(raw, sentinel)
    try:
        result = _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require("ZIP64" in str(exc) and "local" in str(exc), "wrong rejection cause")
    else:
        _require(result == (len(_PAYLOAD), 1), "unexpected admission result")
        pytest.fail("bit3 local sentinels and nonempty ZIP64 admitted")


def test_bit3_sentinel_optimized_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_bit3_empty_local_zip64_is_allowed",
            f"{__file__}::test_bit3_sentinel_local_zip64_must_be_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
