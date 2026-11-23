"""ZIP64 central disk32 controls for Common ZIP single-disk admission.

Test-only: files are synthetic; use the real _raw_zip_crc_and_budget.
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

NAME = "K3241.K03200Y2.D60808.EMPRECSV"
DATA = b"1;2\n"
METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)


class _NonSeekable(io.BytesIO):
    def seek(self, *_args: object, **_kwargs: object) -> None:
        raise OSError("force bit3")


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def _fixture(method: int, bit3: bool, disk32: int) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as writer:
        writer.writestr(NAME, DATA)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    _require(cd > 0 and eocd > cd, "original central and EOCD")
    _require(struct.unpack_from("<H", raw, cd + 34)[0] == 0, "original disk16")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3, "original bit3")
    name_len, extra_len = struct.unpack_from("<HH", raw, cd + 28)
    _require(extra_len == 0, "original central extra length")
    _require(raw[cd + 46 : cd + 46 + name_len] == NAME.encode(), "original name")

    # Only mutate per-entry central disk16 and add its sentinel-driven ZIP64 disk32.
    struct.pack_into("<H", raw, cd + 34, 0xFFFF)
    extra = struct.pack("<HHI", 0x0001, 4, disk32)
    raw[cd + 46 + name_len : cd + 46 + name_len] = extra
    struct.pack_into("<H", raw, cd + 30, len(extra))
    eocd += len(extra)
    central_size = struct.unpack_from("<I", raw, eocd + 12)[0]
    struct.pack_into("<I", raw, eocd + 12, central_size + len(extra))
    return bytes(raw)


def _check_fixture(raw: bytes, bit3: bool, disk32: int) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as reader:
        (entry,) = reader.infolist()
        _require(entry.filename == NAME, "member name")
        _require(bool(entry.flag_bits & 8) == bit3, "member bit3")
        _require(reader.read(entry) == DATA, "member payload")
        _require(reader.testzip() is None, "member CRC")
        cd = reader.start_dir
        _require(struct.unpack_from("<H", raw, cd + 34)[0] == 0xFFFF, "disk16 sentinel")
        _require(entry.extra == struct.pack("<HHI", 1, 4, disk32), "disk32 extra")
        _require(struct.unpack_from("<I", raw, cd + 46 + len(NAME) + 4)[0] == disk32, "raw disk32")


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "central-disk32.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
def test_central_zip64_disk32_zero_allowed(tmp_path: Path, method: int, bit3: bool) -> None:
    raw = _fixture(method, bit3, 0)
    _check_fixture(raw, bit3, 0)
    _require(_preflight(tmp_path, raw) == (len(DATA), 1), "disk32 zero rejected")


@pytest.mark.parametrize("method", METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("disk32", [1, 2, 0xFFFFFFFF], ids=["one", "two", "max32"])
def test_central_zip64_disk32_nonzero_rejected(
    tmp_path: Path, method: int, bit3: bool, disk32: int
) -> None:
    raw = _fixture(method, bit3, disk32)
    _check_fixture(raw, bit3, disk32)
    try:
        _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require(
            "raw preflight: unsupported ZIP64 central disk" in str(exc),
            f"unexpected rejection gate: {exc}",
        )
    else:
        raise AssertionError("nonzero ZIP64 disk32 admitted")


def test_optimized_central_disk32_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_central_zip64_disk32_zero_allowed",
            f"{__file__}::test_central_zip64_disk32_nonzero_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
