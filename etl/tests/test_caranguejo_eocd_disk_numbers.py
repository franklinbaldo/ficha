"""Characterize EOCD disk fields in the real Ficha raw ZIP preflight.

These are single-file synthetic ZIPs. A nonzero EOCD disk field signals a
multi-volume archive, outside the proposed single-disk admission policy.
This is not evidence of a malformed original Receita Federal ZIP.
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
_INVALID_DISKS = ((1, 0), (0, 1), (1, 1), (65534, 0))


class _NonSeekable(io.BytesIO):
    def seek(self, *_args: object, **_kwargs: object) -> None:
        raise OSError("force bit 3")


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise AssertionError(reason)


def _make(method: int, bit3: bool, disks: tuple[int, int]) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as writer:
        writer.writestr(_NAME, _DATA)
    raw = bytearray(sink.getvalue())
    eocd = raw.rfind(b"PK\x05\x06")
    central = raw.find(b"PK\x01\x02")
    _require(eocd == len(raw) - 22 and central > 0, "canonical EOCD layout")
    _require(
        bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3,
        "expected bit 3",
    )
    _require(
        struct.unpack_from("<HH", raw, eocd + 4) == (0, 0),
        "original EOCD disk numbers",
    )
    _require(
        struct.unpack_from("<HH", raw, eocd + 8) == (1, 1),
        "original EOCD entry counts",
    )
    _require(
        struct.unpack_from("<H", raw, central + 34)[0] == 0,
        "central per-member disk unchanged",
    )
    struct.pack_into("<HH", raw, eocd + 4, *disks)
    return bytes(raw)


def _verify(raw: bytes, bit3: bool, disks: tuple[int, int]) -> None:
    eocd = raw.rfind(b"PK\x05\x06")
    _require(
        struct.unpack_from("<HH", raw, eocd + 4) == disks,
        "exact modified EOCD fields",
    )
    _require(
        struct.unpack_from("<HH", raw, eocd + 8) == (1, 1),
        "entry counts unchanged",
    )
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        (entry,) = archive.infolist()
        _require(entry.filename == _NAME, "single CSV member")
        _require(bool(entry.flag_bits & 8) == bit3, "member bit 3")
        _require(entry.file_size == len(_DATA), "declared size")
        _require(archive.read(entry) == _DATA, "member data")
        _require(archive.testzip() is None, "CRC")
        _require(
            struct.unpack_from("<H", raw, archive.start_dir + 34)[0] == 0,
            "central per-member disk still zero",
        )


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "eocd-disk.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
def test_eocd_single_disk_positive(tmp_path: Path, method: int, bit3: bool) -> None:
    raw = _make(method, bit3, (0, 0))
    _verify(raw, bit3, (0, 0))
    _require(
        _preflight(tmp_path, raw) == (len(_DATA), 1),
        "single-disk ZIP rejected",
    )


@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="EOCD disk numbers not checked by preflight under single-disk policy",
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("disks", _INVALID_DISKS)
def test_eocd_multidisk_metadata_rejected(
    tmp_path: Path, method: int, bit3: bool, disks: tuple[int, int]
) -> None:
    raw = _make(method, bit3, disks)
    _verify(raw, bit3, disks)
    try:
        result = _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require("EOCD disk" in str(exc), "unexpected rejection reason")
    else:
        _require(result == (len(_DATA), 1), "unexpected acceptance result")
        pytest.fail("nonzero EOCD disk metadata admitted by raw preflight")


def test_eocd_disk_optimized_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_eocd_single_disk_positive",
            f"{__file__}::test_eocd_multidisk_metadata_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
