"""Characterize raw ZIP EOCD entry-count inconsistencies at the real preflight.

The synthetic files have one intact member, but their 16-bit EOCD counters
lie about how many central-directory entries exist. This is a single-disk
admission policy experiment, not evidence about original RFB ZIPs.
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
_BAD_COUNTS = ((0, 1), (1, 0), (0, 0), (2, 2), (2, 1), (1, 2))


class _NonSeekable(io.BytesIO):
    def seek(self, *_args: object, **_kwargs: object) -> None:
        raise OSError("force bit 3")


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise AssertionError(reason)


def _make(method: int, bit3: bool, counts: tuple[int, int]) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as writer:
        writer.writestr(_NAME, _DATA)
    raw = bytearray(sink.getvalue())
    eocd = raw.rfind(b"PK\x05\x06")
    central = raw.find(b"PK\x01\x02")
    _require(eocd == len(raw) - 22 and central > 0, "canonical EOCD")
    _require(
        struct.unpack_from("<HH", raw, eocd + 4) == (0, 0),
        "single-disk metadata untouched",
    )
    _require(
        struct.unpack_from("<HH", raw, eocd + 8) == (1, 1),
        "original entry counts",
    )
    _require(
        struct.unpack_from("<H", raw, central + 34)[0] == 0,
        "central member still on disk zero",
    )
    _require(
        bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3,
        "expected bit 3",
    )
    struct.pack_into("<HH", raw, eocd + 8, *counts)
    return bytes(raw)


def _verify(raw: bytes, bit3: bool, counts: tuple[int, int]) -> None:
    eocd = raw.rfind(b"PK\x05\x06")
    _require(
        struct.unpack_from("<HH", raw, eocd + 4) == (0, 0),
        "EOCD disk fields remain zero",
    )
    _require(
        struct.unpack_from("<HH", raw, eocd + 8) == counts,
        "EOCD entry counters match mutation",
    )
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        _require(len(entries) == 1, "actual central member count")
        entry = entries[0]
        _require(entry.filename == _NAME, "member name")
        _require(bool(entry.flag_bits & 8) == bit3, "member bit 3")
        _require(entry.file_size == len(_DATA), "declared size")
        _require(archive.read(entry) == _DATA, "member payload")
        _require(archive.testzip() is None, "member CRC")
        _require(
            struct.unpack_from("<H", raw, archive.start_dir + 34)[0] == 0,
            "central member disk remains zero",
        )


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "eocd-count.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
def test_eocd_entry_counts_valid(tmp_path: Path, method: int, bit3: bool) -> None:
    raw = _make(method, bit3, (1, 1))
    _verify(raw, bit3, (1, 1))
    _require(
        _preflight(tmp_path, raw) == (len(_DATA), 1),
        "valid single-disk count rejected",
    )


@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="EOCD entry counters not yet validated against actual members",
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("counts", _BAD_COUNTS)
def test_eocd_entry_counts_rejected(
    tmp_path: Path, method: int, bit3: bool, counts: tuple[int, int]
) -> None:
    raw = _make(method, bit3, counts)
    _verify(raw, bit3, counts)
    try:
        result = _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require("EOCD entry count" in str(exc), "rejected for unrelated reason")
    else:
        _require(result == (len(_DATA), 1), "unexpected acceptance result")
        pytest.fail("inconsistent EOCD entry counts admitted by raw preflight")


def test_eocd_entry_counts_python_optimized_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_eocd_entry_counts_valid",
            f"{__file__}::test_eocd_entry_counts_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
