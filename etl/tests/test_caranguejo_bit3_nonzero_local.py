"""Research #493: bit-3 local CRC/size fields versus data descriptor.

Synthetic ZIPs only. The production preflight, not zipfile.testzip(), is
exercised; this matrix does not establish APPNOTE conformity or RFB impact.
The F cases are typed strict XFAILs for observed parser tolerance, not
approved admission formats. No productive ETL, HTTP or manifest operations.
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
        raise OSError("force bit-3 data descriptor")


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_F_CASES = (
    "crc_one",
    "crc_actual",
    "compressed_one",
    "uncompressed_one",
    "sizes_actual",
    "all_fake",
)
_NEGATIVE_CASES = ("descriptor_bad_crc", "local_flags_mismatch")


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise AssertionError(reason)


def _source_zip(method: int) -> bytes:
    sink = _NonSeekable()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = sink.getvalue()
    _require(raw.startswith(b"PK\x03\x04"), "local signature")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "descriptor flag")
    _require(struct.unpack_from("<III", raw, 14) == (0, 0, 0), "zero local fields")
    _require(struct.unpack_from("<H", raw, 28)[0] == 0, "no local extra")
    return raw


def _mutate(raw: bytes, case: str) -> bytes:
    forged = bytearray(raw)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry, = archive.infolist()
        data_end = (
            entry.header_offset + 30 + len(entry.filename.encode()) + entry.compress_size
        )
        _require(forged[data_end : data_end + 4] == b"PK\x07\x08", "signed descriptor")
        _require(archive.start_dir == data_end + 16, "16-byte data descriptor")
        if case == "crc_one":
            struct.pack_into("<I", forged, 14, 1)
        elif case == "crc_actual":
            struct.pack_into("<I", forged, 14, entry.CRC)
        elif case == "compressed_one":
            struct.pack_into("<I", forged, 18, 1)
        elif case == "uncompressed_one":
            struct.pack_into("<I", forged, 22, 1)
        elif case == "sizes_actual":
            struct.pack_into(
                "<III", forged, 14, entry.CRC, entry.compress_size, entry.file_size
            )
        elif case == "all_fake":
            struct.pack_into("<III", forged, 14, 1234, 5678, 91011)
        elif case == "descriptor_bad_crc":
            struct.pack_into("<I", forged, data_end + 4, entry.CRC ^ 1)
        elif case == "local_flags_mismatch":
            flags = struct.unpack_from("<H", forged, 6)[0]
            struct.pack_into("<H", forged, 6, flags & ~8)
        else:
            raise AssertionError(case)
    return bytes(forged)


def _verify_fixture(raw: bytes, case: str) -> None:
    local = struct.unpack_from("<III", raw, 14)
    is_f = case in _F_CASES
    _require((local != (0, 0, 0)) == is_f, "local header mutation classified")
    _require(raw.startswith(b"PK\x03\x04"), "local magic")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry, = archive.infolist()
        _require(len(archive.infolist()) == 1, "exactly one member")
        _require(entry.filename == _NAME, "member name unchanged")
        _require(bool(entry.flag_bits & 8), "central descriptor bit set")
        _require(entry.CRC == zipfile.crc32(_PAYLOAD), "central CRC authentic")
        _require(entry.file_size == len(_PAYLOAD), "central size authentic")
        data_end = (
            entry.header_offset + 30 + len(entry.filename.encode()) + entry.compress_size
        )
        _require(raw[data_end : data_end + 4] == b"PK\x07\x08", "descriptor signature")
        descriptor_crc, packed, unpacked = struct.unpack_from("<III", raw, data_end + 4)
        _require(
            (packed, unpacked) == (entry.compress_size, entry.file_size), "descriptor sizes"
        )
        _require(
            (descriptor_crc == entry.CRC) == (case != "descriptor_bad_crc"),
            "descriptor CRC changed only in negative control",
        )
        _require(
            bool(struct.unpack_from("<H", raw, 6)[0] & 8)
            == (case != "local_flags_mismatch"),
            "local flags changed only in negative control",
        )
        if is_f:
            _require(archive.testzip() is None, "stdlib CRC of F fixture")
            _require(archive.read(_NAME) == _PAYLOAD, "stdlib payload of F fixture")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
def test_bit3_zero_local_baseline(tmp_path: Path, method: int) -> None:
    raw = _source_zip(method)
    source = tmp_path / "baseline.zip"
    source.write_bytes(raw)
    _require(
        _raw_zip_crc_and_budget(source, expanded=0, members=0) == (len(_PAYLOAD), 1),
        "valid baseline rejected",
    )


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("case", _F_CASES)
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="#493 profile F: bit-3 nonzero local CRC/sizes accepted; policy unresolved",
)
def test_bit3_nonzero_local_fields_rejected(
    tmp_path: Path, method: int, case: str
) -> None:
    raw = _mutate(_source_zip(method), case)
    _verify_fixture(raw, case)
    source = tmp_path / "f.zip"
    source.write_bytes(raw)
    try:
        result = _raw_zip_crc_and_budget(source, expanded=0, members=0)
    except RuntimeError as exc:
        _require("ZIP local" in str(exc), "unexpected rejection cause")
    else:
        _require(result == (len(_PAYLOAD), 1), "unexpected accepted result")
        pytest.fail("#493: nonzero bit-3 local fields accepted")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("case", _NEGATIVE_CASES)
def test_bit3_negative_controls_rejected(
    tmp_path: Path, method: int, case: str
) -> None:
    raw = _mutate(_source_zip(method), case)
    _verify_fixture(raw, case)
    source = tmp_path / "negative.zip"
    source.write_bytes(raw)
    expected = (
        "ZIP descriptor mismatch"
        if case == "descriptor_bad_crc"
        else "ZIP local header mismatch"
    )
    with pytest.raises(RuntimeError, match=expected):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)


def test_python_optimized_replays_bit3_nonzero_local() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_bit3_zero_local_baseline",
            f"{__file__}::test_bit3_nonzero_local_fields_rejected",
            f"{__file__}::test_bit3_negative_controls_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
