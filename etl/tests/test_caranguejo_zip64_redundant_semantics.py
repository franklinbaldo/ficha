"""Synthetic ZIP64 no-sentinel counterexample for the real Ficha raw preflight."""

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
PAYLOAD = b"1;2\n"
METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)


class NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force bit-3 descriptor")


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def make_zip(method: int, bit3: bool, boundary: str, contradict: bool) -> bytes:
    sink = NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(NAME, PAYLOAD)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    require(cd > 0 and eocd > cd, "header locations")
    compressed, uncompressed = struct.unpack_from("<II", raw, cd + 20)
    require(uncompressed == len(PAYLOAD), "original file size")
    require(compressed != 0xFFFFFFFF, "no original compressed sentinel")
    require(uncompressed != 0xFFFFFFFF, "no original uncompressed sentinel")
    require(bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3, "bit3")
    if not boundary:
        return bytes(raw)
    sizes = (uncompressed, compressed)
    if contradict:
        sizes = (uncompressed + 123, compressed + 456)
    extra = struct.pack("<HHQQ", 1, 16, *sizes)
    if boundary == "local":
        n, x = struct.unpack_from("<HH", raw, 26)
        require(x == 0, "original local extra")
        raw[30 + n : 30 + n] = extra
        struct.pack_into("<H", raw, 28, len(extra))
        eocd += len(extra)
        offset = struct.unpack_from("<I", raw, eocd + 16)[0]
        struct.pack_into("<I", raw, eocd + 16, offset + len(extra))
    elif boundary == "central":
        n, x, comment = struct.unpack_from("<HHH", raw, cd + 28)
        require(x == 0 and comment == 0, "original central extra")
        raw[cd + 46 + n : cd + 46 + n] = extra
        struct.pack_into("<H", raw, cd + 30, len(extra))
        eocd += len(extra)
        size = struct.unpack_from("<I", raw, eocd + 12)[0]
        struct.pack_into("<I", raw, eocd + 12, size + len(extra))
    else:
        raise ValueError(boundary)
    return bytes(raw)


def verify_zip(raw: bytes, bit3: bool, boundary: str, contradict: bool) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        require(len(entries) == 1, "one member")
        entry = entries[0]
        require(entry.filename == NAME, "member name")
        require(bool(entry.flag_bits & 8) == bit3, "descriptor flag")
        require(entry.file_size == len(PAYLOAD), "member size")
        require(archive.read(entry) == PAYLOAD, "payload")
        require(archive.testzip() is None, "CRC")
        size32 = struct.unpack_from("<II", raw, 18)
        expected = (0, 0) if bit3 else (entry.compress_size, entry.file_size)
        require(size32 == expected, "local sizes without sentinels")
        cd = archive.start_dir
        central32 = struct.unpack_from("<II", raw, cd + 20)
        require(central32 == (entry.compress_size, entry.file_size), "central 32-bit")
        local_extra = struct.unpack_from("<H", raw, 28)[0]
        require(local_extra == (20 if boundary == "local" else 0), "local extra length")
        expected_extra = 20 if boundary == "central" else 0
        require(len(entry.extra) == expected_extra, "central extra")
        if boundary:
            at = 30 + len(NAME) if boundary == "local" else cd + 46 + len(NAME)
            kind, width, a, b = struct.unpack_from("<HHQQ", raw, at)
            require((kind, width) == (1, 16), "ZIP64 TLV framing")
            values = (len(PAYLOAD), entry.compress_size)
            if contradict:
                values = (len(PAYLOAD) + 123, entry.compress_size + 456)
            require((a, b) == values, "ZIP64 redundant values")


def preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "zip64.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
def test_no_extra_control(tmp_path: Path, method: int, bit3: bool):
    raw = make_zip(method, bit3, "", False)
    verify_zip(raw, bit3, "", False)
    require(preflight(tmp_path, raw) == (len(PAYLOAD), 1), "baseline")


@pytest.mark.parametrize("method", METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True])
@pytest.mark.parametrize("boundary", ["local", "central"])
@pytest.mark.parametrize("contradict", [False, True])
def test_redundant_zip64_rejected(
    tmp_path: Path,
    method: int,
    bit3: bool,
    boundary: str,
    contradict: bool,
):
    raw = make_zip(method, bit3, boundary, contradict)
    verify_zip(raw, bit3, boundary, contradict)
    try:
        result = preflight(tmp_path, raw)
    except RuntimeError as exc:
        require("ZIP64" in str(exc), "wrong rejection cause")
        require(boundary in str(exc), "wrong rejection boundary")
    else:
        require(result == (len(PAYLOAD), 1), "unexpected admission value")
        pytest.fail("redundant ZIP64 accepted without size sentinels")


def test_optimized_replay():
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_no_extra_control",
            f"{__file__}::test_redundant_zip64_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
