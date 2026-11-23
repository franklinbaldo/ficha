"""Positive Common ZIP ZIP64 sentinel controls against the real raw preflight."""

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
SENTINELS = ("none", "compressed", "uncompressed", "both")
BOUNDARIES = ("local", "central")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def make_zip(method: int, boundary: str, sentinels: str) -> bytes:
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(NAME, PAYLOAD)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    require(cd > 0 and eocd > cd, "original directory/EOCD")
    require(not (struct.unpack_from("<H", raw, 6)[0] & 8), "bit3 must be off")
    compressed, uncompressed = struct.unpack_from("<II", raw, cd + 20)
    require(uncompressed == len(PAYLOAD), "original uncompressed size")
    require(compressed not in (0, 0xFFFFFFFF), "original compressed size")
    require(struct.unpack_from("<II", raw, 18) == (compressed, uncompressed), "local sizes")
    require(struct.unpack_from("<H", raw, 28)[0] == 0, "no original local extra")
    require(struct.unpack_from("<H", raw, cd + 30)[0] == 0, "no original central extra")

    values = []
    if sentinels in ("uncompressed", "both"):
        values.append(uncompressed)
    if sentinels in ("compressed", "both"):
        values.append(compressed)
    require(sentinels in SENTINELS, "recognized sentinel profile")
    payload = b"".join(struct.pack("<Q", value) for value in values)
    extra = struct.pack("<HH", 1, len(payload)) + payload
    if boundary == "local":
        name_len = struct.unpack_from("<H", raw, 26)[0]
        if sentinels in ("compressed", "both"):
            struct.pack_into("<I", raw, 18, 0xFFFFFFFF)
        if sentinels in ("uncompressed", "both"):
            struct.pack_into("<I", raw, 22, 0xFFFFFFFF)
        raw[30 + name_len : 30 + name_len] = extra
        struct.pack_into("<H", raw, 28, len(extra))
        # Local insertion shifts the start of the central directory.
        struct.pack_into("<I", raw, eocd + len(extra) + 16, cd + len(extra))
    elif boundary == "central":
        name_len = struct.unpack_from("<H", raw, cd + 28)[0]
        if sentinels in ("compressed", "both"):
            struct.pack_into("<I", raw, cd + 20, 0xFFFFFFFF)
        if sentinels in ("uncompressed", "both"):
            struct.pack_into("<I", raw, cd + 24, 0xFFFFFFFF)
        raw[cd + 46 + name_len : cd + 46 + name_len] = extra
        struct.pack_into("<H", raw, cd + 30, len(extra))
        old_size = struct.unpack_from("<I", raw, eocd + len(extra) + 12)[0]
        struct.pack_into("<I", raw, eocd + len(extra) + 12, old_size + len(extra))
    else:
        raise ValueError(boundary)
    return bytes(raw)


def verify_fixture(raw: bytes, boundary: str, sentinels: str) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        (entry,) = archive.infolist()
        require(entry.filename == NAME, "member name")
        require(entry.flag_bits & 8 == 0, "bit3 off")
        require(entry.file_size == len(PAYLOAD), "decoded size")
        require(archive.read(entry) == PAYLOAD, "unmodified member")
        require(archive.testzip() is None, "CRC intact")
        at = 30 + len(NAME) if boundary == "local" else archive.start_dir + 46 + len(NAME)
        kind, width = struct.unpack_from("<HH", raw, at)
        expected_width = {"none": 0, "compressed": 8, "uncompressed": 8, "both": 16}[sentinels]
        require((kind, width) == (1, expected_width), "exact ZIP64 payload width")
        payload = raw[at + 4 : at + 4 + width]
        expected_values = []
        if sentinels in ("uncompressed", "both"):
            expected_values.append(entry.file_size)
        if sentinels in ("compressed", "both"):
            expected_values.append(entry.compress_size)
        require(
            payload == b"".join(struct.pack("<Q", value) for value in expected_values),
            "ZIP64 values in prescribed order",
        )
        local_sizes = struct.unpack_from("<II", raw, 18)
        central_sizes = struct.unpack_from("<II", raw, archive.start_dir + 20)
        chosen = local_sizes if boundary == "local" else central_sizes
        require(
            chosen
            == (
                0xFFFFFFFF if sentinels in ("compressed", "both") else entry.compress_size,
                0xFFFFFFFF if sentinels in ("uncompressed", "both") else entry.file_size,
            ),
            "sentinels must match nonempty ZIP64 values",
        )
        other = central_sizes if boundary == "local" else local_sizes
        require(other == (entry.compress_size, entry.file_size), "other boundary unchanged")
        require(
            len(entry.extra) == (4 + width if boundary == "central" else 0),
            "central extra presence",
        )


@pytest.mark.parametrize("method", METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("sentinels", SENTINELS)
def test_valid_zip64_local_or_central(
    tmp_path: Path, method: int, boundary: str, sentinels: str
) -> None:
    raw = make_zip(method, boundary, sentinels)
    verify_fixture(raw, boundary, sentinels)
    path = tmp_path / "zip64-valid.zip"
    path.write_bytes(raw)
    require(
        _raw_zip_crc_and_budget(path, expanded=0, members=0) == (len(PAYLOAD), 1),
        "valid ZIP64 format must remain accepted",
    )


def test_valid_zip64_optimized_replay() -> None:
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", f"{__file__}::test_valid_zip64_local_or_central"],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
