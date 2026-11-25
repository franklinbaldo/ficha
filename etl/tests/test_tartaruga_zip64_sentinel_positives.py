"""Positive ZIP64 size-sentinel combinations, exercised by real raw preflight.

The one-field and two-field forms are valid even when actual sizes fit in
32 bits; only the field(s) carrying 0xffffffff require ZIP64 payload bytes.
All fixtures are synthetic and do not authenticate any RFB ZIP.
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
_SENTINELS = ("compressed", "uncompressed", "both")


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force bit3 descriptor")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _make(method: int, bit3: bool, boundary: str, sentinel: str) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    cd = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    _require(cd > 0 and eocd > cd, "original central directory")
    compressed, uncompressed = struct.unpack_from("<II", raw, cd + 20)
    _require(uncompressed == len(_PAYLOAD), "original uncompressed size")
    _require(compressed < 0xFFFFFFFF, "original compressed size")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3, "bit3")
    if sentinel == "compressed":
        packed32, unpacked32 = 0xFFFFFFFF, uncompressed
        payload = struct.pack("<Q", compressed)
    elif sentinel == "uncompressed":
        packed32, unpacked32 = compressed, 0xFFFFFFFF
        payload = struct.pack("<Q", uncompressed)
    elif sentinel == "both":
        packed32, unpacked32 = 0xFFFFFFFF, 0xFFFFFFFF
        payload = struct.pack("<QQ", uncompressed, compressed)
    else:
        raise ValueError(sentinel)
    extra = struct.pack("<HH", 1, len(payload)) + payload
    if boundary == "local":
        _require(not bit3, "bit3 local sentinels must not be positive")
        name_len, extra_len = struct.unpack_from("<HH", raw, 26)
        _require(extra_len == 0, "original local extra")
        struct.pack_into("<II", raw, 18, packed32, unpacked32)
        raw[30 + name_len : 30 + name_len] = extra
        struct.pack_into("<H", raw, 28, len(extra))
        eocd += len(extra)
        struct.pack_into("<I", raw, eocd + 16, cd + len(extra))
    elif boundary == "central":
        name_len, extra_len = struct.unpack_from("<HH", raw, cd + 28)
        _require(extra_len == 0, "original central extra")
        struct.pack_into("<II", raw, cd + 20, packed32, unpacked32)
        raw[cd + 46 + name_len : cd + 46 + name_len] = extra
        struct.pack_into("<H", raw, cd + 30, len(extra))
        eocd += len(extra)
        old_size = struct.unpack_from("<I", raw, eocd + 12)[0]
        struct.pack_into("<I", raw, eocd + 12, old_size + len(extra))
    else:
        raise ValueError(boundary)
    return bytes(raw)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
@pytest.mark.parametrize(
    ("boundary", "bit3"),
    [("local", False), ("central", False), ("central", True)],
)
@pytest.mark.parametrize("sentinel", _SENTINELS)
def test_valid_zip64_size_sentinels(
    tmp_path: Path, method: int, boundary: str, bit3: bool, sentinel: str
) -> None:
    raw = _make(method, bit3, boundary, sentinel)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        (entry,) = archive.infolist()
        _require(entry.file_size == len(_PAYLOAD), "decoded size")
        _require(archive.read(entry) == _PAYLOAD, "payload")
        _require(archive.testzip() is None, "CRC")
    path = tmp_path / "valid-sentinels.zip"
    path.write_bytes(raw)
    _require(
        _raw_zip_crc_and_budget(path, expanded=0, members=0) == (len(_PAYLOAD), 1),
        "legitimate ZIP64 sentinels rejected",
    )


def test_optimized_zip64_positive_replay() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_valid_zip64_size_sentinels",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
