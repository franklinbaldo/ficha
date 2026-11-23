"""Research-only counterexample: redundant ZIP64 payloads without size sentinels.

The real Ficha raw preflight is exercised, with synthetic ZIP members only.
This does not authenticate RFB originals, establish remote impact or authorize upload.
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


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force data descriptor")


def _require(condition: bool, description: str) -> None:
    if not condition:
        raise AssertionError(description)


def _fixture(method: int, *, bit3: bool, boundary: str | None, contradict: bool) -> bytes:
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    central = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    _require(central > 0 and eocd > central, "ZIP header positions")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8) == bit3, "local bit 3")
    _require(bool(struct.unpack_from("<H", raw, central + 8)[0] & 8) == bit3, "central bit 3")
    real_comp, real_uncomp = struct.unpack_from("<II", raw, central + 20)
    _require(real_uncomp == len(_PAYLOAD), "central original uncompressed size")
    _require(real_comp != 0xFFFFFFFF and real_uncomp != 0xFFFFFFFF, "no central sentinel")
    local_comp, local_uncomp = struct.unpack_from("<II", raw, 18)
    _require(
        (local_comp, local_uncomp) == ((0, 0) if bit3 else (real_comp, real_uncomp)),
        "local original sizes",
    )

    if boundary is None:
        return bytes(raw)
    _require(boundary in ("local", "central"), "boundary")
    values = (real_uncomp + 123, real_comp + 456) if contradict else (real_uncomp, real_comp)
    extra = struct.pack("<HHQQ", 0x0001, 16, *values)
    if boundary == "local":
        name_len, old_len = struct.unpack_from("<HH", raw, 26)
        _require(old_len == 0, "no original local extra")
        raw[30 + name_len : 30 + name_len] = extra
        struct.pack_into("<H", raw, 28, len(extra))
        eocd += len(extra)
        start_dir = struct.unpack_from("<I", raw, eocd + 16)[0]
        struct.pack_into("<I", raw, eocd + 16, start_dir + len(extra))
    else:
        name_len, old_len, comment_len = struct.unpack_from("<HHH", raw, central + 28)
        _require(old_len == 0 and comment_len == 0, "no original central extra/comment")
        raw[central + 46 + name_len : central + 46 + name_len] = extra
        struct.pack_into("<H", raw, central + 30, len(extra))
        eocd += len(extra)
        cd_size = struct.unpack_from("<I", raw, eocd + 12)[0]
        struct.pack_into("<I", raw, eocd + 12, cd_size + len(extra))
    return bytes(raw)


def _verify(raw: bytes, *, bit3: bool, boundary: str | None, contradict: bool) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        _require(len(entries) == 1, "exactly one ZIP member")
        entry = entries[0]
        _require(entry.filename == _NAME and bool(entry.flag_bits & 8) == bit3, "identity/flags")
        _require(entry.file_size == len(_PAYLOAD), "central uncompressed size")
        _require(archive.read(entry) == _PAYLOAD and archive.testzip() is None, "payload/CRC")
        local_size32 = struct.unpack_from("<II", raw, 18)
        _require(
            local_size32 == ((0, 0) if bit3 else (entry.compress_size, entry.file_size)),
            "local no sentinels",
        )
        with io.BytesIO(raw) as handle:
            handle.seek(archive.start_dir)
            cd = handle.read(46)
        _require(
            struct.unpack_from("<II", cd, 20) == (entry.compress_size, entry.file_size),
            "central no sentinels",
        )
        local_extra_len = struct.unpack_from("<H", raw, 28)[0]
        if boundary == "local":
            _require(local_extra_len == 20 and entry.extra == b"", "local-only extra")
        elif boundary == "central":
            _require(local_extra_len == 0 and len(entry.extra) == 20, "central-only extra")
        else:
            _require(local_extra_len == 0 and entry.extra == b"", "no extra control")
        if boundary is not None:
            first = (30 + len(_NAME)) if boundary == "local" else (
                archive.start_dir + 46 + len(_NAME)
            )
            _require(raw[first : first + 4] == struct.pack("<HH", 1, 16), "ZIP64 framing")
            a, b = struct.unpack_from("<QQ", raw, first + 4)
            wanted = (
                (len(_PAYLOAD) + 123, entry.compress_size + 456)
                if contradict
                else (len(_PAYLOAD), entry.compress_size)
            )
            _require((a, b) == wanted, "ZIP64 extra values")


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "zip64-semantics.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True], ids=["no_bit3", "bit3"])
def test_without_redundant_zip64_is_admitted(tmp_path: Path, method: int, bit3: bool):
    raw = _fixture(method, bit3=bit3, boundary=None, contradict=False)
    _verify(raw, bit3=bit3, boundary=None, contradict=False)
    _require(_preflight(tmp_path, raw) == (len(_PAYLOAD), 1), "baseline preflight")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("bit3", [False, True], ids=["no_bit3", "bit3"])
@pytest.mark.parametrize("boundary", ["local", "central"])
@pytest.mark.parametrize("contradict", [False, True], ids=["matching", "contradictory"])
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="Common ZIP fileSizes: nonempty ZIP64 size extra without 32-bit sentinel is accepted",
)
def test_zip64_without_sentinel_rejected(
    tmp_path: Path, method: int, bit3: bool, boundary: str, contradict: bool
):
    raw = _fixture(method, bit3=bit3, boundary=boundary, contradict=contradict)
    _verify(raw, bit3=bit3, boundary=boundary, contradict=contradict)
    try:
        result = _preflight(tmp_path, raw)
    except RuntimeError as exc:
        _require("ZIP64" in str(exc) and boundary in str(exc), f"unexpected rejection: {exc}")
    else:
        _require(result == (len(_PAYLOAD), 1), "unexpected accepted result")
        pytest.fail("redundant ZIP64 extra admitted without any size sentinel")


def test_python_optimized_replays_zip64_semantics():
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_without_redundant_zip64_is_admitted",
            f"{__file__}::test_zip64_without_sentinel_rejected",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
