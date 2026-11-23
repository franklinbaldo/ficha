"""Research #493: mixed ZIP64 sentinels in a bit-3 local header.

Replays Lagarto R200 against the actual raw preflight. ZIPs are entirely
synthetic; a successful stdlib read is not an APPNOTE conformance oracle.
A one-sentinel ZIP64 extra with one 8-byte word is noncanonical under the
local-size-pair reading of APPNOTE 6.3.10 §4.5.3. The decision whether
Ficha must reject that layout is deliberately still open (#493).

No producer, uploader, HTTP or manifest is modified by this test-only PR.
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
        raise OSError("force data descriptor")


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_SENTINELS = ("compressed", "uncompressed")
_VARIANTS = (
    pytest.param(
        "framed",
        marks=pytest.mark.xfail(
            strict=True,
            reason="#493: normative local ZIP64 pair policy pending",
        ),
    ),
    pytest.param(
        "framed_prefixed",
        marks=pytest.mark.xfail(
            strict=True,
            reason="#493: normative local ZIP64 pair policy pending",
        ),
    ),
    "short",
    "missing",
    "duplicate",
)


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def _source_zip(method: int) -> bytes:
    sink = _NonSeekable()
    info = zipfile.ZipInfo(_NAME, (2020, 1, 1, 0, 0, 0))
    info.compress_type = method
    with zipfile.ZipFile(sink, "w", allowZip64=True) as archive:
        with archive.open(info, "w", force_zip64=True) as member:
            member.write(_PAYLOAD)
    raw = sink.getvalue()
    _require(struct.unpack_from("<I", raw)[0] == 0x04034B50, "ZIP local signature")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "descriptor bit 3")
    _require(
        struct.unpack_from("<II", raw, 18) == (0xFFFFFFFF, 0xFFFFFFFF),
        "canonical two-sentinel source",
    )
    name_len, extra_len = struct.unpack_from("<HH", raw, 26)
    _require(extra_len == 20, "canonical ZIP64 extra width")
    _require(
        raw[30 + name_len : 30 + name_len + extra_len] == b"\x01\x00\x10\x00" + bytes(16),
        "canonical ZIP64 size pair",
    )
    return raw


def _mixed_sentinel(raw: bytes, sentinel: str, variant: str) -> bytes:
    forged = bytearray(raw)
    if sentinel == "uncompressed":
        struct.pack_into("<I", forged, 18, 0)
    elif sentinel == "compressed":
        struct.pack_into("<I", forged, 22, 0)
    else:
        raise AssertionError(sentinel)

    name_len, old_extra_len = struct.unpack_from("<HH", forged, 26)
    start = 30 + name_len
    zip64 = struct.pack("<HHQ", 1, 8, 0)
    if variant == "framed":
        new_extra = zip64
    elif variant == "framed_prefixed":
        new_extra = struct.pack("<HH", 0x7777, 1) + b"x" + zip64
    elif variant == "short":
        new_extra = struct.pack("<HH", 1, 7) + bytes(7)
    elif variant == "missing":
        new_extra = struct.pack("<HHQ", 0x9999, 8, 0)
    elif variant == "duplicate":
        new_extra = zip64 + zip64
    else:
        raise AssertionError(variant)

    forged = bytearray(forged[:start] + new_extra + forged[start + old_extra_len :])
    struct.pack_into("<H", forged, 28, len(new_extra))
    eocd = forged.rfind(b"PK\x05\x06")
    _require(eocd > start, "end of central directory exists")
    old_cd_offset = struct.unpack_from("<I", forged, eocd + 16)[0]
    struct.pack_into(
        "<I",
        forged,
        eocd + 16,
        old_cd_offset + len(new_extra) - old_extra_len,
    )
    return bytes(forged)


def _verify_fixture(raw: bytes, *, sentinel: str, variant: str) -> None:
    compressed, uncompressed = struct.unpack_from("<II", raw, 18)
    _require(
        sum(x == 0xFFFFFFFF for x in (compressed, uncompressed)) == 1,
        "exactly one local size sentinel",
    )
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "descriptor bit 3")
    _require(
        (compressed == 0xFFFFFFFF) == (sentinel == "compressed"),
        "chosen size sentinel",
    )
    _require(
        (uncompressed == 0xFFFFFFFF) == (sentinel == "uncompressed"),
        "other size sentinel",
    )
    name_len, extra_len = struct.unpack_from("<HH", raw, 26)
    local_extra = raw[30 + name_len : 30 + name_len + extra_len]
    offset, zip64_count, width = 0, 0, None
    while offset < len(local_extra):
        _require(len(local_extra) - offset >= 4, "fixture TLV header")
        tag, length = struct.unpack_from("<HH", local_extra, offset)
        offset += 4
        _require(length <= len(local_extra) - offset, "fixture TLV length")
        if tag == 1:
            zip64_count += 1
            width = length
        offset += length
    _require(
        (zip64_count == 1 and width == 8) == variant.startswith("framed") or variant == "duplicate",
        "independent framing oracle",
    )
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        _require(len(entries) == 1, "one ZIP member")
        _require(entries[0].filename == _NAME, "member name")
        _require(archive.testzip() is None, "stdlib CRC read")
        _require(archive.read(entries[0]) == _PAYLOAD, "stdlib payload")
    # This check is necessary but not sufficient for canonical ZIP64.
    _require(
        (compressed == 0xFFFFFFFF) != (uncompressed == 0xFFFFFFFF),
        "noncanonical one-sentinel header",
    )


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
def test_canonical_two_local_zip64_sizes_accepted(tmp_path: Path, method: int) -> None:
    source = tmp_path / "canonical.zip"
    source.write_bytes(_source_zip(method))
    result = _raw_zip_crc_and_budget(source, expanded=0, members=0)
    _require(result == (len(_PAYLOAD), 1), "canonical ZIP64 refused")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("sentinel", _SENTINELS)
@pytest.mark.parametrize("variant", _VARIANTS)
def test_single_local_zip64_sentinel_rejected_by_preflight(
    tmp_path: Path, method: int, sentinel: str, variant: str
) -> None:
    raw = _mixed_sentinel(_source_zip(method), sentinel, variant)
    _verify_fixture(raw, sentinel=sentinel, variant=variant)
    source = tmp_path / "mixed.zip"
    source.write_bytes(raw)
    # For framed / framed_prefixed this currently XFAILs if the production
    # parser accepts the structurally valid but noncanonical local layout.
    # Policy question remains open: strict APPNOTE rejection or documented
    # compatibility tolerance. Do not silently promote this to a PASS.
    with pytest.raises(RuntimeError, match="ZIP64 local"):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)


def test_python_optimized_replays_single_sentinel_matrix() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_canonical_two_local_zip64_sizes_accepted",
            f"{__file__}::test_single_local_zip64_sentinel_rejected_by_preflight",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode != 0:
        raise AssertionError(child.stdout + "\n" + child.stderr)

