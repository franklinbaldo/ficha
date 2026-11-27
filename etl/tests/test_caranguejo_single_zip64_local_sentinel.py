"""Research #493: mixed ZIP64 sentinels in a bit-3 local header.

Replays Lagarto R200 against the actual raw preflight. ZIPs are entirely
synthetic; a successful stdlib read is not an APPNOTE conformance oracle.
Under bit 3, profiles A (0/0, no ZIP64) and B (paired ZIP64 from stdlib)
are separate interoperability baselines. C (single sentinel, 8-byte ZIP64),
D (single sentinel, 16-byte ZIP64) and E (0/0, 16-byte ZIP64) are
under investigation under APPNOTE 6.3.10; no layout is certified by
zipfile.read(). Admission policy remains pending in #493.

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
            raises=pytest.fail.Exception,
            reason="#493: normative local ZIP64 pair policy pending",
        ),
    ),
    pytest.param(
        "framed_prefixed",
        marks=pytest.mark.xfail(
            strict=True,
            raises=pytest.fail.Exception,
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


def _source_zip(method: int, *, force_zip64: bool = True) -> bytes:
    sink = _NonSeekable()
    info = zipfile.ZipInfo(_NAME, (2020, 1, 1, 0, 0, 0))
    info.compress_type = method
    with zipfile.ZipFile(sink, "w", allowZip64=True) as archive:
        with archive.open(info, "w", force_zip64=force_zip64) as member:
            member.write(_PAYLOAD)
    raw = sink.getvalue()
    _require(struct.unpack_from("<I", raw)[0] == 0x04034B50, "ZIP local signature")
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "descriptor bit 3")
    sizes = struct.unpack_from("<II", raw, 18)
    name_len, extra_len = struct.unpack_from("<HH", raw, 26)
    local_extra = raw[30 + name_len : 30 + name_len + extra_len]
    if force_zip64:
        _require(sizes == (0xFFFFFFFF, 0xFFFFFFFF), "stdlib paired ZIP64 sizes")
        _require(local_extra == b"\x01\x00\x10\x00" + bytes(16), "paired ZIP64 extra")
    else:
        _require(sizes == (0, 0), "stdlib zero local sizes")
        _require(extra_len == 0, "zero local sizes without ZIP64 extra")
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
def test_stdlib_paired_local_zip64_sizes_accepted(tmp_path: Path, method: int) -> None:
    source = tmp_path / "paired.zip"
    source.write_bytes(_source_zip(method))
    result = _raw_zip_crc_and_budget(source, expanded=0, members=0)
    _require(result == (len(_PAYLOAD), 1), "stdlib paired ZIP64 refused")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
def test_zero_local_sizes_without_zip64_extra_accepted(tmp_path: Path, method: int) -> None:
    source = tmp_path / "zero.zip"
    raw = _source_zip(method, force_zip64=False)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        _require(archive.testzip() is None, "zero baseline CRC")
        _require(archive.read(_NAME) == _PAYLOAD, "zero baseline payload")
    source.write_bytes(raw)
    result = _raw_zip_crc_and_budget(source, expanded=0, members=0)
    _require(result == (len(_PAYLOAD), 1), "zero baseline refused")


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
    if variant.startswith("framed"):
        # Typed XFAIL must arise exclusively from *preflight accepting C*.
        # Fixture failures, wrong rejection causes and CRC faults are FAIL.
        try:
            _raw_zip_crc_and_budget(source, expanded=0, members=0)
        except RuntimeError as exc:
            _require("ZIP64 local" in str(exc), "wrong ZIP64 rejection reason")
        else:
            pytest.fail("#493: single-sentinel ZIP64 local accepted")
    else:
        with pytest.raises(RuntimeError, match="ZIP64 local"):
            _raw_zip_crc_and_budget(source, expanded=0, members=0)


def _presence_variant(raw: bytes, case: str) -> bytes:
    forged = bytearray(raw)
    _require(
        struct.unpack_from("<II", forged, 18) == (0xFFFFFFFF, 0xFFFFFFFF),
        "paired baseline expected",
    )
    if case == "single_compressed_full_extra":
        struct.pack_into("<I", forged, 18, 0)
    elif case == "single_uncompressed_full_extra":
        struct.pack_into("<I", forged, 22, 0)
    elif case in ("zero_with_zip64_extra", "zero_with_zip64_nonzero_extra"):
        struct.pack_into("<II", forged, 18, 0, 0)
        if case == "zero_with_zip64_nonzero_extra":
            name_len, extra_len = struct.unpack_from("<HH", forged, 26)
            _require(extra_len == 20, "expected paired local ZIP64 extra")
            struct.pack_into("<QQ", forged, 30 + name_len + 4, 42, 24)
    else:
        raise AssertionError(case)
    return bytes(forged)


def _verify_presence_fixture(raw: bytes, case: str) -> None:
    expected = {
        "single_compressed_full_extra": (0, 0xFFFFFFFF),
        "single_uncompressed_full_extra": (0xFFFFFFFF, 0),
        "zero_with_zip64_extra": (0, 0),
        "zero_with_zip64_nonzero_extra": (0, 0),
    }
    _require(bool(struct.unpack_from("<H", raw, 6)[0] & 8), "presence bit 3")
    _require(
        struct.unpack_from("<II", raw, 18) == expected[case],
        "presence local sizes",
    )
    name_len, extra_len = struct.unpack_from("<HH", raw, 26)
    _require(extra_len == 20, "presence ZIP64 extra width")
    local_extra = raw[30 + name_len : 30 + name_len + extra_len]
    expected_extra = (
        struct.pack("<HHQQ", 1, 16, 42, 24)
        if case == "zero_with_zip64_nonzero_extra"
        else b"\x01\x00\x10\x00" + bytes(16)
    )
    _require(local_extra == expected_extra, "presence ZIP64 extra values")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        _require(len(archive.infolist()) == 1, "presence member count")
        if case == "zero_with_zip64_nonzero_extra":
            entry = archive.infolist()[0]
            _require(
                entry.compress_size != 42 and entry.file_size != 24,
                "unused ZIP64 values must contradict central directory sizes",
            )
        _require(archive.testzip() is None, "presence CRC")
        _require(archive.read(_NAME) == _PAYLOAD, "presence payload")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize(
    "case",
    (
        "single_compressed_full_extra",
        "single_uncompressed_full_extra",
        "zero_with_zip64_extra",
        "zero_with_zip64_nonzero_extra",
    ),
)
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="#493: ZIP64 local presence policy for D/E pending",
)
def test_zip64_extra_presence_noncanonical_rejected(tmp_path: Path, method: int, case: str) -> None:
    raw = _presence_variant(_source_zip(method), case)
    _verify_presence_fixture(raw, case)
    source = tmp_path / "presence.zip"
    source.write_bytes(raw)
    try:
        _raw_zip_crc_and_budget(source, expanded=0, members=0)
    except RuntimeError as exc:
        _require("ZIP64 local" in str(exc), "wrong ZIP64 presence rejection reason")
    else:
        pytest.fail("#493: ZIP64 local extra presence accepted")


def test_python_optimized_replays_single_sentinel_matrix() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_stdlib_paired_local_zip64_sizes_accepted",
            f"{__file__}::test_zero_local_sizes_without_zip64_extra_accepted",
            f"{__file__}::test_zip64_extra_presence_noncanonical_rejected",
            f"{__file__}::test_single_local_zip64_sentinel_rejected_by_preflight",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode != 0:
        raise AssertionError(child.stdout + "\n" + child.stderr)
