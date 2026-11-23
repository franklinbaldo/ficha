"""Offline regression matrix: ZIP local extra fields with descriptor bit 3.

Synthetic members only. ZIP CRC validity is not proof of extra-field framing.
Malformed local ZIP64 metadata must be refused before any raw upload.
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
        raise OSError("nonseekable test writer")


_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_MUTATIONS = (
    "relabel_zip64",
    "overrun_tlv_width",
    "orphan_tlv_byte",
    "duplicate_zip64",
    "missing_zip64",
    "short_zip64_value",
)
_PAYLOAD = b"a;b\n"
_NAME = "K3241.K03200Y2.D60808.EMPRECSV"


def _fixture(method: int) -> bytes:
    sink = _NonSeekable()
    info = zipfile.ZipInfo(_NAME, (2020, 1, 1, 0, 0, 0))
    info.compress_type = method
    with zipfile.ZipFile(sink, mode="w", allowZip64=True) as archive:
        with archive.open(info, mode="w", force_zip64=True) as member:
            member.write(_PAYLOAD)
    raw = sink.getvalue()
    _, flags, local_comp, local_uncomp, name_len, extra_len = _local_fields(raw)
    local_extra = raw[30 + name_len : 30 + name_len + extra_len]
    if (
        not flags & 8
        or (local_comp, local_uncomp) != (0xFFFFFFFF, 0xFFFFFFFF)
        or extra_len != 20
        or local_extra != b"\x01\x00\x10\x00" + bytes(16)
    ):
        raise AssertionError("ZIP64 descriptor fixture preconditions failed")
    return raw


def _local_fields(raw: bytes) -> tuple[int, int, int, int, int, int]:
    sig, method, flags, comp, uncomp, name_len, extra_len = (
        struct.unpack_from("<I", raw, 0)[0],
        struct.unpack_from("<H", raw, 8)[0],
        struct.unpack_from("<H", raw, 6)[0],
        struct.unpack_from("<I", raw, 18)[0],
        struct.unpack_from("<I", raw, 22)[0],
        struct.unpack_from("<H", raw, 26)[0],
        struct.unpack_from("<H", raw, 28)[0],
    )
    if sig != 0x04034B50:
        raise AssertionError("missing local ZIP header")
    return method, flags, comp, uncomp, name_len, extra_len


def _mutant(raw: bytes, mutation: str) -> bytes:
    _, _, _, _, name_len, extra_len = _local_fields(raw)
    start = 30 + name_len
    old = raw[start : start + extra_len]
    if mutation == "relabel_zip64":
        new = b"\x99\x99" + old[2:]
    elif mutation == "overrun_tlv_width":
        new = old[:2] + b"\xff\xff" + old[4:]
    elif mutation == "orphan_tlv_byte":
        new = old + b"Q"
    elif mutation == "duplicate_zip64":
        new = old + old
    elif mutation == "missing_zip64":
        new = b""
    elif mutation == "short_zip64_value":
        # Both local size fields use ZIP64 sentinels. A complete TLV with
        # only one 64-bit value is still too short for two required sizes.
        # The second 8 bytes form an unrelated, well-framed extra field.
        new = struct.pack("<HH8sHH4s", 1, 8, bytes(8), 0x9999, 4, bytes(4))
    else:
        raise AssertionError(f"unknown mutation {mutation}")
    forged = bytearray(raw[:start] + new + raw[start + extra_len :])
    struct.pack_into("<H", forged, 28, len(new))
    end = forged.rfind(b"PK\x05\x06")
    if end < 0:
        raise AssertionError("missing ZIP EOCD")
    offset = struct.unpack_from("<I", forged, end + 16)[0]
    struct.pack_into("<I", forged, end + 16, offset + len(new) - extra_len)
    return bytes(forged)


def _local_tlv_oracle(raw: bytes) -> bool:
    _, _, comp, uncomp, name_len, extra_len = _local_fields(raw)
    local_extra = raw[30 + name_len : 30 + name_len + extra_len]
    cursor = 0
    zip64_count = 0
    while cursor < len(local_extra):
        if len(local_extra) - cursor < 4:
            return False
        tag, width = struct.unpack_from("<HH", local_extra, cursor)
        cursor += 4
        if width > len(local_extra) - cursor:
            return False
        if tag == 1:
            zip64_count += 1
            if width < 8 * (int(comp == 0xFFFFFFFF) + int(uncomp == 0xFFFFFFFF)):
                return False
        cursor += width
    return zip64_count == 1


def _verify_fixture(raw: bytes, *, malformed: bool) -> None:
    if _local_tlv_oracle(raw) == malformed:
        raise AssertionError("structural oracle misclassified fixture")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entry = archive.infolist()[0]
        if (
            not entry.flag_bits & 8
            or archive.testzip() is not None
            or archive.read(entry) != _PAYLOAD
        ):
            raise AssertionError("invalid stdlib control")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
def test_valid_local_zip64_extra_with_bit3(tmp_path: Path, method: int):
    raw = _fixture(method)
    _verify_fixture(raw, malformed=False)
    source = tmp_path / "valid.zip"
    source.write_bytes(raw)
    if _raw_zip_crc_and_budget(source, expanded=0, members=0) != (len(_PAYLOAD), 1):
        raise AssertionError("valid local ZIP64 field rejected")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("mutation", _MUTATIONS)
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="Known gap: bit 3 bypasses local ZIP64/TLV structure validation (#433)",
)
def test_malformed_local_zip64_extra_must_be_rejected(tmp_path: Path, method: int, mutation: str):
    raw = _mutant(_fixture(method), mutation)
    _verify_fixture(raw, malformed=True)
    source = tmp_path / "malformed.zip"
    source.write_bytes(raw)
    with pytest.raises(RuntimeError, match="ZIP local extra|ZIP64 local"):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)


def test_optimized_python_replays_local_extra_matrix():
    selectors = [
        f"{__file__}::test_valid_local_zip64_extra_with_bit3",
        f"{__file__}::test_malformed_local_zip64_extra_must_be_rejected",
    ]
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
