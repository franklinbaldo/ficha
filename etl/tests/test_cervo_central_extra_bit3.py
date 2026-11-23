"""Synthetic ZIP central-directory extra-field framing controls (#433).

Distinct from local-header ZIP64/TLV tests in #487/#488. No remote data or egress.
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
        raise OSError("force descriptor bit 3")


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_VALID = (b"", b"\x34\x12\x01\x00x")
_INVALID = (b"Q", b"\x34\x12\x01")  # orphan byte; partial TLV header


def _make_zip(method: int, central_extra: bytes) -> bytes:
    sink = _NonSeekable()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    central = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    if central < 0 or eocd <= central:
        raise AssertionError("ZIP fixture missing central directory/EOCD")
    name_len, old_extra_len, comment_len = struct.unpack_from("<HHH", raw, central + 28)
    if old_extra_len or comment_len:
        raise AssertionError("ZIP fixture unexpectedly contains central extras or comment")
    insertion = central + 46 + name_len
    raw[insertion:insertion] = central_extra
    struct.pack_into("<H", raw, central + 30, len(central_extra))
    eocd += len(central_extra)
    central_size = struct.unpack_from("<I", raw, eocd + 12)[0]
    struct.pack_into("<I", raw, eocd + 12, central_size + len(central_extra))
    return bytes(raw)


def _well_framed_tlvs(extra: bytes) -> bool:
    cursor = 0
    while cursor < len(extra):
        if len(extra) - cursor < 4:
            return False
        _kind, width = struct.unpack_from("<HH", extra, cursor)
        cursor += 4
        if width > len(extra) - cursor:
            return False
        cursor += width
    return True


def _assert_stdlib_accepts(raw: bytes, central_extra: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        if len(entries) != 1:
            raise AssertionError("expected exactly one ZIP member")
        entry = entries[0]
        if not entry.flag_bits & 8 or entry.extra != central_extra:
            raise AssertionError("central-extra fixture not preserved with bit 3")
        if archive.testzip() is not None or archive.read(entry) != _PAYLOAD:
            raise AssertionError("ZIP payload/CRC changed")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("extra", _VALID)
def test_well_framed_central_extra_is_accepted(tmp_path: Path, method: int, extra: bytes):
    if not _well_framed_tlvs(extra):
        raise AssertionError("positive control is malformed")
    raw = _make_zip(method, extra)
    _assert_stdlib_accepts(raw, extra)
    path = tmp_path / "central-valid.zip"
    path.write_bytes(raw)
    if _raw_zip_crc_and_budget(path, expanded=0, members=0) != (len(_PAYLOAD), 1):
        raise AssertionError("central-extra positive control incorrectly rejected")


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("extra", _INVALID)
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="Known gap: central-directory extra TLV framing unchecked (#433)",
)
def test_malformed_central_extra_must_be_rejected(tmp_path: Path, method: int, extra: bytes):
    if _well_framed_tlvs(extra):
        raise AssertionError("negative control is structurally valid")
    raw = _make_zip(method, extra)
    _assert_stdlib_accepts(raw, extra)
    path = tmp_path / "central-malformed.zip"
    path.write_bytes(raw)
    try:
        _raw_zip_crc_and_budget(path, expanded=0, members=0)
    except RuntimeError as exc:
        if "central" not in str(exc).lower() or "extra" not in str(exc).lower():
            raise AssertionError("rejected for an unrelated reason") from exc
        return
    pytest.fail("malformed central extra accepted by raw preflight")


def test_optimized_python_replays_central_extra_matrix():
    selectors = [
        f"{__file__}::test_well_framed_central_extra_is_accepted",
        f"{__file__}::test_malformed_central_extra_must_be_rejected",
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
