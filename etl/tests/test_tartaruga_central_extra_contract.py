"""Offline ZIP central/local extra profile controls for Ficha #490/#433.

Contract oracles are separate: APPNOTE §4.5.1 defines complete TLVs;
Common ZIP §extraFields permits up to 3 trailing zero bytes and prohibits
repeated critical ZIP64 tag 0x0001. XFAIL(strict) records undecided behavior;
it does not authorize accepting a file from the RFB or an IA upload.
"""

from __future__ import annotations

import io
import struct
import zipfile
from pathlib import Path

import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force bit 3 data descriptor")


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
_UNKNOWN = struct.pack("<HH", 0x1234, 0)


def _make_zip(
    method: int,
    *,
    descriptor: bool,
    central_extra: bytes = b"",
    local_extra: bytes = b"",
    zip64_central: str | None = None,
) -> bytes:
    """Patch real ZIP records; retain payload, CRC, bit3 and EOCD offsets."""
    sink = _NonSeekable() if descriptor else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    central = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    assert central > 0 and eocd > central

    local_name, old_local_extra = struct.unpack_from("<HH", raw, 26)
    assert not old_local_extra
    if local_extra:
        raw[30 + local_name : 30 + local_name] = local_extra
        struct.pack_into("<H", raw, 28, len(local_extra))
        central += len(local_extra)
        eocd += len(local_extra)
        old_start = struct.unpack_from("<I", raw, eocd + 16)[0]
        struct.pack_into("<I", raw, eocd + 16, old_start + len(local_extra))

    central_name, old_central_extra, comment = struct.unpack_from("<HHH", raw, central + 28)
    assert not old_central_extra and not comment
    compressed_size, uncompressed_size = struct.unpack_from("<II", raw, central + 20)
    assert uncompressed_size == len(_PAYLOAD)
    if zip64_central is not None:
        first = struct.pack("<HHQQ", 1, 16, uncompressed_size, compressed_size)
        if zip64_central == "single":
            central_extra = first
        elif zip64_central == "duplicate_equal":
            central_extra = first + first
        elif zip64_central == "duplicate_conflict":
            central_extra = first + struct.pack(
                "<HHQQ", 1, 16, uncompressed_size + 1, compressed_size + 1
            )
        else:
            raise ValueError(zip64_central)
        struct.pack_into("<II", raw, central + 20, 0xFFFFFFFF, 0xFFFFFFFF)
        struct.pack_into("<H", raw, central + 6, 45)  # minimum ZIP64 reader version

    insertion = central + 46 + central_name
    raw[insertion:insertion] = central_extra
    struct.pack_into("<H", raw, central + 30, len(central_extra))
    eocd += len(central_extra)
    old_central_size = struct.unpack_from("<I", raw, eocd + 12)[0]
    struct.pack_into("<I", raw, eocd + 12, old_central_size + len(central_extra))
    return bytes(raw)


def _check_zipfile(raw: bytes, central_extra: bytes, *, descriptor: bool) -> None:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        assert len(archive.infolist()) == 1
        entry = archive.infolist()[0]
        assert entry.extra == central_extra
        assert bool(entry.flag_bits & 8) == descriptor
        assert archive.read(entry) == _PAYLOAD
        assert archive.testzip() is None


def _preflight(tmp_path: Path, raw: bytes) -> tuple[int, int]:
    path = tmp_path / "extra-contract.zip"
    path.write_bytes(raw)
    return _raw_zip_crc_and_budget(path, expanded=0, members=0)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
def test_complete_unknown_tlv_and_zero_tail_control(tmp_path: Path, method: int, descriptor: bool):
    raw = _make_zip(method, descriptor=descriptor, central_extra=_UNKNOWN)
    _check_zipfile(raw, _UNKNOWN, descriptor=descriptor)
    assert _preflight(tmp_path, raw) == (len(_PAYLOAD), 1)


@pytest.mark.xfail(
    strict=True, reason="Common ZIP terminal zero padding not admitted by current central TLV guard"
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
@pytest.mark.parametrize("count", [1, 2, 3], ids=["pad1", "pad2", "pad3"])
def test_common_zip_central_zero_padding(
    tmp_path: Path, method: int, descriptor: bool, count: int
):
    extra = _UNKNOWN + bytes(count)
    raw = _make_zip(method, descriptor=descriptor, central_extra=extra)
    _check_zipfile(raw, extra, descriptor=descriptor)
    assert _preflight(tmp_path, raw) == (len(_PAYLOAD), 1)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
def test_nonzero_central_tail_is_rejected(tmp_path: Path, method: int, descriptor: bool):
    extra = _UNKNOWN + b"Q"
    raw = _make_zip(method, descriptor=descriptor, central_extra=extra)
    _check_zipfile(raw, extra, descriptor=descriptor)
    with pytest.raises(RuntimeError, match="malformed ZIP central extra"):
        _preflight(tmp_path, raw)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
def test_single_zip64_central_with_sentinel(tmp_path: Path, method: int, descriptor: bool):
    raw = _make_zip(method, descriptor=descriptor, zip64_central="single")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        extra = archive.infolist()[0].extra
    _check_zipfile(raw, extra, descriptor=descriptor)
    assert _preflight(tmp_path, raw) == (len(_PAYLOAD), 1)


@pytest.mark.xfail(
    strict=True, reason="Common ZIP prohibits duplicate critical ZIP64 central tag 0x0001"
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
@pytest.mark.parametrize("variant", ["duplicate_equal", "duplicate_conflict"])
def test_common_zip_duplicate_zip64_central_is_rejected(
    tmp_path: Path, method: int, descriptor: bool, variant: str
):
    raw = _make_zip(method, descriptor=descriptor, zip64_central=variant)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        extra = archive.infolist()[0].extra
    _check_zipfile(raw, extra, descriptor=descriptor)
    with pytest.raises(RuntimeError, match="duplicate ZIP64 central extra"):
        _preflight(tmp_path, raw)


@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
def test_duplicate_unknown_tags_are_allowed(tmp_path: Path, method: int, descriptor: bool):
    extra = _UNKNOWN * 2
    raw = _make_zip(method, descriptor=descriptor, central_extra=extra)
    _check_zipfile(raw, extra, descriptor=descriptor)
    assert _preflight(tmp_path, raw) == (len(_PAYLOAD), 1)


@pytest.mark.xfail(
    strict=True, reason="Common ZIP zero padding rejected by existing local guard on main"
)
@pytest.mark.parametrize("method", _METHODS, ids=["stored", "deflated"])
@pytest.mark.parametrize("descriptor", [False, True], ids=["no_bit3", "bit3"])
@pytest.mark.parametrize("placement", ["local", "both"])
def test_common_zip_zero_padding_local_boundary(
    tmp_path: Path, method: int, descriptor: bool, placement: str
):
    extra = _UNKNOWN + b"\x00"
    raw = _make_zip(
        method,
        descriptor=descriptor,
        local_extra=extra,
        central_extra=extra if placement == "both" else b"",
    )
    _check_zipfile(raw, extra if placement == "both" else b"", descriptor=descriptor)
    assert _preflight(tmp_path, raw) == (len(_PAYLOAD), 1)
