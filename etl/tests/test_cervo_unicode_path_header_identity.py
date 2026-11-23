"""Synthetic 0x7075 ZIP Unicode Path cross-reader identity regression (#477).

The central raw name can differ from a local raw name while a CRC-valid
Unicode Path alias makes ZipInfo.filename match the local name. The ZIP
consumer's zipfile.open still checks the original central name, so a raw
preflight accepting the same bytes is a differential error. This test makes no
claim about RFB original archives or their naming convention.
"""

from __future__ import annotations

import io
import struct
import subprocess
import sys
import zipfile
import zlib
from pathlib import Path

import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget


_NAME = "K3241.K03200Y2.D60808.EMPRECSV"
_PAYLOAD = b"1;2\n"


class _NonSeekable(io.BytesIO):
    def seek(self, *_args, **_kwargs):
        raise OSError("force bit-3 ZIP descriptor")


def _make_zip(method: int, bit3: bool, alias: str, *, change_local: bool) -> bytes:
    """Central raw name remains A, Unicode Path advertises B; local is A/B."""
    original = _NAME.encode("ascii")
    alternative = alias.encode("ascii")
    if len(original) != len(alternative):
        raise ValueError("alias must preserve name width for precise local mutation")
    sink = _NonSeekable() if bit3 else io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=method) as archive:
        archive.writestr(_NAME, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    name_len, extra_len = struct.unpack_from("<HH", raw, 26)
    if name_len != len(original) or extra_len:
        raise RuntimeError("unexpected local filename/extra layout")
    if change_local:
        raw[30 : 30 + len(original)] = alternative
    central = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    if central <= 0 or eocd <= central:
        raise RuntimeError("cannot locate ZIP directory")
    central_name_len, central_extra_len, comment_len = struct.unpack_from(
        "<HHH", raw, central + 28
    )
    if central_name_len != len(original) or central_extra_len or comment_len:
        raise RuntimeError("unexpected central filename/extra layout")
    value = b"\x01" + struct.pack("<I", zlib.crc32(original)) + alternative
    field = struct.pack("<HH", 0x7075, len(value)) + value
    insertion = central + 46 + len(original)
    raw[insertion:insertion] = field
    struct.pack_into("<H", raw, central + 30, len(field))
    eocd += len(field)
    central_size = struct.unpack_from("<I", raw, eocd + 12)[0]
    struct.pack_into("<I", raw, eocd + 12, central_size + len(field))
    return bytes(raw)


@pytest.mark.parametrize(
    "method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED], ids=["stored", "deflated"]
)
@pytest.mark.parametrize("bit3", [False, True], ids=["no-descriptor", "descriptor"])
def test_matching_central_and_local_name_with_unicode_path_is_valid(
    tmp_path: Path, method: int, bit3: bool
) -> None:
    """CRC-correct 0x7075 is not itself a reason to reject a ZIP."""
    path = tmp_path / "matching.zip"
    path.write_bytes(_make_zip(method, bit3, _NAME, change_local=False))
    with zipfile.ZipFile(path) as archive:
        info = archive.infolist()[0]
        if info.filename != _NAME or archive.read(info) != _PAYLOAD:
            raise AssertionError("invalid positive Unicode Path fixture")
    result = _raw_zip_crc_and_budget(path, expanded=0, members=0)
    if result != (len(_PAYLOAD), 1):
        raise AssertionError(f"positive Unicode Path rejected: {result!r}")


@pytest.mark.parametrize(
    "method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED], ids=["stored", "deflated"]
)
@pytest.mark.parametrize("bit3", [False, True], ids=["no-descriptor", "descriptor"])
@pytest.mark.parametrize("alias_kind", ["benign", "traversal"])
@pytest.mark.xfail(
    strict=True,
    raises=pytest.fail.Exception,
    reason="#477: raw central/local filename disagreement obscured by 0x7075 alias",
)
def test_alias_cannot_hide_raw_central_local_name_disagreement(
    tmp_path: Path, method: int, bit3: bool, alias_kind: str
) -> None:
    """Raw preflight must reject the archive zipfile.open cannot read."""
    alias = "Z" * len(_NAME) if alias_kind == "benign" else "../" + "X" * (len(_NAME) - 3)
    path = tmp_path / "split-name.zip"
    path.write_bytes(_make_zip(method, bit3, alias, change_local=True))
    with zipfile.ZipFile(path) as archive:
        info = archive.infolist()[0]
        if info.filename != alias or info.orig_filename != _NAME:
            raise AssertionError("Unicode Path alias fixture was not selected")
        try:
            archive.read(info)
        except zipfile.BadZipFile as exc:
            if "differ" not in str(exc):
                raise AssertionError("stdlib rejected for a different reason") from exc
        else:
            raise AssertionError("stdlib unexpectedly read discordant raw names")
    try:
        _raw_zip_crc_and_budget(path, expanded=0, members=0)
    except RuntimeError as exc:
        if "name mismatch" not in str(exc) and "name disagreement" not in str(exc):
            raise AssertionError("preflight rejected for an unrelated reason") from exc
        return
    pytest.fail("preflight accepted raw local/central filename disagreement hidden by 0x7075")


def test_optimized_python_replays_unicode_path_identity() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{__file__}::test_matching_central_and_local_name_with_unicode_path_is_valid",
            f"{__file__}::test_alias_cannot_hide_raw_central_local_name_disagreement",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
