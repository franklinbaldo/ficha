"""Synthetic 0x7075 ZIP Unicode Path cross-reader identity regression (#477).

The central raw name can differ from the local name. On Python runtimes that
interpret a CRC-valid Unicode Path alias, ZipInfo.filename may instead match
the local name and hide that difference from preflight. Older runtimes ignore
the alias and correctly reject the mismatch. No claim about RFB original ZIPs.
"""

from __future__ import annotations

import io
import struct
import subprocess
import sys
import zipfile
import zlib
from pathlib import Path

import duckdb
import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget
from ficha_etl.streaming import create_table_from_zip_streaming, load_zips_parallel


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
    central_name_len, central_extra_len, comment_len = struct.unpack_from("<HHH", raw, central + 28)
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


def _runtime_selects_unicode_path_alias() -> bool:
    """Detect zipfile 0x7075 behavior without assuming a Python version."""
    alias = "Z" * len(_NAME)
    fixture = _make_zip(zipfile.ZIP_STORED, False, alias, change_local=False)
    with zipfile.ZipFile(io.BytesIO(fixture)) as archive:
        info = archive.infolist()[0]
        if info.orig_filename != _NAME or info.filename not in (_NAME, alias):
            raise AssertionError("unexpected stdlib ZIP Unicode Path interpretation")
        return info.filename == alias


_UNICODE_PATH_ALIAS_SELECTED = _runtime_selects_unicode_path_alias()


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


def _unicode_field(name: str, alias: str | None, *, active: bool = True) -> bytes:
    if alias is None:
        return b""
    crc = zlib.crc32(name.encode("ascii")) ^ (0 if active else 1)
    value = b"\x01" + struct.pack("<I", crc) + alias.encode("utf-8")
    return struct.pack("<HH", 0x7075, len(value)) + value


def _paired_zip(
    method: int, bit3: bool, central_zip64: bool, local: bytes, central: bytes
) -> bytes:
    """Keep physical names equal, independently set each boundary's extras."""
    sink = _NonSeekable() if bit3 else io.BytesIO()
    member = zipfile.ZipInfo(_NAME, date_time=(1980, 1, 1, 0, 0, 0))
    member.compress_type = method
    member.extra = local
    with zipfile.ZipFile(sink, "w") as archive:
        archive.writestr(member, _PAYLOAD)
    raw = bytearray(sink.getvalue())
    start = raw.find(b"PK\x01\x02")
    eocd = raw.rfind(b"PK\x05\x06")
    name_len, extra_len, comment_len = struct.unpack_from("<HHH", raw, start + 28)
    if extra_len != len(local) or comment_len or name_len != len(_NAME):
        raise AssertionError("unexpected central layout")
    if central_zip64:
        compressed, uncompressed = struct.unpack_from("<II", raw, start + 20)
        central = struct.pack("<HHQQ", 1, 16, uncompressed, compressed) + central
        struct.pack_into("<II", raw, start + 20, 0xFFFFFFFF, 0xFFFFFFFF)
        struct.pack_into("<H", raw, start + 6, 45)
    offset = start + 46 + name_len
    raw[offset : offset + extra_len] = central
    struct.pack_into("<H", raw, start + 30, len(central))
    delta = len(central) - extra_len
    eocd += delta
    size = struct.unpack_from("<I", raw, eocd + 12)[0]
    struct.pack_into("<I", raw, eocd + 12, size + delta)
    return bytes(raw)


_B = "renamed.CSV"
_C = "other.CSV"
_D = "renamed/"


@pytest.mark.parametrize(
    ("local_alias", "central_alias", "local_active", "central_active"),
    [
        (_NAME, _NAME, True, True),
        (_B, _B, True, True),
        (_B, None, True, True),
        (None, _B, True, True),
        (_C, _B, True, True),
        (_B, _B, True, False),
        (_B, _B, False, True),
        (_D, _D, True, True),
    ],
    ids=[
        "same",
        "both-b",
        "local-b",
        "central-b",
        "different",
        "inactive-central",
        "inactive-local",
        "class-flip",
    ],
)
@pytest.mark.parametrize(
    "method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED], ids=["stored", "deflated"]
)
@pytest.mark.parametrize("bit3", [False, True], ids=["no-descriptor", "descriptor"])
@pytest.mark.parametrize("central_zip64", [False, True], ids=["classic", "central-zip64"])
def test_paired_alias_cross_reader_characterization(
    tmp_path: Path,
    local_alias: str | None,
    central_alias: str | None,
    local_active: bool,
    central_active: bool,
    method: int,
    bit3: bool,
    central_zip64: bool,
) -> None:
    """Snapshot current outcomes, not a decision to admit/reject aliases."""
    source = tmp_path / "paired.zip"
    source.write_bytes(
        _paired_zip(
            method,
            bit3,
            central_zip64,
            _unicode_field(_NAME, local_alias, active=local_active),
            _unicode_field(_NAME, central_alias, active=central_active),
        )
    )
    effective = (
        central_alias
        if _UNICODE_PATH_ALIAS_SELECTED and central_active and central_alias is not None
        else _NAME
    )
    with zipfile.ZipFile(source) as archive:
        info = archive.infolist()[0]
        if (info.orig_filename, info.filename, archive.read(info)) != (_NAME, effective, _PAYLOAD):
            raise AssertionError("unexpected fixture interpretation")
        if info.is_dir() != effective.endswith("/"):
            raise AssertionError("unexpected member class")

    # Current preflight compares the physical local name with the central alias.
    # Revise these observations when the shared contract is decided.
    if effective != _NAME:
        with pytest.raises(RuntimeError, match="ZIP member name mismatch"):
            _raw_zip_crc_and_budget(source, expanded=0, members=0)
    elif _raw_zip_crc_and_budget(source, expanded=0, members=0) != (len(_PAYLOAD), 1):
        raise AssertionError("unexpected preflight counters")

    expected_rows = [] if effective.endswith("/") else [("1", "2")]
    for parallel in (False, True):
        con = duckdb.connect()
        try:
            work = tmp_path / ("parallel" if parallel else "streaming")
            if parallel:
                load_zips_parallel(
                    con, [("source_rows", source, ("a", "b"))], work_dir=work, decompress_workers=1
                )
            else:
                create_table_from_zip_streaming(
                    con, "source_rows", source, ("a", "b"), work_dir=work
                )
            rows = con.execute("SELECT a, b FROM source_rows").fetchall()
            if rows != expected_rows:
                raise AssertionError(f"unexpected rows (parallel={parallel}): {rows!r}")
        finally:
            con.close()


@pytest.mark.parametrize(
    "method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED], ids=["stored", "deflated"]
)
@pytest.mark.parametrize("bit3", [False, True], ids=["no-descriptor", "descriptor"])
@pytest.mark.parametrize("alias_kind", ["benign", "traversal"])
@pytest.mark.xfail(
    _UNICODE_PATH_ALIAS_SELECTED,
    strict=True,
    raises=pytest.fail.Exception,
    reason="#477: raw central/local filename disagreement obscured by 0x7075 alias",
)
def test_alias_cannot_hide_raw_central_local_name_disagreement(
    tmp_path: Path, method: int, bit3: bool, alias_kind: str
) -> None:
    """Raw preflight must reject the archive zipfile.open cannot read."""
    if alias_kind == "benign":
        alias = "Z" * len(_NAME)
    else:
        alias = "../" + "X" * (len(_NAME) - 3)
    path = tmp_path / "split-name.zip"
    path.write_bytes(_make_zip(method, bit3, alias, change_local=True))
    with zipfile.ZipFile(path) as archive:
        info = archive.infolist()[0]
        expected = alias if _UNICODE_PATH_ALIAS_SELECTED else _NAME
        if info.filename != expected or info.orig_filename != _NAME:
            raise AssertionError("unexpected Unicode Path runtime behavior")
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
    base = f"{__file__}::"
    child = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            base + "test_matching_central_and_local_name_with_unicode_path_is_valid",
            base + "test_paired_alias_cross_reader_characterization",
            base + "test_alias_cannot_hide_raw_central_local_name_disagreement",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)
