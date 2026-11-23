"""Offline differential controls for physical ZIP framing and the RFB consumer.

These are synthetic inputs, not original Receita Federal archives. The semantic
member-name contract remains undecided in #477. No network or production ETL.
"""

from __future__ import annotations

import struct
import zipfile
import zlib
from pathlib import Path

import duckdb
import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget
from ficha_etl.streaming import create_table_from_zip_streaming, load_zips_parallel


def _require_equal(actual: object, expected: object) -> None:
    # Do not rely on assert: this test must also run under python -O.
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def _make_empty_zip(path: Path, compression: int) -> None:
    member = zipfile.ZipInfo("empty.CSV", date_time=(1980, 1, 1, 0, 0, 0))
    member.compress_type = compression
    member.create_system = 3
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, b"")


def _alter_empty_deflate(path: Path, replacement: bytes) -> None:
    """Replace exactly the compressed payload, correcting ZIP size/offset fields."""
    with zipfile.ZipFile(path) as archive:
        member = archive.infolist()[0]
        old_central_start = archive.start_dir

    raw = bytearray(path.read_bytes())
    local = struct.unpack_from("<IHHHHHIIIHH", raw, member.header_offset)
    if local[0] != 0x04034B50 or member.compress_type != zipfile.ZIP_DEFLATED:
        raise AssertionError("expected ordinary DEFLATE local header")
    data_start = member.header_offset + 30 + local[-2] + local[-1]
    raw[data_start : data_start + member.compress_size] = replacement
    new_central_start = old_central_start + len(replacement) - member.compress_size
    if raw[new_central_start : new_central_start + 4] != b"PK\x01\x02":
        raise AssertionError("central directory lost")

    # Local compressed_size; central compressed_size; EOCD central offset.
    struct.pack_into("<I", raw, member.header_offset + 18, len(replacement))
    struct.pack_into("<I", raw, new_central_start + 20, len(replacement))
    end_record = raw.rfind(b"PK\x05\x06")
    if end_record < 0:
        raise AssertionError("missing EOCD")
    struct.pack_into("<I", raw, end_record + 16, new_central_start)
    path.write_bytes(raw)


def _insert_unclaimed_stored_bytes(path: Path, extra: bytes) -> None:
    """Insert bytes before central directory without extending declared member."""
    with zipfile.ZipFile(path) as archive:
        central_start = archive.start_dir
    raw = bytearray(path.read_bytes())
    raw[central_start:central_start] = extra
    end_record = raw.rfind(b"PK\x05\x06")
    if end_record < 0:
        raise AssertionError("missing EOCD")
    struct.pack_into("<I", raw, end_record + 16, central_start + len(extra))
    path.write_bytes(raw)


def _fixture(path: Path, kind: str) -> None:
    method = zipfile.ZIP_STORED if kind.startswith("stored") else zipfile.ZIP_DEFLATED
    _make_empty_zip(path, method)
    if kind == "stored_gap":
        _insert_unclaimed_stored_bytes(path, b"JUNK")
    elif kind == "deflate_truncated":
        _alter_empty_deflate(path, b"\x03")
    elif kind == "deflate_trailing":
        _alter_empty_deflate(path, b"\x03\x00JUNK")
    elif kind not in {"stored_clean", "deflate_clean"}:
        raise ValueError(f"unknown fixture {kind}")
    with zipfile.ZipFile(path) as archive:
        info = archive.infolist()
        if len(info) != 1 or info[0].file_size != 0 or archive.testzip() is not None:
            raise AssertionError(f"unexpected ZIP fixture structure: {kind}")


@pytest.mark.parametrize("kind", ["stored_clean", "deflate_clean"])
def test_integral_empty_member_is_accepted_in_both_paths(tmp_path: Path, kind: str) -> None:
    source = tmp_path / "empty.zip"
    _fixture(source, kind)
    _require_equal(_raw_zip_crc_and_budget(source, expanded=0, members=0), (0, 1))
    con = duckdb.connect()
    try:
        create_table_from_zip_streaming(
            con, "source_rows", source, ("a", "b"), work_dir=tmp_path / "work"
        )
        _require_equal(con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0], 0)
    finally:
        con.close()


@pytest.mark.parametrize(
    ("kind", "preflight_message"),
    [
        ("stored_gap", "unclaimed ZIP payload bytes"),
        ("deflate_truncated", "truncated ZIP deflate stream"),
        ("deflate_trailing", "trailing ZIP deflate bytes"),
    ],
)
def test_invalid_empty_member_preflight_vs_consumer(
    tmp_path: Path, kind: str, preflight_message: str
) -> None:
    """The same fixture must be rejected on both paths, not silently emptied.

    The preflight assertion is always strict. Only the known consumer gap is
    marked XFAIL, *after* confirming its zero-row table was materialized.
    When the consumer is repaired, this test will pass without changing a mark.
    """
    source = tmp_path / "empty-malformed.zip"
    _fixture(source, kind)
    with pytest.raises(RuntimeError, match=preflight_message):
        _raw_zip_crc_and_budget(source, expanded=0, members=0)

    con = duckdb.connect()
    try:
        try:
            create_table_from_zip_streaming(
                con, "source_rows", source, ("a", "b"), work_dir=tmp_path / "work"
            )
        except (RuntimeError, zipfile.BadZipFile, zlib.error):
            _require_equal(
                con.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'source_rows'"
                ).fetchone()[0],
                0,
            )
        else:
            _require_equal(con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0], 0)
            pytest.xfail(f"#477: {kind} accepted as valid empty table by ZIP consumer")
    finally:
        con.close()


@pytest.mark.parametrize(
    ("name", "payload", "expected_rows"),
    [
        ("folder/", b"", 0),
        ("folder/", b"1;2\n", 0),
        ("notes.txt", b"", 0),
        ("notes.txt", b"1;2\n", 1),
    ],
)
@pytest.mark.parametrize("parallel", [False, True])
def test_member_semantics_characterization_pending_primary_rfb_contract(
    tmp_path: Path, name: str, payload: bytes, expected_rows: int, parallel: bool
) -> None:
    """Characterize both consumers, without deciding the #477 RFB policy."""
    source = tmp_path / "semantic.zip"
    member = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    member.compress_type = zipfile.ZIP_STORED
    member.create_system = 3
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member, payload)
    with zipfile.ZipFile(source) as archive:
        _require_equal(archive.testzip(), None)
        _require_equal(len(archive.infolist()), 1)
        _require_equal(archive.infolist()[0].is_dir(), name.endswith("/"))

    # Physical framing/CRC does not establish admissible RFB member semantics.
    physical = _raw_zip_crc_and_budget(source, expanded=0, members=0)
    _require_equal(physical, (len(payload), 1))
    con = duckdb.connect()
    try:
        work = tmp_path / "work"
        table = "source_rows"
        columns = ("a", "b")
        if parallel:
            specs = [(table, source, columns)]
            load_zips_parallel(con, specs, work_dir=work, decompress_workers=1)
        else:
            create_table_from_zip_streaming(con, table, source, columns, work_dir=work)
        count = con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
        _require_equal(count, expected_rows)
        if expected_rows:
            rows = con.execute("SELECT a, b FROM source_rows").fetchall()
            _require_equal(rows, [("1", "2")])
    finally:
        con.close()
