"""Regression: a zero-byte first CSV must not conceal a second ZIP member.

The raw preflight may accept an archive with two distinct CRC-valid members.
The consumer must fail closed rather than materialize an empty table.
All ZIPs in this file are synthetic. No RFB/IA or uploads.
"""

from __future__ import annotations

import zipfile

import duckdb
import pytest

from ficha_etl.streaming import create_table_from_zip_streaming, load_zips_parallel


def require_equal(actual: object, expected: object) -> None:
    # Check survives python -O.
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def make_zip(path, entries: list[tuple[str, bytes]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, data in entries:
            archive.writestr(name, data)
    with zipfile.ZipFile(path) as archive:
        require_equal(archive.testzip(), None)


@pytest.mark.parametrize(
    "entries",
    [
        [("empty.CSV", b""), ("data.CSV", b"1;2\n")],
        [("empty.CSV", b""), ("also-empty.CSV", b"")],
        [("data.CSV", b"1;2\n"), ("empty.CSV", b"")],
        [("dir/", b""), ("empty.CSV", b""), ("data.CSV", b"3;4\n")],
    ],
    ids=["empty-first", "both-empty", "nonempty-first", "directory-plus-two-files"],
)
def test_multi_member_zip_is_rejected_even_if_first_is_empty(tmp_path, entries):
    source = tmp_path / "source.zip"
    make_zip(source, entries)
    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="expected 1 CSV"):
            create_table_from_zip_streaming(
                con, "source_rows", source, ("a", "b"), work_dir=tmp_path / "work"
            )
        # No conversion into an apparently valid zero-row output.
        count = con.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='source_rows'"
        ).fetchone()[0]
        require_equal(count, 0)
    finally:
        con.close()


@pytest.mark.parametrize(
    "entries",
    [[("empty.CSV", b"")], [("dir/", b""), ("empty.CSV", b"")]],
    ids=["only-empty-csv", "directory-and-empty-csv"],
)
def test_single_empty_csv_still_loads_empty_table(tmp_path, entries):
    source = tmp_path / "single-empty.zip"
    make_zip(source, entries)
    con = duckdb.connect()
    try:
        create_table_from_zip_streaming(
            con, "source_rows", source, ("a", "b"), work_dir=tmp_path / "work"
        )
        require_equal(con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0], 0)
    finally:
        con.close()


def test_single_nonempty_csv_still_loads_data(tmp_path):
    source = tmp_path / "single.csv.zip"
    make_zip(source, [("data.CSV", b"123;ABC\n")])
    con = duckdb.connect()
    try:
        create_table_from_zip_streaming(
            con, "source_rows", source, ("a", "b"), work_dir=tmp_path / "work"
        )
        require_equal(con.execute("SELECT a, b FROM source_rows").fetchall(), [("123", "ABC")])
    finally:
        con.close()


def test_parallel_loader_rejects_empty_first_multi_member(tmp_path):
    """The parallel public API must enforce the same one-CSV contract."""
    source = tmp_path / "parallel.zip"
    make_zip(source, [("empty.CSV", b""), ("data.CSV", b"1;2\n")])
    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match=r"1 ZIP\\(s\\) failed to decompress"):
            load_zips_parallel(
                con,
                [("source_rows", source, ("a", "b"))],
                work_dir=tmp_path / "work",
                decompress_workers=1,
            )
        count = con.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='source_rows'"
        ).fetchone()[0]
        require_equal(count, 0)
    finally:
        con.close()
