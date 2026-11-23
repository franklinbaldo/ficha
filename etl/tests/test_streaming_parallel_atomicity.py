"""Offline characterization of DuckDB batch atomicity after late ZIP CRC failure.

The expected whole-batch atomicity regression is strict-XFAIL; the caller-owned
transaction is a positive control. No production ZIP, network, or IA operations.
"""

from __future__ import annotations

import struct
import threading
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import streaming


def _zip(path: Path, payload: bytes) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("rows.csv", payload)
    return path


def _break_member_crc(path: Path) -> None:
    """Change the last STORED payload byte without updating central CRC."""
    with zipfile.ZipFile(path) as archive:
        member = archive.infolist()[0]
    with path.open("r+b") as handle:
        handle.seek(member.header_offset + 26)
        name_len, extra_len = struct.unpack("<HH", handle.read(4))
        offset = member.header_offset + 30 + name_len + extra_len
        handle.seek(offset + member.compress_size - 1)
        old_byte = handle.read(1)
        handle.seek(-1, 1)
        handle.write(bytes([old_byte[0] ^ 1]))


@pytest.mark.parametrize(
    "outer_transaction",
    [
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                strict=True,
                reason="batch has no all-or-nothing transaction: earlier table persists",
            ),
            id="default-batch-strict-xfail",
        ),
        pytest.param(True, id="explicit-caller-rollback"),
    ],
)
def test_late_crc_failure_preserves_pre_batch_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outer_transaction: bool,
) -> None:
    """A CRC failure after a successful load must not commit a partial batch.

    A caller-managed DuckDB transaction is the positive rollback control.
    The default API currently violates this desired guarantee (strict XFAIL).
    """
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    good = _zip(tmp_path / "good.zip", b"NEW;VALUE\n" * 9)
    bad = _zip(tmp_path / "bad.zip", b"BROKEN;ROW\n" * 150000)
    _break_member_crc(bad)
    good_loaded = threading.Event()
    original_decompress = streaming._decompress_zip
    original_load = streaming._load_from_result

    def ordered_decompress(zip_path, table, columns, *, tmp_dir):
        if zip_path == bad and not good_loaded.wait(15):
            raise AssertionError("good table was not loaded before bad ZIP")
        return original_decompress(zip_path, table, columns, tmp_dir=tmp_dir)

    def signal_after_load(con, result):
        original_load(con, result)
        if result.table == "good":
            good_loaded.set()

    monkeypatch.setattr(streaming, "_decompress_zip", ordered_decompress)
    monkeypatch.setattr(streaming, "_load_from_result", signal_after_load)

    db_path = tmp_path / "batch.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE good (a VARCHAR, b VARCHAR)")
        con.execute("INSERT INTO good VALUES ('ORIGINAL', 'KEEP')")
    finally:
        con.close()

    work_dir = tmp_path / "scratch"
    con = duckdb.connect(str(db_path))
    try:
        if outer_transaction:
            con.execute("BEGIN TRANSACTION")
        with pytest.raises(RuntimeError, match="failed to decompress") as error:
            streaming.load_zips_parallel(
                con,
                [
                    ("good", good, ("a", "b")),
                    ("bad", bad, ("a", "b")),
                ],
                work_dir=work_dir,
                decompress_workers=2,
            )
        if not isinstance(error.value.__cause__, zipfile.BadZipFile):
            raise AssertionError(f"wrong failure cause: {error.value.__cause__!r}")
        if not good_loaded.is_set():
            raise AssertionError("valid ZIP did not load before CRC failure")
        if list(work_dir.iterdir()):
            raise AssertionError("scratch files survived failed batch")
        if outer_transaction:
            con.execute("ROLLBACK")
    finally:
        con.close()

    # A fresh connection rules out a merely visible but uncommitted row.
    con = duckdb.connect(str(db_path))
    try:
        actual = con.execute("SELECT a, b FROM good").fetchall()
        if actual != [("ORIGINAL", "KEEP")]:
            raise AssertionError(f"batch was only partially committed: {actual!r}")
    finally:
        con.close()
