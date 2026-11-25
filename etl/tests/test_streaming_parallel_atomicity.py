"""Offline characterization of DuckDB batch atomicity after late ZIP CRC failure.

The expected whole-batch atomicity regression is strict-XFAIL; the caller-owned
transaction is a positive control. No production ZIP, network, or IA operations.
"""

from __future__ import annotations

import os
import struct
import subprocess
import sys
import threading
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import streaming


class PartialBatchCommit(AssertionError):
    """Expected only when an earlier table persists after a later CRC failure."""


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
                raises=PartialBatchCommit,
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
    """Characterize a late CRC failure with and without caller rollback.

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
        con.execute("CREATE TABLE caller_state (v INTEGER)")
        con.execute("INSERT INTO caller_state VALUES (1)")
        con.execute("CREATE TABLE bad (a VARCHAR, b VARCHAR)")
        con.execute("INSERT INTO bad VALUES ('OLD', 'BAD')")
    finally:
        con.close()

    work_dir = tmp_path / "scratch"
    con = duckdb.connect(str(db_path))
    try:
        if outer_transaction:
            con.execute("BEGIN TRANSACTION")
            con.execute("INSERT INTO caller_state VALUES (2)")
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
        if con.execute("SELECT v FROM caller_state ORDER BY v").fetchall() != [(1,)]:
            raise AssertionError("caller transaction was committed by the loader")
        if con.execute("SELECT a, b FROM bad").fetchall() != [("OLD", "BAD")]:
            raise AssertionError("CRC-invalid table was replaced")
        actual = con.execute("SELECT a, b FROM good").fetchall()
        if actual != [("ORIGINAL", "KEEP")]:
            if not outer_transaction and actual == [("NEW", "VALUE")] * 9:
                raise PartialBatchCommit(f"earlier table persisted after late CRC: {actual!r}")
            raise AssertionError(f"unexpected post-batch table contents: {actual!r}")
    finally:
        con.close()


def _equal(actual: object, expected: object) -> None:
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def test_late_sql_failure_preserves_caller_rollback(tmp_path, monkeypatch):
    """The real SQL consumer fails only after the first replacement succeeds."""
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    good = _zip(tmp_path / "good.zip", b"NEW;VALUE\n" * 9)
    bad = _zip(tmp_path / "bad.zip", b"VALID;CSV\n")
    good_loaded = threading.Event()
    original_decompress = streaming._decompress_zip
    original_load = streaming._load_from_result

    def ordered_decompress(zip_path, table, columns, *, tmp_dir):
        if zip_path == bad and not good_loaded.wait(15):
            raise AssertionError("good table was not loaded before SQL failure")
        return original_decompress(zip_path, table, columns, tmp_dir=tmp_dir)

    def signal_after_load(con, result):
        original_load(con, result)
        if result.table == "good":
            good_loaded.set()

    monkeypatch.setattr(streaming, "_decompress_zip", ordered_decompress)
    monkeypatch.setattr(streaming, "_load_from_result", signal_after_load)
    db_path = tmp_path / "sql.duckdb"
    work = tmp_path / "scratch"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE good (a VARCHAR, b VARCHAR)")
        con.execute("INSERT INTO good VALUES ('ORIGINAL', 'KEEP')")
        con.execute("CREATE TABLE caller_state (v INTEGER)")
        con.execute("INSERT INTO caller_state VALUES (1)")
        con.execute("BEGIN TRANSACTION")
        con.execute("INSERT INTO caller_state VALUES (2)")
        with pytest.raises(duckdb.ParserException):
            streaming.load_zips_parallel(
                con,
                [("good", good, ("a", "b")), ("bad-name", bad, ("a", "b"))],
                work_dir=work,
                decompress_workers=2,
            )
        _equal(good_loaded.is_set(), True)
        _equal(list(work.iterdir()), [])
        con.execute("ROLLBACK")
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        _equal(con.execute("SELECT a, b FROM good").fetchall(), [("ORIGINAL", "KEEP")])
        _equal(con.execute("SELECT v FROM caller_state ORDER BY v").fetchall(), [(1,)])
    finally:
        con.close()


@pytest.mark.parametrize("finish", ["COMMIT", "ROLLBACK"])
def test_success_leaves_transaction_decision_to_caller(tmp_path, monkeypatch, finish):
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    good = _zip(tmp_path / "good.zip", b"NEW;VALUE\n" * 9)
    db_path = tmp_path / "success.duckdb"
    work = tmp_path / "scratch"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE good (a VARCHAR, b VARCHAR)")
        con.execute("INSERT INTO good VALUES ('ORIGINAL', 'KEEP')")
        con.execute("CREATE TABLE caller_state (v INTEGER)")
        con.execute("INSERT INTO caller_state VALUES (1)")
        con.execute("BEGIN TRANSACTION")
        con.execute("INSERT INTO caller_state VALUES (2)")
        streaming.load_zips_parallel(
            con,
            [("good", good, ("a", "b"))],
            work_dir=work,
        )
        _equal(con.execute("SELECT a, b FROM good").fetchall(), [("NEW", "VALUE")] * 9)
        _equal(con.execute("SELECT v FROM caller_state ORDER BY v").fetchall(), [(1,), (2,)])
        _equal(list(work.iterdir()), [])
        con.execute(finish)
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        expected = [("NEW", "VALUE")] * 9 if finish == "COMMIT" else [("ORIGINAL", "KEEP")]
        caller_expected = [(1,), (2,)] if finish == "COMMIT" else [(1,)]
        _equal(con.execute("SELECT a, b FROM good").fetchall(), expected)
        _equal(con.execute("SELECT v FROM caller_state ORDER BY v").fetchall(), caller_expected)
    finally:
        con.close()


def test_optimized_python_replays_atomicity():
    if not __debug__:
        return
    result = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", __file__, "-k", "not optimized_python"],
        cwd=Path(__file__).parent,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode:
        raise AssertionError(result.stdout[-2000:] + "\n" + result.stderr[-2000:])
