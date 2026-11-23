"""Offline DuckDB regression for Ficha #478: unique ZIP scratch and error cleanup.

Synthetic local ZIPs only. No HTTP, RFB data, or publication.
The race is ordered by threading.Event, not scheduling or sleeps.
"""

from __future__ import annotations

import concurrent.futures
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

COLS = ("a", "b")


def equal(actual: object, expected: object) -> None:
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, received {actual!r}")


def wait(event: threading.Event, label: str) -> None:
    if not event.wait(15):
        raise AssertionError(f"timed out waiting for {label}")


def make_zip(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("rows.CSV", payload)
    return path


def corrupt_last_member_byte(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        entry = archive.infolist()[0]
    with path.open("r+b") as handle:
        handle.seek(entry.header_offset + 26)
        name_len, extra_len = struct.unpack("<HH", handle.read(4))
        end = entry.header_offset + 30 + name_len + extra_len + entry.compress_size
        handle.seek(end - 1)
        byte = handle.read(1)
        handle.seek(end - 1)
        handle.write(bytes([byte[0] ^ 1]))


def empty_scratch(work_dir: Path) -> None:
    equal(list(work_dir.iterdir()), [])


@pytest.mark.parametrize(
    "corrupt_rival",
    [False, True],
    ids=["valid-rival", "crc-rival"],
)
def test_concurrent_same_stem_isolated_with_real_duckdb(
    tmp_path,
    monkeypatch,
    corrupt_rival,
):
    """A must consume its own CSV even when B stages later or fails CRC."""
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    original_decompress = streaming._decompress_zip
    original_load = streaming._load_from_result

    a = make_zip(tmp_path / "A" / "source.zip", b"A;SAFE\n" * 11)
    b = make_zip(tmp_path / "B" / "source.zip", b"B;OTHER\n" * 240000)
    if corrupt_rival:
        corrupt_last_member_byte(b)

    a_staged = threading.Event()
    b_attempted = threading.Event()
    a_loaded = threading.Event()

    def ordered_decompress(zip_path, table, columns, *, tmp_dir):
        if zip_path == a:
            result = original_decompress(zip_path, table, columns, tmp_dir=tmp_dir)
            a_staged.set()
            wait(b_attempted, "B to stage or fail CRC")
            return result
        wait(a_staged, "A to stage")
        try:
            result = original_decompress(zip_path, table, columns, tmp_dir=tmp_dir)
        finally:
            b_attempted.set()
        wait(a_loaded, "A to consume before B")
        return result

    def ordered_load(con, result):
        try:
            return original_load(con, result)
        finally:
            if con is ca:
                a_loaded.set()

    monkeypatch.setattr(streaming, "_decompress_zip", ordered_decompress)
    monkeypatch.setattr(streaming, "_load_from_result", ordered_load)
    work = tmp_path / "work"
    ca, cb = duckdb.connect(), duckdb.connect()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(
                streaming.create_table_from_zip_streaming,
                ca,
                "rows",
                a,
                COLS,
                work_dir=work,
            )
            wait(a_staged, "A producer")
            future_b = executor.submit(
                streaming.create_table_from_zip_streaming,
                cb,
                "rows",
                b,
                COLS,
                work_dir=work,
            )
            future_a.result(timeout=30)
            if corrupt_rival:
                with pytest.raises(zipfile.BadZipFile):
                    future_b.result(timeout=30)
            else:
                future_b.result(timeout=30)
        equal(ca.execute("SELECT DISTINCT a, b FROM rows").fetchall(), [("A", "SAFE")])
        if not corrupt_rival:
            equal(
                cb.execute("SELECT DISTINCT a, b FROM rows").fetchall(),
                [("B", "OTHER")],
            )
        empty_scratch(work)
    finally:
        ca.close()
        cb.close()


def test_late_crc_failure_removes_partial_scratch(tmp_path, monkeypatch):
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    source = make_zip(tmp_path / "crc.zip", b"BAD;ROW\n" * 300000)
    corrupt_last_member_byte(source)
    work = tmp_path / "work"
    con = duckdb.connect()
    try:
        with pytest.raises(zipfile.BadZipFile):
            streaming.create_table_from_zip_streaming(
                con, "rows", source, COLS, work_dir=work
            )
        empty_scratch(work)
    finally:
        con.close()


@pytest.mark.parametrize("bad_branch", ["load", "empty"])
def test_parallel_pending_futures_cleaned_after_duckdb_error(
    tmp_path,
    monkeypatch,
    bad_branch,
):
    """Worker B stages only after DuckDB executes failing branch A."""
    monkeypatch.setattr(streaming, "_SHM_DIR", None)
    original_decompress = streaming._decompress_zip
    original_load = streaming._load_from_result
    original_empty = streaming._empty_table
    signal = threading.Event()

    bad_bytes = b"BAD;STOP\n" if bad_branch == "load" else b""
    bad = make_zip(tmp_path / "A" / "bad.zip", bad_bytes)
    good = make_zip(tmp_path / "B" / "good.zip", b"GOOD;KEEP\n")

    def ordered_decompress(zip_path, table, columns, *, tmp_dir):
        if zip_path == good:
            wait(signal, "failing DuckDB consumer")
        return original_decompress(zip_path, table, columns, tmp_dir=tmp_dir)

    def failing_load(con, result):
        if result.table == "bad-name":
            signal.set()
        return original_load(con, result)

    def failing_empty(con, table, columns):
        if table == "bad-name":
            signal.set()
        return original_empty(con, table, columns)

    monkeypatch.setattr(streaming, "_decompress_zip", ordered_decompress)
    monkeypatch.setattr(streaming, "_load_from_result", failing_load)
    monkeypatch.setattr(streaming, "_empty_table", failing_empty)
    work = tmp_path / "work"
    con = duckdb.connect()
    try:
        with pytest.raises(duckdb.Error):
            streaming.load_zips_parallel(
                con,
                [("bad-name", bad, COLS), ("good", good, COLS)],
                work_dir=work,
                decompress_workers=2,
            )
        empty_scratch(work)
    finally:
        con.close()


def test_optimized_python_replays_regressions():
    if not __debug__:
        return
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            __file__,
            "-k",
            "not optimized_python",
        ],
        cwd=Path(__file__).parent,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode:
        raise AssertionError(result.stdout[-2000:] + "\n" + result.stderr[-2000:])
