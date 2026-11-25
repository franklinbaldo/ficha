"""Offline differential controls for physical ZIP framing and the RFB consumer.

These are synthetic inputs, not original Receita Federal archives. The semantic
member-name contract remains undecided in #477. No network or production ETL.
"""

from __future__ import annotations

import struct
import subprocess
import sys
import zipfile
import zlib
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree

import duckdb
import pytest

from ficha_etl.raw_preflight import _raw_zip_crc_and_budget
from ficha_etl.streaming import create_table_from_zip_streaming, load_zips_parallel


def _require_equal(actual: object, expected: object) -> None:
    # Do not rely on assert: this test must also run under python -O.
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


@pytest.fixture(autouse=True)
def _require_xfail_enabled(request: pytest.FixtureRequest) -> None:
    if request.config.getoption("runxfail"):
        pytest.fail("ZIP differential controls require XFAIL semantics")


def _load(con: duckdb.DuckDBPyConnection, source: Path, work: Path, parallel: bool) -> None:
    if parallel:
        load_zips_parallel(
            con,
            [("source_rows", source, ("a", "b"))],
            work_dir=work,
            decompress_workers=1,
        )
    else:
        create_table_from_zip_streaming(con, "source_rows", source, ("a", "b"), work_dir=work)


def _require_physical_rejection(exc: Exception, message: str, parallel: bool) -> None:
    # Only unwrap the actual single-worker aggregate, not arbitrary chained errors.
    cause = exc
    if parallel and isinstance(exc, RuntimeError) and str(exc) == "1 ZIP(s) failed to decompress":
        if not isinstance(exc.__cause__, Exception):
            raise exc
        cause = exc.__cause__
    if isinstance(cause, (zipfile.BadZipFile, zlib.error)):
        return
    if isinstance(cause, RuntimeError) and message in str(cause):
        return
    raise exc


def _make_empty_zip(path: Path, compression: int) -> None:
    member = zipfile.ZipInfo("empty.CSV", date_time=(1980, 1, 1, 0, 0, 0))
    member.compress_type = compression
    member.create_system = 3
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, b"")


def _alter_member_deflate(path: Path, replacement: bytes) -> None:
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
        _alter_member_deflate(path, b"\x03")
    elif kind == "deflate_trailing":
        _alter_member_deflate(path, b"\x03\x00JUNK")
    elif kind not in {"stored_clean", "deflate_clean"}:
        raise ValueError(f"unknown fixture {kind}")
    with zipfile.ZipFile(path) as archive:
        info = archive.infolist()
        if len(info) != 1 or info[0].file_size != 0 or archive.testzip() is not None:
            raise AssertionError(f"unexpected ZIP fixture structure: {kind}")


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize("kind", ["stored_clean", "deflate_clean"])
def test_integral_empty_member_is_accepted_in_both_paths(
    tmp_path: Path,
    kind: str,
    parallel: bool,
) -> None:
    source = tmp_path / "empty.zip"
    _fixture(source, kind)
    _require_equal(_raw_zip_crc_and_budget(source, expanded=0, members=0), (0, 1))
    con = duckdb.connect()
    try:
        _load(con, source, tmp_path / "work", parallel)
        _require_equal(con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0], 0)
    finally:
        con.close()


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize(
    ("kind", "preflight_message"),
    [
        ("stored_gap", "unclaimed ZIP payload bytes"),
        ("deflate_truncated", "truncated ZIP deflate stream"),
        ("deflate_trailing", "trailing ZIP deflate bytes"),
    ],
    ids=["stored_gap", "deflate_truncated", "deflate_trailing"],
)
def test_invalid_empty_member_preflight_vs_consumer(
    tmp_path: Path,
    kind: str,
    preflight_message: str,
    parallel: bool,
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
            _load(con, source, tmp_path / "work", parallel)
        except (RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
            _require_physical_rejection(exc, preflight_message, parallel)
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
    ids=["dir-empty", "dir-data", "text-empty", "text-data"],
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
        _load(con, source, tmp_path / "work", parallel)
        count = con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
        _require_equal(count, expected_rows)
        if expected_rows:
            rows = con.execute("SELECT a, b FROM source_rows").fetchall()
            _require_equal(rows, [("1", "2")])
    finally:
        con.close()


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize(
    ("kind", "preflight_message"),
    [
        ("stored_clean", None),
        ("deflate_clean", None),
        ("stored_gap", "unclaimed ZIP payload bytes"),
        ("deflate_truncated", "truncated ZIP deflate stream"),
        ("deflate_trailing", "trailing ZIP deflate bytes"),
    ],
    ids=["stored_clean", "deflate_clean", "stored_gap", "deflate_truncated", "deflate_trailing"],
)
def test_nonempty_member_physical_differential(
    tmp_path: Path, kind: str, preflight_message: str | None, parallel: bool
) -> None:
    """The same nonempty bytes must not bypass framing through either consumer.

    The positive controls are physical ZIPs only, not a primary RFB name policy.
    Mutants currently accepted by a consumer become XFAIL after strict preflight.
    """
    source = tmp_path / "nonempty.zip"
    payload = b"1;2\n"
    member = zipfile.ZipInfo("DATA.ESTABELE", date_time=(1980, 1, 1, 0, 0, 0))
    member.compress_type = zipfile.ZIP_STORED if kind.startswith("stored") else zipfile.ZIP_DEFLATED
    member.create_system = 3
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(member, payload)

    if kind == "stored_gap":
        _insert_unclaimed_stored_bytes(source, b"JUNK")
    elif kind in {"deflate_truncated", "deflate_trailing"}:
        with zipfile.ZipFile(source) as archive:
            entry = archive.infolist()[0]
        raw = source.read_bytes()
        local = struct.unpack_from("<IHHHHHIIIHH", raw, entry.header_offset)
        offset = entry.header_offset + 30 + local[-2] + local[-1]
        compressed = raw[offset : offset + entry.compress_size]
        replacement = compressed[:-1] if kind == "deflate_truncated" else compressed + b"JUNK"
        _alter_member_deflate(source, replacement)

    with zipfile.ZipFile(source) as archive:
        _require_equal(archive.testzip(), None)
        _require_equal(archive.read(archive.infolist()[0]), payload)

    if preflight_message is None:
        _require_equal(_raw_zip_crc_and_budget(source, expanded=0, members=0), (4, 1))
    else:
        with pytest.raises(RuntimeError, match=preflight_message):
            _raw_zip_crc_and_budget(source, expanded=0, members=0)

    con = duckdb.connect()
    try:
        try:
            _load(con, source, tmp_path / "work", parallel)
        except (RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
            if preflight_message is None:
                raise
            _require_physical_rejection(exc, preflight_message, parallel)
            _require_equal(
                con.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'source_rows'"
                ).fetchone()[0],
                0,
            )
        else:
            _require_equal(
                con.execute("SELECT a, b FROM source_rows").fetchall(),
                [("1", "2")],
            )
            if preflight_message is not None:
                pytest.xfail(f"#477: {kind} nonempty ZIP admitted by consumer")
    finally:
        con.close()


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize("damaged", [False, True], ids=["aux-clean", "aux-crc"])
def test_ignored_directory_payload_crc_differential(
    tmp_path: Path, damaged: bool, parallel: bool
) -> None:
    """A selected data member is not evidence of integrity of ignored members."""
    source = tmp_path / "auxiliary.zip"
    with zipfile.ZipFile(source, "w") as archive:
        for name, payload in (("aux/", b"aux"), ("DATA.ESTABELE", b"1;2\n")):
            member = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            member.compress_type = zipfile.ZIP_STORED
            member.create_system = 3
            archive.writestr(member, payload)

    with zipfile.ZipFile(source) as archive:
        offset = archive.infolist()[0].header_offset
    if damaged:
        raw = bytearray(source.read_bytes())
        header = struct.unpack_from("<IHHHHHIIIHH", raw, offset)
        raw[offset + 30 + header[-2] + header[-1]] ^= 1
        source.write_bytes(raw)

    with zipfile.ZipFile(source) as archive:
        _require_equal(archive.testzip(), "aux/" if damaged else None)
        selected = [member for member in archive.infolist() if not member.is_dir()]
        _require_equal(len(selected), 1)
        _require_equal(archive.read(selected[0]), b"1;2\n")

    message = "ZIP CRC failed"
    if damaged:
        with pytest.raises(RuntimeError, match=message):
            _raw_zip_crc_and_budget(source, expanded=0, members=0)
    else:
        _require_equal(_raw_zip_crc_and_budget(source, expanded=0, members=0), (7, 2))

    con = duckdb.connect()
    try:
        try:
            _load(con, source, tmp_path / "work", parallel)
        except (RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
            if not damaged:
                raise
            _require_physical_rejection(exc, message, parallel)
            _require_equal(
                con.execute(
                    "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'source_rows'"
                ).fetchone()[0],
                0,
            )
        else:
            _require_equal(
                con.execute("SELECT a, b FROM source_rows").fetchall(),
                [("1", "2")],
            )
            if damaged:
                pytest.xfail("#477: corrupt ignored directory payload admitted by consumer")
    finally:
        con.close()


def test_optimized_python_replays_zip_controls(tmp_path: Path) -> None:
    """Run the real ETL differential tests under Python -O, without recursion."""
    tests = (
        "test_integral_empty_member_is_accepted_in_both_paths",
        "test_invalid_empty_member_preflight_vs_consumer",
        "test_member_semantics_characterization_pending_primary_rfb_contract",
        "test_nonempty_member_physical_differential",
        "test_ignored_directory_payload_crc_differential",
    )
    selectors = [f"{__file__}::{name}" for name in tests]
    report = tmp_path / "optimized.xml"
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", f"--junitxml={report}", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if child.returncode:
        raise AssertionError(child.stdout + "\n" + child.stderr)

    clean = ("stored_clean", "deflate_clean")
    invalid = ("stored_gap", "deflate_truncated", "deflate_trailing")
    semantic = ("dir-empty", "dir-data", "text-empty", "text-data")
    expected = Counter()
    for name, cases in ((tests[0], clean), (tests[1], invalid), (tests[3], clean + invalid)):
        for case in cases:
            for parallel in (False, True):
                expected[f"{name}[{case}-{parallel}]"] += 1
    for parallel in (False, True):
        for case in ("aux-clean", "aux-crc"):
            expected[f"{tests[4]}[{case}-{parallel}]"] += 1
        for case in semantic:
            expected[f"{tests[2]}[{parallel}-{case}]"] += 1
    allowed_xfail = {
        f"{name}[{case}-{parallel}]"
        for name in (tests[1], tests[3])
        for case in invalid
        for parallel in (False, True)
    }
    allowed_xfail.update(f"{tests[4]}[aux-crc-{parallel}]" for parallel in (False, True))
    # The report must contain precisely the selected child cases, not just exit 0.
    cases = ElementTree.parse(report).findall(".//testcase")
    _require_equal(Counter(case.attrib["name"] for case in cases), expected)
    for case in cases:
        if case.find("failure") is not None or case.find("error") is not None:
            raise AssertionError(f"child failure: {case.attrib['name']}")
        skipped = case.find("skipped")
        if skipped is not None:
            _require_equal(skipped.get("type"), "pytest.xfail")
            if case.attrib["name"] not in allowed_xfail:
                raise AssertionError(f"unexpected child XFAIL: {case.attrib['name']}")
