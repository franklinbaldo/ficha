"""W3.1: stream ZIP entries into DuckDB without intermediate CSV files on SSD.

v1 (FIFO approach) — superseded
    FIFO is non-seekable → DuckDB falls back to sequential CSV parsing.
    Faster than disk at <6 MB CSV; 2-3× slower at production scale (>100 MB).

v2 (this module) — two improvements
    1. /dev/shm instead of FIFO
       tmpfs is seekable → DuckDB uses parallel=true CSV parsing (all CPUs).
       Falls back to work_dir when /dev/shm is unavailable (non-Linux).

    2. Parallel decompression pipeline via load_zips_parallel()
       Decompress N ZIPs concurrently in a thread pool; DuckDB loads each
       seekable tmpfs file with full parallelism as soon as it is ready.
       For 37 RFB ZIPs with 4 CPUs: ~4× speedup on the loading phase.

    Single-pass sniff: reads first 1 MB while writing to tmpfs — no
    double decompression.
"""

import concurrent.futures
import logging
import shutil
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

_CHUNK = 512 * 1024  # 512 KB — fewer syscalls than 64 KB
_SNIFF_BYTES = 1024 * 1024  # 1 MB encoding sniff window

# /dev/shm is always tmpfs (RAM) on Linux; seekable unlike FIFOs.
_SHM_DIR = Path("/dev/shm") if sys.platform == "linux" and Path("/dev/shm").exists() else None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sniff_encoding(sample: bytes) -> tuple[str, bool]:
    """Return (encoding, ignore_errors) from a byte sample."""
    try:
        sample.decode("utf-8", errors="strict")
        return "utf-8", True
    except UnicodeDecodeError:
        return "latin-1", False


def _csv_columns_clause(cols: tuple[str, ...]) -> str:
    pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in cols)
    return "{" + pairs + "}"


def _tmpfs_dir(work_dir: Path) -> Path:
    """Return the best seekable RAM-backed directory available."""
    return _SHM_DIR if _SHM_DIR is not None else work_dir


@dataclass
class _DecompressResult:
    table: str
    tmp_path: Path
    encoding: str
    ignore_errors: bool
    columns: tuple[str, ...]


# ---------------------------------------------------------------------------
# Single-ZIP loader
# ---------------------------------------------------------------------------


def _decompress_zip(
    zip_path: Path,
    table: str,
    columns: tuple[str, ...],
    *,
    tmp_dir: Path,
) -> _DecompressResult | None:
    """Decompress one ZIP entry to a seekable tmpfs file.

    Returns None if the ZIP has no non-empty members (caller creates empty table).
    Raises RuntimeError if the ZIP contains more than one non-directory member.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if not members or members[0].file_size == 0:
            return None
        if len(members) > 1:
            raise RuntimeError(
                f"{zip_path.name}: expected 1 CSV, got {len(members)}: "
                f"{[m.filename for m in members]}"
            )
        member_name = members[0].filename

        # Single-pass: sniff first _SNIFF_BYTES while streaming to tmpfs.
        tmp_path = tmp_dir / f"_ficha_{table}_{zip_path.stem}.csv"
        head = b""
        with zf.open(member_name) as src, open(tmp_path, "wb") as dst:
            # Read first chunk for encoding sniff, write it too.
            head = src.read(_SNIFF_BYTES)
            if not head:
                tmp_path.unlink(missing_ok=True)
                return None
            dst.write(head)
            shutil.copyfileobj(src, dst, length=_CHUNK)

    encoding, ignore_errors = _sniff_encoding(head)
    return _DecompressResult(
        table=table,
        tmp_path=tmp_path,
        encoding=encoding,
        ignore_errors=ignore_errors,
        columns=columns,
    )


def _load_from_result(con: duckdb.DuckDBPyConnection, r: _DecompressResult) -> None:
    """CREATE TABLE from a decompressed tmpfs file, then delete the file."""
    cols_clause = _csv_columns_clause(r.columns)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {r.table} AS
            SELECT * FROM read_csv(
                '{r.tmp_path}',
                delim=';',
                header=false,
                quote='"',
                encoding='{r.encoding}',
                columns={cols_clause},
                null_padding=true,
                strict_mode=false,
                max_line_size=16777216,
                ignore_errors={"true" if r.ignore_errors else "false"},
                parallel=true
            )
            """
        )
    finally:
        r.tmp_path.unlink(missing_ok=True)

    log.info(
        "loaded: table=%s rows=%d encoding=%s",
        r.table,
        con.execute(f"SELECT COUNT(*) FROM {r.table}").fetchone()[0],
        r.encoding,
    )


def _empty_table(con: duckdb.DuckDBPyConnection, table: str, columns: tuple[str, ...]) -> None:
    col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
    con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_table_from_zip_streaming(
    con: duckdb.DuckDBPyConnection,
    table: str,
    zip_path: Path,
    columns: tuple[str, ...],
    *,
    work_dir: Path,
) -> None:
    """Load a ZIP's single CSV into DuckDB via seekable tmpfs — no SSD extract.

    Uses /dev/shm (RAM disk) when available so DuckDB can parse with parallel=true.
    Falls back to work_dir on non-Linux systems.

    Raises RuntimeError if the ZIP contains more than one non-directory member.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = _tmpfs_dir(work_dir)

    result = _decompress_zip(zip_path, table, columns, tmp_dir=tmp_dir)
    if result is None:
        _empty_table(con, table, columns)
        return
    _load_from_result(con, result)


def load_zips_parallel(
    con: duckdb.DuckDBPyConnection,
    specs: list[tuple[str, Path, tuple[str, ...]]],
    *,
    work_dir: Path,
    decompress_workers: int = 4,
) -> None:
    """Load multiple ZIPs into DuckDB using a parallel decompress → sequential load pipeline.

    specs: list of (table_name, zip_path, columns)

    Architecture:
        decompress_workers threads decompress ZIPs to /dev/shm (RAM) concurrently.
        Main thread picks up each result via as_completed and runs CREATE TABLE
        with parallel=true as soon as each file is ready.

    This overlaps CPU-bound ZIP decompression with DuckDB's parallel CSV parsing,
    giving ~decompress_workers× speedup on the loading phase compared to sequential.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = _tmpfs_dir(work_dir)

    errors: list[BaseException] = []

    def _decompress(spec: tuple[str, Path, tuple[str, ...]]) -> _DecompressResult | None:
        table, zip_path, columns = spec
        return _decompress_zip(zip_path, table, columns, tmp_dir=tmp_dir)

    with concurrent.futures.ThreadPoolExecutor(max_workers=decompress_workers) as pool:
        future_to_spec = {pool.submit(_decompress, spec): spec for spec in specs}
        for fut in concurrent.futures.as_completed(future_to_spec):
            table, _, columns = future_to_spec[fut]
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                log.error("failed to decompress ZIP for table %s: %s", table, exc)
                continue
            if result is None:
                _empty_table(con, table, columns)
            else:
                _load_from_result(con, result)

    if errors:
        raise RuntimeError(f"{len(errors)} ZIP(s) failed to decompress") from errors[0]
