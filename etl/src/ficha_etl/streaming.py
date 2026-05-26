"""W3.1 POC: stream ZIP entries directly into DuckDB without intermediate CSV files.

Current approach (transform.py):
    ZIP on disk → extract CSV to disk (200 MB–2 GB) → DuckDB read_csv from path

Streaming approach (this module):
    ZIP on disk → FIFO pipe (0 bytes on disk) → DuckDB read_csv from pipe path

The key property: DuckDB's read_csv can read from a named FIFO on Linux.
A background thread streams the ZIP entry to the FIFO while DuckDB consumes
the other end. Peak disk = 0 for the intermediate; peak memory = encoding
sniff buffer (1 MB) + DuckDB's own read buffer.

For ZIPs that don't fit in memory this is critical: the ZIP decompressor
never materialises the full CSV in RAM; it streams 64 KB chunks.
"""

import logging
import os
import shutil
import threading
import zipfile
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

_CHUNK = 65_536  # 64 KB stream chunks


def _sniff_encoding(sample: bytes) -> tuple[str, bool]:
    """Return (encoding, ignore_errors) from first-1 MB sample."""
    try:
        sample.decode("utf-8", errors="strict")
        return "utf-8", True
    except UnicodeDecodeError:
        return "latin-1", False


def _csv_columns_clause(cols: tuple[str, ...]) -> str:
    pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in cols)
    return "{" + pairs + "}"


def create_table_from_zip_streaming(
    con: duckdb.DuckDBPyConnection,
    table: str,
    zip_path: Path,
    columns: tuple[str, ...],
    *,
    work_dir: Path,
) -> None:
    """Load ZIP's single CSV into DuckDB via a named FIFO — no disk extract.

    Replaces extract_zip() + _create_table_from_csvs() for the common case of
    one CSV per ZIP. The FIFO is created under *work_dir* and removed after use.

    Raises RuntimeError if the ZIP contains ≠1 non-directory member.
    """
    work_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if not members:
            col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
            con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
            return
        if len(members) > 1:
            raise RuntimeError(
                f"{zip_path.name}: expected 1 CSV, got {len(members)}: "
                f"{[m.filename for m in members]}"
            )
        member_name = members[0].filename
        member_size = members[0].file_size

        if member_size == 0:
            col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
            con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
            return

        # Sniff encoding from first 1 MB without fully extracting
        with zf.open(member_name) as f:
            sample = f.read(1024 * 1024)

    encoding, ignore_errors = _sniff_encoding(sample)
    cols_clause = _csv_columns_clause(columns)

    fifo_path = work_dir / f"_stream_{table}.fifo"
    fifo_path.unlink(missing_ok=True)
    os.mkfifo(fifo_path)

    stream_exc: list[BaseException] = []

    def _stream() -> None:
        try:
            with zipfile.ZipFile(zip_path) as zf2:
                with zf2.open(member_name) as src:
                    with open(fifo_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=_CHUNK)
        except Exception as exc:  # noqa: BLE001
            stream_exc.append(exc)
            # Open+close FIFO write-end so DuckDB's read_csv doesn't hang.
            try:
                open(fifo_path, "wb").close()  # noqa: WPS515
            except OSError:
                pass

    t = threading.Thread(target=_stream, daemon=True)
    t.start()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_csv(
                '{fifo_path}',
                delim=';',
                header=false,
                quote='"',
                encoding='{encoding}',
                columns={cols_clause},
                null_padding=true,
                strict_mode=false,
                max_line_size=16777216,
                ignore_errors={"true" if ignore_errors else "false"}
            )
            """
        )
    finally:
        t.join()
        fifo_path.unlink(missing_ok=True)

    if stream_exc:
        raise RuntimeError(f"streaming thread failed: {stream_exc[0]}") from stream_exc[0]

    log.info(
        "streaming load: table=%s rows=%d zip=%s encoding=%s",
        table,
        con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
        zip_path.name,
        encoding,
    )
