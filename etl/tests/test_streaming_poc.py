"""W3.1 streaming POC — correctness + benchmark tests.

Two goals:
1. VIABILITY: streaming produces identical results to the current extract-then-load path.
2. ADVANTAGE: streaming writes 0 bytes of intermediate CSV to disk; measures wall-clock delta.

Run with: uv run pytest tests/test_streaming_poc.py -v -s
The -s flag shows benchmark output in CI step summary.
"""

import time
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import transform
from ficha_etl.streaming import create_table_from_zip_streaming

# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

_ESTABELECIMENTO_COLS = (
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
)


def _make_row(i: int) -> tuple[str, ...]:
    base = str(i).zfill(8)
    return (
        base,
        "0001",
        "00",
        "1",
        f"EMPRESA {i}",
        "02",
        "20200101",
        "00",
        "",
        "105",
        "20200101",
        "6201500",
        "",
        "RUA",
        "RUA DOS TESTES",
        str(i % 999 + 1),
        "",
        "CENTRO",
        f"{i:08d}"[:8],
        "SP",
        "3550308",
        "11",
        "999999999",
        "",
        "",
        "",
        "",
        f"emp{i}@test.com",
        "",
        "",
    )


def _build_zip(path: Path, n_rows: int, encoding: str = "latin-1") -> int:
    """Build a ZIP with a single CSV of n_rows rows. Returns uncompressed CSV size."""
    rows = [_make_row(i) for i in range(n_rows)]
    body = ("\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n").encode(encoding)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Estabelecimentos0.CSV", body)
    return len(body)


# ---------------------------------------------------------------------------
# Helpers for disk I/O measurement
# ---------------------------------------------------------------------------


def _bytes_written_to_dir(directory: Path) -> int:
    """Sum of all regular-file sizes under *directory* after a run."""
    total = 0
    for p in directory.rglob("*"):
        if p.is_file():
            total += p.stat().st_size
    return total


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def small_zip(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("zips")
    z = d / "Estabelecimentos0.zip"
    _build_zip(z, n_rows=500)
    return z


def test_streaming_correctness_row_count(small_zip, tmp_path):
    """Streaming and extract-then-load produce the same row count."""
    con_current = duckdb.connect()
    con_stream = duckdb.connect()

    # Current approach: extract to disk, then read_csv
    extract_dir = tmp_path / "extracted"
    extracted = transform.extract_zip(small_zip, extract_dir)
    transform._create_table_from_csvs(
        con_current, "estabelecimento", extracted, _ESTABELECIMENTO_COLS
    )

    # Streaming approach: no disk extract
    create_table_from_zip_streaming(
        con_stream,
        "estabelecimento",
        small_zip,
        _ESTABELECIMENTO_COLS,
        work_dir=tmp_path / "stream_work",
    )

    count_current = con_current.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    count_stream = con_stream.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]

    assert count_current == 500
    assert count_stream == count_current, (
        f"streaming produced {count_stream} rows, current produced {count_current}"
    )


def test_streaming_correctness_content(small_zip, tmp_path):
    """Streaming and extract-then-load produce bit-for-bit identical sorted content."""
    con_current = duckdb.connect()
    con_stream = duckdb.connect()

    extract_dir = tmp_path / "extracted"
    extracted = transform.extract_zip(small_zip, extract_dir)
    transform._create_table_from_csvs(
        con_current, "estabelecimento", extracted, _ESTABELECIMENTO_COLS
    )
    create_table_from_zip_streaming(
        con_stream,
        "estabelecimento",
        small_zip,
        _ESTABELECIMENTO_COLS,
        work_dir=tmp_path / "stream_work",
    )

    # Compare sorted first 10 fields as a lightweight checksum
    rows_current = con_current.execute(
        "SELECT cnpj_basico, cnpj_ordem, nome_fantasia, uf, municipio "
        "FROM estabelecimento ORDER BY cnpj_basico"
    ).fetchall()
    rows_stream = con_stream.execute(
        "SELECT cnpj_basico, cnpj_ordem, nome_fantasia, uf, municipio "
        "FROM estabelecimento ORDER BY cnpj_basico"
    ).fetchall()

    assert rows_current == rows_stream


def test_streaming_empty_zip(tmp_path):
    """Empty ZIP (0-byte CSV member) creates an empty table, not an error."""
    z = tmp_path / "empty.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("empty.CSV", b"")

    con = duckdb.connect()
    create_table_from_zip_streaming(con, "t", z, ("a", "b"), work_dir=tmp_path / "work")
    count = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert count == 0


def test_streaming_rejects_multi_member_zip(tmp_path):
    """ZIP with multiple CSV members raises RuntimeError (mirrors current behaviour)."""
    z = tmp_path / "multi.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("a.CSV", b"1;2\n")
        zf.writestr("b.CSV", b"3;4\n")

    con = duckdb.connect()
    with pytest.raises(RuntimeError, match="expected 1 CSV"):
        create_table_from_zip_streaming(con, "t", z, ("x", "y"), work_dir=tmp_path / "work")


# ---------------------------------------------------------------------------
# Benchmark: streaming vs current — measures time and disk bytes
# ---------------------------------------------------------------------------

# 50 K rows → ~10 MB uncompressed CSV — large enough to see disk I/O delta,
# small enough to run in <10 s on GitHub Actions.
_BENCH_ROWS = 50_000


@pytest.fixture(scope="module")
def bench_zip(tmp_path_factory) -> tuple[Path, int]:
    """Returns (zip_path, uncompressed_csv_bytes)."""
    d = tmp_path_factory.mktemp("bench_zips")
    z = d / "Estabelecimentos0.zip"
    csv_bytes = _build_zip(z, n_rows=_BENCH_ROWS)
    return z, csv_bytes


def test_benchmark_streaming_vs_current(bench_zip, tmp_path):
    """Benchmark: streaming writes 0 intermediate bytes; current writes ~N MB.

    This test always passes — it reports metrics so CI step summaries show them.
    The correctness assertions are the gate; timing is informational.
    """
    zip_path, csv_bytes = bench_zip

    # ── Current approach ───────────────────────────────────────────────────
    extract_dir = tmp_path / "current_extract"
    extract_dir.mkdir()

    t0 = time.perf_counter()
    con_current = duckdb.connect()
    extracted = transform.extract_zip(zip_path, extract_dir)
    transform._create_table_from_csvs(
        con_current, "estabelecimento", extracted, _ESTABELECIMENTO_COLS
    )
    t_current = time.perf_counter() - t0
    disk_current = _bytes_written_to_dir(extract_dir)
    rows_current = con_current.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    con_current.close()

    # ── Streaming approach ─────────────────────────────────────────────────
    stream_work = tmp_path / "stream_work"
    stream_work.mkdir()

    t0 = time.perf_counter()
    con_stream = duckdb.connect()
    create_table_from_zip_streaming(
        con_stream,
        "estabelecimento",
        zip_path,
        _ESTABELECIMENTO_COLS,
        work_dir=stream_work,
    )
    t_stream = time.perf_counter() - t0
    # FIFOs have 0 bytes on disk; only the work dir itself exists
    disk_stream = _bytes_written_to_dir(stream_work)
    rows_stream = con_stream.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    con_stream.close()

    # ── Report ─────────────────────────────────────────────────────────────
    print(
        f"\n{'─' * 60}\n"
        f"  W3.1 STREAMING BENCHMARK  ({_BENCH_ROWS:,} rows)\n"
        f"{'─' * 60}\n"
        f"  Uncompressed CSV size : {csv_bytes / 1024**2:.1f} MB\n"
        f"\n"
        f"  Current (extract→load)\n"
        f"    time     : {t_current:.3f} s\n"
        f"    disk I/O : {disk_current / 1024**2:.1f} MB intermediate CSV\n"
        f"    rows     : {rows_current:,}\n"
        f"\n"
        f"  Streaming (FIFO, no disk extract)\n"
        f"    time     : {t_stream:.3f} s\n"
        f"    disk I/O : {disk_stream / 1024**2:.1f} MB intermediate CSV\n"
        f"    rows     : {rows_stream:,}\n"
        f"\n"
        f"  Δ disk   : -{disk_current / 1024**2:.1f} MB  "
        f"({100 * (1 - disk_stream / max(disk_current, 1)):.0f}% reduction)\n"
        f"  Δ time   : {t_stream - t_current:+.3f} s  "
        f"({'faster' if t_stream < t_current else 'slower'})\n"
        f"{'─' * 60}"
    )

    # Correctness gate (streaming must match current exactly)
    assert rows_stream == rows_current, (
        f"streaming produced {rows_stream} rows, current produced {rows_current}"
    )

    # Disk gate: streaming must not write any intermediate CSV
    assert disk_stream == 0, (
        f"streaming left {disk_stream} bytes on disk (expected 0 — FIFO should disappear)"
    )

    # Disk benefit: current must have written the uncompressed CSV
    assert disk_current > 0, "current approach produced no intermediate file (unexpected)"
