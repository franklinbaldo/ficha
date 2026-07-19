"""A/B transient Parquet codecs for the production chunked CNPJ writer.

Only the transient chunk-part codec changes between variants.  The final
Parquet remains ZSTD in both cases, matching production and isolating the
question under test: does LZ4 reduce CPU enough to justify its larger
intermediate footprint while parts, DuckDB spill, and the final output coexist?

The two variants run in separate file-backed DuckDB states with production
PRAGMAs.  Output equivalence is proven once before timing.  Every timed run
records wall/CPU time, exact part sizes, final size, database size, and sampled
peaks for parts, DuckDB temp, the whole variant state, and filesystem usage.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any

import duckdb
import ibis

from _profile import (
    ABResult,
    assert_parquet_equivalent,
    capture_environment,
    open_production_connection,
    run_ab,
)
from ficha_etl import metrics, registry, transform
from ficha_etl.transform import ExtractedFile

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
STATE_ROOT = Path("bench/.work/codecs")
RESULT_ROOT = Path("bench/.evidence")
N = 5
SEED = 20260719


def _state_dir(codec: str) -> Path:
    return STATE_ROOT / codec.lower()


def _db_path(codec: str) -> Path:
    return _state_dir(codec) / "bench.duckdb"


def _setup_state(codec: str) -> tuple[list[Path], int]:
    """Create one isolated production-profile state for a codec variant."""
    state_dir = _state_dir(codec)
    shutil.rmtree(state_dir, ignore_errors=True)
    db_path = _db_path(codec)
    con = open_production_connection(db_path)
    try:
        est_paths = sorted(DATA.glob("estabelecimento-*.csv"))
        if not est_paths:
            raise SystemExit(
                "no estabelecimento-*.csv found under bench/.work/data -- generate fixtures first"
            )
        for kind in (
            "cnaes",
            "municipios",
            "naturezas",
            "qualificacoes",
            "paises",
            "motivos",
        ):
            transform.load_lookup_into_duckdb(con, kind, DATA / f"lookup_{kind}.csv")
        files = [
            ExtractedFile(
                kind="empresas", zip_name="empresa.zip", csv_path=DATA / "empresa.csv"
            ),
            ExtractedFile(
                kind="simples", zip_name="simples.zip", csv_path=DATA / "simples.csv"
            ),
            ExtractedFile(
                kind="socios", zip_name="socio.zip", csv_path=DATA / "socio.csv"
            ),
            *(
                ExtractedFile(
                    kind="estabelecimentos", zip_name=path.name, csv_path=path
                )
                for path in est_paths
            ),
        ]
        duplicates = transform.load_main_tables_into_duckdb(con, files)
        con.execute("DROP TABLE IF EXISTS estabelecimento")
        return est_paths, duplicates
    finally:
        con.close()


def _write_chunked_with_parts_codec(
    con: duckdb.DuckDBPyConnection,
    est_paths: list[Path],
    output_path: Path,
    parts_codec: str,
) -> dict[str, int]:
    """Production writer shape with only the transient parts codec parameterized."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    parts_dir = output_path.parent / f"{output_path.stem}.parts"
    shutil.rmtree(parts_dir, ignore_errors=True)
    parts_dir.mkdir(parents=True, exist_ok=True)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _cnae_map AS
        SELECT MAP(list(codigo), list(descricao)) AS m FROM (
            SELECT codigo, ANY_VALUE(descricao) AS descricao
            FROM lookup_cnaes
            GROUP BY codigo
        )
        """
    )
    icon = ibis.duckdb.from_connection(con)

    def materialize(table: str, expr) -> None:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
        )

    written_parts: list[Path] = []
    try:
        for index, csv_path in enumerate(est_paths):
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                continue
            transform._create_table_from_csvs(
                con,
                "estabelecimento",
                [csv_path],
                registry.main_table("estabelecimento").source,
            )
            if con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0] == 0:
                con.execute("DROP TABLE IF EXISTS estabelecimento")
                continue

            estabelecimento = icon.table("estabelecimento")
            materialize(
                "_emp_c",
                icon.table("empresa").semi_join(estabelecimento, "cnpj_basico"),
            )
            materialize(
                "_smp_c",
                icon.table("simples").semi_join(estabelecimento, "cnpj_basico"),
            )
            select_sql = transform._cnpjs_chunk_select_sql(
                "estabelecimento", "_emp_c", "_smp_c", "_cnae_map", order_by=False
            )
            part_path = parts_dir / f"chunk-{index}.parquet"
            con.execute(
                f"COPY ({select_sql}) TO '{part_path}' "
                f"(FORMAT PARQUET, COMPRESSION {parts_codec}, ROW_GROUP_SIZE 200000)"
            )
            written_parts.append(part_path)
            con.execute("DROP TABLE IF EXISTS estabelecimento")
            con.execute("DROP TABLE IF EXISTS _emp_c")
            con.execute("DROP TABLE IF EXISTS _smp_c")

        if not written_parts:
            raise RuntimeError("codec benchmark wrote no chunk parts")

        part_sizes = [path.stat().st_size for path in written_parts]
        parts_glob = parts_dir / "chunk-*.parquet"
        # Final output stays ZSTD for both variants.  The only changed variable
        # is the codec used for transient chunk parts.
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{parts_glob}') ORDER BY cnpj) "
            f"TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
        )
        return {
            "parts_total_bytes": sum(part_sizes),
            "largest_part_bytes": max(part_sizes),
            "part_count": len(part_sizes),
        }
    finally:
        con.execute("DROP TABLE IF EXISTS _cnae_map")
        con.execute("DROP TABLE IF EXISTS estabelecimento")
        con.execute("DROP TABLE IF EXISTS _emp_c")
        con.execute("DROP TABLE IF EXISTS _smp_c")
        shutil.rmtree(parts_dir, ignore_errors=True)


def _run_variant(
    con: duckdb.DuckDBPyConnection,
    est_paths: list[Path],
    codec: str,
    tag: str,
    *,
    keep: bool = False,
) -> dict[str, Any]:
    state_dir = _state_dir(codec)
    output_dir = state_dir / "out"
    output_path = output_dir / f"{tag}.parquet"
    parts_dir = output_dir / f"{output_path.stem}.parts"
    output_path.unlink(missing_ok=True)
    shutil.rmtree(parts_dir, ignore_errors=True)

    sampler = metrics._DiskPeakSampler(  # noqa: SLF001 -- benchmark reuses production sampler
        {
            "duckdb_tmp": state_dir / "duckdb_tmp",
            "parts": parts_dir,
            "state": state_dir,
        },
        interval=0.1,
        filesystem_path=state_dir,
    )
    sampler.start()
    wall_start = time.monotonic()
    cpu_start = time.process_time()
    try:
        part_stats = _write_chunked_with_parts_codec(con, est_paths, output_path, codec)
        wall_seconds = time.monotonic() - wall_start
        cpu_seconds = time.process_time() - cpu_start
    finally:
        peaks = sampler.stop()

    result: dict[str, Any] = {
        "codec": codec,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "final_bytes": output_path.stat().st_size,
        "database_bytes": _db_path(codec).stat().st_size,
        "duckdb_tmp_peak_bytes": peaks.get("duckdb_tmp", 0),
        "parts_peak_bytes": peaks.get("parts", 0),
        "state_peak_bytes": peaks.get("state", 0),
        "filesystem_used_peak_bytes": peaks.get("filesystem", 0),
        "filesystem_total_bytes": peaks.get("filesystem_total", 0),
        **part_stats,
    }
    if not keep:
        output_path.unlink(missing_ok=True)
    else:
        result["output_path"] = str(output_path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=N)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--json", type=Path, default=RESULT_ROOT / "codecs.json")
    args = parser.parse_args()

    shutil.rmtree(STATE_ROOT, ignore_errors=True)
    zstd_paths, zstd_duplicates = _setup_state("ZSTD")
    lz4_paths, lz4_duplicates = _setup_state("LZ4")
    if zstd_paths != lz4_paths:
        raise AssertionError(
            "codec states resolved different estabelecimento chunk lists"
        )
    if zstd_duplicates != lz4_duplicates:
        raise AssertionError("codec states observed different duplicate counts")

    con_zstd = open_production_connection(_db_path("ZSTD"))
    con_lz4 = open_production_connection(_db_path("LZ4"))
    try:
        environments = {
            "zstd": capture_environment(con_zstd, _db_path("ZSTD")),
            "lz4": capture_environment(con_lz4, _db_path("LZ4")),
        }
        print("verifying ZSTD/LZ4 transient-part outputs equivalent before timing...")
        verify_zstd = _run_variant(con_zstd, zstd_paths, "ZSTD", "verify", keep=True)
        verify_lz4 = _run_variant(con_lz4, lz4_paths, "LZ4", "verify", keep=True)
        zstd_output = Path(verify_zstd["output_path"])
        lz4_output = Path(verify_lz4["output_path"])
        try:
            assert_parquet_equivalent(
                zstd_output, lz4_output, "zstd-parts", "lz4-parts"
            )
            print("  outputs verified equivalent\n")
        finally:
            zstd_output.unlink(missing_ok=True)
            lz4_output.unlink(missing_ok=True)

        zstd_runs: list[dict[str, Any]] = []
        lz4_runs: list[dict[str, Any]] = []

        def run_zstd() -> float:
            run = _run_variant(con_zstd, zstd_paths, "ZSTD", f"zstd-{len(zstd_runs)}")
            zstd_runs.append(run)
            return float(run["wall_seconds"])

        def run_lz4() -> float:
            run = _run_variant(con_lz4, lz4_paths, "LZ4", f"lz4-{len(lz4_runs)}")
            lz4_runs.append(run)
            return float(run["wall_seconds"])

        result: ABResult = run_ab(
            n=args.repeats,
            seed=args.seed,
            fn_a=run_zstd,
            fn_b=run_lz4,
            label_a="zstd",
            label_b="lz4",
        )
        print("transient-parts codec results:")
        result.print_summary()
    finally:
        con_zstd.close()
        con_lz4.close()

    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(
        json.dumps(
            {
                "question": "transient chunk parts codec; final output fixed at ZSTD",
                "environments": environments,
                "duplicate_rows_collapsed_during_setup": zstd_duplicates,
                "runs": {"zstd": zstd_runs, "lz4": lz4_runs},
                **result.to_dict(),
            },
            indent=2,
        )
    )
    print(f"wrote {args.json}")
    shutil.rmtree(STATE_ROOT, ignore_errors=True)


if __name__ == "__main__":
    main()
