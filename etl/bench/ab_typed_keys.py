"""A/B: does a UINTEGER companion join key beat VARCHAR(8) cnpj_basico for the
per-chunk cnpjs join `write_cnpjs_parquet_chunked` actually runs in production?

Uses the shared production profile (`bench/_profile.py`): file-backed
connections with production PRAGMAs, `load_main_tables_into_duckdb` (dedup
included -- the synthetic empresa/simples have injected duplicates, see
`benchmark.py`'s `generate()`), strict AB/BA alternation (seed picks only the
starting side), median + spread reported, never a single "best run".

The two variants have independent execution state, not merely different table
names in one database:

* VARCHAR runs in `bench/.work/typed-keys/varchar/bench.duckdb`, with its own
  `duckdb_tmp`, and contains only the production VARCHAR `empresa`/`simples`.
* UINTEGER runs in `bench/.work/typed-keys/uint/bench.duckdb`, with its own
  `duckdb_tmp`; it materializes `empresa_typed`/`simples_typed` via CTAS,
  includes that one-time cost in the typed total, then drops the untyped source
  tables before the A/B starts.

The CSV fixture and representative chunk are shared, and an untimed parquet
comparison proves both variants return the same schema and multiset of rows
before any timing is accepted.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from pathlib import Path

import duckdb
import ibis

from _profile import (
    ABResult,
    assert_parquet_equivalent,
    capture_environment,
    open_production_connection,
    run_ab,
)
from ficha_etl import registry, transform
from ficha_etl.transform import ExtractedFile

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
STATE_ROOT = Path("bench/.work/typed-keys")
OUT = STATE_ROOT / "out"
VARCHAR_DB_PATH = STATE_ROOT / "varchar" / "bench.duckdb"
UINT_DB_PATH = STATE_ROOT / "uint" / "bench.duckdb"
N = 5
SEED = 20260719  # fixed -- same starting side + alternation every run, not "randomized"


def _setup(con: duckdb.DuckDBPyConnection, *, typed: bool, label: str) -> tuple[list[Path], float]:
    """Load one isolated benchmark state and return (est CSVs, key setup cost).

    Both states use the real production lookup/main-table loaders, including
    empresa/simples dedup. The VARCHAR state keeps the resulting tables exactly
    as production has them today. The typed state CTASes independent typed
    tables and drops the untyped sources before timing, so the A/B never runs
    with both competing table sets resident in the same database.
    """
    est_paths = sorted(DATA.glob("estabelecimento-*.csv"))
    for kind in ("cnaes", "municipios", "naturezas", "qualificacoes", "paises", "motivos"):
        transform.load_lookup_into_duckdb(con, kind, DATA / f"lookup_{kind}.csv")

    main_table_files = [
        ExtractedFile(kind="empresas", zip_name="empresa.zip", csv_path=DATA / "empresa.csv"),
        ExtractedFile(kind="simples", zip_name="simples.zip", csv_path=DATA / "simples.csv"),
        ExtractedFile(kind="socios", zip_name="socio.zip", csv_path=DATA / "socio.csv"),
        *(ExtractedFile(kind="estabelecimentos", zip_name=p.name, csv_path=p) for p in est_paths),
    ]
    dupes = transform.load_main_tables_into_duckdb(con, main_table_files)
    print(f"  {label}: load_main_tables_into_duckdb collapsed {dupes} duplicate cnpj_basico row(s)")
    con.execute("DROP TABLE IF EXISTS estabelecimento")

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

    key_cost = 0.0
    if typed:
        t0 = time.monotonic()
        for table in ("empresa", "simples"):
            con.execute(
                f"CREATE OR REPLACE TABLE {table}_typed AS "
                f"SELECT *, TRY_CAST(cnpj_basico AS UINTEGER) AS cnpj_basico_key FROM {table}"
            )
            con.execute(f"DROP TABLE {table}")
        key_cost = time.monotonic() - t0

    return est_paths, key_cost


def _run_chunk(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    typed: bool,
    tag: str,
    *,
    keep: bool = False,
) -> tuple[float, Path]:
    """Run one production-shaped chunk iteration in the selected state.

    The VARCHAR connection loads the chunk directly and joins its unmodified
    `empresa`/`simples`. The typed connection stages the same CSV, CTASes the
    chunk-local key, and joins `empresa_typed`/`simples_typed`. Both use the
    real semi-join/materialization path, final projection, ZSTD codec and row
    group size from the production writer.
    """
    started = time.monotonic()
    spec = registry.main_table("estabelecimento").source
    part_path = OUT / f"{tag}.parquet"

    try:
        if typed:
            transform._create_table_from_csvs(con, "_est_raw", [csv_path], spec)
            con.execute(
                "CREATE OR REPLACE TABLE estabelecimento AS "
                "SELECT *, TRY_CAST(cnpj_basico AS UINTEGER) AS cnpj_basico_key FROM _est_raw"
            )
            con.execute("DROP TABLE IF EXISTS _est_raw")
            join_col = "cnpj_basico_key"
            empresa_source = "empresa_typed"
            simples_source = "simples_typed"
        else:
            transform._create_table_from_csvs(con, "estabelecimento", [csv_path], spec)
            join_col = "cnpj_basico"
            empresa_source = "empresa"
            simples_source = "simples"

        icon = ibis.duckdb.from_connection(con)
        estabelecimento = icon.table("estabelecimento")

        def _materialize(table: str, expr) -> None:
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
            )

        _materialize("_emp_c", icon.table(empresa_source).semi_join(estabelecimento, join_col))
        _materialize("_smp_c", icon.table(simples_source).semi_join(estabelecimento, join_col))

        select_sql = transform._cnpjs_chunk_select_sql(
            "estabelecimento", "_emp_c", "_smp_c", "_cnae_map", order_by=False
        )
        if typed:
            old_empresa_join = (
                "LEFT JOIN _emp_c ON _emp_c.cnpj_basico = estabelecimento.cnpj_basico"
            )
            new_empresa_join = (
                "LEFT JOIN _emp_c ON _emp_c.cnpj_basico_key = estabelecimento.cnpj_basico_key"
            )
            old_simples_join = (
                "LEFT JOIN _smp_c ON _smp_c.cnpj_basico = estabelecimento.cnpj_basico"
            )
            new_simples_join = (
                "LEFT JOIN _smp_c ON _smp_c.cnpj_basico_key = estabelecimento.cnpj_basico_key"
            )
            assert old_empresa_join in select_sql and old_simples_join in select_sql, (
                "join predicate not found -- _cnpjs_chunk_select_sql's SQL shape changed; "
                "update this benchmark's replace targets"
            )
            select_sql = select_sql.replace(old_empresa_join, new_empresa_join).replace(
                old_simples_join, new_simples_join
            )

        con.execute(
            f"COPY ({select_sql}) TO '{part_path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
        )
        elapsed = time.monotonic() - started
        if not keep:
            part_path.unlink(missing_ok=True)
        return elapsed, part_path
    finally:
        con.execute("DROP TABLE IF EXISTS estabelecimento")
        con.execute("DROP TABLE IF EXISTS _est_raw")
        con.execute("DROP TABLE IF EXISTS _emp_c")
        con.execute("DROP TABLE IF EXISTS _smp_c")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=N, help="A/B iterations")
    parser.add_argument("--seed", type=int, default=SEED, help="alternation seed")
    parser.add_argument("--json", type=Path, default=None)
    args = parser.parse_args()

    shutil.rmtree(STATE_ROOT, ignore_errors=True)
    OUT.mkdir(parents=True, exist_ok=True)

    con_varchar = open_production_connection(VARCHAR_DB_PATH)
    con_uint = open_production_connection(UINT_DB_PATH)
    try:
        environments = {
            "varchar": capture_environment(con_varchar, VARCHAR_DB_PATH),
            "uint": capture_environment(con_uint, UINT_DB_PATH),
        }
        print(
            "production profile: "
            f"threads={environments['varchar']['threads']} "
            f"memory_limit={environments['varchar']['memory_limit']} "
            f"duckdb={environments['varchar']['duckdb_version']}"
        )
        print(f"  varchar db: {VARCHAR_DB_PATH}")
        print(f"  uint db:    {UINT_DB_PATH}")

        varchar_paths, varchar_setup_cost = _setup(con_varchar, typed=False, label="varchar")
        uint_paths, key_cost = _setup(con_uint, typed=True, label="uint")
        if not varchar_paths or not uint_paths:
            raise SystemExit(
                "no estabelecimento-*.csv found under bench/.work/data -- run benchmark.py first"
            )
        if varchar_paths != uint_paths:
            raise AssertionError("varchar and uint states resolved different CSV chunk lists")
        if varchar_setup_cost != 0.0:
            raise AssertionError("varchar setup unexpectedly reported a typed-key cost")

        chunk = varchar_paths[0]
        chunk_mib = chunk.stat().st_size / (1024 * 1024)
        print(
            f"chunk={chunk.name} ({chunk_mib:.1f} MiB); "
            f"companion-key setup on uint empresa/simples = {key_cost:.2f}s"
        )

        print("verifying varchar/typed output equivalent before timing...")
        _, varchar_path = _run_chunk(
            con_varchar, chunk, typed=False, tag="verify_varchar", keep=True
        )
        _, uint_path = _run_chunk(con_uint, chunk, typed=True, tag="verify_uint", keep=True)
        try:
            assert_parquet_equivalent(varchar_path, uint_path, "varchar", "uint")
            print("  varchar/uint output verified equivalent\n")
        finally:
            varchar_path.unlink(missing_ok=True)
            uint_path.unlink(missing_ok=True)

        result: ABResult = run_ab(
            n=args.repeats,
            seed=args.seed,
            fn_a=lambda: _run_chunk(con_varchar, chunk, typed=False, tag="varchar")[0],
            fn_b=lambda: _run_chunk(con_uint, chunk, typed=True, tag="uint")[0],
            label_a="varchar",
            label_b="uint",
        )
        print(
            f"{args.repeats} AB/BA-alternated iterations (seed={args.seed}), "
            "one representative chunk:"
        )
        result.print_summary()

        total_varchar = sum(result.times_a)
        total_uint = key_cost + sum(result.times_b)
        print(
            f"\n  end-to-end total across {args.repeats} chunk(s), typed side including "
            f"its one-time {key_cost:.2f}s key setup:"
        )
        print(f"    varchar total = {total_varchar:.3f}s")
        print(
            f"    uint    total = {total_uint:.3f}s  "
            f"(= {key_cost:.3f}s setup + {sum(result.times_b):.3f}s chunks)"
        )
    finally:
        con_varchar.close()
        con_uint.close()

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "environments": environments,
                    "state_isolation": {
                        "varchar_db": str(VARCHAR_DB_PATH),
                        "uint_db": str(UINT_DB_PATH),
                    },
                    "key_setup_seconds": round(key_cost, 4),
                    "end_to_end_total_seconds": {
                        "varchar": round(total_varchar, 4),
                        "uint": round(total_uint, 4),
                    },
                    **result.to_dict(),
                },
                indent=2,
            )
        )
        print(f"  wrote {args.json}")


if __name__ == "__main__":
    main()
