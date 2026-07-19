"""A/B: does a UINTEGER companion join key beat VARCHAR(8) cnpj_basico for the
per-chunk cnpjs join `write_cnpjs_parquet_chunked` actually runs in production?

Uses the shared production profile (`bench/_profile.py`): file-backed
connection with production PRAGMAs, `load_main_tables_into_duckdb` (dedup
included -- the synthetic empresa/simples have injected duplicates, see
`benchmark.py`'s `generate()`), strict AB/BA alternation (seed picks only the
starting side), median + spread reported, never a single "best run".

The varchar and typed sides use INDEPENDENT table state: `empresa`/`simples`
stay pure VARCHAR (what the varchar baseline actually joins against in
production -- it never carries a key column at all) and a separate
`empresa_typed`/`simples_typed` pair carries the UINTEGER companion key,
materialized via CREATE...AS SELECT (one CTAS pass, the shape a typed-load
step would actually take) rather than ALTER TABLE + UPDATE on the shared
table (which would (a) contaminate the varchar baseline's join with an unused
extra column production doesn't have, and (b) measure an in-place schema
mutation, not a load-time materialization). Per-chunk estabelecimento is the
same: the typed variant CTASes its own typed `estabelecimento` from a raw
staging load, the varchar variant never gains the column at all.
"""

from __future__ import annotations

import argparse
import json
import logging
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
OUT = Path("bench/.work/ab")
DB_PATH = Path("bench/.work/ab_typed_keys.duckdb")
N = 5
SEED = 20260719  # fixed -- same starting side + alternation every run, not "randomized"


def _setup(con: duckdb.DuckDBPyConnection) -> tuple[list[Path], float]:
    """Real production loader for lookups + empresa/simples/socio/estabelecimento
    (dedup included), leaving `empresa`/`simples` pure VARCHAR -- exactly what
    production has -- then a ONE-TIME `empresa_typed`/`simples_typed` pair
    materialized via CREATE...AS SELECT with the UINTEGER companion key. This
    is a real one-time cost paid once per snapshot run; it's returned
    separately here but folded into the typed side's reported end-to-end
    total in `main()`, not left as a side observation nobody adds up.

    load_main_tables_into_duckdb full-loads estabelecimento as a side effect
    (that's what it does in production too, for the scan-based writers) --
    dropped immediately after, mirroring transform_snapshot's own "drop
    estabelecimento before the chunked writer reloads it per-chunk" step,
    since this script only benchmarks the per-chunk join.
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
    print(f"  load_main_tables_into_duckdb collapsed {dupes} duplicate cnpj_basico row(s)")
    con.execute("DROP TABLE IF EXISTS estabelecimento")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _cnae_map AS
        SELECT MAP(list(codigo), list(descricao)) AS m FROM (
            SELECT codigo, ANY_VALUE(descricao) AS descricao FROM lookup_cnaes GROUP BY codigo
        )
        """
    )

    t0 = time.monotonic()
    for tbl in ("empresa", "simples"):
        con.execute(
            f"CREATE OR REPLACE TABLE {tbl}_typed AS "
            f"SELECT *, TRY_CAST(cnpj_basico AS UINTEGER) AS cnpj_basico_key FROM {tbl}"
        )
    key_cost = time.monotonic() - t0

    return est_paths, key_cost


def _run_chunk(
    con: duckdb.DuckDBPyConnection, csv_path: Path, typed: bool, tag: str, *, keep: bool = False
) -> tuple[float, Path]:
    """One iteration of the REAL write_cnpjs_parquet_chunked inner-loop body
    for a single chunk: load the chunk's estabelecimento CSV fresh (exactly
    as production does -- `_create_table_from_csvs`, not a table already
    resident from a prior iteration), semi-join empresa/simples down to
    `_emp_c`/`_smp_c` for this chunk's keys, project + COPY (ZSTD, matching
    production's actual codec -- LZ4 was never validated for production disk
    peak and isn't what write_cnpjs_parquet_chunked uses).

    `typed=True` loads into a raw staging table first, then CTASes the real
    `estabelecimento` with its own `cnpj_basico_key` added (a per-chunk cost,
    since production would pay it fresh each chunk too) and joins
    `empresa_typed`/`simples_typed` on that key. `typed=False` loads straight
    into `estabelecimento` and joins the untouched `empresa`/`simples` on
    `cnpj_basico` -- no key column exists anywhere on this path, matching
    what production's varchar-only pipeline actually has today.
    """
    t0 = time.monotonic()
    spec = registry.main_table("estabelecimento").source
    if typed:
        transform._create_table_from_csvs(con, "_est_raw", [csv_path], spec)
        con.execute(
            "CREATE OR REPLACE TABLE estabelecimento AS "
            "SELECT *, TRY_CAST(cnpj_basico AS UINTEGER) AS cnpj_basico_key FROM _est_raw"
        )
        con.execute("DROP TABLE IF EXISTS _est_raw")
        join_col = "cnpj_basico_key"
        emp_src, smp_src = "empresa_typed", "simples_typed"
    else:
        transform._create_table_from_csvs(con, "estabelecimento", [csv_path], spec)
        join_col = "cnpj_basico"
        emp_src, smp_src = "empresa", "simples"

    icon = ibis.duckdb.from_connection(con)
    est = icon.table("estabelecimento")

    def _materialize(table: str, expr) -> None:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
        )

    _materialize("_emp_c", icon.table(emp_src).semi_join(est, join_col))
    _materialize("_smp_c", icon.table(smp_src).semi_join(est, join_col))

    select_sql = transform._cnpjs_chunk_select_sql(
        "estabelecimento", "_emp_c", "_smp_c", "_cnae_map", order_by=False
    )
    if typed:
        old_a = "LEFT JOIN _emp_c ON _emp_c.cnpj_basico = estabelecimento.cnpj_basico"
        new_a = "LEFT JOIN _emp_c ON _emp_c.cnpj_basico_key = estabelecimento.cnpj_basico_key"
        old_b = "LEFT JOIN _smp_c ON _smp_c.cnpj_basico = estabelecimento.cnpj_basico"
        new_b = "LEFT JOIN _smp_c ON _smp_c.cnpj_basico_key = estabelecimento.cnpj_basico_key"
        assert old_a in select_sql and old_b in select_sql, (
            "join predicate not found — _cnpjs_chunk_select_sql's SQL shape changed, "
            "update this script's replace targets"
        )
        select_sql = select_sql.replace(old_a, new_a).replace(old_b, new_b)

    part_path = OUT / f"{tag}.parquet"
    con.execute(
        f"COPY ({select_sql}) TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
    )
    if not keep:
        part_path.unlink(missing_ok=True)

    con.execute("DROP TABLE IF EXISTS estabelecimento")
    con.execute("DROP TABLE IF EXISTS _emp_c")
    con.execute("DROP TABLE IF EXISTS _smp_c")
    return time.monotonic() - t0, part_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repeats", type=int, default=N, help="A/B iterations")
    ap.add_argument("--seed", type=int, default=SEED, help="alternation seed")
    ap.add_argument("--json", type=Path, default=None)
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    DB_PATH.unlink(missing_ok=True)
    con = open_production_connection(DB_PATH)
    env = capture_environment(con, DB_PATH)
    print(
        f"production profile: threads={env['threads']} memory_limit={env['memory_limit']} "
        f"duckdb={env['duckdb_version']}"
    )

    est_paths, key_cost = _setup(con)
    if not est_paths:
        raise SystemExit(
            "no estabelecimento-*.csv found under bench/.work/data — run benchmark.py first"
        )
    chunk = est_paths[0]  # fixed representative chunk — same file for every iteration
    chunk_mib = chunk.stat().st_size / (1024 * 1024)
    print(
        f"chunk={chunk.name} ({chunk_mib:.1f} MiB); companion-key setup on empresa/simples = "
        f"{key_cost:.2f}s"
    )

    print("verifying varchar/typed output equivalent before timing...")
    _, varchar_path = _run_chunk(con, chunk, typed=False, tag="verify_varchar", keep=True)
    _, typed_path = _run_chunk(con, chunk, typed=True, tag="verify_uint", keep=True)
    try:
        assert_parquet_equivalent(varchar_path, typed_path, "varchar", "uint")
        print("  varchar/uint output verified equivalent\n")
    finally:
        varchar_path.unlink(missing_ok=True)
        typed_path.unlink(missing_ok=True)

    result: ABResult = run_ab(
        n=args.repeats,
        seed=args.seed,
        fn_a=lambda: _run_chunk(con, chunk, typed=False, tag="varchar")[0],
        fn_b=lambda: _run_chunk(con, chunk, typed=True, tag="uint")[0],
        label_a="varchar",
        label_b="uint",
    )
    print(
        f"{args.repeats} AB/BA-alternated iterations (seed={args.seed}), one representative chunk:"
    )
    result.print_summary()

    total_varchar = sum(result.times_a)
    total_uint = key_cost + sum(result.times_b)
    print(
        f"\n  end-to-end total across {args.repeats} chunk(s), typed side including its "
        f"one-time {key_cost:.2f}s key setup:"
    )
    print(f"    varchar total = {total_varchar:.3f}s")
    print(
        f"    uint    total = {total_uint:.3f}s  (= {key_cost:.3f}s setup + {sum(result.times_b):.3f}s chunks)"
    )

    con.close()

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "environment": env,
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
