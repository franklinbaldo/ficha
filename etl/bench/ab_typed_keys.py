"""A/B: does a UINTEGER companion join key beat VARCHAR(8) cnpj_basico for the
per-chunk cnpjs join `write_cnpjs_parquet_chunked` actually runs in production?

Methodology review on the original version of this script (before this
rewrite) found it measured a different shape than production: whole tables
loaded once via `duckdb.connect()` (in-memory, default threads), the typed
key added via a single `ALTER TABLE` + `UPDATE` on already-resident tables.
`write_cnpjs_parquet_chunked` never does that -- it loads ONE estabelecimento
CSV chunk at a time (via `_create_table_from_csvs`, exactly as production's
loader does) and semi-joins `empresa`/`simples` down to `_emp_c`/`_smp_c` for
JUST that chunk's keys before the big projection join. This rewrite runs the
SAME per-chunk semi-join + materialize + COPY sequence
`write_cnpjs_parquet_chunked` uses, on a fixed representative chunk, so the
"does the join key type matter" question is asked about the actual code path
the answer would change.

Uses the shared production profile (`bench/_profile.py`): file-backed
connection with production PRAGMAs, `load_main_tables_into_duckdb` (dedup
included -- the synthetic empresa/simples have injected duplicates, see
`benchmark.py`'s `generate()`), deterministic AB/BA alternation (fixed seed),
median + spread reported, never a single "best run".
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import duckdb
import ibis

from _profile import ABResult, capture_environment, open_production_connection, run_ab
from ficha_etl import registry, transform
from ficha_etl.transform import ExtractedFile

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
OUT = Path("bench/.work/ab")
DB_PATH = Path("bench/.work/ab_typed_keys.duckdb")
N = 5
SEED = 20260719  # fixed -- same alternation sequence every run, not "randomized"


def _setup(con: duckdb.DuckDBPyConnection) -> tuple[list[Path], float]:
    """Real production loader for lookups + empresa/simples/socio/estabelecimento
    (dedup included), then a one-time UINTEGER companion key materialized on
    the now-deduped empresa/simples -- a legitimate one-time cost paid once
    per snapshot run, not per chunk (the per-chunk key cost for
    estabelecimento is measured separately, inside the timed A/B, since
    production loads estabelecimento fresh per chunk).

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
        con.execute(f"ALTER TABLE {tbl} ADD COLUMN cnpj_basico_key UINTEGER")
        con.execute(f"UPDATE {tbl} SET cnpj_basico_key = TRY_CAST(cnpj_basico AS UINTEGER)")
    key_cost = time.monotonic() - t0

    return est_paths, key_cost


def _run_chunk(con: duckdb.DuckDBPyConnection, csv_path: Path, typed: bool, tag: str) -> float:
    """One iteration of the REAL write_cnpjs_parquet_chunked inner-loop body
    for a single chunk: load the chunk's estabelecimento CSV fresh (exactly
    as production does -- `_create_table_from_csvs`, not a table already
    resident from a prior iteration), semi-join empresa/simples down to
    `_emp_c`/`_smp_c` for this chunk's keys, project + COPY. `typed=True`
    additionally materializes the chunk's own `cnpj_basico_key` (a per-chunk
    cost, since production would pay it fresh each chunk too) and switches
    the semi-join + final join predicates to the typed column.
    """
    t0 = time.monotonic()
    transform._create_table_from_csvs(
        con, "estabelecimento", [csv_path], registry.main_table("estabelecimento").source
    )
    join_col = "cnpj_basico"
    if typed:
        con.execute("ALTER TABLE estabelecimento ADD COLUMN cnpj_basico_key UINTEGER")
        con.execute(
            "UPDATE estabelecimento SET cnpj_basico_key = TRY_CAST(cnpj_basico AS UINTEGER)"
        )
        join_col = "cnpj_basico_key"

    icon = ibis.duckdb.from_connection(con)
    est = icon.table("estabelecimento")

    def _materialize(table: str, expr) -> None:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
        )

    _materialize("_emp_c", icon.table("empresa").semi_join(est, join_col))
    _materialize("_smp_c", icon.table("simples").semi_join(est, join_col))

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
        f"COPY ({select_sql}) TO '{part_path}' (FORMAT PARQUET, COMPRESSION LZ4, ROW_GROUP_SIZE 200000)"
    )
    part_path.unlink(missing_ok=True)

    con.execute("DROP TABLE IF EXISTS estabelecimento")
    con.execute("DROP TABLE IF EXISTS _emp_c")
    con.execute("DROP TABLE IF EXISTS _smp_c")
    return time.monotonic() - t0


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

    result: ABResult = run_ab(
        n=args.repeats,
        seed=args.seed,
        fn_a=lambda: _run_chunk(con, chunk, typed=False, tag="varchar"),
        fn_b=lambda: _run_chunk(con, chunk, typed=True, tag="uint"),
        label_a="varchar",
        label_b="uint",
    )
    print(
        f"\n{args.repeats} AB/BA-alternated iterations (seed={args.seed}), one representative chunk:"
    )
    result.print_summary()

    con.close()

    if args.json:
        args.json.write_text(
            json.dumps(
                {"environment": env, "key_setup_seconds": round(key_cost, 4), **result.to_dict()},
                indent=2,
            )
        )
        print(f"  wrote {args.json}")


if __name__ == "__main__":
    main()
