"""A/B: does a UINTEGER companion join key beat VARCHAR(8) cnpj_basico for the
two big cnpjs joins (empresa, simples)?

Runs the REAL `_cnpjs_chunk_select_sql` projection both ways — identical except
the two LEFT JOIN predicates switch from `cnpj_basico` (VARCHAR) to
`cnpj_basico_key` (UINTEGER) — interleaved in one process on the same loaded
tables, N times, min reported. Also reports the one-time cost of materializing
the companion key column, so we can tell whether the join win pays for it.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

from ficha_etl import transform

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
OUT = Path("bench/.work/ab")
OUT.mkdir(parents=True, exist_ok=True)
N = 5


def _load(con):
    for kind in ("cnaes", "municipios", "naturezas", "qualificacoes", "paises", "motivos"):
        transform.load_lookup_into_duckdb(con, kind, DATA / f"lookup_{kind}.csv")
    transform._create_table_from_csvs(
        con, "empresa", [DATA / "empresa.csv"], transform._EMPRESA_COLUMNS
    )
    transform._create_table_from_csvs(
        con, "simples", [DATA / "simples.csv"], transform._SIMPLES_COLUMNS
    )
    est = sorted(DATA.glob("estabelecimento-*.csv"))
    transform._create_table_from_csvs(con, "est", est, transform._ESTABELECIMENTO_COLUMNS)
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _cnae_map AS
        SELECT MAP(list(codigo), list(descricao)) AS m FROM (
            SELECT codigo, ANY_VALUE(descricao) AS descricao FROM lookup_cnaes GROUP BY codigo
        )
        """
    )
    # emp/smp aliases the real query expects
    con.execute("CREATE OR REPLACE TABLE emp AS SELECT * FROM empresa")
    con.execute("CREATE OR REPLACE TABLE smp AS SELECT * FROM simples")


def _add_keys(con) -> float:
    """Materialize UINTEGER companion keys on est/emp/smp. Returns seconds."""
    t0 = time.monotonic()
    for tbl in ("est", "emp", "smp"):
        con.execute(f"ALTER TABLE {tbl} ADD COLUMN cnpj_basico_key UINTEGER")
        con.execute(f"UPDATE {tbl} SET cnpj_basico_key = TRY_CAST(cnpj_basico AS UINTEGER)")
    return time.monotonic() - t0


def _copy(con, sql, tag, i) -> float:
    path = OUT / f"{tag}-{i}.parquet"
    t0 = time.monotonic()
    con.execute(
        f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION LZ4, ROW_GROUP_SIZE 200000)"
    )
    dt = time.monotonic() - t0
    path.unlink(missing_ok=True)
    return dt


def main():
    con = duckdb.connect()
    _load(con)
    n = con.execute("SELECT COUNT(*) FROM est").fetchone()[0]

    str_sql = transform._cnpjs_chunk_select_sql("est", "emp", "smp", "_cnae_map", order_by=False)
    int_sql = str_sql.replace(
        "LEFT JOIN emp ON emp.cnpj_basico = est.cnpj_basico",
        "LEFT JOIN emp ON emp.cnpj_basico_key = est.cnpj_basico_key",
    ).replace(
        "LEFT JOIN smp ON smp.cnpj_basico = est.cnpj_basico",
        "LEFT JOIN smp ON smp.cnpj_basico_key = est.cnpj_basico_key",
    )
    # sanity: the replace must actually have changed the two join lines
    assert int_sql != str_sql, "join-predicate replace matched nothing — check spacing"
    assert int_sql.count("cnpj_basico_key = est.cnpj_basico_key") == 2

    key_cost = _add_keys(con)
    print(f"est={n:,} rows; companion-key materialization (3 tables) = {key_cost:.2f}s")
    print(f"{N} interleaved iterations per variant\n")

    s_times, i_times = [], []
    for k in range(N):
        s_times.append(_copy(con, str_sql, "str", k))
        i_times.append(_copy(con, int_sql, "int", k))
        print(f"  iter {k}: varchar={s_times[-1]:.2f}s  uint={i_times[-1]:.2f}s")

    s, i = min(s_times), min(i_times)
    print("\n=== min seconds (least noise) ===")
    print(f"  varchar cnpj_basico join : {s:.3f}s")
    print(f"  uint    companion join   : {i:.3f}s   ratio uint/varchar={i / s:.2f}")
    saved = s - i
    print(f"  per-run join delta       : {saved:+.3f}s   (one-time key cost {key_cost:.2f}s)")
    if saved <= 0:
        print("  -> VARCHAR join already as fast/faster; typed key not worth it")
    else:
        print(
            f"  -> uint saves {saved:.3f}s/run; pays back the key cost after "
            f"{key_cost / saved:.1f} runs of this join"
        )
    con.close()


if __name__ == "__main__":
    main()
