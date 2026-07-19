"""A/B: old multi-scan vs new one-scan queries for cnpj_contatos/cnpj_cnaes.

Uses the shared production profile (`bench/_profile.py`): file-backed
connection with production PRAGMAs (memory_limit, temp_directory,
preserve_insertion_order, threads=1), deterministic AB/BA alternation (fixed
seed -- not "always OLD before NEW", which lets warm-cache/CPU-throttle drift
always favor the same side), median + spread reported, never a single "best
run wins".

Scope note: this script only reads `estabelecimento` (the queries under test
don't touch empresa/simples), so it loads that one table directly via
`_create_table_from_csvs` rather than the full `load_main_tables_into_duckdb`
-- there's no dedup path to exercise here (see `ab_typed_keys.py` and
`benchmark.py` for where empresa/simples dedup is actually measured).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import duckdb

from _profile import (
    ABResult,
    assert_parquet_equivalent,
    capture_environment,
    open_production_connection,
    run_ab,
)
from ficha_etl import registry, transform

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
OUT = Path("bench/.work/ab")
DB_PATH = Path("bench/.work/ab_contatos_cnaes.duckdb")
N = 5
SEED = 20260719  # fixed -- same alternation sequence every run, not "randomized"

OLD_CONTATOS = """
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj, cnpj_basico AS cnpj_base,
         'telefone' AS tipo, ddd_1 || telefone_1 AS valor, 1::INTEGER AS posicao
  FROM estabelecimento
  WHERE telefone_1 IS NOT NULL AND telefone_1 <> '' AND ddd_1 IS NOT NULL AND ddd_1 <> ''
  UNION ALL
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv, cnpj_basico, 'telefone', ddd_2 || telefone_2, 2::INTEGER
  FROM estabelecimento
  WHERE telefone_2 IS NOT NULL AND telefone_2 <> '' AND ddd_2 IS NOT NULL AND ddd_2 <> ''
  UNION ALL
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv, cnpj_basico, 'fax', ddd_fax || fax, 0::INTEGER
  FROM estabelecimento
  WHERE fax IS NOT NULL AND fax <> '' AND ddd_fax IS NOT NULL AND ddd_fax <> ''
  UNION ALL
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv, cnpj_basico, 'email', correio_eletronico, 0::INTEGER
  FROM estabelecimento
  WHERE correio_eletronico IS NOT NULL AND correio_eletronico <> ''
  ORDER BY tipo, valor, cnpj
"""

NEW_CONTATOS = """
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj, cnpj_basico AS cnpj_base,
         v.tipo AS tipo, v.valor AS valor, v.posicao::INTEGER AS posicao
  FROM estabelecimento,
       LATERAL (VALUES
         ('telefone', nullif(ddd_1, '')   || nullif(telefone_1, ''), 1),
         ('telefone', nullif(ddd_2, '')   || nullif(telefone_2, ''), 2),
         ('fax',      nullif(ddd_fax, '') || nullif(fax, ''),        0),
         ('email',    nullif(correio_eletronico, ''),                0)
       ) AS v(tipo, valor, posicao)
  WHERE v.valor IS NOT NULL AND v.valor <> ''
  ORDER BY tipo, valor, cnpj
"""

OLD_CNAES = """
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj, cnpj_basico AS cnpj_base,
         cnae_fiscal_principal AS cnae_codigo, 0::INTEGER AS posicao
  FROM estabelecimento
  WHERE cnae_fiscal_principal IS NOT NULL AND cnae_fiscal_principal <> ''
  UNION ALL
  SELECT cnpj_basico || cnpj_ordem || cnpj_dv, cnpj_basico, trim(s.value) AS cnae_codigo,
         s.idx::INTEGER AS posicao
  FROM estabelecimento,
       LATERAL (SELECT idx, unnest AS value FROM (
         SELECT generate_subscripts(arr, 1) AS idx, unnest(arr) AS unnest
         FROM (SELECT str_split(cnae_fiscal_secundaria, ',') AS arr) t)) s
  WHERE cnae_fiscal_secundaria IS NOT NULL AND cnae_fiscal_secundaria <> ''
  ORDER BY cnae_codigo, posicao, cnpj_base
"""

NEW_CNAES = """
  WITH _c AS (
    SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj, cnpj_basico AS cnpj_base,
      list_concat(
        CASE WHEN nullif(cnae_fiscal_principal, '') IS NOT NULL
             THEN [{'codigo': cnae_fiscal_principal, 'posicao': 0::INTEGER}]
             ELSE []::STRUCT(codigo VARCHAR, posicao INTEGER)[] END,
        CASE WHEN nullif(cnae_fiscal_secundaria, '') IS NOT NULL
             THEN list_transform(str_split(cnae_fiscal_secundaria, ','),
                    (x, i) -> {'codigo': trim(x), 'posicao': i::INTEGER})
             ELSE []::STRUCT(codigo VARCHAR, posicao INTEGER)[] END
      ) AS cnaes
    FROM estabelecimento
  )
  SELECT cnpj, cnpj_base, u.codigo AS cnae_codigo, u.posicao::INTEGER AS posicao
  FROM _c, UNNEST(cnaes) AS t(u)
  ORDER BY cnae_codigo, posicao, cnpj_base
"""


def _copy(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> float:
    t0 = time.monotonic()
    con.execute(
        f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
    )
    return time.monotonic() - t0


def _time_copy(con: duckdb.DuckDBPyConnection, sql: str, tag: str) -> float:
    path = OUT / f"{tag}.parquet"
    dt = _copy(con, sql, path)
    path.unlink(missing_ok=True)
    return dt


def _verify_equivalent(
    con: duckdb.DuckDBPyConnection, pair: str, old_sql: str, new_sql: str
) -> None:
    """Untimed, run once before the timed A/B loop -- proves `old_sql` and
    `new_sql` produce the same rows before a wall-clock difference between
    them is allowed to mean anything.
    """
    old_path = OUT / f"{pair}_verify_old.parquet"
    new_path = OUT / f"{pair}_verify_new.parquet"
    _copy(con, old_sql, old_path)
    _copy(con, new_sql, new_path)
    try:
        assert_parquet_equivalent(old_path, new_path, "old", "new")
        print(f"  {pair}: old/new output verified equivalent")
    finally:
        old_path.unlink(missing_ok=True)
        new_path.unlink(missing_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repeats", type=int, default=N, help="A/B iterations per pair")
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

    est = sorted(DATA.glob("estabelecimento-*.csv"))
    if not est:
        raise SystemExit(
            "no estabelecimento-*.csv found under bench/.work/data — run benchmark.py first"
        )
    print(f"loading estabelecimento from {len(est)} CSVs...")
    transform._create_table_from_csvs(
        con, "estabelecimento", est, registry.main_table("estabelecimento").source
    )
    n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    print(f"  {n:,} rows; {args.repeats} AB/BA-alternated iterations per pair (seed={args.seed})\n")

    pairs = (
        ("contatos", OLD_CONTATOS, NEW_CONTATOS),
        ("cnaes", OLD_CNAES, NEW_CNAES),
    )
    print("verifying old/new produce equivalent output before timing...")
    for pair, old_sql, new_sql in pairs:
        _verify_equivalent(con, pair, old_sql, new_sql)
    print()

    results: dict[str, ABResult] = {}
    for pair, old_sql, new_sql in pairs:
        results[pair] = run_ab(
            n=args.repeats,
            seed=args.seed,
            fn_a=lambda sql=old_sql, tag=f"{pair}_old": _time_copy(con, sql, tag),
            fn_b=lambda sql=new_sql, tag=f"{pair}_new": _time_copy(con, sql, tag),
            label_a="old",
            label_b="new",
        )

    print("=== results (median + spread, never a single best run) ===")
    for pair, result in results.items():
        print(f"\n{pair}:")
        result.print_summary()

    con.close()

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {"environment": env, "results": {k: v.to_dict() for k, v in results.items()}},
                indent=2,
            )
        )
        print(f"\n  wrote {args.json}")


if __name__ == "__main__":
    main()
