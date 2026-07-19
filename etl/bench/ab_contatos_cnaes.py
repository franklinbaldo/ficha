"""Definitive A/B: old multi-scan vs new one-scan, same process, same table,
interleaved iterations — cancels all cross-session thermal/background drift.

Loads the cached 1M estabelecimento once, then runs each variant's real COPY
(to a throwaway parquet, so write cost is included) N times, alternating
old/new so any drift hits both equally. Reports min (least-noise) per variant.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

from ficha_etl import registry, transform

logging.getLogger("ficha_etl").setLevel(logging.ERROR)

DATA = Path("bench/.work/data")
OUT = Path("bench/.work/ab")
OUT.mkdir(parents=True, exist_ok=True)
N = 5

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


def time_copy(con, sql, tag, i):
    path = OUT / f"{tag}-{i}.parquet"
    t0 = time.monotonic()
    con.execute(
        f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
    )
    dt = time.monotonic() - t0
    path.unlink(missing_ok=True)
    return dt


def main():
    con = duckdb.connect()
    est = sorted(DATA.glob("estabelecimento-*.csv"))
    print(f"loading estabelecimento from {len(est)} CSVs...")
    transform._create_table_from_csvs(
        con, "estabelecimento", est, registry.main_table("estabelecimento").source
    )
    n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    print(f"  {n:,} rows; {N} interleaved iterations per variant\n")

    results = {"contatos_old": [], "contatos_new": [], "cnaes_old": [], "cnaes_new": []}
    for i in range(N):
        # interleave so thermal drift hits old and new equally
        results["contatos_old"].append(time_copy(con, OLD_CONTATOS, "cont_old", i))
        results["contatos_new"].append(time_copy(con, NEW_CONTATOS, "cont_new", i))
        results["cnaes_old"].append(time_copy(con, OLD_CNAES, "cnae_old", i))
        results["cnaes_new"].append(time_copy(con, NEW_CNAES, "cnae_new", i))
        print(
            f"  iter {i}: "
            f"cont old={results['contatos_old'][-1]:.2f} new={results['contatos_new'][-1]:.2f}  "
            f"cnae old={results['cnaes_old'][-1]:.2f} new={results['cnaes_new'][-1]:.2f}"
        )

    print("\n=== min seconds (least noise) ===")
    for pair in ("contatos", "cnaes"):
        o = min(results[f"{pair}_old"])
        w = min(results[f"{pair}_new"])
        verdict = "NEW faster" if w < o else "OLD faster"
        print(f"  {pair:<10} old={o:.3f}  new={w:.3f}  ratio new/old={w / o:.2f}  -> {verdict}")
    con.close()


if __name__ == "__main__":
    main()
