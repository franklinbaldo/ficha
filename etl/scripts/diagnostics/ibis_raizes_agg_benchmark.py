#!/usr/bin/env python3
"""Benchmark: raizes `LIST(DISTINCT)` aggregation — the historical OOM locus.

This is the companion to `ibis_cnpjs_benchmark.py` and the piece ADR 0017 flags
as the *risky* target: the `raizes` aggregation that builds `ufs_atuacao` and
`cnaes_principais_distintos` (one distinct list per cnpj_base). Production
(`transform.write_raizes_parquet`) deliberately does NOT use `LIST(DISTINCT ...)`
— per docs/perf-plan-2026-05.md §1.1, DuckDB's hash-aggregate cannot spill the
per-group DISTINCT hash-set state, so with ~50M groups it OOM'd at 5.5 GiB
(PR #24, run 25522678418). Production replaced it with a two-step pre-dedup
(`SELECT DISTINCT` → `GROUP BY list()`), a pair of vanilla aggregates DuckDB
*can* spill.

The migration danger this benchmark quantifies: idiomatic Ibis compiles the
distinct-list aggregation straight back to the OOM shape.

    t.uf.collect(distinct=True)   ->  ARRAY_AGG(DISTINCT uf)     [== LIST(DISTINCT), unsafe]
    t.select(..).distinct()
      .agg(uf.collect())          ->  ARRAY_AGG(uf) over (SELECT DISTINCT ..) [safe, == prod]

So it measures FOUR paths on the same data, under `threads=1` and a tight
`memory_limit`, reporting peak temp-spill + wall time, and checks all four
produce set-equivalent lists per group:

    sql_naive       hand SQL, LIST(DISTINCT)                — the OOM shape
    sql_predup      hand SQL, two-step pre-dedup            — the production shape
    ibis_idiomatic  Ibis collect(distinct=True)             — compiles to LIST(DISTINCT)
    ibis_predup     Ibis .distinct() then .collect()        — compiles to the safe shape

Run
---
    uv run python scripts/diagnostics/ibis_raizes_agg_benchmark.py
    FICHA_BENCH_EMPRESA_ROWS=20000000 FICHA_BENCH_MEMORY_GB=2 \
        uv run python scripts/diagnostics/ibis_raizes_agg_benchmark.py

Env: FICHA_BENCH_EMPRESA_ROWS (groups), FICHA_BENCH_DUP (rows per group),
FICHA_BENCH_MEMORY_GB, FICHA_BENCH_THREADS, FICHA_BENCH_KEEP.
"""

from __future__ import annotations

import os
import shutil
import threading
import time
from pathlib import Path

import duckdb
import ibis

EMP_ROWS = int(os.environ.get("FICHA_BENCH_EMPRESA_ROWS", "5000000"))
DUP = int(os.environ.get("FICHA_BENCH_DUP", "5"))  # estabelecimentos per group
EST_ROWS = EMP_ROWS * DUP
MEMORY_GB = int(os.environ.get("FICHA_BENCH_MEMORY_GB", "2"))
THREADS = int(os.environ.get("FICHA_BENCH_THREADS", "1"))
KEEP = bool(os.environ.get("FICHA_BENCH_KEEP"))

WORK = Path(os.environ.get("FICHA_BENCH_DIR", "/tmp/ficha_raizes_bench")).resolve()


def build_data(con: duckdb.DuckDBPyConnection) -> None:
    # empresa: one row per group (cnpj_base). Production's raizes is DRIVEN by
    # empresa — every path joins its aggregate(s) onto it — so all four paths
    # pay the same join cost and differ ONLY in the aggregation shape.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE empresa AS
        SELECT printf('%08d', i) AS cnpj_basico
        FROM range({EMP_ROWS}) t(i)
        """
    )
    # estabelecimento: EST_ROWS rows over EMP_ROWS groups (DUP rows/group).
    # Each group spans up to DUP distinct UFs and CNAEs — the exact shape that
    # makes per-group DISTINCT hash-sets expensive (many groups, few-but->1
    # distinct values each). Some empty UF/CNAE to exercise the NOT NULL/<>''
    # filter that production applies before dedup.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE estabelecimento AS
        SELECT
            printf('%08d', i % {EMP_ROWS}) AS cnpj_basico,
            -- 27 UFs; (i // EMP_ROWS) shifts the UF per repeat so each group
            -- accumulates several distinct UFs across its DUP rows.
            CASE WHEN i % 11 = 0 THEN ''
                 ELSE ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
                       'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
                      [((i / {EMP_ROWS})::BIGINT % 27) + 1] END AS uf,
            CASE WHEN i % 13 = 0 THEN ''
                 ELSE printf('%07d', ((i / {EMP_ROWS})::BIGINT % 1300) * 1000) END
                 AS cnae_fiscal_principal
        FROM range({EST_ROWS}) t(i)
        """
    )


# --------------------------------------------------------------------------- #
# Hand-SQL paths
# --------------------------------------------------------------------------- #
def sql_naive() -> str:
    # The OOM shape: DISTINCT inside the list aggregate, single GROUP BY.
    # The OOM shape: DISTINCT inside the list aggregate (single GROUP BY),
    # joined onto empresa. NULLIF(...,'') so empties are excluded (array_agg
    # ignores NULLs) to match the other paths.
    return """
        SELECT
            emp.cnpj_basico AS cnpj_base,
            COALESCE(agg.ufs_atuacao, [])          AS ufs_atuacao,
            COALESCE(agg.cnaes_principais_distintos, []) AS cnaes_principais_distintos
        FROM empresa emp
        LEFT JOIN (
            SELECT
                cnpj_basico,
                LIST(DISTINCT NULLIF(uf, ''))                    AS ufs_atuacao,
                LIST(DISTINCT NULLIF(cnae_fiscal_principal, '')) AS cnaes_principais_distintos
            FROM estabelecimento
            GROUP BY cnpj_basico
        ) agg ON agg.cnpj_basico = emp.cnpj_basico
    """


def sql_predup_ctes() -> str:
    # Production shape (transform.write_raizes_parquet): pre-dedup with SELECT
    # DISTINCT, then flat LIST() per group (two vanilla, spillable aggregates),
    # joined onto empresa — no extra _groups DISTINCT (empresa is the driver).
    return """
        WITH _ufs AS (
            SELECT DISTINCT cnpj_basico, uf FROM estabelecimento
            WHERE uf IS NOT NULL AND uf <> ''
        ),
        _ufs_agg AS (
            SELECT cnpj_basico, list(uf) AS ufs_atuacao FROM _ufs GROUP BY cnpj_basico
        ),
        _cnaes AS (
            SELECT DISTINCT cnpj_basico, cnae_fiscal_principal FROM estabelecimento
            WHERE cnae_fiscal_principal IS NOT NULL AND cnae_fiscal_principal <> ''
        ),
        _cnaes_agg AS (
            SELECT cnpj_basico, list(cnae_fiscal_principal) AS cnaes_principais_distintos
            FROM _cnaes GROUP BY cnpj_basico
        )
        SELECT
            emp.cnpj_basico AS cnpj_base,
            COALESCE(u.ufs_atuacao, [])          AS ufs_atuacao,
            COALESCE(c.cnaes_principais_distintos, []) AS cnaes_principais_distintos
        FROM empresa emp
        LEFT JOIN _ufs_agg u ON u.cnpj_basico = emp.cnpj_basico
        LEFT JOIN _cnaes_agg c ON c.cnpj_basico = emp.cnpj_basico
    """


# --------------------------------------------------------------------------- #
# Ibis paths
# --------------------------------------------------------------------------- #
_EMPTY = ibis.literal([], type="array<string>")


def ibis_idiomatic(con: duckdb.DuckDBPyConnection) -> str:
    icon = ibis.duckdb.from_connection(con)
    emp = icon.table("empresa")
    t = icon.table("estabelecimento")
    agg = t.group_by("cnpj_basico").agg(
        ufs_atuacao=t.uf.nullif("").collect(distinct=True),
        cnaes_principais_distintos=t.cnae_fiscal_principal.nullif("").collect(distinct=True),
    )
    expr = emp.left_join(agg, emp.cnpj_basico == agg.cnpj_basico).select(
        cnpj_base=emp.cnpj_basico,
        ufs_atuacao=ibis.coalesce(agg.ufs_atuacao, _EMPTY),
        cnaes_principais_distintos=ibis.coalesce(agg.cnaes_principais_distintos, _EMPTY),
    )
    return ibis.to_sql(expr, dialect="duckdb")


def ibis_predup(con: duckdb.DuckDBPyConnection) -> str:
    icon = ibis.duckdb.from_connection(con)
    emp = icon.table("empresa")
    t = icon.table("estabelecimento")
    ufs = (
        t.filter(t.uf.notnull() & (t.uf != ""))
        .select("cnpj_basico", "uf")
        .distinct()
        .group_by("cnpj_basico")
        .agg(ufs_atuacao=ibis._.uf.collect())
    )
    cnaes = (
        t.filter(t.cnae_fiscal_principal.notnull() & (t.cnae_fiscal_principal != ""))
        .select("cnpj_basico", "cnae_fiscal_principal")
        .distinct()
        .group_by("cnpj_basico")
        .agg(cnaes_principais_distintos=ibis._.cnae_fiscal_principal.collect())
    )
    expr = (
        emp.left_join(ufs, emp.cnpj_basico == ufs.cnpj_basico)
        .left_join(cnaes, emp.cnpj_basico == cnaes.cnpj_basico)
        .select(
            cnpj_base=emp.cnpj_basico,
            ufs_atuacao=ibis.coalesce(ufs.ufs_atuacao, _EMPTY),
            cnaes_principais_distintos=ibis.coalesce(cnaes.cnaes_principais_distintos, _EMPTY),
        )
    )
    return ibis.to_sql(expr, dialect="duckdb")


# --------------------------------------------------------------------------- #
# Measurement (peak temp spill + wall time)
# --------------------------------------------------------------------------- #
def _dir_bytes(p: Path) -> int:
    total = 0
    if not p.exists():
        return 0
    for f in p.rglob("*"):
        try:
            total += f.stat().st_size
        except OSError:
            pass
    return total


class SpillSampler(threading.Thread):
    def __init__(self, temp_dir: Path):
        super().__init__(daemon=True)
        self.temp_dir = temp_dir
        self.peak = 0
        self._stop_evt = threading.Event()

    def run(self):
        while not self._stop_evt.is_set():
            self.peak = max(self.peak, _dir_bytes(self.temp_dir))
            time.sleep(0.05)

    def stop(self):
        self._stop_evt.set()
        self.join(timeout=2)


def run_path(name: str, select_sql: str, out_parquet: Path) -> dict:
    temp_dir = WORK / f"tmp_{name}"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WORK / "bench.duckdb"))
    con.execute(f"PRAGMA memory_limit='{MEMORY_GB}GB'")
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"PRAGMA temp_directory='{temp_dir}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    sampler = SpillSampler(temp_dir)
    sampler.start()
    t0 = time.monotonic()
    status, err = "ok", ""
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{out_parquet}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
        )
    except Exception as exc:  # noqa: BLE001
        status = "FAILED"
        err = str(exc).splitlines()[0]
    dt = time.monotonic() - t0
    sampler.stop()
    con.close()
    return {
        "name": name,
        "status": status,
        "err": err,
        "seconds": dt,
        "peak_spill_bytes": sampler.peak,
        "parquet_bytes": out_parquet.stat().st_size if out_parquet.exists() else 0,
    }


def checksum(con: duckdb.DuckDBPyConnection, parquet: Path) -> tuple[int, str]:
    # Set-equivalence: sort each list before hashing so element order (which
    # differs between LIST(DISTINCT) and the two-step) doesn't cause a mismatch.
    row = con.execute(
        f"""
        SELECT COUNT(*),
               md5(string_agg(
                   cnpj_base || '|' ||
                   array_to_string(list_sort(COALESCE(ufs_atuacao, [])), ',') || '|' ||
                   array_to_string(list_sort(COALESCE(cnaes_principais_distintos, [])), ','),
                   '' ORDER BY cnpj_base))
        FROM read_parquet('{parquet}')
        """
    ).fetchone()
    return int(row[0]), row[1]


def human(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if f < 1024 or unit == "GB":
            return f"{f:.1f} {unit}"
        f /= 1024
    return f"{f:.1f} GB"


def main() -> None:
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir(parents=True, exist_ok=True)
    print(
        f"ibis-raizes-agg-benchmark · groups={EMP_ROWS:,} rows={EST_ROWS:,} "
        f"memory_limit={MEMORY_GB}GB threads={THREADS} "
        f"duckdb={duckdb.__version__} ibis={ibis.__version__}"
    )
    gen = duckdb.connect(str(WORK / "bench.duckdb"))
    gen.execute(f"PRAGMA memory_limit='{max(MEMORY_GB, 6)}GB'")
    gen.execute("PRAGMA threads=4")
    print("generating synthetic data...")
    t0 = time.monotonic()
    build_data(gen)
    gen.close()
    print(f"  generated in {time.monotonic() - t0:.1f}s")

    paths = [
        ("sql_naive", sql_naive()),
        ("sql_predup", sql_predup_ctes()),
        ("ibis_idiomatic", ibis_idiomatic(duckdb.connect(str(WORK / "bench.duckdb")))),
        ("ibis_predup", ibis_predup(duckdb.connect(str(WORK / "bench.duckdb")))),
    ]
    results = []
    for name, sql in paths:
        r = run_path(name, sql, WORK / f"{name}.parquet")
        results.append(r)
        print(
            f"{r['name']:16}: {r['status']:6} {r['seconds']:7.1f}s  "
            f"peak_spill={human(r['peak_spill_bytes']):>10}  out={human(r['parquet_bytes'])}"
            + (f"  {r['err']}" if r["err"] else "")
        )

    # equivalence across all successful paths
    vc = duckdb.connect()
    sums = {}
    for r in results:
        if r["status"] == "ok":
            sums[r["name"]] = checksum(vc, WORK / f"{r['name']}.parquet")
    vc.close()
    if sums:
        ref_name, ref = next(iter(sums.items()))
        all_match = all(v == ref for v in sums.values())
        print(f"equivalence   : ref={ref_name} rows={ref[0]:,}  all_set_equal={all_match}")
        if not all_match:
            for name, v in sums.items():
                print(f"  {name}: rows={v[0]:,} checksum={'match' if v == ref else 'DIFFER'}")

    ok = {r["name"]: r for r in results if r["status"] == "ok"}
    if "sql_predup" in ok:
        base = ok["sql_predup"]["peak_spill_bytes"] + 1
        print("spill vs sql_predup (production shape):")
        for name in ("sql_naive", "ibis_idiomatic", "ibis_predup"):
            if name in ok:
                print(f"  {name:16}: {(ok[name]['peak_spill_bytes'] + 1) / base:.2f}x")

    if not KEEP:
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    main()
