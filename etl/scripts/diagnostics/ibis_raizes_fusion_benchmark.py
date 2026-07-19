#!/usr/bin/env python3
"""Benchmark: can raizes' counts/empresa/matriz aggregates be fused (not materialized)?

Follow-up to `ibis_raizes_agg_benchmark.py` (which settled the `ufs_atuacao`/
`cnaes_principais_distintos` two-step dedup question) and PR #59's Ibis
migration of `write_raizes_parquet_from_cnpjs`. That migration preserved ALL
SIX of the original raw-SQL's materialization boundaries verbatim, on the
principle that the exact execution shape is what holds the historical OOM at
bay. An audit of that choice (2026-07-18) found that only FOUR of those six
boundaries have a *documented* OOM mechanism requiring eager materialization
— the `_raizes_ufs`/`_raizes_ufs_agg`/`_raizes_cnaes`/`_raizes_cnaes_agg`
two-step pre-dedup pair, covered by the sibling benchmark. The other three —
`_raizes_counts` (COUNT/COUNT FILTER), `_raizes_empresa` (`.arbitrary()` per
group over 7 columns), `_raizes_matriz` (filter + ROW_NUMBER window) — have
no distinct-list OOM history and were flagged "worth testing lazy, NOT safe
to defer without evidence": production's actual raizes OOM (PR #24, run
25522678418) came from *pipelining* multiple heavy operators together, not
from any one of these three aggregates in isolation, so inlining them back
into the final join risks recreating that co-pipelining even though none of
them individually looks dangerous.

This benchmark tests that directly: build the same final `raizes`-shaped
output two ways —

    eager  materialize _raizes_counts / _raizes_empresa / _raizes_matriz as
           their own TEMP TABLEs before the final join (current PR #59 code)
    fused  compile counts/empresa/matriz as inline Ibis sub-expressions
           joined directly in the SAME final query as ufs_agg/cnaes_agg
           (which stay eager either way — that boundary is out of scope here)

— under production settings (threads=1, tight memory_limit, dedicated temp
dir), measuring peak temp-spill + wall time, and asserting row-equivalence.

Run
---
    uv run python scripts/diagnostics/ibis_raizes_fusion_benchmark.py

    FICHA_BENCH_EMPRESA_ROWS=20000000 FICHA_BENCH_MEMORY_GB=2 \\
        uv run python scripts/diagnostics/ibis_raizes_fusion_benchmark.py

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
from ibis import _

EMP_ROWS = int(os.environ.get("FICHA_BENCH_EMPRESA_ROWS", "5000000"))
DUP = int(os.environ.get("FICHA_BENCH_DUP", "5"))  # estabelecimentos per group
EST_ROWS = EMP_ROWS * DUP
MEMORY_GB = int(os.environ.get("FICHA_BENCH_MEMORY_GB", "2"))
THREADS = int(os.environ.get("FICHA_BENCH_THREADS", "1"))
KEEP = bool(os.environ.get("FICHA_BENCH_KEEP"))

WORK = Path(os.environ.get("FICHA_BENCH_DIR", "/tmp/ficha_raizes_fusion_bench")).resolve()

_EMPRESA_FIELDS = (
    "razao_social",
    "razao_social_normalizada",
    "natureza_juridica_codigo",
    "natureza_juridica_descricao",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
)


def build_data(con: duckdb.DuckDBPyConnection) -> None:
    # _cnpjs_slim-shaped: EST_ROWS rows over EMP_ROWS groups (DUP rows/group).
    # Company fields (razao_social etc.) repeat identically across a group's
    # rows — same as the real cnpjs.parquet, where they come from `empresa`
    # denormalized onto every estabelecimento row. identificador_matriz_filial
    # is '1' for exactly one row per group (the matriz), '2' otherwise, same
    # invariant `_raizes_matriz`'s ROW_NUMBER/QUALIFY relies on.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE cnpjs_slim AS
        SELECT
            printf('%08d', i % {EMP_ROWS}) || printf('%04d', i // {EMP_ROWS}) || '00' AS cnpj,
            printf('%08d', i % {EMP_ROWS}) AS cnpj_base,
            CASE WHEN i // {EMP_ROWS} = 0 THEN '1' ELSE '2' END AS identificador_matriz_filial,
            CASE WHEN i % 7 = 0 THEN '01' ELSE '02' END AS situacao_cadastral,
            printf('2020-%02d-01', ((i / {EMP_ROWS})::BIGINT % 12) + 1) AS data_inicio_atividade,
            printf('%07d', ((i / {EMP_ROWS})::BIGINT % 1300) * 1000) AS cnae_principal_codigo,
            'CNAE description' AS cnae_principal_descricao,
            'RAZAO SOCIAL ' || (i % {EMP_ROWS}) AS razao_social,
            'RAZAO SOCIAL NORMALIZADA ' || (i % {EMP_ROWS}) AS razao_social_normalizada,
            printf('%04d', (i % {EMP_ROWS}) % 90) AS natureza_juridica_codigo,
            'natureza descricao' AS natureza_juridica_descricao,
            (1000.0 + (i % {EMP_ROWS}))::DOUBLE AS capital_social,
            CASE (i % {EMP_ROWS}) % 3 WHEN 0 THEN '01' WHEN 1 THEN '03' ELSE '05' END
                AS porte_empresa,
            NULL AS ente_federativo_responsavel,
            printf('%04d', (i % {EMP_ROWS}) % 5570) AS municipio_codigo,
            'municipio nome' AS municipio_nome
        FROM range({EST_ROWS}) t(i)
        """
    )


def _dedup_agg(icon, col: str, out: str):
    slim = icon.table("cnpjs_slim")
    dedup = slim.filter(slim[col].notnull() & (slim[col] != "")).select("cnpj_base", col).distinct()
    return dedup.group_by("cnpj_base").agg(**{out: dedup[col].collect().sort()})


def _materialize(con: duckdb.DuckDBPyConnection, table: str, expr) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}")


def _counts_expr(t):
    return t.group_by("cnpj_base").agg(
        qtd_estabelecimentos=_.count().cast("int32"),
        qtd_estabelecimentos_ativos=_.count(where=_.situacao_cadastral == "02").cast("int32"),
    )


def _empresa_expr(t):
    return t.group_by("cnpj_base").agg(**{f: t[f].arbitrary() for f in _EMPRESA_FIELDS})


def _matriz_expr(t):
    mf = t.filter(t.identificador_matriz_filial == "1")
    rn = ibis.row_number().over(group_by=mf.cnpj_base, order_by=mf.cnpj)
    return (
        mf.mutate(_rn=rn)
        .filter(_._rn == 0)
        .select(
            "cnpj_base",
            data_inicio_atividade_matriz=_.data_inicio_atividade,
            uf_matriz=_.municipio_codigo,  # placeholder col reuse — shape-only benchmark
            municipio_matriz_codigo=_.municipio_codigo,
            municipio_matriz_nome=_.municipio_nome,
            cnae_principal_matriz_codigo=_.cnae_principal_codigo,
            cnae_principal_matriz_descricao=_.cnae_principal_descricao,
        )
    )


def eager_sql(con: duckdb.DuckDBPyConnection) -> str:
    """Mirrors PR #59's current write_raizes_parquet_from_cnpjs exactly."""
    icon = ibis.duckdb.from_connection(con)
    _materialize(con, "_raizes_ufs_agg", _dedup_agg(icon, "cnae_principal_codigo", "_unused"))
    # (uf column omitted from this synthetic dataset — cnaes dedup stands in
    # for both list-aggregate boundaries; they're identical in shape and are
    # NOT what's under test here.)
    slim = icon.table("cnpjs_slim")
    _materialize(con, "_raizes_counts", _counts_expr(slim))
    _materialize(con, "_raizes_empresa", _empresa_expr(slim))
    _materialize(con, "_raizes_matriz", _matriz_expr(slim))

    emp = icon.table("_raizes_empresa")
    cnt = icon.table("_raizes_counts")
    mat = icon.table("_raizes_matriz")
    expr = (
        emp.left_join(cnt, emp.cnpj_base == cnt.cnpj_base)
        .left_join(mat, emp.cnpj_base == mat.cnpj_base)
        .select(
            "cnpj_base",
            *_EMPRESA_FIELDS,
            qtd_estabelecimentos=cnt.qtd_estabelecimentos.coalesce(0),
            qtd_estabelecimentos_ativos=cnt.qtd_estabelecimentos_ativos.coalesce(0),
            data_inicio_atividade_matriz=mat.data_inicio_atividade_matriz,
            municipio_matriz_codigo=mat.municipio_matriz_codigo,
            municipio_matriz_nome=mat.municipio_matriz_nome,
            cnae_principal_matriz_codigo=mat.cnae_principal_matriz_codigo,
            cnae_principal_matriz_descricao=mat.cnae_principal_matriz_descricao,
        )
    )
    return ibis.to_sql(expr, dialect="duckdb")


def fused_sql(con: duckdb.DuckDBPyConnection) -> str:
    """Same output, but counts/empresa/matriz are inlined — not materialized."""
    icon = ibis.duckdb.from_connection(con)
    slim = icon.table("cnpjs_slim")
    emp = _empresa_expr(slim)
    cnt = _counts_expr(slim)
    mat = _matriz_expr(slim)
    expr = (
        emp.left_join(cnt, emp.cnpj_base == cnt.cnpj_base)
        .left_join(mat, emp.cnpj_base == mat.cnpj_base)
        .select(
            "cnpj_base",
            *_EMPRESA_FIELDS,
            qtd_estabelecimentos=cnt.qtd_estabelecimentos.coalesce(0),
            qtd_estabelecimentos_ativos=cnt.qtd_estabelecimentos_ativos.coalesce(0),
            data_inicio_atividade_matriz=mat.data_inicio_atividade_matriz,
            municipio_matriz_codigo=mat.municipio_matriz_codigo,
            municipio_matriz_nome=mat.municipio_matriz_nome,
            cnae_principal_matriz_codigo=mat.cnae_principal_matriz_codigo,
            cnae_principal_matriz_descricao=mat.cnae_principal_matriz_descricao,
        )
    )
    return ibis.to_sql(expr, dialect="duckdb")


# --------------------------------------------------------------------------- #
# Measurement (peak temp spill + wall time) — identical harness to the
# sibling raizes-agg benchmark.
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


def run_path(name: str, build_sql, out_parquet: Path) -> dict:
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
        select_sql = build_sql(con)
        con.execute(
            f"COPY ({select_sql}) TO '{out_parquet}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
        )
    except Exception as exc:  # noqa: BLE001
        status = "FAILED"
        err = str(exc).splitlines()[0]
    dt = time.monotonic() - t0
    sampler.stop()
    con.execute("DROP TABLE IF EXISTS _raizes_ufs_agg")
    con.execute("DROP TABLE IF EXISTS _raizes_counts")
    con.execute("DROP TABLE IF EXISTS _raizes_empresa")
    con.execute("DROP TABLE IF EXISTS _raizes_matriz")
    con.close()
    return {
        "name": name,
        "status": status,
        "err": err,
        "seconds": dt,
        "peak_spill_bytes": sampler.peak,
        "parquet_bytes": out_parquet.stat().st_size if out_parquet.exists() else 0,
    }


_ALL_OUT_COLS = (
    "cnpj_base",
    *_EMPRESA_FIELDS,
    "qtd_estabelecimentos",
    "qtd_estabelecimentos_ativos",
    "data_inicio_atividade_matriz",
    "municipio_matriz_codigo",
    "municipio_matriz_nome",
    "cnae_principal_matriz_codigo",
    "cnae_principal_matriz_descricao",
)


def checksum(con: duckdb.DuckDBPyConnection, parquet: Path) -> tuple[int, str]:
    # Hash EVERY output column (not a subset) — a partial checksum could
    # miss a real divergence introduced by the rewrite.
    concat = " || '|' || ".join(f"COALESCE({c}::VARCHAR, '')" for c in _ALL_OUT_COLS)
    row = con.execute(
        f"""
        SELECT COUNT(*), md5(string_agg({concat}, '' ORDER BY cnpj_base))
        FROM read_parquet('{parquet}')
        """
    ).fetchone()
    return int(row[0]), row[1]


def full_diff(con: duckdb.DuckDBPyConnection, a: Path, b: Path) -> tuple[int, int]:
    """Symmetric row-level EXCEPT diff over every output column — the strongest
    possible equivalence check, run within the same process/paths as the writer
    (avoids any shell/path-translation ambiguity in ad-hoc post-hoc checks)."""
    a_only = con.execute(
        f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet('{a}') EXCEPT SELECT * FROM read_parquet('{b}'))"
    ).fetchone()[0]
    b_only = con.execute(
        f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet('{b}') EXCEPT SELECT * FROM read_parquet('{a}'))"
    ).fetchone()[0]
    return a_only, b_only


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
        f"ibis-raizes-fusion-benchmark · groups={EMP_ROWS:,} rows={EST_ROWS:,} "
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

    paths = [("eager", eager_sql), ("fused", fused_sql)]
    results = []
    for name, build_sql in paths:
        r = run_path(name, build_sql, WORK / f"{name}.parquet")
        results.append(r)
        print(
            f"{r['name']:8}: {r['status']:6} {r['seconds']:7.1f}s  "
            f"peak_spill={human(r['peak_spill_bytes']):>10}  out={human(r['parquet_bytes'])}"
            + (f"  {r['err']}" if r["err"] else "")
        )

    ok = {r["name"]: r for r in results if r["status"] == "ok"}

    vc = duckdb.connect()
    sums = {}
    for r in results:
        if r["status"] == "ok":
            sums[r["name"]] = checksum(vc, WORK / f"{r['name']}.parquet")
    if sums:
        ref_name, ref = next(iter(sums.items()))
        all_match = all(v == ref for v in sums.values())
        print(f"equivalence   : ref={ref_name} rows={ref[0]:,}  all_equal={all_match}")
        if not all_match:
            for name, v in sums.items():
                print(f"  {name}: rows={v[0]:,} checksum={'match' if v == ref else 'DIFFER'}")
    if "eager" in ok and "fused" in ok:
        a_only, b_only = full_diff(vc, WORK / "eager.parquet", WORK / "fused.parquet")
        print(f"full row-level diff: eager-only={a_only:,}  fused-only={b_only:,}")
    vc.close()

    if "eager" in ok:
        base = ok["eager"]["peak_spill_bytes"] + 1
        print("spill vs eager (current PR #59 shape):")
        for name in ("fused",):
            if name in ok:
                print(f"  {name:8}: {(ok[name]['peak_spill_bytes'] + 1) / base:.2f}x")

    if not KEEP:
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    main()
