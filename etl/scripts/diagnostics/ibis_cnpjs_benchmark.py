#!/usr/bin/env python3
"""Benchmark: cnpjs denormalization join — hand-SQL vs Ibis-compiled.

ADR 0017 keeps the heavy `cnpjs`/`raizes` joins in raw SQL and requires that any
future migration to Ibis come with a *memory benchmark* comparing the plan Ibis
compiles against the hand-tuned SQL — not a blind swap. This is that benchmark.

What it does
------------
1. Generates production-shaped synthetic data (empresa / estabelecimento /
   simples + small lookups) at a configurable scale, directly in DuckDB.
2. Builds `cnpjs` two ways over the SAME column set (the memory-dominant
   3-big-table join + representative lookup LEFT JOINs, CASE mappings,
   TRY_CAST, date transforms, strip_accents):
     - PATH A: hand SQL, mirroring `transform._cnpjs_chunk_select_sql` structure.
     - PATH B: an Ibis expression compiled to DuckDB SQL via `ibis.to_sql`.
3. Runs each under production settings (`threads=1`, an explicit `memory_limit`,
   a dedicated temp dir) and measures wall time + PEAK temp-spill bytes (a
   sampler thread polls the temp dir), then COPYs to parquet.
4. Asserts row-equivalence via a sorted checksum over the shared columns.

The comparison is apples-to-apples: both paths compute the identical column set,
so any difference in spill/time is attributable to the plan Ibis compiles.

Run
---
    # local smoke (default ~6M estab rows, fits a 16 GB box):
    uv run python scripts/diagnostics/ibis_cnpjs_benchmark.py

    # push toward production scale in CI (est≈71M, emp≈69M):
    FICHA_BENCH_ESTAB_ROWS=70000000 FICHA_BENCH_MEMORY_GB=9 \
        uv run python scripts/diagnostics/ibis_cnpjs_benchmark.py

Env knobs: FICHA_BENCH_ESTAB_ROWS, FICHA_BENCH_EMPRESA_ROWS,
FICHA_BENCH_MEMORY_GB, FICHA_BENCH_THREADS, FICHA_BENCH_KEEP (keep parquet output).
"""

from __future__ import annotations

import os
import shutil
import threading
import time
from pathlib import Path

import duckdb
import ibis

EST_ROWS = int(os.environ.get("FICHA_BENCH_ESTAB_ROWS", "6000000"))
EMP_ROWS = int(os.environ.get("FICHA_BENCH_EMPRESA_ROWS", str(max(1, EST_ROWS * 5 // 6))))
MEMORY_GB = int(os.environ.get("FICHA_BENCH_MEMORY_GB", "4"))
THREADS = int(os.environ.get("FICHA_BENCH_THREADS", "1"))
KEEP = bool(os.environ.get("FICHA_BENCH_KEEP"))

WORK = Path(os.environ.get("FICHA_BENCH_DIR", "/tmp/ficha_bench")).resolve()


# --------------------------------------------------------------------------- #
# Synthetic data — production-shaped, generated in-DB (no CSV parsing noise).
# --------------------------------------------------------------------------- #
def build_data(con: duckdb.DuckDBPyConnection) -> None:
    # empresa: one row per cnpj_basico (8-digit, zero-padded). Capital uses the
    # RFB comma-decimal so TRY_CAST(REPLACE(...)) is exercised.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE empresa AS
        SELECT
            printf('%08d', i)                         AS cnpj_basico,
            'EMPRESA ' || printf('%08d', i) || ' LTDA' AS razao_social,
            ['2062','2135','2135','2240'][(i % 4) + 1] AS natureza_juridica,
            ['49','05','49','23'][(i % 4) + 1]         AS qualificacao_responsavel,
            printf('%d,%02d', i % 1000000, i % 100)    AS capital_social,
            ['01','03','05'][(i % 3) + 1]              AS porte_empresa,
            ''                                         AS ente_federativo_responsavel
        FROM range({EMP_ROWS}) t(i)
        """
    )
    # estabelecimento: EST_ROWS rows, each pointing at an existing empresa
    # (matriz + filiais). Dates as YYYYMMDD strings; some empty/'0' to exercise
    # the date CASE. situacao/pais/municipio spread across the lookup domains.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE estabelecimento AS
        SELECT
            printf('%08d', i % {EMP_ROWS})            AS cnpj_basico,
            printf('%04d', (i / {EMP_ROWS})::BIGINT)  AS cnpj_ordem,
            printf('%02d', i % 100)                   AS cnpj_dv,
            CASE WHEN i % {EMP_ROWS} = 0 THEN '1' ELSE '2' END AS identificador_matriz_filial,
            'FANTASIA ' || printf('%08d', i)          AS nome_fantasia,
            ['01','02','02','03','04','08'][(i % 6) + 1] AS situacao_cadastral,
            CASE WHEN i % 7 = 0 THEN '' ELSE printf('202%01d%02d%02d', i%10, (i%12)+1, (i%28)+1) END
                                                       AS data_situacao_cadastral,
            ['00','01','00'][(i % 3) + 1]             AS motivo_situacao_cadastral,
            ''                                        AS nome_cidade_exterior,
            '105'                                      AS pais,
            printf('20%02d%02d%02d', i%23, (i%12)+1, (i%28)+1) AS data_inicio_atividade,
            ['0111301','4711301','6201500'][(i % 3) + 1] AS cnae_fiscal_principal,
            ''                                        AS cnae_fiscal_secundaria,
            'RUA'                                      AS tipo_logradouro,
            'LOGRADOURO ' || printf('%06d', i % 900000) AS logradouro,
            printf('%d', i % 5000)                    AS numero,
            ''                                        AS complemento,
            'CENTRO'                                   AS bairro,
            printf('%08d', i % 99999999)              AS cep,
            ['SP','RJ','MG','RS','BA'][(i % 5) + 1]   AS uf,
            ['3550308','3304557'][(i % 2) + 1]        AS municipio,
            '11'                                       AS ddd_1,
            printf('%09d', i % 1000000000)            AS telefone_1,
            '', '', '', '',
            'x@example.com'                            AS correio_eletronico,
            ''                                        AS situacao_especial,
            ''                                        AS data_situacao_especial
        FROM range({EST_ROWS}) t(i)
        """
    )
    # simples: one row per empresa.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE simples AS
        SELECT
            printf('%08d', i)                         AS cnpj_basico,
            ['S','N'][(i % 2) + 1]                    AS opcao_simples,
            printf('20%02d0101', i%23)                AS data_opcao_simples,
            ''                                        AS data_exclusao_simples,
            ['S','N'][(i % 2) + 1]                    AS opcao_mei,
            '', ''
        FROM range({EMP_ROWS}) t(i)
        """
    )
    # small lookups (broadcast joins).
    con.execute(
        """
        CREATE OR REPLACE TABLE lookup_naturezas AS
            SELECT * FROM (VALUES ('2062','Sociedade Empresária Limitada'),
                                  ('2135','Empresário Individual'),
                                  ('2240','Sociedade Simples')) t(codigo, descricao);
        CREATE OR REPLACE TABLE lookup_municipios AS
            SELECT * FROM (VALUES ('3550308','São Paulo'),
                                  ('3304557','Rio de Janeiro')) t(codigo, descricao);
        CREATE OR REPLACE TABLE lookup_paises AS
            SELECT * FROM (VALUES ('105','Brasil')) t(codigo, descricao);
        CREATE OR REPLACE TABLE lookup_motivos AS
            SELECT * FROM (VALUES ('00','Sem motivo'),('01','Extinção')) t(codigo, descricao);
        """
    )


# --------------------------------------------------------------------------- #
# PATH A — hand SQL (mirrors transform._cnpjs_chunk_select_sql structure).
# --------------------------------------------------------------------------- #
def _date_sql(col: str) -> str:
    return (
        f"CASE WHEN {col} IS NULL OR {col} = '' OR {col} = '0' THEN NULL "
        f"ELSE SUBSTR({col},1,4)||'-'||SUBSTR({col},5,2)||'-'||SUBSTR({col},7,2) END"
    )


def sql_select() -> str:
    return f"""
        SELECT
            est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
            est.cnpj_basico AS cnpj_base,
            est.identificador_matriz_filial,
            emp.razao_social,
            UPPER(strip_accents(emp.razao_social)) AS razao_social_normalizada,
            COALESCE(nj.descricao, '') AS natureza_juridica_descricao,
            TRY_CAST(REPLACE(emp.capital_social, ',', '.') AS DOUBLE) AS capital_social,
            emp.porte_empresa,
            est.situacao_cadastral,
            CASE est.situacao_cadastral
                WHEN '01' THEN 'Nula' WHEN '02' THEN 'Ativa' WHEN '03' THEN 'Suspensa'
                WHEN '04' THEN 'Inapta' WHEN '08' THEN 'Baixada' ELSE '' END
                AS situacao_cadastral_descricao,
            {_date_sql("est.data_situacao_cadastral")} AS data_situacao_cadastral,
            COALESCE(mt.descricao, '') AS motivo_situacao_cadastral_descricao,
            {_date_sql("est.data_inicio_atividade")} AS data_inicio_atividade,
            est.cnae_fiscal_principal AS cnae_principal_codigo,
            est.logradouro, est.numero, est.bairro, est.cep, est.uf,
            est.municipio AS municipio_codigo,
            COALESCE(mn.descricao, '') AS municipio_nome,
            est.pais AS pais_codigo,
            COALESCE(ps.descricao, '') AS pais_nome,
            est.correio_eletronico,
            CASE smp.opcao_simples WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                AS opcao_simples,
            {_date_sql("smp.data_opcao_simples")} AS data_opcao_simples
        FROM estabelecimento est
        LEFT JOIN empresa emp ON emp.cnpj_basico = est.cnpj_basico
        LEFT JOIN simples smp ON smp.cnpj_basico = est.cnpj_basico
        LEFT JOIN lookup_naturezas nj ON nj.codigo = emp.natureza_juridica
        LEFT JOIN lookup_motivos mt ON mt.codigo = est.motivo_situacao_cadastral
        LEFT JOIN lookup_municipios mn ON mn.codigo = est.municipio
        LEFT JOIN lookup_paises ps ON ps.codigo = est.pais
    """


# --------------------------------------------------------------------------- #
# PATH B — Ibis expression compiled to DuckDB SQL (same column set).
# --------------------------------------------------------------------------- #
@ibis.udf.scalar.builtin
def strip_accents(s: str) -> str:  # DuckDB builtin
    ...


def ibis_select(con: duckdb.DuckDBPyConnection) -> str:
    icon = ibis.duckdb.from_connection(con)
    est = icon.table("estabelecimento")
    emp = icon.table("empresa")
    smp = icon.table("simples")
    nj = icon.table("lookup_naturezas").rename(nj_codigo="codigo", nj_descricao="descricao")
    mt = icon.table("lookup_motivos").rename(mt_codigo="codigo", mt_descricao="descricao")
    mn = icon.table("lookup_municipios").rename(mn_codigo="codigo", mn_descricao="descricao")
    ps = icon.table("lookup_paises").rename(ps_codigo="codigo", ps_descricao="descricao")

    def date(c):
        return ibis.cases(
            (c.isnull() | (c == "") | (c == "0"), ibis.null("string")),
            else_=c.substr(0, 4) + "-" + c.substr(4, 2) + "-" + c.substr(6, 2),
        )

    sc = est.situacao_cadastral
    op = smp.opcao_simples
    j = (
        est.left_join(emp, est.cnpj_basico == emp.cnpj_basico)
        .left_join(smp, est.cnpj_basico == smp.cnpj_basico)
        .left_join(nj, emp.natureza_juridica == nj.nj_codigo)
        .left_join(mt, est.motivo_situacao_cadastral == mt.mt_codigo)
        .left_join(mn, est.municipio == mn.mn_codigo)
        .left_join(ps, est.pais == ps.ps_codigo)
    )
    expr = j.select(
        cnpj=est.cnpj_basico + est.cnpj_ordem + est.cnpj_dv,
        cnpj_base=est.cnpj_basico,
        identificador_matriz_filial=est.identificador_matriz_filial,
        razao_social=emp.razao_social,
        razao_social_normalizada=strip_accents(emp.razao_social).upper(),
        natureza_juridica_descricao=nj.nj_descricao.coalesce(""),
        capital_social=emp.capital_social.replace(",", ".").try_cast("float64"),
        porte_empresa=emp.porte_empresa,
        situacao_cadastral=sc,
        situacao_cadastral_descricao=ibis.cases(
            (sc == "01", "Nula"), (sc == "02", "Ativa"), (sc == "03", "Suspensa"),
            (sc == "04", "Inapta"), (sc == "08", "Baixada"), else_="",
        ),
        data_situacao_cadastral=date(est.data_situacao_cadastral),
        motivo_situacao_cadastral_descricao=mt.mt_descricao.coalesce(""),
        data_inicio_atividade=date(est.data_inicio_atividade),
        cnae_principal_codigo=est.cnae_fiscal_principal,
        logradouro=est.logradouro,
        numero=est.numero,
        bairro=est.bairro,
        cep=est.cep,
        uf=est.uf,
        municipio_codigo=est.municipio,
        municipio_nome=mn.mn_descricao.coalesce(""),
        pais_codigo=est.pais,
        pais_nome=ps.ps_descricao.coalesce(""),
        correio_eletronico=est.correio_eletronico,
        opcao_simples=ibis.cases((op == "S", True), (op == "N", False), else_=ibis.null("boolean")),
        data_opcao_simples=date(smp.data_opcao_simples),
    )
    return ibis.to_sql(expr, dialect="duckdb")


# --------------------------------------------------------------------------- #
# Measurement
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
    status = "ok"
    err = ""
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
    row = con.execute(
        f"""
        SELECT COUNT(*),
               md5(string_agg(md5(CAST(t AS VARCHAR)), '' ORDER BY cnpj, cnpj_base))
        FROM (SELECT * FROM read_parquet('{parquet}')) t
        """
    ).fetchone()
    return int(row[0]), row[1]


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def main() -> None:
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir(parents=True, exist_ok=True)
    print(
        f"ibis-cnpjs-benchmark · est={EST_ROWS:,} emp={EMP_ROWS:,} "
        f"memory_limit={MEMORY_GB}GB threads={THREADS} duckdb={duckdb.__version__} "
        f"ibis={ibis.__version__}"
    )
    gen = duckdb.connect(str(WORK / "bench.duckdb"))
    gen.execute(f"PRAGMA memory_limit='{max(MEMORY_GB, 6)}GB'")
    gen.execute("PRAGMA threads=4")
    print("generating synthetic data...")
    t0 = time.monotonic()
    build_data(gen)
    gen.close()
    print(f"  generated in {time.monotonic() - t0:.1f}s")

    a = run_path("sql", sql_select(), WORK / "cnpjs_sql.parquet")
    print(
        f"PATH A  sql   : {a['status']:6} {a['seconds']:7.1f}s  "
        f"peak_spill={human(a['peak_spill_bytes']):>10}  out={human(a['parquet_bytes'])}"
        + (f"  {a['err']}" if a["err"] else "")
    )
    b = run_path("ibis", ibis_select(duckdb.connect(str(WORK / "bench.duckdb"))),
                 WORK / "cnpjs_ibis.parquet")
    print(
        f"PATH B  ibis  : {b['status']:6} {b['seconds']:7.1f}s  "
        f"peak_spill={human(b['peak_spill_bytes']):>10}  out={human(b['parquet_bytes'])}"
        + (f"  {b['err']}" if b["err"] else "")
    )

    if a["status"] == "ok" and b["status"] == "ok":
        vc = duckdb.connect()
        na, ha = checksum(vc, WORK / "cnpjs_sql.parquet")
        nb, hb = checksum(vc, WORK / "cnpjs_ibis.parquet")
        vc.close()
        same = na == nb and ha == hb
        print(f"equivalence   : rows sql={na:,} ibis={nb:,}  checksum_match={same}")
        if a["seconds"] and b["seconds"]:
            print(
                f"delta         : ibis/sql time={b['seconds'] / a['seconds']:.2f}x  "
                f"spill={(b['peak_spill_bytes'] + 1) / (a['peak_spill_bytes'] + 1):.2f}x"
            )
        if not same:
            print("!! WARNING: outputs differ — not a valid comparison")

    if not KEEP:
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    main()
