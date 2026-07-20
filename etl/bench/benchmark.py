#!/usr/bin/env python
"""Scaled, repeatable benchmark for the ficha ETL transform stages.

Purpose
-------
Give each performance change (typed join keys, one-pass chunk fan-out, ...) a
*measured* wall-clock number instead of a hunch, on synthetic RFB-shaped data
big enough to be representative but small enough to iterate on a laptop.

It exercises the real stage functions from ``ficha_etl.transform`` in the same
order ``transform_snapshot`` runs them (including ``load_main_tables_into_duckdb``
itself, dedup and all -- not a hand-rolled substitute), under the same
production-profile DuckDB connection (`bench/_profile.py`: file-backed,
production `memory_limit`/`temp_directory`/`preserve_insertion_order`/`threads`),
so the numbers reflect the production code path, not a differently-configured
approximation of it. See `bench/README.md` "Method notes" for the methodology
review that drove this shape (RFC 0001 §7.10).

What it does NOT do: download, IA upload, the protobuf pack. Those are separate
concerns; this harness is scoped to the DuckDB/Parquet transform.

Usage
-----
    uv run --all-extras python bench/benchmark.py --scale 500000 --chunks 8
    uv run --all-extras python bench/benchmark.py --scale 2000000 --chunks 16 --repeats 3 --json out.json

``--scale`` is the number of empresas (unique cnpj_basico); establishments come
out to ~1.33x that (one matriz each, plus a filial for every third base). A
small, fixed fraction of bases (1 in 500) get an injected duplicate empresa/
simples row with a different payload, so `load_main_tables_into_duckdb`'s
dedup path is actually exercised and its cost measured, not bypassed. The
generated CSVs are cached under ``--workdir`` keyed by (scale, chunks), so a
re-run with the same parameters skips regeneration and only re-times stages.

``--repeats`` (default 1) reruns the ENTIRE stage sequence that many times,
each against a fresh DuckDB file + fresh production connection (some stages
DROP tables, so state can't safely carry across reps), and reports median +
spread per stage rather than a single wall-clock number. Default 1 is for
quick dev iteration; a genuine measurement decision should use --repeats 3+
(RFC 0001 §7.10: a single run is exploration, not a decision).
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import statistics
import time
from pathlib import Path

import duckdb

from _profile import capture_environment, open_production_connection
from ficha_etl import registry, transform
from ficha_etl.transform import ExtractedFile

# The transform module logs an INFO/WARNING per table load (e.g. the utf-8
# encoding fallback, which always fires on our ASCII synthetic CSVs). That's
# noise for a benchmark — keep only real errors.
logging.getLogger("ficha_etl").setLevel(logging.ERROR)

# Lookup code spaces — kept small; join match-rate barely moves timing, but we
# make the codes line up with the establishment fields so the LEFT JOINs mostly
# hit (realistic) rather than all-miss.
_N_CNAES = 1000
_N_MUNICIPIOS = 200
_N_NATUREZAS = 100
_N_QUALIF = 60
_N_PAISES = 30
_N_MOTIVOS = 40

# Every 500th base gets an injected duplicate empresa/simples row (different
# payload) so the dedup path in load_main_tables_into_duckdb is actually
# exercised. Small enough not to dominate total row counts at any --scale
# worth benchmarking, present enough to give the dedup queries real work.
_DUP_EVERY_N = 500

_UFS = [
    "RO",
    "AC",
    "AM",
    "RR",
    "PA",
    "AP",
    "TO",
    "MA",
    "PI",
    "CE",
    "RN",
    "PB",
    "PE",
    "AL",
    "SE",
    "BA",
    "MG",
    "ES",
    "RJ",
    "SP",
    "PR",
    "SC",
    "RS",
    "MS",
    "MT",
    "GO",
    "DF",
]


def _copy_csv(con: duckdb.DuckDBPyConnection, select_sql: str, path: Path) -> None:
    """Write a query to an RFB-shaped CSV: ';'-delimited, all fields quoted, no header."""
    con.execute(
        f"COPY ({select_sql}) TO '{path}' "
        "(FORMAT CSV, DELIMITER ';', HEADER false, QUOTE '\"', FORCE_QUOTE *)"
    )


def _uf_case(base_expr: str) -> str:
    """SQL CASE mapping base % 27 → a UF string."""
    whens = " ".join(f"WHEN {i} THEN '{uf}'" for i, uf in enumerate(_UFS))
    return f"CASE ({base_expr} % 27) {whens} END"


def _estabelecimento_select(lo: int, hi: int) -> str:
    """SELECT producing estabelecimento rows (30 cols) for base in (lo, hi].

    One matriz (ordem 0001) per base plus a filial (0002) for every third base,
    so the establishment count is ~1.33x the empresa count — the same shape that
    makes cnpjs.parquet have more rows than empresa.
    """
    b = "bs.base"
    o = "ord.o"
    # Contact fields vary by base so the contatos fan-out is realistic:
    # every row has telefone_1; ~30% telefone_2; ~10% fax; ~70% email.
    tel1 = "'11'"
    ddd2 = f"CASE WHEN {b} % 10 < 3 THEN '21' ELSE '' END"
    tel2 = f"CASE WHEN {b} % 10 < 3 THEN lpad(({b} % 100000000)::VARCHAR, 8, '0') ELSE '' END"
    dfax = f"CASE WHEN {b} % 10 = 0 THEN '11' ELSE '' END"
    fax = f"CASE WHEN {b} % 10 = 0 THEN lpad(({b} % 100000000)::VARCHAR, 8, '0') ELSE '' END"
    email = f"CASE WHEN {b} % 10 < 7 THEN 'e' || {b} || '@x.com' ELSE '' END"
    # Secondary CNAEs for ~40% of rows: a comma-joined list of 1-3 codes.
    cnae_sec = (
        f"CASE WHEN {b} % 10 < 4 THEN "
        f"lpad((({b} * 7) % {_N_CNAES})::VARCHAR, 7, '0') || ',' || "
        f"lpad((({b} * 13) % {_N_CNAES})::VARCHAR, 7, '0') "
        "ELSE '' END"
    )
    # Logradouro with abbreviation prefixes to exercise enderecos normalization.
    logr = (
        f"CASE {b} % 4 "
        "WHEN 0 THEN 'R DAS FLORES' WHEN 1 THEN 'AV BRASIL' "
        "WHEN 2 THEN 'TV DOS ANJOS' ELSE 'RUA XV DE NOVEMBRO' END"
    )
    return f"""
        SELECT
            lpad({b}::VARCHAR, 8, '0')                          AS cnpj_basico,
            {o}                                                 AS cnpj_ordem,
            lpad(({b} % 100)::VARCHAR, 2, '0')                  AS cnpj_dv,
            CASE WHEN {o} = '0001' THEN '1' ELSE '2' END        AS identificador_matriz_filial,
            'FANTASIA ' || {b}                                  AS nome_fantasia,
            (ARRAY['02','08','03','04'])[({b} % 4) + 1]         AS situacao_cadastral,
            '20200101'                                          AS data_situacao_cadastral,
            lpad(({b} % {_N_MOTIVOS})::VARCHAR, 2, '0')         AS motivo_situacao_cadastral,
            ''                                                  AS nome_cidade_exterior,
            lpad(({b} % {_N_PAISES})::VARCHAR, 3, '0')          AS pais,
            '20180101'                                          AS data_inicio_atividade,
            lpad(({b} % {_N_CNAES})::VARCHAR, 7, '0')           AS cnae_fiscal_principal,
            {cnae_sec}                                          AS cnae_fiscal_secundaria,
            'RUA'                                               AS tipo_logradouro,
            {logr}                                              AS logradouro,
            ({b} % 5000)::VARCHAR                               AS numero,
            ''                                                  AS complemento,
            'CENTRO'                                            AS bairro,
            lpad(({b} % 99999999)::VARCHAR, 8, '0')             AS cep,
            {_uf_case(b)}                                       AS uf,
            lpad(({b} % {_N_MUNICIPIOS})::VARCHAR, 7, '0')      AS municipio,
            {tel1}                                              AS ddd_1,
            lpad(({b} % 100000000)::VARCHAR, 8, '0')            AS telefone_1,
            {ddd2}                                              AS ddd_2,
            {tel2}                                              AS telefone_2,
            {dfax}                                              AS ddd_fax,
            {fax}                                               AS fax,
            {email}                                             AS correio_eletronico,
            ''                                                  AS situacao_especial,
            ''                                                  AS data_situacao_especial
        FROM (SELECT range AS base FROM range({lo} + 1, {hi} + 1)) bs,
             LATERAL (
                 SELECT o FROM (VALUES ('0001'), ('0002')) v(o)
                 WHERE o = '0001' OR bs.base % 3 = 0
             ) ord
    """


def generate(scale: int, chunks: int, data_dir: Path) -> list[Path]:
    """Generate (or reuse cached) synthetic RFB CSVs. Returns estabelecimento CSV paths."""
    marker = data_dir / f".scale-{scale}-chunks-{chunks}-dup{_DUP_EVERY_N}"
    est_paths = sorted(data_dir.glob("estabelecimento-*.csv"))
    if marker.exists() and est_paths:
        print(f"  reusing cached data in {data_dir} ({len(est_paths)} estab CSVs)")
        return est_paths

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    gen = duckdb.connect()
    try:
        t0 = time.monotonic()
        # empresa: one row per base, PLUS an injected duplicate (different
        # razao_social, so it's a genuinely conflicting duplicate, not a
        # trivially-collapsible exact one) for every _DUP_EVERY_N-th base --
        # exercises load_main_tables_into_duckdb's dedup path for real.
        _copy_csv(
            gen,
            f"""
            SELECT lpad(base::VARCHAR, 8, '0')                    AS cnpj_basico,
                   'EMPRESA ' || base                            AS razao_social,
                   lpad((base % {_N_NATUREZAS})::VARCHAR, 4, '0') AS natureza_juridica,
                   lpad((base % {_N_QUALIF})::VARCHAR, 2, '0')    AS qualificacao_responsavel,
                   (base % 1000000)::VARCHAR || ',00'            AS capital_social,
                   lpad(((base % 5) + 1)::VARCHAR, 2, '0')        AS porte_empresa,
                   ''                                            AS ente_federativo_responsavel
            FROM (SELECT range AS base FROM range(1, {scale} + 1))
            UNION ALL
            SELECT lpad(base::VARCHAR, 8, '0'),
                   'EMPRESA ' || base || ' DUPLICADA',
                   lpad((base % {_N_NATUREZAS})::VARCHAR, 4, '0'),
                   lpad((base % {_N_QUALIF})::VARCHAR, 2, '0'),
                   (base % 1000000)::VARCHAR || ',00',
                   lpad(((base % 5) + 1)::VARCHAR, 2, '0'),
                   ''
            FROM (SELECT range AS base FROM range(1, {scale} + 1))
            WHERE base % {_DUP_EVERY_N} = 0
            """,
            data_dir / "empresa.csv",
        )
        # simples: ~60% of bases, same duplicate-injection pattern.
        _copy_csv(
            gen,
            f"""
            SELECT lpad(base::VARCHAR, 8, '0') AS cnpj_basico,
                   CASE WHEN base % 2 = 0 THEN 'S' ELSE 'N' END AS opcao_simples,
                   '20180101' AS data_opcao_simples, '' AS data_exclusao_simples,
                   CASE WHEN base % 3 = 0 THEN 'S' ELSE 'N' END AS opcao_mei,
                   '20180101' AS data_opcao_mei, '' AS data_exclusao_mei
            FROM (SELECT range AS base FROM range(1, {scale} + 1)) WHERE base % 5 < 3
            UNION ALL
            SELECT lpad(base::VARCHAR, 8, '0'),
                   CASE WHEN base % 2 = 0 THEN 'N' ELSE 'S' END,  -- deliberately flipped: conflicting payload
                   '20180101', '',
                   CASE WHEN base % 3 = 0 THEN 'S' ELSE 'N' END,
                   '20180101', ''
            FROM (SELECT range AS base FROM range(1, {scale} + 1))
            WHERE base % 5 < 3 AND base % {_DUP_EVERY_N} = 0
            """,
            data_dir / "simples.csv",
        )
        # socio: ~1 PF socio per other base.
        _copy_csv(
            gen,
            f"""
            SELECT lpad(base::VARCHAR, 8, '0') AS cnpj_basico,
                   '2' AS identificador_socio,
                   'SOCIO ' || base AS nome_socio_razao_social,
                   '***' || lpad((base % 1000000)::VARCHAR, 6, '0') || '**' AS cnpj_cpf_socio,
                   lpad((base % {_N_QUALIF})::VARCHAR, 2, '0') AS qualificacao_socio,
                   '20190101' AS data_entrada_sociedade,
                   lpad((base % {_N_PAISES})::VARCHAR, 3, '0') AS pais,
                   '' AS representante_legal, '' AS nome_representante_legal,
                   '' AS qualificacao_representante_legal,
                   ((base % 8) + 1)::VARCHAR AS faixa_etaria
            FROM (SELECT range AS base FROM range(1, {scale} + 1)) WHERE base % 2 = 0
            """,
            data_dir / "socio.csv",
        )
        # estabelecimento: split into `chunks` files by base-range, so the
        # chunked cnpjs writer sees the same one-CSV-at-a-time shape as prod.
        step = max(1, scale // chunks)
        est_paths = []
        for c in range(chunks):
            lo = c * step
            hi = scale if c == chunks - 1 else (c + 1) * step
            if lo >= hi:
                continue
            p = data_dir / f"estabelecimento-{c:03d}.csv"
            _copy_csv(gen, _estabelecimento_select(lo, hi), p)
            est_paths.append(p)

        # lookups (codigo;descricao), covering the code spaces used above.
        for kind, n in (
            ("cnaes", _N_CNAES),
            ("municipios", _N_MUNICIPIOS),
            ("naturezas", _N_NATUREZAS),
            ("qualificacoes", _N_QUALIF),
            ("paises", _N_PAISES),
            ("motivos", _N_MOTIVOS),
        ):
            width = {"cnaes": 7, "municipios": 7, "naturezas": 4, "paises": 3}.get(kind, 2)
            _copy_csv(
                gen,
                f"SELECT lpad(range::VARCHAR, {width}, '0'), '{kind} ' || range FROM range(0, {n})",
                data_dir / f"lookup_{kind}.csv",
            )
        marker.write_text(f"scale={scale} chunks={chunks} dup_every_n={_DUP_EVERY_N}\n")
        print(f"  generated synthetic data in {time.monotonic() - t0:.1f}s -> {data_dir}")
    finally:
        gen.close()
    return sorted(data_dir.glob("estabelecimento-*.csv"))


def _row_count(con: duckdb.DuckDBPyConnection, table_or_path: str, is_path: bool) -> int:
    src = f"'{table_or_path}'" if is_path else table_or_path
    return con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]


def _extracted_files(data_dir: Path, est_paths: list[Path]) -> list[ExtractedFile]:
    """Build the ExtractedFile list load_main_tables_into_duckdb (and the
    lookup loader) actually consume -- the real production input shape, not
    a hand-rolled substitute that skips whatever that function does.
    """
    files = [
        ExtractedFile(kind="empresas", zip_name="empresa.zip", csv_path=data_dir / "empresa.csv"),
        ExtractedFile(kind="simples", zip_name="simples.zip", csv_path=data_dir / "simples.csv"),
        ExtractedFile(kind="socios", zip_name="socio.zip", csv_path=data_dir / "socio.csv"),
    ]
    files.extend(
        ExtractedFile(kind="estabelecimentos", zip_name=f"estab-{p.stem}.zip", csv_path=p)
        for p in est_paths
    )
    for kind in ("cnaes", "municipios", "naturezas", "qualificacoes", "paises", "motivos"):
        files.append(
            ExtractedFile(
                kind=kind, zip_name=f"{kind}.zip", csv_path=data_dir / f"lookup_{kind}.csv"
            )
        )
    return files


def run_once(
    scale: int, chunks: int, data_dir: Path, out_dir: Path, db_path: Path
) -> tuple[list[tuple[str, float, int]], dict]:
    """One full pass through the stage sequence against a fresh production
    connection. Returns (per-stage timings, effective connection metadata).
    """
    est_paths = sorted(data_dir.glob("estabelecimento-*.csv"))
    extracted = _extracted_files(data_dir, est_paths)

    con = open_production_connection(db_path)
    env = capture_environment(con, db_path)

    timings: list[tuple[str, float, int]] = []

    def stage(name: str, fn, count_src=None, is_path=False) -> None:
        t0 = time.monotonic()
        fn()
        dt = time.monotonic() - t0
        rows = _row_count(con, count_src, is_path) if count_src else 0
        timings.append((name, dt, rows))
        print(f"    {name:<24} {dt:8.2f}s   {rows:>12,} rows")

    print(
        f"  running stages (threads={env['threads']}, memory_limit={env['memory_limit']}, "
        f"{len(est_paths)} estab chunks)..."
    )

    # Real production loader for lookups AND main tables — including the
    # dedup pass (empresa/simples get injected duplicates, see generate()) —
    # instead of calling _create_table_from_csvs per table by hand and
    # silently skipping whatever load_main_tables_into_duckdb does. Mirrors
    # transform_snapshot's actual PHASE 1/2 structure: lookups first, then
    # ONE load_main_tables_into_duckdb call (production wraps this in a
    # single "load_duckdb" stage too — see transform.py's recorder.stage
    # call site — so this benchmark's granularity matches production's, not
    # an artificially finer breakdown production doesn't actually have).
    def _load_lookups():
        for ef in extracted:
            if ef.kind in transform._LOOKUP_KINDS:
                transform.load_lookup_into_duckdb(con, ef.kind, ef.csv_path)

    stage("load_lookups", _load_lookups)

    dupes_found = 0

    def _load_main_tables():
        nonlocal dupes_found
        dupes_found = transform.load_main_tables_into_duckdb(con, extracted)

    stage("load_main_tables", _load_main_tables, "estabelecimento")
    print(f"    (load_main_tables collapsed {dupes_found} duplicate cnpj_basico row(s))")

    # Scan-based writers (item 4 targets these — each is a full estab scan today).
    stage(
        "write_cnpj_contatos",
        lambda: transform.write_cnpj_contatos_parquet(con, out_dir / "cnpj_contatos.parquet"),
        str(out_dir / "cnpj_contatos.parquet"),
        is_path=True,
    )
    stage(
        "write_cnpj_cnaes",
        lambda: transform.write_cnpj_cnaes_parquet(con, out_dir / "cnpj_cnaes.parquet"),
        str(out_dir / "cnpj_cnaes.parquet"),
        is_path=True,
    )
    stage(
        "write_enderecos",
        lambda: transform.write_enderecos_parquet(con, out_dir / "enderecos.parquet"),
        str(out_dir / "enderecos.parquet"),
        is_path=True,
    )

    # Drop the full table, mirror prod, then the chunked cnpjs writer.
    stage("drop_estabelecimento", lambda: con.execute("DROP TABLE IF EXISTS estabelecimento"))
    stage(
        "write_cnpjs_chunked",
        lambda: transform.write_cnpjs_parquet_chunked(con, est_paths, out_dir / "cnpjs.parquet"),
        str(out_dir / "cnpjs.parquet"),
        is_path=True,
    )

    # Roundtrip verify (reloads estab, like prod).
    def _verify():
        transform._create_table_from_csvs(
            con, "estabelecimento", est_paths, registry.main_table("estabelecimento").source
        )
        transform.assert_roundtrip(con, out_dir / "cnpjs.parquet", sample_size=1000)

    stage("verify_roundtrip", _verify)

    con.close()
    return timings, env


def run(scale: int, chunks: int, workdir: Path, repeats: int) -> dict:
    data_dir = workdir / "data"
    est_paths = generate(scale, chunks, data_dir)

    per_stage: dict[str, list[float]] = {}
    row_counts: dict[str, int] = {}
    env: dict = {}
    for rep in range(repeats):
        out_dir = workdir / f"out-rep{rep}"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        db_path = workdir / f"bench-rep{rep}.duckdb"
        if db_path.exists():
            db_path.unlink()
        print(f"  --- rep {rep + 1}/{repeats} ---")
        timings, env = run_once(scale, chunks, data_dir, out_dir, db_path)
        for name, dt, rows in timings:
            per_stage.setdefault(name, []).append(dt)
            row_counts[name] = rows
        shutil.rmtree(out_dir, ignore_errors=True)
        db_path.unlink(missing_ok=True)
        shutil.rmtree(db_path.parent / "duckdb_tmp", ignore_errors=True)

    stage_results = []
    total_median = 0.0
    for name, times in per_stage.items():
        med = statistics.median(times)
        spread = (max(times) - min(times)) if len(times) > 1 else 0.0
        total_median += med
        stage_results.append(
            {
                "name": name,
                "rows": row_counts[name],
                "times": [round(t, 4) for t in times],
                "median_seconds": round(med, 4),
                "spread_seconds": round(spread, 4),
            }
        )
        print(f"  {name:<24} median={med:8.2f}s  spread={spread:6.2f}s  n={len(times)}")
    print(f"  {'TOTAL (sum of medians)':<24} {total_median:8.2f}s")

    return {
        "scale": scale,
        "chunks": chunks,
        "repeats": repeats,
        "est_chunks": len(est_paths),
        "environment": env,
        "stages": stage_results,
        "total_median_seconds": round(total_median, 3),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--scale", type=int, default=500_000, help="number of empresas (unique cnpj_basico)"
    )
    ap.add_argument(
        "--chunks", type=int, default=8, help="how many estabelecimento CSVs to split into"
    )
    ap.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="full stage-sequence repetitions (median+spread reported); use 3+ for a real "
        "measurement decision, 1 (default) for quick dev iteration",
    )
    ap.add_argument(
        "--workdir",
        type=Path,
        default=Path("bench/.work"),
        help="where generated CSVs + output parquets live (data cached by scale/chunks)",
    )
    ap.add_argument(
        "--json", type=Path, default=None, help="also write the timing table as JSON here"
    )
    args = ap.parse_args()

    print(
        f"ficha ETL benchmark — scale={args.scale:,} empresas, chunks={args.chunks}, "
        f"repeats={args.repeats}"
    )
    result = run(args.scale, args.chunks, args.workdir, args.repeats)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, indent=2))
        print(f"  wrote {args.json}")


if __name__ == "__main__":
    main()
