from pathlib import Path

path = Path("src/ficha_etl/transform.py")
text = path.read_text(encoding="utf-8")
start_marker = '    log.info("    joining + writing raizes.parquet from cnpjs...")'
end_marker = "    # Free all temp tables."
if start_marker not in text:
    raise SystemExit("expected pre-#118 raizes join marker not found")
start = text.index(start_marker)
end = text.index(end_marker, start)
replacement = '''    log.info("    joining + writing raizes.parquet from cnpjs (bucketed)...")
    # Production run 31435386961 proved that the global four-way LEFT JOIN
    # spills ~71 GiB and fills 99.5% of the Actions filesystem. The eager
    # aggregates above are already safe; bound only this final operator by
    # first digit of cnpj_base, preserving the exact projection/join semantics.
    parts_dir = output_path.parent / f"{output_path.stem}.parts"
    shutil.rmtree(parts_dir, ignore_errors=True)
    parts_dir.mkdir(parents=True, exist_ok=True)
    bucket_tables = (
        "_raizes_empresa_b",
        "_raizes_counts_b",
        "_raizes_ufs_agg_b",
        "_raizes_cnaes_agg_b",
        "_raizes_matriz_b",
    )
    bucket_sources = (
        ("_raizes_empresa", "_raizes_empresa_b"),
        ("_raizes_counts", "_raizes_counts_b"),
        ("_raizes_ufs_agg", "_raizes_ufs_agg_b"),
        ("_raizes_cnaes_agg", "_raizes_cnaes_agg_b"),
        ("_raizes_matriz", "_raizes_matriz_b"),
    )
    _empty = ibis.literal([], type="array<string>")
    try:
        written_parts: list[Path] = []
        for prefix in "0123456789":
            log.info("      raizes bucket %s/9: materializing inputs", prefix)
            for source, target in bucket_sources:
                con.execute(
                    f"CREATE OR REPLACE TEMP TABLE {target} AS "
                    f"SELECT * FROM {source} WHERE LEFT(cnpj_base, 1) = '{prefix}'"
                )
            roots = con.execute("SELECT COUNT(*) FROM _raizes_empresa_b").fetchone()[0]
            if roots == 0:
                for table in bucket_tables:
                    con.execute(f"DROP TABLE IF EXISTS {table}")
                continue
            emp = icon.table("_raizes_empresa_b")
            cnt = icon.table("_raizes_counts_b")
            ufs = icon.table("_raizes_ufs_agg_b")
            cnaes = icon.table("_raizes_cnaes_agg_b")
            mat = icon.table("_raizes_matriz_b")
            expr = (
                emp.left_join(cnt, emp.cnpj_base == cnt.cnpj_base)
                .left_join(ufs, emp.cnpj_base == ufs.cnpj_base)
                .left_join(cnaes, emp.cnpj_base == cnaes.cnpj_base)
                .left_join(mat, emp.cnpj_base == mat.cnpj_base)
                .select(
                    "cnpj_base",
                    *_empresa_fields,
                    qtd_estabelecimentos=cnt.qtd_estabelecimentos.coalesce(0),
                    qtd_estabelecimentos_ativos=cnt.qtd_estabelecimentos_ativos.coalesce(0),
                    ufs_atuacao=ufs.ufs_atuacao.coalesce(_empty),
                    cnaes_principais_distintos=cnaes.cnaes_principais_distintos.coalesce(_empty),
                    data_inicio_atividade_matriz=mat.data_inicio_atividade_matriz,
                    uf_matriz=mat.uf_matriz,
                    municipio_matriz_codigo=mat.municipio_matriz_codigo,
                    municipio_matriz_nome=mat.municipio_matriz_nome,
                    cnae_principal_matriz_codigo=mat.cnae_principal_matriz_codigo,
                    cnae_principal_matriz_descricao=mat.cnae_principal_matriz_descricao,
                )
            )
            part = parts_dir / f"bucket-{prefix}.parquet"
            con.execute(
                f"COPY ({ibis.to_sql(expr, dialect='duckdb')}) "
                f"TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
            )
            written_parts.append(part)
            log.info("      raizes bucket %s: wrote %d roots", prefix, roots)
            for table in bucket_tables:
                con.execute(f"DROP TABLE IF EXISTS {table}")
        if not written_parts:
            raise RuntimeError("raizes final join produced no bucket parts")
        parts = ", ".join(repr(str(p)) for p in written_parts)
        con.execute(
            f"COPY (SELECT * FROM read_parquet([{parts}])) "
            f"TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
        )
    finally:
        for table in bucket_tables:
            con.execute(f"DROP TABLE IF EXISTS {table}")
        shutil.rmtree(parts_dir, ignore_errors=True)

'''
path.write_text(text[:start] + replacement + text[end:], encoding="utf-8")
