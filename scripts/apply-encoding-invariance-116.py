from pathlib import Path


transform_path = Path("etl/src/ficha_etl/transform.py")
text = transform_path.read_text()
start = text.index("def _create_table_from_csvs(")
end = text.index("\n\n_OS_HEADROOM_GB", start)
replacement = '''def _create_table_from_single_csv(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_path: Path,
    spec: registry.CsvSpec,
) -> None:
    """Carrega exatamente um CSV usando a política de encoding RFB.

    A resolução de encoding é deliberadamente por arquivo. Um arquivo que
    exige fallback não pode mudar a forma como os outros arquivos do mesmo
    snapshot são interpretados; caso contrário o load bulk e o writer
    chunked observam conjuntos de linhas diferentes (#116).
    """
    with open(csv_path, "rb") as f:
        sample = f.read(1024 * 1024)
    attempts = registry.encoding_attempts(sample)

    for encoding, ignore_errors in attempts:
        select_sql = registry.read_csv_select_sql(
            spec, [csv_path], encoding=encoding, ignore_errors=ignore_errors
        )
        try:
            con.execute(f"CREATE OR REPLACE TABLE {table} AS\\n{select_sql}")
            if encoding != "latin-1" or ignore_errors:
                log.warning(
                    "tabela '%s' (%s) carregada com encoding=%s "
                    "ignore_errors=%s (fallback)",
                    table,
                    csv_path.name,
                    encoding,
                    ignore_errors,
                )
            return
        except Exception as exc:
            msg = str(exc).lower()
            if "not latin-1 encoded" in msg or "not utf-8" in msg or "encoding" in msg:
                log.warning(
                    "encoding=%s ignore_errors=%s falhou para '%s' (%s): %s "
                    "-- tentando proximo",
                    encoding,
                    ignore_errors,
                    table,
                    csv_path.name,
                    exc,
                )
                continue
            raise
    raise RuntimeError(
        f"Falha ao carregar tabela '{table}' de {csv_path.name!r}: "
        "nenhum encoding funcionou (latin-1, utf-8, utf-8+ignore_errors)"
    )


def _create_table_from_csvs(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_paths: Iterable[Path],
    spec: registry.CsvSpec,
) -> None:
    """Cria/recria ``table`` como união de leituras resolvidas por arquivo.

    O contrato é intencionalmente ``bulk(paths) == UNION(read(path)...)``.
    Antes de #116, uma falha de encoding em qualquer membro de um lote fazia
    `_create_table_from_csvs` reaplicar o fallback ao lote inteiro. O writer
    de `cnpjs.parquet`, porém, relê `estabelecimento` arquivo a arquivo. Em
    2026-05 isso fez o load bulk usar `utf-8 + ignore_errors=True` nos dez
    arquivos enquanto só alguns chunks precisavam do fallback, produzindo
    contagens incompatíveis no roundtrip.

    Cada arquivo agora passa pela mesma `_create_table_from_single_csv` usada
    semanticamente pelo caminho chunked; arquivos saudáveis nunca herdam
    `ignore_errors=True` de um vizinho problemático.
    """
    paths = [p for p in csv_paths if p.exists() and p.stat().st_size > 0]
    if not paths:
        col_defs = ", ".join(f"{c} VARCHAR" for c in spec.columns)
        con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
        return

    _create_table_from_single_csv(con, table, paths[0], spec)
    staging = f"__ficha_{table}_csv_part"
    try:
        for csv_path in paths[1:]:
            _create_table_from_single_csv(con, staging, csv_path, spec)
            con.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
    finally:
        con.execute(f"DROP TABLE IF EXISTS {staging}")
'''
transform_path.write_text(text[:start] + replacement + text[end:])


test_path = Path("etl/tests/test_encoding_invariance_116.py")
test_path.write_text('''from pathlib import Path\n\nimport duckdb\n\nfrom ficha_etl import registry, transform\n\n\ndef _write(path: Path, code: str) -> None:\n    path.write_bytes(f'"{code}";"descricao"\\n'.encode("latin-1"))\n\n\ndef test_bulk_csv_read_resolves_encoding_per_file(tmp_path, monkeypatch):\n    first = tmp_path / "first.csv"\n    second = tmp_path / "second.csv"\n    _write(first, "01")\n    _write(second, "02")\n\n    spec = registry.CsvSpec(columns=("codigo", "descricao"))\n    calls = []\n    original = registry.read_csv_select_sql\n\n    def spy(spec_arg, paths, *, encoding, ignore_errors):\n        path_list = list(paths)\n        calls.append((tuple(path_list), encoding, ignore_errors))\n        return original(\n            spec_arg, path_list, encoding=encoding, ignore_errors=ignore_errors\n        )\n\n    monkeypatch.setattr(registry, "read_csv_select_sql", spy)\n    con = duckdb.connect()\n    transform._create_table_from_csvs(con, "lookup_test", [first, second], spec)\n\n    assert con.execute("SELECT COUNT(*) FROM lookup_test").fetchone()[0] == 2\n    assert {row[0] for row in con.execute("SELECT codigo FROM lookup_test").fetchall()} == {\n        "01",\n        "02",\n    }\n    assert calls\n    assert all(len(paths) == 1 for paths, _encoding, _ignore in calls)\n\n\ndef test_bulk_count_equals_sum_of_individual_reads(tmp_path):\n    paths = [tmp_path / f"part-{i}.csv" for i in range(3)]\n    for i, path in enumerate(paths):\n        _write(path, f"0{i}")\n\n    spec = registry.CsvSpec(columns=("codigo", "descricao"))\n    con = duckdb.connect()\n    transform._create_table_from_csvs(con, "bulk", paths, spec)\n    bulk = con.execute("SELECT COUNT(*) FROM bulk").fetchone()[0]\n\n    individual = 0\n    for i, path in enumerate(paths):\n        transform._create_table_from_csvs(con, f"single_{i}", [path], spec)\n        individual += con.execute(f"SELECT COUNT(*) FROM single_{i}").fetchone()[0]\n\n    assert bulk == individual == len(paths)\n''')
