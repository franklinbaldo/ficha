from pathlib import Path


registry_path = Path("etl/src/ficha_etl/registry.py")
registry = registry_path.read_text()
needle = '''        "SELECT * FROM read_csv(\\n"
        f"    {literal},\\n"
        f"    delim='{spec.delimiter}',\\n"
'''
replacement = '''        "SELECT * FROM read_csv(\\n"
        f"    {literal},\\n"
        "    auto_detect=false,\\n"
        f"    delim='{spec.delimiter}',\\n"
'''
if needle not in registry:
    raise SystemExit("read_csv_select_sql insertion point not found")
registry = registry.replace(needle, replacement, 1)
registry = registry.replace(
    '    """Gera o ``SELECT * FROM read_csv(...)`` semanticamente equivalente ao legado."""',
    '    """Gera ``read_csv`` com dialeto/schema pinados e sem sniff heurístico."""',
    1,
)
registry_path.write_text(registry)


transform_test_path = Path("etl/tests/test_transform.py")
transform_test = transform_test_path.read_text()
old = '''def test_create_table_from_csvs_sniff_utf8(tmp_path, caplog):
    import logging
    from ficha_etl.transform import _create_table_from_csvs
    import duckdb

    csv_path = tmp_path / "data_utf8.csv"
    # Write some utf-8 characters
    csv_path.write_bytes('1;2;"Olá Mundo"'.encode("utf-8"))

    con = duckdb.connect()
    try:
        with caplog.at_level(logging.WARNING):
            spec = registry.CsvSpec(columns=("c1", "c2", "c3"))
            _create_table_from_csvs(con, "test_table", [csv_path], spec)

        assert (
            "tabela 'test_table' carregada com encoding=utf-8 ignore_errors=True (fallback)"
            in caplog.text
        )

        res = con.execute("SELECT * FROM test_table").fetchall()
        assert res == [("1", "2", "Olá Mundo")]
    finally:
        con.close()
'''
new = '''def test_create_table_from_csvs_reads_true_utf8_without_fallback(tmp_path, caplog):
    import logging
    from ficha_etl.transform import _create_table_from_csvs
    import duckdb

    csv_path = tmp_path / "data_utf8.csv"
    csv_path.write_bytes('1;2;"Olá Mundo"'.encode("utf-8"))

    con = duckdb.connect()
    try:
        with caplog.at_level(logging.WARNING):
            spec = registry.CsvSpec(columns=("c1", "c2", "c3"))
            _create_table_from_csvs(con, "test_table", [csv_path], spec)

        assert "fallback" not in caplog.text
        res = con.execute("SELECT * FROM test_table").fetchall()
        assert res == [("1", "2", "Olá Mundo")]
    finally:
        con.close()
'''
if old not in transform_test:
    raise SystemExit("legacy UTF-8 fallback test block not found")
transform_test = transform_test.replace(old, new, 1)
transform_test_path.write_text(transform_test)


registry_test_path = Path("etl/tests/test_registry.py")
registry_test = registry_test_path.read_text()
registry_test = registry_test.replace(
    '''    Precisa de ao menos uma linha bem-formada além da ragged: com só a linha
    ragged no arquivo, o sniffer de dialeto do DuckDB não tem referência de
    largura e falha mesmo com ignore_errors=true (comportamento real
    verificado manualmente contra duckdb 1.5.2 — não é bug introduzido aqui;
    é exatamente o cenário real da RFB, onde ragged rows são raras entre
    milhões de linhas bem-formadas).
''',
    '''    O dialeto e o schema são declarados pelo CsvSpec e o reader roda com
    auto_detect=false. Portanto null_padding — e não ignore_errors — é quem
    define a semântica da linha curta, sem pedir ao sniffer para decidir se o
    arquivo é aceitável.
''',
    1,
)
registry_test = registry_test.replace(
    'def test_mixed_encoding_loads_via_fallback(tmp_path):\n'
    '    """Bytes latin-1 inválidos como utf-8 — carrega via fallback sem exceção."""',
    'def test_latin1_bytes_load_via_lossless_transcode(tmp_path):\n'
    '    """Bytes latin-1 não-UTF-8 sobrevivem à transcodificação sem descarte."""',
    1,
)
old_custom = '''def test_create_table_from_csvs_honors_custom_delimiter(tmp_path):
    """CsvSpec.delimiter é realmente usado pelo reader, não apenas aceito e descartado.

    Prova em dois lados, com o MESMO arquivo (separado por vírgula, sem ';'):
    - CsvSpec(delimiter=',') lê corretamente as 3 colunas;
    - CsvSpec() (default, delimiter=';') falha ao carregar — o sniffer do
      DuckDB só encontra 1 campo por linha (não há ';' no arquivo), o que
      diverge do schema de 3 colunas declarado e aborta com RuntimeError.
    Isso prova que o delimiter do spec chega de fato ao SQL executado — se
    `_create_table_from_csvs` reconstruísse um CsvSpec com defaults (o bug
    reportado), o primeiro caso falharia do mesmo jeito que o segundo.
    """
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "comma.csv"
    _write_bytes(csv_path, b'"1","2","3"\\n')

    con = duckdb.connect()
    try:
        comma_spec = CsvSpec(columns=("c1", "c2", "c3"), delimiter=",")
        _create_table_from_csvs(con, "t_comma", [csv_path], comma_spec)
        rows = con.execute("SELECT * FROM t_comma").fetchall()
        assert rows == [("1", "2", "3")]

        default_spec = CsvSpec(columns=("c1", "c2", "c3"))  # delimiter=';' default
        with pytest.raises(RuntimeError, match="nenhum encoding funcionou"):
            _create_table_from_csvs(con, "t_default_delim", [csv_path], default_spec)
    finally:
        con.close()
'''
new_custom = '''def test_create_table_from_csvs_honors_custom_delimiter(tmp_path):
    """CsvSpec.delimiter chega ao reader mesmo com auto_detect desabilitado.

    O arquivo usa vírgula; se `_create_table_from_csvs` reconstruísse o spec
    com o default `;`, a primeira asserção já deixaria de produzir três
    colunas corretas. Não dependemos mais do sniffer falhar no caso negativo,
    porque o spec é agora deliberadamente a autoridade do dialeto.
    """
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "comma.csv"
    _write_bytes(csv_path, b'"1","2","3"\\n')

    con = duckdb.connect()
    try:
        comma_spec = CsvSpec(columns=("c1", "c2", "c3"), delimiter=",")
        _create_table_from_csvs(con, "t_comma", [csv_path], comma_spec)
        rows = con.execute("SELECT * FROM t_comma").fetchall()
        assert rows == [("1", "2", "3")]
    finally:
        con.close()
'''
if old_custom not in registry_test:
    raise SystemExit("custom delimiter test block not found")
registry_test = registry_test.replace(old_custom, new_custom, 1)
registry_test_path.write_text(registry_test)
