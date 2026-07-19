"""Testes do schema registry (Fase 1 da RFC 0001 — sem mudança de comportamento).

Cobertura:
  a) SQL golden — read_csv_select_sql gera exatamente as opções esperadas.
  b) Layout congelado — as tuplas de colunas não mudam por acidente.
  c) encoding_attempts — mesma semântica do sniff atual em transform.py.
  d) Fixtures comportamentais load-bearing via _create_table_from_csvs real.
  e) paths_literal — escaping de apóstrofo.
  f) _create_table_from_csvs honra o CsvSpec passado (não reconstrói com
     defaults) — e main_table() como acesso canônico a TableSpec.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ficha_etl import registry
from ficha_etl.registry import CsvSpec


# -----------------------------------------------------------------------------
# a) Golden SQL
# -----------------------------------------------------------------------------


def test_read_csv_select_sql_golden():
    spec = CsvSpec(columns=("c1", "c2"))
    paths = [Path("/tmp/a.csv"), Path("/tmp/b.csv")]

    sql = registry.read_csv_select_sql(spec, paths, encoding="latin-1", ignore_errors=False)

    assert "SELECT * FROM read_csv(" in sql
    assert "['/tmp/a.csv', '/tmp/b.csv']" in sql
    assert "delim=';'" in sql
    assert "header=false" in sql
    assert "quote='\"'" in sql
    assert "encoding='latin-1'" in sql
    assert "columns={'c1': 'VARCHAR', 'c2': 'VARCHAR'}" in sql
    assert "null_padding=true" in sql
    assert "strict_mode=false" in sql
    assert "max_line_size=16777216" in sql
    assert "parallel=false" in sql
    assert "ignore_errors=false" in sql


def test_read_csv_select_sql_ignore_errors_true():
    spec = CsvSpec(columns=("c1",))
    sql = registry.read_csv_select_sql(
        spec, [Path("/tmp/a.csv")], encoding="utf-8", ignore_errors=True
    )
    assert "encoding='utf-8'" in sql
    assert "ignore_errors=true" in sql


def test_read_csv_select_sql_exact_string():
    """Pina o formato exato — mudanças de whitespace aqui são intencionais, não acidentais."""
    spec = CsvSpec(columns=("c1", "c2"))
    sql = registry.read_csv_select_sql(spec, [Path("a.csv")], encoding="utf-8", ignore_errors=True)
    expected = (
        "SELECT * FROM read_csv(\n"
        "    ['a.csv'],\n"
        "    delim=';',\n"
        "    header=false,\n"
        "    quote='\"',\n"
        "    encoding='utf-8',\n"
        "    columns={'c1': 'VARCHAR', 'c2': 'VARCHAR'},\n"
        "    null_padding=true,\n"
        "    strict_mode=false,\n"
        "    max_line_size=16777216,\n"
        "    parallel=false,\n"
        "    ignore_errors=true\n"
        ")"
    )
    assert sql == expected


# -----------------------------------------------------------------------------
# b) Layout congelado
# -----------------------------------------------------------------------------


def test_empresa_columns_layout_frozen():
    assert registry.EMPRESA_COLUMNS == (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    )


def test_estabelecimento_columns_layout_frozen():
    assert registry.ESTABELECIMENTO_COLUMNS == (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    )


def test_socio_columns_layout_frozen():
    assert registry.SOCIO_COLUMNS == (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante_legal",
        "qualificacao_representante_legal",
        "faixa_etaria",
    )


def test_simples_columns_layout_frozen():
    assert registry.SIMPLES_COLUMNS == (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    )


def test_main_tables_order_and_shape():
    """Pina ordem e pares (name, kind) — é isto que load_main_tables_into_duckdb
    consome hoje via `for spec in registry.MAIN_TABLES`, então qualquer
    reordenação ou renomeação acidental aqui quebra o load real."""
    names = [t.name for t in registry.MAIN_TABLES]
    kinds = [t.kind for t in registry.MAIN_TABLES]
    name_kind_pairs = [(t.name, t.kind) for t in registry.MAIN_TABLES]
    assert names == ["empresa", "estabelecimento", "simples", "socio"]
    assert kinds == ["empresas", "estabelecimentos", "simples", "socios"]
    assert name_kind_pairs == [
        ("empresa", "empresas"),
        ("estabelecimento", "estabelecimentos"),
        ("simples", "simples"),
        ("socio", "socios"),
    ]
    by_name = {t.name: t for t in registry.MAIN_TABLES}
    assert by_name["empresa"].source.columns == registry.EMPRESA_COLUMNS
    assert by_name["estabelecimento"].source.columns == registry.ESTABELECIMENTO_COLUMNS
    assert by_name["simples"].source.columns == registry.SIMPLES_COLUMNS
    assert by_name["socio"].source.columns == registry.SOCIO_COLUMNS


def test_csv_spec_defaults_are_load_bearing():
    spec = CsvSpec(columns=("c1",))
    assert spec.delimiter == ";"
    assert spec.quote == '"'
    assert spec.header is False
    assert spec.null_padding is True
    assert spec.strict_mode is False
    assert spec.max_line_size == 16_777_216
    assert spec.parallel is False


# -----------------------------------------------------------------------------
# c) encoding_attempts
# -----------------------------------------------------------------------------


def test_encoding_attempts_valid_utf8():
    sample = "Olá Mundo".encode("utf-8")
    assert registry.encoding_attempts(sample) == (("utf-8", True),)


def test_encoding_attempts_invalid_utf8_byte():
    sample = b"caf\xe9"  # \xe9 = 'é' em latin-1, inválido como utf-8 solo
    assert registry.encoding_attempts(sample) == (("latin-1", False), ("utf-8", True))


def test_encoding_attempts_ascii_only():
    sample = b"1;2;3"
    assert registry.encoding_attempts(sample) == (("utf-8", True),)


# -----------------------------------------------------------------------------
# e) paths_literal
# -----------------------------------------------------------------------------


def test_paths_literal_escapes_apostrophe():
    paths = [Path("/tmp/o'brien.csv")]
    assert registry.paths_literal(paths) == "['/tmp/o''brien.csv']"


def test_paths_literal_multiple_paths():
    paths = [Path("/tmp/a.csv"), Path("/tmp/b.csv")]
    assert registry.paths_literal(paths) == "['/tmp/a.csv', '/tmp/b.csv']"


# -----------------------------------------------------------------------------
# d) Fixtures comportamentais load-bearing (via _create_table_from_csvs real)
# -----------------------------------------------------------------------------


def _write_bytes(path: Path, data: bytes) -> None:
    path.write_bytes(data)


def test_quoted_newline_preserved(tmp_path):
    """Campo com newline entre aspas conta como 1 linha só, valor com \\n preservado."""
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "data.csv"
    _write_bytes(csv_path, b'"1";"linha1\nlinha2";"3"\n')

    con = duckdb.connect()
    try:
        _create_table_from_csvs(con, "t_newline", [csv_path], CsvSpec(columns=("c1", "c2", "c3")))
        rows = con.execute("SELECT * FROM t_newline").fetchall()
        assert rows == [("1", "linha1\nlinha2", "3")]
    finally:
        con.close()


def test_ragged_row_null_padded(tmp_path):
    """Linha com menos campos que o schema — null_padding preenche o resto com NULL.

    Precisa de ao menos uma linha bem-formada além da ragged: com só a linha
    ragged no arquivo, o sniffer de dialeto do DuckDB não tem referência de
    largura e falha mesmo com ignore_errors=true (comportamento real
    verificado manualmente contra duckdb 1.5.2 — não é bug introduzido aqui;
    é exatamente o cenário real da RFB, onde ragged rows são raras entre
    milhões de linhas bem-formadas).
    """
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "ragged.csv"
    _write_bytes(csv_path, b'"a1";"b1";"c1"\n"a2";"b2";"c2"\n"1";"2"\n')  # última linha: ragged

    con = duckdb.connect()
    try:
        _create_table_from_csvs(con, "t_ragged", [csv_path], CsvSpec(columns=("c1", "c2", "c3")))
        rows = con.execute("SELECT * FROM t_ragged ORDER BY c1").fetchall()
        assert rows == [("1", "2", None), ("a1", "b1", "c1"), ("a2", "b2", "c2")]
    finally:
        con.close()


def test_mixed_encoding_loads_via_fallback(tmp_path):
    """Bytes latin-1 inválidos como utf-8 — carrega via fallback sem exceção."""
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "mixed.csv"
    _write_bytes(csv_path, '"1";"2";"Olá Mundo"'.encode("latin-1") + b"\n")

    con = duckdb.connect()
    try:
        _create_table_from_csvs(con, "t_mixed", [csv_path], CsvSpec(columns=("c1", "c2", "c3")))
        rows = con.execute("SELECT * FROM t_mixed").fetchall()
        assert rows == [("1", "2", "Olá Mundo")]
    finally:
        con.close()


def test_empty_file_among_nonempty_is_skipped(tmp_path):
    """Arquivo de 0 bytes na lista, junto com um não-vazio — só o não-vazio é lido."""
    from ficha_etl.transform import _create_table_from_csvs

    empty_path = tmp_path / "empty.csv"
    _write_bytes(empty_path, b"")
    nonempty_path = tmp_path / "data.csv"
    _write_bytes(nonempty_path, b'"1";"2";"3"\n')

    con = duckdb.connect()
    try:
        _create_table_from_csvs(
            con,
            "t_mixed_empty",
            [empty_path, nonempty_path],
            CsvSpec(columns=("c1", "c2", "c3")),
        )
        rows = con.execute("SELECT * FROM t_mixed_empty").fetchall()
        assert rows == [("1", "2", "3")]
    finally:
        con.close()


def test_all_empty_paths_creates_empty_table_with_schema(tmp_path):
    """Lista de paths toda vazia — tabela vazia criada com schema correto (VARCHAR)."""
    from ficha_etl.transform import _create_table_from_csvs

    con = duckdb.connect()
    try:
        _create_table_from_csvs(con, "t_empty", [], CsvSpec(columns=("c1", "c2", "c3")))
        rows = con.execute("SELECT * FROM t_empty").fetchall()
        assert rows == []
        cols = con.execute("DESCRIBE t_empty").fetchall()
        assert [(c[0], c[1]) for c in cols] == [
            ("c1", "VARCHAR"),
            ("c2", "VARCHAR"),
            ("c3", "VARCHAR"),
        ]
    finally:
        con.close()


def test_path_with_apostrophe_loads_without_sql_error(tmp_path):
    """Path contendo apóstrofo no nome do arquivo — carrega sem erro de SQL."""
    from ficha_etl.transform import _create_table_from_csvs

    csv_path = tmp_path / "o'brien.csv"
    _write_bytes(csv_path, b'"1";"2";"3"\n')

    con = duckdb.connect()
    try:
        spec = CsvSpec(columns=("c1", "c2", "c3"))
        _create_table_from_csvs(con, "t_apostrophe", [csv_path], spec)
        rows = con.execute("SELECT * FROM t_apostrophe").fetchall()
        assert rows == [("1", "2", "3")]
    finally:
        con.close()


# -----------------------------------------------------------------------------
# f) _create_table_from_csvs honra o CsvSpec — regressão do bypass reportado
#    pelo owner na PR #69 (a função reconstruía CsvSpec(columns=...) com
#    defaults, ignorando qualquer override declarado no registry).
# -----------------------------------------------------------------------------


def test_create_table_from_csvs_honors_custom_delimiter(tmp_path):
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
    _write_bytes(csv_path, b'"1","2","3"\n')

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


def test_main_table_found():
    spec = registry.main_table("estabelecimento")
    assert spec.name == "estabelecimento"
    assert spec.kind == "estabelecimentos"
    assert spec.source.columns == registry.ESTABELECIMENTO_COLUMNS


def test_main_table_not_found_raises_value_error():
    with pytest.raises(ValueError, match="nao_existe"):
        registry.main_table("nao_existe")
