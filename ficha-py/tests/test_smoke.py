"""Smoke tests using a tiny synthetic parquet fixture.

No network. Builds a minimal fixture for all 7 main parquets + the 6
lookup parquets (the full set `connect_local` requires -- see ADRs
0008/0019/0020/0021/0023/0024), writes them to a tmp dir laid out like a
real snapshot, opens via `connect_local`, and exercises every table ref +
view.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import ficha_py
from ficha_py.tables import LOOKUP_KINDS


def _write_fixture(parquet_dir: Path) -> None:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE cnpjs AS SELECT * FROM (VALUES
            ('00000001000191', '00000001', 'BANCO DO BRASIL', 'SP'),
            ('00000002000176', '00000002', 'PETROBRAS', 'RJ'),
            ('00000003000150', '00000003', 'VALE', 'MG')
        ) AS t(cnpj, cnpj_base, razao_social, uf)
        """
    )
    con.execute(
        """
        CREATE TABLE raizes AS SELECT * FROM (VALUES
            ('00000001', 'BANCO DO BRASIL', 1),
            ('00000002', 'PETROBRAS', 1)
        ) AS t(cnpj_base, razao_social, qtd_estab)
        """
    )
    con.execute(
        """
        CREATE TABLE socios AS SELECT * FROM (VALUES
            ('00000001', 'PF', 'JOAO SILVA'),
            ('00000001', 'PF', 'MARIA SOUZA'),
            ('00000002', 'PJ', 'EMPRESA HOLDING'),
            ('00000003', 'PF', 'PEDRO LIMA')
        ) AS t(cnpj_base, tipo, nome_socio)
        """
    )
    con.execute(
        """
        CREATE TABLE enderecos AS SELECT * FROM (VALUES
            ('SP', '7107', 'AVENIDA PAULISTA', '00000001000191'),
            ('RJ', '6001', 'AVENIDA RIO BRANCO', '00000002000176')
        ) AS t(uf, municipio_codigo, logradouro_normalizado, cnpj)
        """
    )
    con.execute(
        """
        CREATE TABLE pessoas AS SELECT * FROM (VALUES
            ('***123456**', 'JOAO SILVA', '00000001'),
            ('***654321**', 'MARIA SOUZA', '00000001')
        ) AS t(cpf_mascarado, nome_normalizado, cnpj_base)
        """
    )
    con.execute(
        """
        CREATE TABLE cnpj_cnaes AS SELECT * FROM (VALUES
            ('00000001000191', '00000001', '6421200', 0),
            ('00000002000176', '00000002', '0600001', 0)
        ) AS t(cnpj, cnpj_base, cnae_codigo, posicao)
        """
    )
    con.execute(
        """
        CREATE TABLE cnpj_contatos AS SELECT * FROM (VALUES
            ('00000001000191', '00000001', 'telefone', '1140028922', 1),
            ('00000001000191', '00000001', 'email', 'contato@bb.com.br', 0)
        ) AS t(cnpj, cnpj_base, tipo, valor, posicao)
        """
    )
    main_tables = (
        "cnpjs",
        "raizes",
        "socios",
        "enderecos",
        "pessoas",
        "cnpj_cnaes",
        "cnpj_contatos",
    )
    for name in main_tables:
        out = parquet_dir / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET)")

    lookups_dir = parquet_dir / "lookups"
    lookups_dir.mkdir(parents=True, exist_ok=True)
    for kind in LOOKUP_KINDS:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE lookup_{kind} AS SELECT * FROM (VALUES
                ('01', 'DESCRICAO 01 ção'),
                ('02', 'DESCRICAO 02')
            ) AS t(codigo, descricao)
            """
        )
        con.execute(f"COPY lookup_{kind} TO '{lookups_dir / f'{kind}.parquet'}' (FORMAT PARQUET)")


@pytest.fixture
def local_con(tmp_path):
    _write_fixture(tmp_path)
    return ficha_py.connect_local(tmp_path)


def test_connect_local_registers_all_tables(local_con):
    names = set(local_con.list_tables())
    expected = {"cnpjs", "raizes", "socios", "enderecos", "pessoas", "cnpj_cnaes", "cnpj_contatos"}
    expected |= {f"lookup_{kind}" for kind in LOOKUP_KINDS}
    assert expected <= names


def test_cnpjs_table_returns_rows(local_con):
    df = ficha_py.cnpjs(local_con).execute()
    assert len(df) == 3
    assert set(df["uf"]) == {"SP", "RJ", "MG"}


def test_raizes_table(local_con):
    df = ficha_py.raizes(local_con).execute()
    assert len(df) == 2


def test_socios_table(local_con):
    df = ficha_py.socios(local_con).execute()
    assert len(df) == 4


def test_socios_de_returns_only_matching_raiz(local_con):
    df = ficha_py.socios_de(local_con, "00000001").execute()
    assert len(df) == 2
    assert set(df["nome_socio"]) == {"JOAO SILVA", "MARIA SOUZA"}


def test_socios_de_validates_cnpj_base():
    with pytest.raises(ValueError, match="8 digits"):
        ficha_py.socios_de(None, "abc")
    with pytest.raises(ValueError, match="8 digits"):
        ficha_py.socios_de(None, "1234567")  # too short


def test_underscore_export_is_ibis_underscore(local_con):
    """Notebook ergonomic: `from ficha_py import _` should be Ibis's deferred."""
    df = ficha_py.cnpjs(local_con).filter(ficha_py._.uf == "SP").execute()
    assert len(df) == 1
    assert df["razao_social"].iloc[0] == "BANCO DO BRASIL"


def test_connect_ia_validates_month():
    with pytest.raises(ValueError, match="YYYY-MM"):
        ficha_py.connect_ia(month="2026-13")
    with pytest.raises(ValueError, match="YYYY-MM"):
        ficha_py.connect_ia(month="not-a-month")


def test_connect_local_fails_fast_when_parquet_missing(tmp_path):
    """Incomplete snapshot dirs (typo, partial copy) must fail at connect()
    time -- not later as an opaque table-not-found inside query code.
    """
    _write_fixture(tmp_path)
    (tmp_path / "socios.parquet").unlink()  # simulate partial copy
    with pytest.raises(FileNotFoundError, match="socios.parquet"):
        ficha_py.connect_local(tmp_path)


def test_connect_local_fails_fast_when_dir_empty(tmp_path):
    with pytest.raises(FileNotFoundError, match="cnpjs.parquet"):
        ficha_py.connect_local(tmp_path)


def test_connect_local_fails_fast_when_lookup_missing(tmp_path):
    """A missing lookups/<kind>.parquet is just as fatal as a missing main parquet."""
    _write_fixture(tmp_path)
    (tmp_path / "lookups" / "paises.parquet").unlink()
    with pytest.raises(FileNotFoundError, match="paises.parquet"):
        ficha_py.connect_local(tmp_path)


def test_enderecos_table(local_con):
    df = ficha_py.enderecos(local_con).execute()
    assert len(df) == 2
    assert set(df["uf"]) == {"SP", "RJ"}


def test_pessoas_table(local_con):
    df = ficha_py.pessoas(local_con).execute()
    assert len(df) == 2


def test_cnpj_cnaes_table(local_con):
    df = ficha_py.cnpj_cnaes(local_con).execute()
    assert len(df) == 2


def test_cnpj_contatos_table(local_con):
    df = ficha_py.cnpj_contatos(local_con).execute()
    assert len(df) == 2


def test_filiais_de_returns_only_matching_raiz(local_con):
    df = ficha_py.filiais_de(local_con, "00000001").execute()
    assert len(df) == 1
    assert df["razao_social"].iloc[0] == "BANCO DO BRASIL"


def test_filiais_de_validates_cnpj_base():
    with pytest.raises(ValueError, match="8 digits"):
        ficha_py.filiais_de(None, "abc")


def test_lookup_returns_raw_table(local_con):
    df = ficha_py.lookup(local_con, "cnaes").execute()
    assert len(df) == 2
    assert set(df.columns) == {"codigo", "descricao"}


def test_lookup_validates_kind(local_con):
    with pytest.raises(ValueError, match="cnaes"):
        ficha_py.lookup(local_con, "not-a-kind")


def test_lookup_normalized_strips_accents_and_uppercases(local_con):
    df = ficha_py.lookup_normalized(local_con, "cnaes").execute()
    assert list(df["codigo"]) == ["01", "02"]  # sorted by codigo
    assert df.loc[df["codigo"] == "01", "descricao_normalizada"].iloc[0] == "DESCRICAO 01 CAO"
