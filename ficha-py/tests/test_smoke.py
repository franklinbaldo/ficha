"""Smoke tests using a tiny synthetic parquet fixture.

No network. Builds a 3-row cnpjs / 2-row raizes / 4-row socios fixture,
writes parquets to a tmp dir, opens via `connect_local`, and exercises
the table refs + the one helper.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import ficha_py


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
    for name in ("cnpjs", "raizes", "socios"):
        out = parquet_dir / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET)")


@pytest.fixture
def local_con(tmp_path):
    _write_fixture(tmp_path)
    return ficha_py.connect_local(tmp_path)


def test_connect_local_registers_three_tables(local_con):
    names = set(local_con.list_tables())
    assert {"cnpjs", "raizes", "socios"} <= names


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
