"""Parquets analíticos carregam descrições inline sem depender dos lookups no consumo."""

from pathlib import Path

import duckdb

from ficha_etl.transform import (
    write_cnpj_cnaes_parquet,
    write_enderecos_parquet,
    write_pessoas_parquet,
)


def _estabelecimento(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE estabelecimento AS
        SELECT
            '12345678'::VARCHAR AS cnpj_basico,
            '0001'::VARCHAR AS cnpj_ordem,
            '90'::VARCHAR AS cnpj_dv,
            '5611201'::VARCHAR AS cnae_fiscal_principal,
            '6201500'::VARCHAR AS cnae_fiscal_secundaria,
            'RO'::VARCHAR AS uf,
            '0003'::VARCHAR AS municipio,
            'AV TESTE'::VARCHAR AS logradouro,
            '10'::VARCHAR AS numero,
            '76800000'::VARCHAR AS cep,
            'CENTRO'::VARCHAR AS bairro
        """
    )


def test_cnpj_cnaes_denormalizes_description(tmp_path: Path) -> None:
    con = duckdb.connect()
    _estabelecimento(con)
    con.execute(
        """
        CREATE TABLE lookup_cnaes(codigo VARCHAR, descricao VARCHAR);
        INSERT INTO lookup_cnaes VALUES
            ('5611201', 'Restaurantes e similares'),
            ('6201500', 'Desenvolvimento de programas');
        """
    )
    output = tmp_path / "cnpj_cnaes.parquet"

    write_cnpj_cnaes_parquet(con, output)

    rows = con.execute(
        f"SELECT cnae_codigo, cnae_descricao, posicao FROM read_parquet('{output}') ORDER BY posicao"
    ).fetchall()
    assert rows == [
        ("5611201", "Restaurantes e similares", 0),
        ("6201500", "Desenvolvimento de programas", 1),
    ]


def test_enderecos_denormalizes_municipio_name(tmp_path: Path) -> None:
    con = duckdb.connect()
    _estabelecimento(con)
    con.execute(
        """
        CREATE TABLE lookup_municipios(codigo VARCHAR, descricao VARCHAR);
        INSERT INTO lookup_municipios VALUES ('0003', 'Porto Velho');
        """
    )
    output = tmp_path / "enderecos.parquet"

    write_enderecos_parquet(con, output)

    assert con.execute(
        f"SELECT municipio_codigo, municipio_nome FROM read_parquet('{output}')"
    ).fetchone() == ("0003", "Porto Velho")


def test_pessoas_denormalizes_qualification_for_both_roles(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE socio AS
        SELECT
            '12345678'::VARCHAR AS cnpj_basico,
            '2'::VARCHAR AS identificador_socio,
            '***123456**'::VARCHAR AS cnpj_cpf_socio,
            'MARIA TESTE'::VARCHAR AS nome_socio_razao_social,
            '22'::VARCHAR AS qualificacao_socio,
            '5'::VARCHAR AS faixa_etaria,
            '***654321**'::VARCHAR AS representante_legal,
            'JOAO TESTE'::VARCHAR AS nome_representante_legal,
            '05'::VARCHAR AS qualificacao_representante_legal
        """
    )
    con.execute(
        """
        CREATE TABLE lookup_qualificacoes(codigo VARCHAR, descricao VARCHAR);
        INSERT INTO lookup_qualificacoes VALUES
            ('22', 'Sócio'),
            ('05', 'Administrador');
        """
    )
    output = tmp_path / "pessoas.parquet"

    write_pessoas_parquet(con, output)

    rows = con.execute(
        f"SELECT papel, qualificacao_codigo, qualificacao_descricao "
        f"FROM read_parquet('{output}') ORDER BY papel"
    ).fetchall()
    assert rows == [
        ("representante", "05", "Administrador"),
        ("socio_pf", "22", "Sócio"),
    ]
