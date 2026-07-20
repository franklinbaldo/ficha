"""Canonical schema contract tests for RFC 0001 Phase 1."""

from __future__ import annotations

from datetime import date

import duckdb

from ficha_etl import canonical_registry


def test_all_rfb_tables_have_versioned_canonical_contract():
    assert [spec.name for spec in canonical_registry.MAIN_TABLES] == [
        "empresa",
        "estabelecimento",
        "simples",
        "socio",
    ]
    assert [spec.name for spec in canonical_registry.LOOKUP_TABLES] == [
        "lookup_cnaes",
        "lookup_motivos",
        "lookup_municipios",
        "lookup_naturezas",
        "lookup_paises",
        "lookup_qualificacoes",
    ]

    for spec in canonical_registry.ALL_TABLES:
        assert spec.canonical.schema_version == "1"
        assert tuple(column.source_name for column in spec.canonical.columns) == (
            spec.source_columns
        )
        assert spec.canonical.codec == "ZSTD"
        assert spec.canonical.lineage_columns == (
            "_source_file",
            "_source_snapshot",
        )


def test_estabelecimento_contract_preserves_identifiers_and_types_dates():
    spec = canonical_registry.main_table("estabelecimento")
    columns = {column.name: column for column in spec.canonical.columns}

    assert spec.canonical.primary_key == (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
    )
    assert spec.canonical.bucket_key == "cnpj_basico"
    assert spec.canonical.duplicate_policy == "fail"

    for name in spec.canonical.primary_key:
        assert columns[name].duckdb_type == "VARCHAR"
        assert columns[name].nullable is False
        assert columns[name].invalid_policy == "fail"
        assert columns[name].critical_for_publication is True

    for name in (
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ):
        assert columns[name].duckdb_type == "DATE"
        assert columns[name].invalid_policy == "null-and-count"
        assert columns[name].cast_sql is not None

    assert columns["cep"].duckdb_type == "VARCHAR"
    assert columns["municipio"].duckdb_type == "VARCHAR"
    assert columns["cnae_fiscal_principal"].duckdb_type == "VARCHAR"


def test_typed_contracts_cover_empresa_simples_and_socio():
    empresa = {
        column.name: column
        for column in canonical_registry.main_table("empresa").canonical.columns
    }
    simples = {
        column.name: column
        for column in canonical_registry.main_table("simples").canonical.columns
    }
    socio = {
        column.name: column
        for column in canonical_registry.main_table("socio").canonical.columns
    }

    assert empresa["capital_social"].duckdb_type == "DECIMAL(18,2)"
    assert empresa["capital_social"].invalid_policy == "null-and-count"

    assert simples["opcao_simples"].duckdb_type == "BOOLEAN"
    assert simples["opcao_mei"].duckdb_type == "BOOLEAN"
    assert simples["data_opcao_simples"].duckdb_type == "DATE"
    assert simples["data_exclusao_mei"].duckdb_type == "DATE"

    assert socio["data_entrada_sociedade"].duckdb_type == "DATE"
    assert canonical_registry.main_table("socio").canonical.primary_key == ()
    assert canonical_registry.main_table("socio").canonical.duplicate_policy == "allow"


def test_canonical_select_sql_executes_estabelecimento_projection():
    spec = canonical_registry.main_table("estabelecimento")
    con = duckdb.connect()
    try:
        definitions = ", ".join(f"{column} VARCHAR" for column in spec.source_columns)
        con.execute(f"CREATE TABLE raw_estabelecimento ({definitions})")

        values = {column: None for column in spec.source_columns}
        values.update(
            {
                "cnpj_basico": "00000001",
                "cnpj_ordem": "0001",
                "cnpj_dv": "91",
                "situacao_cadastral": "02",
                "data_situacao_cadastral": "20260719",
                "data_inicio_atividade": "19991231",
                "data_situacao_especial": "0",
                "cep": "01001000",
                "municipio": "7107",
            }
        )
        ordered_values = [values[column] for column in spec.source_columns]
        placeholders = ", ".join("?" for _ in ordered_values)
        con.execute(
            f"INSERT INTO raw_estabelecimento VALUES ({placeholders})",
            ordered_values,
        )

        sql = canonical_registry.canonical_select_sql(
            spec,
            "raw_estabelecimento",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
        con.execute(f"CREATE TABLE canonical_estabelecimento AS {sql}")

        row = con.execute(
            """
            SELECT
                cnpj_basico,
                cnpj_ordem,
                cnpj_dv,
                data_situacao_cadastral,
                data_inicio_atividade,
                data_situacao_especial,
                cep,
                municipio,
                _source_file,
                _source_snapshot
            FROM canonical_estabelecimento
            """
        ).fetchone()
        assert row == (
            "00000001",
            "0001",
            "91",
            date(2026, 7, 19),
            date(1999, 12, 31),
            None,
            "01001000",
            "7107",
            "Estabelecimentos0.zip",
            "2026-07",
        )

        described = {
            name: duckdb_type
            for name, duckdb_type, *_ in con.execute(
                "DESCRIBE canonical_estabelecimento"
            ).fetchall()
        }
        assert described["cnpj_basico"] == "VARCHAR"
        assert described["data_inicio_atividade"] == "DATE"
    finally:
        con.close()


def test_canonical_projection_nulls_invalid_typed_values_without_dropping_row():
    spec = canonical_registry.main_table("simples")
    con = duckdb.connect()
    try:
        definitions = ", ".join(f"{column} VARCHAR" for column in spec.source_columns)
        con.execute(f"CREATE TABLE raw_simples ({definitions})")
        con.execute(
            "INSERT INTO raw_simples VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "00000001",
                "talvez",
                "20261340",
                "",
                "X",
                "0",
                "not-a-date",
            ],
        )
        sql = canonical_registry.canonical_select_sql(
            spec,
            "raw_simples",
            source_file="Simples.zip",
            source_snapshot="2026-07",
        )
        row = con.execute(sql).fetchone()
        assert row[:7] == (
            "00000001",
            None,
            None,
            None,
            None,
            None,
            None,
        )
    finally:
        con.close()


def test_canonical_select_sql_escapes_lineage_literals_and_rejects_bad_identifiers():
    spec = canonical_registry.main_table("empresa")
    sql = canonical_registry.canonical_select_sql(
        spec,
        "raw_empresa",
        source_file="Empresa's.zip",
        source_snapshot="2026-07",
    )

    assert "'Empresa''s.zip' AS _source_file" in sql

    try:
        canonical_registry.canonical_select_sql(
            spec,
            "raw_empresa; DROP TABLE empresa",
            source_file="Empresa.zip",
            source_snapshot="2026-07",
        )
    except ValueError as exc:
        assert "invalid SQL identifier" in str(exc)
    else:
        raise AssertionError("unsafe source relation was accepted")


def test_registry_self_validation_and_lookup_accessors():
    canonical_registry.validate_registry()

    assert canonical_registry.table("estabelecimento") is canonical_registry.main_table(
        "estabelecimento"
    )
    assert canonical_registry.lookup_table("cnaes").name == "lookup_cnaes"
    assert canonical_registry.lookup_table("cnaes").canonical.primary_key == ("codigo",)
    assert canonical_registry.lookup_table("cnaes").canonical.row_group_size == 100_000
