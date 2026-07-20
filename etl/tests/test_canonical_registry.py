"""Canonical estabelecimento contract tests for RFC 0001 Phase 1."""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pytest

from ficha_etl import canonical_registry, registry


def _row_dict(cursor, row):
    return {
        description[0]: value
        for description, value in zip(cursor.description, row, strict=True)
    }


def _raw_values(**overrides):
    values = {column: None for column in registry.ESTABELECIMENTO_COLUMNS}
    values.update(
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "identificador_matriz_filial": "1",
            "situacao_cadastral": "02",
        }
    )
    values.update(overrides)
    return [values[column] for column in registry.ESTABELECIMENTO_COLUMNS]


def _create_raw_table(con):
    definitions = ", ".join(
        f'"{column}" VARCHAR' for column in registry.ESTABELECIMENTO_COLUMNS
    )
    con.execute(f'CREATE TABLE "raw_estabelecimento" ({definitions})')


def test_estabelecimento_declares_complete_canonical_contract():
    contract = canonical_registry.table("estabelecimento")
    canonical = contract.canonical
    assert canonical is not None

    mapped_sources = tuple(
        column.source for column in canonical.columns if column.source is not None
    )
    assert mapped_sources == registry.ESTABELECIMENTO_COLUMNS
    assert canonical.schema_version == "1"
    assert canonical.primary_key == ("cnpj_base", "cnpj_ordem", "cnpj_dv")
    assert canonical.expected_cardinality == "one row per full CNPJ establishment key"
    assert canonical.lineage_columns == ("_source_file", "_source_snapshot")

    # Physical decisions belong to Phase 2 evidence, not the Phase 1 contract.
    assert canonical.bucket_key is None
    assert canonical.codec is None
    assert canonical.row_group_size is None

    columns = {column.name: column for column in canonical.columns}
    assert columns["cnpj_base"].source == "cnpj_basico"
    assert columns["cnpj_base"].duckdb_type == "VARCHAR"
    assert columns["cnpj_base"].invalid_policy == "fail"
    assert columns["cnpj_base"].nullable is False
    assert columns["cnpj_base"].allow_empty is False

    for name in (
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ):
        assert columns[name].duckdb_type == "DATE"
        assert columns[name].nullable is True
        assert columns[name].invalid_policy == "null-and-count"
        assert columns[name].cast_sql is not None
        assert columns[name].invalid_when_sql is not None

    for name in canonical.lineage_columns:
        assert columns[name].source is None
        assert columns[name].duckdb_type == "VARCHAR"
        assert columns[name].nullable is False
        assert columns[name].allow_empty is False
        assert columns[name].invalid_policy == "fail"


def test_only_estabelecimento_is_in_the_first_canonical_slice():
    assert [contract.name for contract in canonical_registry.TABLES] == [
        "estabelecimento"
    ]
    for name in ("empresa", "simples", "socio"):
        with pytest.raises(ValueError, match="no canonical contract"):
            canonical_registry.table(name)


def test_canonical_projection_types_dates_and_preserves_identifier_strings():
    contract = canonical_registry.table("estabelecimento")
    con = duckdb.connect()
    try:
        _create_raw_table(con)
        placeholders = ", ".join("?" for _ in registry.ESTABELECIMENTO_COLUMNS)
        con.execute(
            f'INSERT INTO "raw_estabelecimento" VALUES ({placeholders})',
            _raw_values(
                data_situacao_cadastral="20240229",
                data_inicio_atividade="19991231",
                data_situacao_especial="0",
                cep="01001000",
                municipio="7107",
            ),
        )

        sql = canonical_registry.canonical_select_sql(
            contract,
            "raw_estabelecimento",
            source_file="Estabelecimentos'0.zip",
            source_snapshot="2026-07",
        )
        con.execute(f'CREATE TABLE "canonical_estabelecimento" AS {sql}')

        row = con.execute(
            """
            SELECT cnpj_base, cnpj_ordem, cnpj_dv,
                   data_situacao_cadastral, data_inicio_atividade,
                   data_situacao_especial, cep, municipio,
                   _source_file, _source_snapshot
            FROM canonical_estabelecimento
            """
        ).fetchone()
        assert row == (
            "00000001",
            "0001",
            "91",
            date(2024, 2, 29),
            date(1999, 12, 31),
            None,
            "01001000",
            "7107",
            "Estabelecimentos'0.zip",
            "2026-07",
        )

        described = {
            name: duckdb_type
            for name, duckdb_type, *_ in con.execute(
                'DESCRIBE "canonical_estabelecimento"'
            ).fetchall()
        }
        assert described["cnpj_base"] == "VARCHAR"
        assert described["data_inicio_atividade"] == "DATE"
    finally:
        con.close()


def test_source_validation_counts_invalid_casts_without_counting_historical_absence():
    contract = canonical_registry.table("estabelecimento")
    con = duckdb.connect()
    try:
        _create_raw_table(con)
        placeholders = ", ".join("?" for _ in registry.ESTABELECIMENTO_COLUMNS)
        con.execute(
            f'INSERT INTO "raw_estabelecimento" VALUES ({placeholders})',
            _raw_values(
                data_situacao_cadastral="20230229",  # invalid non-leap date
                data_inicio_atividade="",
                data_situacao_especial="not-a-date",
            ),
        )
        con.execute(
            f'INSERT INTO "raw_estabelecimento" VALUES ({placeholders})',
            _raw_values(
                cnpj_basico="",
                cnpj_ordem="0002",
                cnpj_dv="08",
                data_situacao_cadastral="20260719",
                data_inicio_atividade="0",
                data_situacao_especial=None,
            ),
        )

        cursor = con.execute(
            canonical_registry.source_validation_sql(contract, "raw_estabelecimento")
        )
        metrics = _row_dict(cursor, cursor.fetchone())

        assert metrics["rows_total"] == 2
        assert metrics["cnpj_base__null"] == 0
        assert metrics["cnpj_base__empty"] == 1
        assert metrics["data_situacao_cadastral__invalid"] == 1
        assert metrics["data_inicio_atividade__invalid"] == 0
        assert metrics["data_situacao_especial__invalid"] == 1
    finally:
        con.close()


def test_canonical_validation_counts_missing_keys_critical_fields_and_duplicates():
    contract = canonical_registry.table("estabelecimento")
    con = duckdb.connect()
    try:
        _create_raw_table(con)
        placeholders = ", ".join("?" for _ in registry.ESTABELECIMENTO_COLUMNS)
        con.execute(
            f'INSERT INTO "raw_estabelecimento" VALUES ({placeholders})',
            _raw_values(),
        )
        con.execute(
            f'INSERT INTO "raw_estabelecimento" VALUES ({placeholders})',
            _raw_values(cnpj_basico="00000002", cnpj_dv="08"),
        )
        projection = canonical_registry.canonical_select_sql(
            contract,
            "raw_estabelecimento",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
        con.execute(f'CREATE TABLE "canonical_estabelecimento" AS {projection}')
        con.execute(
            """
            INSERT INTO canonical_estabelecimento
            SELECT * FROM canonical_estabelecimento WHERE cnpj_base = '00000001'
            """
        )
        con.execute(
            """
            INSERT INTO canonical_estabelecimento
            SELECT * REPLACE ('' AS cnpj_base)
            FROM canonical_estabelecimento
            WHERE cnpj_base = '00000002'
            LIMIT 1
            """
        )

        cursor = con.execute(
            canonical_registry.canonical_validation_sql(
                contract, "canonical_estabelecimento"
            )
        )
        metrics = _row_dict(cursor, cursor.fetchone())

        assert metrics == {
            "rows_total": 4,
            "primary_key_missing_rows": 1,
            "critical_missing_rows": 1,
            "duplicate_primary_key_rows": 1,
        }
    finally:
        con.close()


def test_manifest_is_stable_json_and_records_open_physical_decisions():
    manifest = canonical_registry.manifest_dict(
        canonical_registry.table("estabelecimento")
    )

    assert manifest["table"] == "estabelecimento"
    assert manifest["schema_version"] == "1"
    assert manifest["primary_key"] == ["cnpj_base", "cnpj_ordem", "cnpj_dv"]
    assert manifest["physical"] == {
        "bucket_key": None,
        "codec": None,
        "row_group_size": None,
    }
    assert json.loads(json.dumps(manifest, sort_keys=True)) == manifest


def test_unknown_contract_is_rejected():
    with pytest.raises(ValueError, match="no canonical contract"):
        canonical_registry.table("empresa")
