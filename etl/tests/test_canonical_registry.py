"""Contrato canônico do estabelecimento (RFC 0001, Fases 1→2).

Esta suíte não escreve Parquet nem altera o pipeline mensal. Ela fixa o
contrato que o próximo PR de shadow ingestion consumirá.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from ficha_etl import registry
from ficha_etl.registry import (
    CanonicalColumn,
    CsvSpec,
    ParquetSpec,
    TableSpec,
)


def test_only_estabelecimento_has_canonical_contract_in_first_slice():
    by_name = {table.name: table for table in registry.MAIN_TABLES}

    assert by_name["estabelecimento"].canonical is registry.ESTABELECIMENTO_CANONICAL
    assert by_name["empresa"].canonical is None
    assert by_name["simples"].canonical is None
    assert by_name["socio"].canonical is None


def test_estabelecimento_canonical_preserves_every_raw_column_once():
    spec = registry.ESTABELECIMENTO_CANONICAL
    mapped_sources = [column.source for column in spec.columns]

    assert mapped_sources == list(registry.ESTABELECIMENTO_COLUMNS)
    assert len(mapped_sources) == len(set(mapped_sources))


def test_estabelecimento_primary_key_is_required_failing_and_critical():
    spec = registry.ESTABELECIMENTO_CANONICAL
    by_name = {column.name: column for column in spec.columns}

    assert spec.schema_version == 1
    assert spec.primary_key == ("cnpj_basico", "cnpj_ordem", "cnpj_dv")
    for name in spec.primary_key:
        column = by_name[name]
        assert column.duckdb_type == "VARCHAR"
        assert column.nullable is False
        assert column.invalid_policy == "fail"
        assert column.publication_critical is True


def test_estabelecimento_date_contract_is_explicit_and_counted():
    spec = registry.ESTABELECIMENTO_CANONICAL
    by_name = {column.name: column for column in spec.columns}
    date_names = {
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    }

    assert {name for name, column in by_name.items() if column.duckdb_type == "DATE"} == date_names
    for name in date_names:
        column = by_name[name]
        assert column.nullable is True
        assert column.invalid_policy == "null-and-count"
        assert column.cast_sql == "try_strptime(nullif({source}, ''), '%Y%m%d')::DATE"


def test_lineage_contract_is_required_and_not_confused_with_raw_columns():
    spec = registry.ESTABELECIMENTO_CANONICAL

    assert [(column.name, column.duckdb_type, column.nullable) for column in spec.lineage] == [
        ("_source_file", "VARCHAR", False),
        ("_source_snapshot", "VARCHAR", False),
    ]
    assert not ({column.name for column in spec.lineage} & set(registry.ESTABELECIMENTO_COLUMNS))


def test_physical_layout_choices_remain_open():
    spec = registry.ESTABELECIMENTO_CANONICAL

    assert spec.bucket_key is None
    assert spec.codec is None
    assert spec.row_group_size is None


def test_canonical_projection_compiles_identity_and_date_casts():
    sql = registry.canonical_projection_sql(
        registry.ESTABELECIMENTO_CANONICAL,
        source_alias="raw_est",
    )

    assert '    "raw_est"."cnpj_basico" AS "cnpj_basico"' in sql
    assert (
        '    try_strptime(nullif("raw_est"."data_inicio_atividade", \'\'), '
        "'%Y%m%d')::DATE AS \"data_inicio_atividade\""
    ) in sql
    assert len(sql.splitlines()) == len(registry.ESTABELECIMENTO_COLUMNS)


def test_canonical_projection_executes_with_expected_types_and_invalid_date_semantics():
    source_columns = registry.ESTABELECIMENTO_COLUMNS
    con = duckdb.connect()
    try:
        definitions = ", ".join(
            f"{registry.quote_identifier(name)} VARCHAR" for name in source_columns
        )
        con.execute(f"CREATE TABLE raw_est ({definitions})")

        valid = {name: "" for name in source_columns}
        valid.update(
            {
                "cnpj_basico": "00000001",
                "cnpj_ordem": "0001",
                "cnpj_dv": "91",
                "data_situacao_cadastral": "20260719",
                "data_inicio_atividade": "19991231",
                "data_situacao_especial": "",
            }
        )
        invalid = dict(valid)
        invalid.update(
            {
                "cnpj_basico": "00000002",
                "data_situacao_cadastral": "00000000",
                "data_inicio_atividade": "não-é-data",
            }
        )

        placeholders = ", ".join("?" for _ in source_columns)
        insert_sql = f"INSERT INTO raw_est VALUES ({placeholders})"
        con.executemany(
            insert_sql,
            [[row[name] for name in source_columns] for row in (valid, invalid)],
        )

        projection = registry.canonical_projection_sql(
            registry.ESTABELECIMENTO_CANONICAL,
            source_alias="raw_est",
        )
        rows = con.execute(
            f"""
            SELECT
            {projection}
            FROM raw_est AS raw_est
            ORDER BY cnpj_basico
            """
        ).fetchall()
        described = con.execute(
            f"""
            DESCRIBE SELECT
            {projection}
            FROM raw_est AS raw_est
            """
        ).fetchall()
    finally:
        con.close()

    positions = {name: index for index, name in enumerate(source_columns)}
    assert rows[0][positions["data_situacao_cadastral"]] == date(2026, 7, 19)
    assert rows[0][positions["data_inicio_atividade"]] == date(1999, 12, 31)
    assert rows[0][positions["data_situacao_especial"]] is None
    assert rows[1][positions["data_situacao_cadastral"]] is None
    assert rows[1][positions["data_inicio_atividade"]] is None

    types = {name: duckdb_type for name, duckdb_type, *_ in described}
    assert types["cnpj_basico"] == "VARCHAR"
    assert types["data_inicio_atividade"] == "DATE"


def test_non_varchar_column_requires_explicit_cast():
    with pytest.raises(ValueError, match="exige cast_sql explícito"):
        CanonicalColumn(
            name="d",
            duckdb_type="DATE",
            source="d",
            invalid_policy="null-and-count",
        )


def test_preserve_as_string_cannot_claim_a_typed_column():
    with pytest.raises(ValueError, match="preserve-as-string só é válida para VARCHAR"):
        CanonicalColumn(
            name="d",
            duckdb_type="DATE",
            source="d",
            invalid_policy="preserve-as-string",
            cast_sql="cast({source} as DATE)",
        )


def test_primary_key_invariants_fail_loudly():
    with pytest.raises(ValueError, match="primary_key não pode conter colunas nullable"):
        ParquetSpec(
            schema_version=1,
            columns=(
                CanonicalColumn(
                    name="id",
                    duckdb_type="VARCHAR",
                    source="id",
                    nullable=True,
                    invalid_policy="fail",
                    publication_critical=True,
                ),
            ),
            primary_key=("id",),
        )

    with pytest.raises(ValueError, match="invalid_policy='fail'"):
        ParquetSpec(
            schema_version=1,
            columns=(
                CanonicalColumn(
                    name="id",
                    duckdb_type="VARCHAR",
                    source="id",
                    nullable=False,
                    invalid_policy="preserve-as-string",
                    publication_critical=True,
                ),
            ),
            primary_key=("id",),
        )


def test_table_spec_rejects_unknown_raw_source_reference():
    canonical = ParquetSpec(
        schema_version=1,
        columns=(
            CanonicalColumn(
                name="id",
                duckdb_type="VARCHAR",
                source="missing",
                nullable=False,
                invalid_policy="fail",
                publication_critical=True,
            ),
        ),
        primary_key=("id",),
    )

    with pytest.raises(ValueError, match="fontes desconhecidas"):
        TableSpec(
            name="x",
            kind="empresas",
            source=CsvSpec(columns=("id",)),
            canonical=canonical,
        )
