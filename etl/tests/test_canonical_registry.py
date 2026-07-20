"""Contrato canônico de estabelecimento (Fase 2) e empresa (Fase 3, issue #97).

Esta suíte não escreve Parquet nem altera o pipeline mensal. Ela fixa o
contrato que os PRs de shadow ingestion consomem -- para empresa, essa é a
PRIMEIRA fatia de #97 (só o contrato do registry, sem writer/run real ainda).
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


def test_estabelecimento_empresa_and_simples_have_canonical_contracts_socio_doesnt():
    by_name = {table.name: table for table in registry.MAIN_TABLES}

    assert by_name["estabelecimento"].canonical is registry.ESTABELECIMENTO_CANONICAL
    assert by_name["empresa"].canonical is registry.EMPRESA_CANONICAL
    assert by_name["simples"].canonical is registry.SIMPLES_CANONICAL
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


# -----------------------------------------------------------------------------
# EMPRESA_CANONICAL -- primeira fatia de #97 (RFC 0001 Fase 3)
# -----------------------------------------------------------------------------


def test_empresa_canonical_preserves_every_raw_column_once_in_source_order():
    spec = registry.EMPRESA_CANONICAL
    mapped_sources = [column.source for column in spec.columns]

    assert mapped_sources == list(registry.EMPRESA_COLUMNS)
    assert len(mapped_sources) == len(set(mapped_sources))


def test_empresa_primary_key_is_required_failing_and_critical():
    spec = registry.EMPRESA_CANONICAL
    by_name = {column.name: column for column in spec.columns}

    assert spec.schema_version == 1
    assert spec.primary_key == ("cnpj_basico",)
    key_column = by_name["cnpj_basico"]
    assert key_column.duckdb_type == "VARCHAR"
    assert key_column.nullable is False
    assert key_column.invalid_policy == "fail"
    assert key_column.publication_critical is True


def test_empresa_capital_social_is_decimal_with_null_and_count_policy():
    spec = registry.EMPRESA_CANONICAL
    by_name = {column.name: column for column in spec.columns}

    capital = by_name["capital_social"]
    assert capital.duckdb_type == "DECIMAL(18,2)"
    assert capital.nullable is True
    assert capital.invalid_policy == "null-and-count"
    assert capital.cast_sql == "TRY_CAST(REPLACE({source}, ',', '.') AS DECIMAL(18,2))"


def test_empresa_other_fields_stay_varchar_and_optional():
    spec = registry.EMPRESA_CANONICAL
    by_name = {column.name: column for column in spec.columns}
    other_fields = {
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "porte_empresa",
        "ente_federativo_responsavel",
    }

    assert other_fields == set(registry.EMPRESA_COLUMNS) - {"cnpj_basico", "capital_social"}
    for name in other_fields:
        column = by_name[name]
        assert column.duckdb_type == "VARCHAR"
        assert column.nullable is True
        assert column.invalid_policy == "preserve-as-string"
        assert column.publication_critical is False


def test_empresa_lineage_and_physical_layout_match_estabelecimento_pattern():
    spec = registry.EMPRESA_CANONICAL

    assert spec.lineage == registry.ESTABELECIMENTO_CANONICAL.lineage
    assert spec.bucket_key is None
    assert spec.codec is None
    assert spec.row_group_size is None


def test_empresa_duplicate_semantics_are_declared_not_estabelecimento_defaults():
    """Empresa's raw input is known NOT to be unique by cnpj_basico --
    `_dedupe_cnpj_basico_table` (transform.py) already collapses both exact
    and conflicting duplicates deterministically. This pins that the
    registry declares the ACTUAL current behavior instead of defaulting to
    estabelecimento's "unique" assumption, which would misrepresent it.
    """
    assert registry.EMPRESA_CANONICAL.source_cardinality == "duplicates-expected"
    assert registry.EMPRESA_CANONICAL.duplicate_policy == "deterministic-collapse"


def test_estabelecimento_duplicate_semantics_are_explicit_unique_fail():
    assert registry.ESTABELECIMENTO_CANONICAL.source_cardinality == "unique"
    assert registry.ESTABELECIMENTO_CANONICAL.duplicate_policy == "fail"


def test_cardinality_duplicate_policy_rejects_inconsistent_pairs():
    base_column = CanonicalColumn(
        name="id",
        duckdb_type="VARCHAR",
        source="id",
        nullable=False,
        invalid_policy="fail",
        publication_critical=True,
    )

    with pytest.raises(ValueError, match="combinação inconsistente"):
        ParquetSpec(
            schema_version=1,
            columns=(base_column,),
            primary_key=("id",),
            source_cardinality="unique",
            duplicate_policy="deterministic-collapse",
        )

    with pytest.raises(ValueError, match="combinação inconsistente"):
        ParquetSpec(
            schema_version=1,
            columns=(base_column,),
            primary_key=("id",),
            source_cardinality="duplicates-expected",
            duplicate_policy="fail",
        )


def test_cardinality_and_duplicate_policy_reject_unknown_values():
    base_column = CanonicalColumn(
        name="id",
        duckdb_type="VARCHAR",
        source="id",
        nullable=False,
        invalid_policy="fail",
        publication_critical=True,
    )

    with pytest.raises(ValueError, match="source_cardinality inválida"):
        ParquetSpec(
            schema_version=1,
            columns=(base_column,),
            primary_key=("id",),
            source_cardinality="mostly-unique",  # type: ignore[arg-type]
            duplicate_policy="fail",
        )

    with pytest.raises(ValueError, match="duplicate_policy inválida"):
        ParquetSpec(
            schema_version=1,
            columns=(base_column,),
            primary_key=("id",),
            source_cardinality="unique",
            duplicate_policy="quarantine",  # type: ignore[arg-type]
        )


def test_empresa_canonical_projection_casts_capital_social_correctly():
    source_columns = registry.EMPRESA_COLUMNS
    con = duckdb.connect()
    try:
        definitions = ", ".join(
            f"{registry.quote_identifier(name)} VARCHAR" for name in source_columns
        )
        con.execute(f"CREATE TABLE raw_emp ({definitions})")

        base = dict.fromkeys(source_columns, "")
        valid = {**base, "cnpj_basico": "00000001", "capital_social": "150000,50"}
        blank = {**base, "cnpj_basico": "00000002", "capital_social": ""}
        malformed = {**base, "cnpj_basico": "00000003", "capital_social": "não-é-decimal"}

        placeholders = ", ".join("?" for _ in source_columns)
        con.executemany(
            f"INSERT INTO raw_emp VALUES ({placeholders})",
            [[row[name] for name in source_columns] for row in (valid, blank, malformed)],
        )

        projection = registry.canonical_projection_sql(
            registry.EMPRESA_CANONICAL, source_alias="raw_emp"
        )
        rows = con.execute(
            f"SELECT {projection} FROM raw_emp AS raw_emp ORDER BY cnpj_basico"
        ).fetchall()
        described = con.execute(f"DESCRIBE SELECT {projection} FROM raw_emp AS raw_emp").fetchall()
    finally:
        con.close()

    positions = {name: index for index, name in enumerate(source_columns)}
    capital_idx = positions["capital_social"]
    assert rows[0][capital_idx] == pytest.approx(150000.50)
    assert rows[1][capital_idx] is None  # blank
    assert rows[2][capital_idx] is None  # malformed nonblank

    types = {name: duckdb_type for name, duckdb_type, *_ in described}
    assert types["cnpj_basico"] == "VARCHAR"
    assert types["capital_social"] == "DECIMAL(18,2)"


# -----------------------------------------------------------------------------
# SIMPLES_CANONICAL -- #97 slice 4 (RFC 0001 Fase 3)
# -----------------------------------------------------------------------------


def test_simples_canonical_preserves_every_raw_column_once_in_source_order():
    spec = registry.SIMPLES_CANONICAL
    mapped_sources = [column.source for column in spec.columns]

    assert mapped_sources == list(registry.SIMPLES_COLUMNS)
    assert len(mapped_sources) == len(set(mapped_sources))


def test_simples_primary_key_is_required_failing_and_critical():
    spec = registry.SIMPLES_CANONICAL
    by_name = {column.name: column for column in spec.columns}

    assert spec.schema_version == 1
    assert spec.primary_key == ("cnpj_basico",)
    key_column = by_name["cnpj_basico"]
    assert key_column.duckdb_type == "VARCHAR"
    assert key_column.nullable is False
    assert key_column.invalid_policy == "fail"
    assert key_column.publication_critical is True


def test_simples_date_columns_are_typed_with_null_and_count_policy():
    spec = registry.SIMPLES_CANONICAL
    by_name = {column.name: column for column in spec.columns}
    date_fields = {
        "data_opcao_simples",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_exclusao_mei",
    }

    for name in date_fields:
        column = by_name[name]
        assert column.duckdb_type == "DATE"
        assert column.nullable is True
        assert column.invalid_policy == "null-and-count"
        assert column.cast_sql == "try_strptime(nullif({source}, ''), '%Y%m%d')::DATE"


def test_simples_other_fields_stay_varchar_and_optional():
    spec = registry.SIMPLES_CANONICAL
    by_name = {column.name: column for column in spec.columns}
    other_fields = {"opcao_simples", "opcao_mei"}
    date_fields = {
        "data_opcao_simples",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_exclusao_mei",
    }

    assert other_fields == set(registry.SIMPLES_COLUMNS) - {"cnpj_basico"} - date_fields
    for name in other_fields:
        column = by_name[name]
        assert column.duckdb_type == "VARCHAR"
        assert column.nullable is True
        assert column.invalid_policy == "preserve-as-string"
        assert column.publication_critical is False


def test_simples_lineage_and_physical_layout_match_estabelecimento_pattern():
    spec = registry.SIMPLES_CANONICAL

    assert spec.lineage == registry.ESTABELECIMENTO_CANONICAL.lineage
    assert spec.bucket_key is None
    assert spec.codec is None
    assert spec.row_group_size is None


def test_simples_duplicate_semantics_are_declared_not_estabelecimento_defaults():
    """Simples' raw input is known NOT to be unique by cnpj_basico --
    `_dedupe_cnpj_basico_table` (transform.py) already collapses both exact
    and conflicting duplicates for it, the same call as for empresa (see
    `load_main_tables_into_duckdb`'s loop over `("empresa", "simples")`).
    This pins that the registry declares that ACTUAL current behavior
    instead of defaulting to estabelecimento's "unique" assumption, which
    would misrepresent it -- regardless of whether any one historical
    snapshot happens to measure zero duplicates.
    """
    assert registry.SIMPLES_CANONICAL.source_cardinality == "duplicates-expected"
    assert registry.SIMPLES_CANONICAL.duplicate_policy == "deterministic-collapse"


def test_simples_canonical_projection_casts_dates_correctly():
    source_columns = registry.SIMPLES_COLUMNS
    con = duckdb.connect()
    try:
        definitions = ", ".join(
            f"{registry.quote_identifier(name)} VARCHAR" for name in source_columns
        )
        con.execute(f"CREATE TABLE raw_simples ({definitions})")

        base = dict.fromkeys(source_columns, "")
        valid = {**base, "cnpj_basico": "00000001", "data_opcao_simples": "20200115"}
        blank = {**base, "cnpj_basico": "00000002", "data_opcao_simples": ""}
        malformed = {**base, "cnpj_basico": "00000003", "data_opcao_simples": "not-a-date"}

        placeholders = ", ".join("?" for _ in source_columns)
        con.executemany(
            f"INSERT INTO raw_simples VALUES ({placeholders})",
            [[row[name] for name in source_columns] for row in (valid, blank, malformed)],
        )

        projection = registry.canonical_projection_sql(
            registry.SIMPLES_CANONICAL, source_alias="raw_simples"
        )
        rows = con.execute(
            f"SELECT {projection} FROM raw_simples AS raw_simples ORDER BY cnpj_basico"
        ).fetchall()
        described = con.execute(
            f"DESCRIBE SELECT {projection} FROM raw_simples AS raw_simples"
        ).fetchall()
    finally:
        con.close()

    positions = {name: index for index, name in enumerate(source_columns)}
    date_idx = positions["data_opcao_simples"]
    assert rows[0][date_idx] == date(2020, 1, 15)
    assert rows[1][date_idx] is None  # blank
    assert rows[2][date_idx] is None  # malformed nonblank

    types = {name: duckdb_type for name, duckdb_type, *_ in described}
    assert types["cnpj_basico"] == "VARCHAR"
    assert types["data_opcao_simples"] == "DATE"
    assert types["opcao_simples"] == "VARCHAR"
