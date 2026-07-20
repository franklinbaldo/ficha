"""Canonical schema contract for RFC 0001 Phase 1.

``ficha_etl.registry`` remains the exact all-VARCHAR source-reader registry
introduced by PR #69. This module adds the other half of that declarative
boundary for ``estabelecimento``: canonical types, casts, nullability, keys,
invalid-value policy, lineage and validation SQL.

Everything here is pure. Phase 2 will use this contract to write a shadow
Parquet, but importing this module does not perform I/O or alter the monthly
pipeline.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from . import registry as source_registry

InvalidPolicy = Literal["fail", "null-and-count", "preserve-as-string", "quarantine"]


@dataclass(frozen=True)
class CanonicalColumn:
    """One source-to-canonical column mapping."""

    name: str
    duckdb_type: str
    source: str | None
    nullable: bool
    invalid_policy: InvalidPolicy
    critical: bool = False
    allow_empty: bool = True
    cast_sql: str | None = None
    invalid_when_sql: str | None = None

    def __post_init__(self) -> None:
        if self.cast_sql is not None and "{source}" not in self.cast_sql:
            raise ValueError(f"cast_sql for {self.name!r} must contain {{source}}")
        if (
            self.invalid_when_sql is not None
            and "{source}" not in self.invalid_when_sql
        ):
            raise ValueError(
                f"invalid_when_sql for {self.name!r} must contain {{source}}"
            )
        if self.invalid_policy == "null-and-count" and self.invalid_when_sql is None:
            raise ValueError(
                f"column {self.name!r} uses null-and-count without an invalid predicate"
            )
        if self.source is None and (
            self.cast_sql is not None or self.invalid_when_sql is not None
        ):
            raise ValueError(f"lineage column {self.name!r} cannot depend on source")


@dataclass(frozen=True)
class ParquetSpec:
    """Logical canonical dataset contract; physical choices may remain open."""

    schema_version: str
    columns: tuple[CanonicalColumn, ...]
    primary_key: tuple[str, ...]
    expected_cardinality: str
    lineage_columns: tuple[str, ...]
    bucket_key: str | None = None
    codec: str | None = None
    row_group_size: int | None = None

    def __post_init__(self) -> None:
        names = tuple(column.name for column in self.columns)
        if len(names) != len(set(names)):
            raise ValueError("duplicate canonical column names")
        by_name = {column.name: column for column in self.columns}
        missing_key = set(self.primary_key) - set(names)
        if missing_key:
            raise ValueError(
                f"primary key references missing columns: {sorted(missing_key)}"
            )
        for key in self.primary_key:
            column = by_name[key]
            if column.nullable or column.allow_empty:
                raise ValueError(
                    f"primary-key column {key!r} must reject NULL and empty"
                )
        missing_lineage = set(self.lineage_columns) - set(names)
        if missing_lineage:
            raise ValueError(
                f"lineage columns missing from schema: {sorted(missing_lineage)}"
            )
        if self.bucket_key is not None and self.bucket_key not in names:
            raise ValueError(f"bucket key is not canonical: {self.bucket_key!r}")


@dataclass(frozen=True)
class TableContract:
    """Existing source-reader spec plus its canonical contract."""

    source: source_registry.TableSpec
    canonical: ParquetSpec

    def __post_init__(self) -> None:
        mapped_sources = tuple(
            column.source
            for column in self.canonical.columns
            if column.source is not None
        )
        if mapped_sources != self.source.source.columns:
            raise ValueError(
                f"{self.source.name}: canonical mapping must cover every source column "
                "exactly once and in source order"
            )
        lineage = set(self.canonical.lineage_columns)
        unbound = {
            column.name
            for column in self.canonical.columns
            if column.source is None and column.name not in lineage
        }
        if unbound:
            raise ValueError(
                f"{self.source.name}: unbound non-lineage columns: {sorted(unbound)}"
            )

    @property
    def name(self) -> str:
        return self.source.name


_DATE_YYYYMMDD_CAST = (
    "CAST(TRY_STRPTIME(NULLIF(NULLIF({source}, ''), '0'), '%Y%m%d') AS DATE)"
)
_DATE_YYYYMMDD_INVALID = (
    "{source} IS NOT NULL AND {source} NOT IN ('', '0') AND "
    + _DATE_YYYYMMDD_CAST
    + " IS NULL"
)


def _varchar(
    name: str,
    *,
    source: str | None = None,
    nullable: bool = True,
    critical: bool = False,
    allow_empty: bool = True,
) -> CanonicalColumn:
    policy: InvalidPolicy = "fail" if not nullable else "preserve-as-string"
    return CanonicalColumn(
        name=name,
        duckdb_type="VARCHAR",
        source=source if source is not None else name,
        nullable=nullable,
        invalid_policy=policy,
        critical=critical,
        allow_empty=allow_empty,
    )


def _date_yyyymmdd(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="DATE",
        source=name,
        nullable=True,
        invalid_policy="null-and-count",
        cast_sql=_DATE_YYYYMMDD_CAST,
        invalid_when_sql=_DATE_YYYYMMDD_INVALID,
    )


def _lineage(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="VARCHAR",
        source=None,
        nullable=False,
        invalid_policy="fail",
        critical=True,
        allow_empty=False,
    )


ESTABELECIMENTO = TableContract(
    source=source_registry.main_table("estabelecimento"),
    canonical=ParquetSpec(
        schema_version="1",
        columns=(
            _varchar(
                "cnpj_base",
                source="cnpj_basico",
                nullable=False,
                critical=True,
                allow_empty=False,
            ),
            _varchar("cnpj_ordem", nullable=False, critical=True, allow_empty=False),
            _varchar("cnpj_dv", nullable=False, critical=True, allow_empty=False),
            _varchar(
                "identificador_matriz_filial",
                nullable=False,
                critical=True,
                allow_empty=False,
            ),
            _varchar("nome_fantasia"),
            _varchar(
                "situacao_cadastral",
                nullable=False,
                critical=True,
                allow_empty=False,
            ),
            _date_yyyymmdd("data_situacao_cadastral"),
            _varchar("motivo_situacao_cadastral"),
            _varchar("nome_cidade_exterior"),
            _varchar("pais"),
            _date_yyyymmdd("data_inicio_atividade"),
            _varchar("cnae_fiscal_principal"),
            _varchar("cnae_fiscal_secundaria"),
            _varchar("tipo_logradouro"),
            _varchar("logradouro"),
            _varchar("numero"),
            _varchar("complemento"),
            _varchar("bairro"),
            _varchar("cep"),
            _varchar("uf"),
            _varchar("municipio"),
            _varchar("ddd_1"),
            _varchar("telefone_1"),
            _varchar("ddd_2"),
            _varchar("telefone_2"),
            _varchar("ddd_fax"),
            _varchar("fax"),
            _varchar("correio_eletronico"),
            _varchar("situacao_especial"),
            _date_yyyymmdd("data_situacao_especial"),
            _lineage("_source_file"),
            _lineage("_source_snapshot"),
        ),
        primary_key=("cnpj_base", "cnpj_ordem", "cnpj_dv"),
        expected_cardinality="one row per full CNPJ establishment key",
        lineage_columns=("_source_file", "_source_snapshot"),
        # Phase 1 fixes the logical contract. Phase 2 must measure these.
        bucket_key=None,
        codec=None,
        row_group_size=None,
    ),
)

TABLES: tuple[TableContract, ...] = (ESTABELECIMENTO,)


def table(name: str) -> TableContract:
    """Return a canonical contract by logical source table name."""
    for contract in TABLES:
        if contract.name == name:
            return contract
    raise ValueError(f"no canonical contract for table {name!r}")


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _source_expression(column: CanonicalColumn, source_alias: str) -> str:
    if column.source is None:
        raise ValueError(f"column {column.name!r} has no source")
    source = f"{_identifier(source_alias)}.{_identifier(column.source)}"
    return column.cast_sql.format(source=source) if column.cast_sql else source


def canonical_select_sql(
    contract: TableContract,
    source_table: str,
    *,
    source_file: str,
    source_snapshot: str,
) -> str:
    """Generate the all-VARCHAR-to-canonical projection for a staging table."""
    lineage: Mapping[str, str] = {
        "_source_file": _string_literal(source_file),
        "_source_snapshot": _string_literal(source_snapshot),
    }
    select_items: list[str] = []
    for column in contract.canonical.columns:
        if column.source is None:
            try:
                expression = lineage[column.name]
            except KeyError as exc:
                raise ValueError(f"no lineage value for {column.name!r}") from exc
        else:
            expression = _source_expression(column, "src")
        select_items.append(f"    {expression} AS {_identifier(column.name)}")
    return (
        "SELECT\n"
        + ",\n".join(select_items)
        + f"\nFROM {_identifier(source_table)} AS {_identifier('src')}"
    )


def source_validation_sql(contract: TableContract, source_table: str) -> str:
    """Generate raw-to-canonical counts before writing the shadow Parquet."""
    metrics = ["    COUNT(*) AS rows_total"]
    for column in contract.canonical.columns:
        if column.source is None:
            continue
        source = f"{_identifier('src')}.{_identifier(column.source)}"
        if not column.nullable:
            metrics.append(
                "    COUNT(*) FILTER (WHERE "
                f"{source} IS NULL) AS {_identifier(column.name + '__null')}"
            )
        if column.duckdb_type == "VARCHAR" and not column.allow_empty:
            metrics.append(
                "    COUNT(*) FILTER (WHERE "
                f"{source} = '') AS {_identifier(column.name + '__empty')}"
            )
        if column.invalid_when_sql is not None:
            predicate = column.invalid_when_sql.format(source=source)
            metrics.append(
                "    COUNT(*) FILTER (WHERE "
                f"{predicate}) AS {_identifier(column.name + '__invalid')}"
            )
    return (
        "SELECT\n"
        + ",\n".join(metrics)
        + f"\nFROM {_identifier(source_table)} AS {_identifier('src')}"
    )


def canonical_validation_sql(contract: TableContract, canonical_table: str) -> str:
    """Generate key, critical-field and duplicate gates for canonical output."""
    canonical = contract.canonical
    table_name = _identifier(canonical_table)
    key_columns = {column.name: column for column in canonical.columns}

    missing_terms: list[str] = []
    valid_terms: list[str] = []
    for name in canonical.primary_key:
        identifier = _identifier(name)
        missing_terms.append(f"{identifier} IS NULL")
        valid_terms.append(f"{identifier} IS NOT NULL")
        if not key_columns[name].allow_empty:
            missing_terms.append(f"{identifier} = ''")
            valid_terms.append(f"{identifier} <> ''")

    critical_missing_terms: list[str] = []
    for column in canonical.columns:
        if not column.critical:
            continue
        identifier = _identifier(column.name)
        if not column.nullable:
            critical_missing_terms.append(f"{identifier} IS NULL")
        if column.duckdb_type == "VARCHAR" and not column.allow_empty:
            critical_missing_terms.append(f"{identifier} = ''")

    missing_predicate = " OR ".join(missing_terms)
    valid_predicate = " AND ".join(valid_terms)
    critical_predicate = " OR ".join(critical_missing_terms) or "FALSE"
    group_by = ", ".join(_identifier(name) for name in canonical.primary_key)
    return (
        "WITH duplicate_groups AS (\n"
        "    SELECT COUNT(*) AS group_size\n"
        f"    FROM {table_name}\n"
        f"    WHERE {valid_predicate}\n"
        f"    GROUP BY {group_by}\n"
        "    HAVING COUNT(*) > 1\n"
        "), duplicate_stats AS (\n"
        "    SELECT COALESCE(SUM(group_size - 1), 0) AS duplicate_primary_key_rows\n"
        "    FROM duplicate_groups\n"
        ")\n"
        "SELECT\n"
        "    COUNT(*) AS rows_total,\n"
        f"    COUNT(*) FILTER (WHERE {missing_predicate}) AS primary_key_missing_rows,\n"
        f"    COUNT(*) FILTER (WHERE {critical_predicate}) AS critical_missing_rows,\n"
        "    (SELECT duplicate_primary_key_rows FROM duplicate_stats) "
        "AS duplicate_primary_key_rows\n"
        f"FROM {table_name}"
    )


def manifest_dict(contract: TableContract) -> dict[str, Any]:
    """Return a stable JSON-serializable representation of the contract."""
    canonical = contract.canonical
    return {
        "table": contract.name,
        "schema_version": canonical.schema_version,
        "primary_key": list(canonical.primary_key),
        "expected_cardinality": canonical.expected_cardinality,
        "lineage_columns": list(canonical.lineage_columns),
        "physical": {
            "bucket_key": canonical.bucket_key,
            "codec": canonical.codec,
            "row_group_size": canonical.row_group_size,
        },
        "columns": [
            {
                "name": column.name,
                "type": column.duckdb_type,
                "source": column.source,
                "nullable": column.nullable,
                "invalid_policy": column.invalid_policy,
                "critical": column.critical,
                "allow_empty": column.allow_empty,
                "cast_sql": column.cast_sql,
                "invalid_when_sql": column.invalid_when_sql,
            }
            for column in canonical.columns
        ],
    }
