"""Canonical schema registry for RFC 0001 Phase 1.

The existing :mod:`ficha_etl.registry` remains the source-reader registry and
continues to generate the exact all-VARCHAR CSV SQL used by production. This
module adds the second half of the contract without changing that reader:
canonical types, casts, nullability, keys, invalid-value policy, lineage and
physical defaults.

All functions here are pure. The Phase 2 shadow ingestor will wrap
``canonical_select_sql`` in COPY/validation steps; importing this module does
not write Parquet or alter the monthly pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from . import registry as source_registry
from .sources import FileKind

InvalidValuePolicy = Literal[
    "fail",
    "null-and-count",
    "preserve-as-string",
    "quarantine",
]
DuplicatePolicy = Literal[
    "fail",
    "allow",
    "current-deterministic-collapse",
]


@dataclass(frozen=True)
class CanonicalColumn:
    """One canonical column and its source-to-canonical policy."""

    name: str
    duckdb_type: str
    source: str | None = None
    cast_sql: str | None = None
    nullable: bool = True
    invalid_policy: InvalidValuePolicy = "preserve-as-string"
    critical_for_publication: bool = False
    reversible: bool = True

    @property
    def source_name(self) -> str:
        return self.source or self.name


@dataclass(frozen=True)
class CanonicalSpec:
    """Logical and physical contract for one canonical dataset."""

    schema_version: str
    columns: tuple[CanonicalColumn, ...]
    primary_key: tuple[str, ...]
    expected_cardinality: str
    duplicate_policy: DuplicatePolicy
    lineage_columns: tuple[str, ...] = ("_source_file", "_source_snapshot")
    codec: str = "ZSTD"
    row_group_size: int = 200_000
    bucket_key: str | None = None


@dataclass(frozen=True)
class CanonicalTableSpec:
    """RFB table identity, source layout and canonical contract."""

    name: str
    kind: FileKind
    source_columns: tuple[str, ...]
    canonical: CanonicalSpec


LOOKUP_COLUMNS: tuple[str, ...] = ("codigo", "descricao")

_DATE_CAST_SQL = (
    "CASE WHEN {source} IS NULL OR TRIM({source}) IN ('', '0') THEN NULL "
    "ELSE TRY_STRPTIME({source}, '%Y%m%d')::DATE END"
)
_BOOLEAN_SN_CAST_SQL = (
    "CASE UPPER(TRIM({source})) WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END"
)
_CAPITAL_CAST_SQL = "TRY_CAST(REPLACE(TRIM({source}), ',', '.') AS DECIMAL(18,2))"


def _varchar(
    name: str,
    *,
    required: bool = False,
    critical: bool = False,
) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="VARCHAR",
        nullable=not required,
        invalid_policy="fail" if required else "preserve-as-string",
        critical_for_publication=critical,
    )


def _date(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="DATE",
        cast_sql=_DATE_CAST_SQL,
        invalid_policy="null-and-count",
    )


def _boolean(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="BOOLEAN",
        cast_sql=_BOOLEAN_SN_CAST_SQL,
        invalid_policy="null-and-count",
    )


def _decimal(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="DECIMAL(18,2)",
        cast_sql=_CAPITAL_CAST_SQL,
        invalid_policy="null-and-count",
    )


def _columns_with_overrides(
    source_columns: tuple[str, ...],
    overrides: dict[str, CanonicalColumn],
    *,
    required: frozenset[str] = frozenset(),
    critical: frozenset[str] = frozenset(),
) -> tuple[CanonicalColumn, ...]:
    return tuple(
        overrides.get(
            name,
            _varchar(name, required=name in required, critical=name in critical),
        )
        for name in source_columns
    )


EMPRESA_CANONICAL = CanonicalSpec(
    schema_version="1",
    columns=_columns_with_overrides(
        source_registry.EMPRESA_COLUMNS,
        {"capital_social": _decimal("capital_social")},
        required=frozenset({"cnpj_basico"}),
        critical=frozenset({"cnpj_basico"}),
    ),
    primary_key=("cnpj_basico",),
    expected_cardinality="one row per cnpj_basico after the current dedup gate",
    duplicate_policy="current-deterministic-collapse",
    bucket_key="cnpj_basico",
)

_ESTABELECIMENTO_KEY = frozenset({"cnpj_basico", "cnpj_ordem", "cnpj_dv"})
ESTABELECIMENTO_CANONICAL = CanonicalSpec(
    schema_version="1",
    columns=_columns_with_overrides(
        source_registry.ESTABELECIMENTO_COLUMNS,
        {
            "data_situacao_cadastral": _date("data_situacao_cadastral"),
            "data_inicio_atividade": _date("data_inicio_atividade"),
            "data_situacao_especial": _date("data_situacao_especial"),
        },
        required=_ESTABELECIMENTO_KEY,
        critical=_ESTABELECIMENTO_KEY,
    ),
    primary_key=("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
    expected_cardinality="one row per full CNPJ establishment key",
    duplicate_policy="fail",
    bucket_key="cnpj_basico",
)

SIMPLES_CANONICAL = CanonicalSpec(
    schema_version="1",
    columns=_columns_with_overrides(
        source_registry.SIMPLES_COLUMNS,
        {
            "opcao_simples": _boolean("opcao_simples"),
            "data_opcao_simples": _date("data_opcao_simples"),
            "data_exclusao_simples": _date("data_exclusao_simples"),
            "opcao_mei": _boolean("opcao_mei"),
            "data_opcao_mei": _date("data_opcao_mei"),
            "data_exclusao_mei": _date("data_exclusao_mei"),
        },
        required=frozenset({"cnpj_basico"}),
        critical=frozenset({"cnpj_basico"}),
    ),
    primary_key=("cnpj_basico",),
    expected_cardinality="zero or one row per cnpj_basico after the current dedup gate",
    duplicate_policy="current-deterministic-collapse",
    bucket_key="cnpj_basico",
)

SOCIO_CANONICAL = CanonicalSpec(
    schema_version="1",
    columns=_columns_with_overrides(
        source_registry.SOCIO_COLUMNS,
        {"data_entrada_sociedade": _date("data_entrada_sociedade")},
        required=frozenset({"cnpj_basico"}),
        critical=frozenset({"cnpj_basico"}),
    ),
    primary_key=(),
    expected_cardinality="zero or more rows per cnpj_basico; no unique source key declared",
    duplicate_policy="allow",
    bucket_key="cnpj_basico",
)

LOOKUP_CANONICAL = CanonicalSpec(
    schema_version="1",
    columns=(
        _varchar("codigo", required=True, critical=True),
        _varchar("descricao"),
    ),
    primary_key=("codigo",),
    expected_cardinality="one row per lookup code",
    duplicate_policy="fail",
    row_group_size=100_000,
)

MAIN_TABLES: tuple[CanonicalTableSpec, ...] = (
    CanonicalTableSpec(
        name="empresa",
        kind="empresas",
        source_columns=source_registry.EMPRESA_COLUMNS,
        canonical=EMPRESA_CANONICAL,
    ),
    CanonicalTableSpec(
        name="estabelecimento",
        kind="estabelecimentos",
        source_columns=source_registry.ESTABELECIMENTO_COLUMNS,
        canonical=ESTABELECIMENTO_CANONICAL,
    ),
    CanonicalTableSpec(
        name="simples",
        kind="simples",
        source_columns=source_registry.SIMPLES_COLUMNS,
        canonical=SIMPLES_CANONICAL,
    ),
    CanonicalTableSpec(
        name="socio",
        kind="socios",
        source_columns=source_registry.SOCIO_COLUMNS,
        canonical=SOCIO_CANONICAL,
    ),
)

LOOKUP_TABLES: tuple[CanonicalTableSpec, ...] = tuple(
    CanonicalTableSpec(
        name=f"lookup_{kind}",
        kind=kind,
        source_columns=LOOKUP_COLUMNS,
        canonical=LOOKUP_CANONICAL,
    )
    for kind in (
        "cnaes",
        "motivos",
        "municipios",
        "naturezas",
        "paises",
        "qualificacoes",
    )
)
ALL_TABLES: tuple[CanonicalTableSpec, ...] = MAIN_TABLES + LOOKUP_TABLES


def table(name: str) -> CanonicalTableSpec:
    """Return any canonical table by logical name."""
    for spec in ALL_TABLES:
        if spec.name == name:
            return spec
    raise ValueError(f"table: no CanonicalTableSpec with name={name!r}")


def main_table(name: str) -> CanonicalTableSpec:
    """Return a main canonical table without accepting a lookup by mistake."""
    for spec in MAIN_TABLES:
        if spec.name == name:
            return spec
    raise ValueError(f"main_table: no CanonicalTableSpec with name={name!r}")


def lookup_table(kind: FileKind) -> CanonicalTableSpec:
    """Return a canonical lookup by source FileKind."""
    for spec in LOOKUP_TABLES:
        if spec.kind == kind:
            return spec
    raise ValueError(f"lookup_table: no canonical lookup with kind={kind!r}")


def _identifier(value: str) -> str:
    if not value or not (value[0].isalpha() or value[0] == "_"):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    if not all(character.isalnum() or character == "_" for character in value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def canonical_select_sql(
    spec: CanonicalTableSpec,
    source_relation: str,
    *,
    source_file: str,
    source_snapshot: str,
    source_alias: str = "src",
) -> str:
    """Generate the all-VARCHAR-to-canonical projection without doing I/O."""
    relation = _identifier(source_relation)
    alias = _identifier(source_alias)
    expressions: list[str] = []
    for column in spec.canonical.columns:
        source = f"{alias}.{_identifier(column.source_name)}"
        expression = column.cast_sql.format(source=source) if column.cast_sql else source
        expressions.append(f"    {expression} AS {_identifier(column.name)}")
    expressions.extend(
        (
            f"    {_sql_literal(source_file)} AS _source_file",
            f"    {_sql_literal(source_snapshot)} AS _source_snapshot",
        )
    )
    return "SELECT\n" + ",\n".join(expressions) + f"\nFROM {relation} AS {alias}"


def validate_registry() -> None:
    """Fail early when the declarative contract is internally inconsistent."""
    names = [spec.name for spec in ALL_TABLES]
    kinds = [spec.kind for spec in ALL_TABLES]
    if len(names) != len(set(names)):
        raise ValueError("duplicate table names in canonical registry")
    if len(kinds) != len(set(kinds)):
        raise ValueError("duplicate FileKind entries in canonical registry")

    for spec in ALL_TABLES:
        canonical_columns = spec.canonical.columns
        source_names = [column.source_name for column in canonical_columns]
        canonical_names = [column.name for column in canonical_columns]

        if len(spec.source_columns) != len(set(spec.source_columns)):
            raise ValueError(f"{spec.name}: duplicate source columns")
        if len(canonical_names) != len(set(canonical_names)):
            raise ValueError(f"{spec.name}: duplicate canonical columns")
        if tuple(source_names) != spec.source_columns:
            raise ValueError(
                f"{spec.name}: canonical projection must cover source columns once and in order"
            )

        canonical_by_name = {column.name: column for column in canonical_columns}
        for key in spec.canonical.primary_key:
            if key not in canonical_by_name:
                raise ValueError(f"{spec.name}: primary-key column {key!r} is not canonical")
            if canonical_by_name[key].nullable:
                raise ValueError(f"{spec.name}: primary-key column {key!r} cannot be nullable")

        bucket_key = spec.canonical.bucket_key
        if bucket_key and bucket_key not in canonical_by_name:
            raise ValueError(f"{spec.name}: bucket key {bucket_key!r} is not canonical")

        lineage = spec.canonical.lineage_columns
        if len(lineage) != len(set(lineage)):
            raise ValueError(f"{spec.name}: duplicate lineage columns")
        if set(lineage) & set(canonical_names):
            raise ValueError(f"{spec.name}: lineage columns collide with canonical columns")

        for column in canonical_columns:
            if column.cast_sql is not None and "{source}" not in column.cast_sql:
                raise ValueError(f"{spec.name}.{column.name}: cast_sql must reference {{source}}")


validate_registry()
