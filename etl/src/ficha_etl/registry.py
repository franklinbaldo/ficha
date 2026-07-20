"""Schema registry — fonte central de metadados das tabelas RFB (RFC 0001).

O registry separa dois contratos:

- ``CsvSpec`` descreve a leitura raw, que precisa permanecer semanticamente
  idêntica ao reader legado;
- ``ParquetSpec`` descreve o schema canônico interno, com tipos, casts,
  políticas de valor inválido, chaves, linhagem e versão.

A orquestração (I/O, tentativas de encoding, escrita e métricas) continua fora
deste módulo. As funções daqui são puras e determinísticas.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .sources import FileKind


InvalidValuePolicy = Literal["fail", "null-and-count", "preserve-as-string", "quarantine"]
_ALLOWED_INVALID_VALUE_POLICIES = frozenset(
    {"fail", "null-and-count", "preserve-as-string", "quarantine"}
)


@dataclass(frozen=True)
class CsvSpec:
    """Opções de leitura CSV para uma tabela RFB — layout fixo, sem header.

    Os defaults capturam decisões load-bearing do reader legado (ver
    ``_create_table_from_csvs`` em transform.py), não meras conveniências:

    - ``parallel=False``: o scanner paralelo do DuckDB fatia um único arquivo
      grande por offset de bytes; com ``null_padding=True`` ele não recupera
      linhas ragged cujo campo tem newline entre aspas atravessando um corte.
    - ``max_line_size=16MiB``: alguns campos podem estourar o default.
    - ``null_padding=True``: linhas RFB ragged são preenchidas com NULL.
    - ``strict_mode=False``: a fonte não segue estritamente RFC 4180.
    """

    columns: tuple[str, ...]
    delimiter: str = ";"
    quote: str = '"'
    header: bool = False
    null_padding: bool = True
    strict_mode: bool = False
    max_line_size: int = 16_777_216
    parallel: bool = False


@dataclass(frozen=True)
class CanonicalColumn:
    """Uma coluna derivada do CSV raw para o Parquet canônico.

    ``cast_sql`` é um template SQL opcional com exatamente um placeholder
    ``{source}``. Quando ausente, a coluna raw é projetada sem transformação.
    ``invalid_policy`` documenta o que o futuro writer deve fazer quando um
    cast ou regra semântica falhar.
    """

    name: str
    duckdb_type: str
    source: str
    nullable: bool = True
    invalid_policy: InvalidValuePolicy = "preserve-as-string"
    cast_sql: str | None = None
    publication_critical: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("CanonicalColumn.name não pode ser vazio")
        if not self.duckdb_type:
            raise ValueError(f"{self.name}: duckdb_type não pode ser vazio")
        if not self.source:
            raise ValueError(f"{self.name}: source não pode ser vazio")
        if self.invalid_policy not in _ALLOWED_INVALID_VALUE_POLICIES:
            raise ValueError(f"{self.name}: invalid_policy inválida: {self.invalid_policy!r}")
        if self.cast_sql is not None and self.cast_sql.count("{source}") != 1:
            raise ValueError(
                f"{self.name}: cast_sql deve conter exatamente um placeholder {{source}}"
            )
        if self.cast_sql is None and self.duckdb_type != "VARCHAR":
            raise ValueError(f"{self.name}: coluna {self.duckdb_type} exige cast_sql explícito")
        if self.invalid_policy == "preserve-as-string" and self.duckdb_type != "VARCHAR":
            raise ValueError(f"{self.name}: preserve-as-string só é válida para VARCHAR")


@dataclass(frozen=True)
class LineageColumn:
    """Coluna adicionada pelo estágio de ingestão, sem origem no CSV."""

    name: str
    duckdb_type: str = "VARCHAR"
    nullable: bool = False

    def __post_init__(self) -> None:
        if not self.name.startswith("_source_"):
            raise ValueError(f"coluna de linhagem deve começar com _source_: {self.name!r}")
        if not self.duckdb_type:
            raise ValueError(f"{self.name}: duckdb_type não pode ser vazio")


@dataclass(frozen=True)
class ParquetSpec:
    """Contrato lógico do Parquet canônico de uma entidade RFB."""

    schema_version: int
    columns: tuple[CanonicalColumn, ...]
    primary_key: tuple[str, ...]
    lineage: tuple[LineageColumn, ...] = (
        LineageColumn("_source_file"),
        LineageColumn("_source_snapshot"),
    )
    bucket_key: str | None = None
    codec: str | None = None
    row_group_size: int | None = None

    def __post_init__(self) -> None:
        if self.schema_version < 1:
            raise ValueError("schema_version deve ser >= 1")
        if not self.columns:
            raise ValueError("ParquetSpec.columns não pode ser vazio")

        names = [column.name for column in self.columns]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"colunas canônicas duplicadas: {duplicates}")

        lineage_names = [column.name for column in self.lineage]
        duplicate_lineage = sorted(
            {name for name in lineage_names if lineage_names.count(name) > 1}
        )
        if duplicate_lineage:
            raise ValueError(f"colunas de linhagem duplicadas: {duplicate_lineage}")

        overlap = sorted(set(names) & set(lineage_names))
        if overlap:
            raise ValueError(f"colunas canônicas e de linhagem colidem: {overlap}")

        missing_key = [name for name in self.primary_key if name not in names]
        if missing_key:
            raise ValueError(f"primary_key referencia colunas inexistentes: {missing_key}")
        key_columns = [column for column in self.columns if column.name in self.primary_key]
        nullable_key = [column.name for column in key_columns if column.nullable]
        if nullable_key:
            raise ValueError(f"primary_key não pode conter colunas nullable: {nullable_key}")
        non_failing_key = [column.name for column in key_columns if column.invalid_policy != "fail"]
        if non_failing_key:
            raise ValueError(f"primary_key deve usar invalid_policy='fail': {non_failing_key}")
        non_critical_key = [
            column.name for column in key_columns if not column.publication_critical
        ]
        if non_critical_key:
            raise ValueError(f"primary_key deve ser publication_critical: {non_critical_key}")

        if self.bucket_key is not None and self.bucket_key not in names:
            raise ValueError(f"bucket_key inexistente: {self.bucket_key!r}")
        if self.row_group_size is not None and self.row_group_size < 1:
            raise ValueError("row_group_size deve ser positivo")


@dataclass(frozen=True)
class TableSpec:
    """Uma tabela RFB: contrato raw e, quando definido, contrato canônico."""

    name: str
    kind: FileKind
    source: CsvSpec
    canonical: ParquetSpec | None = None

    def __post_init__(self) -> None:
        if self.canonical is None:
            return

        source_columns = set(self.source.columns)
        unknown_sources = sorted(
            {
                column.source
                for column in self.canonical.columns
                if column.source not in source_columns
            }
        )
        if unknown_sources:
            raise ValueError(
                f"{self.name}: colunas canônicas referenciam fontes desconhecidas: "
                f"{unknown_sources}"
            )


# Layout RFB CNPJ — colunas em ordem (sem header no CSV). Tipos: tudo VARCHAR
# para evitar inferência silenciosa; conversões acontecem na fronteira canônica.
EMPRESA_COLUMNS: tuple[str, ...] = (
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
)
ESTABELECIMENTO_COLUMNS: tuple[str, ...] = (
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
SOCIO_COLUMNS: tuple[str, ...] = (
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
SIMPLES_COLUMNS: tuple[str, ...] = (
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
)


def _string_column(
    name: str,
    *,
    required: bool = False,
    publication_critical: bool = False,
) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="VARCHAR",
        source=name,
        nullable=not required,
        invalid_policy="fail" if required else "preserve-as-string",
        publication_critical=publication_critical,
    )


def _date_column(name: str) -> CanonicalColumn:
    return CanonicalColumn(
        name=name,
        duckdb_type="DATE",
        source=name,
        nullable=True,
        invalid_policy="null-and-count",
        cast_sql="try_strptime(nullif({source}, ''), '%Y%m%d')::DATE",
    )


_ESTABELECIMENTO_DATE_COLUMNS = frozenset(
    {
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    }
)
_ESTABELECIMENTO_REQUIRED_COLUMNS = frozenset({"cnpj_basico", "cnpj_ordem", "cnpj_dv"})

ESTABELECIMENTO_CANONICAL = ParquetSpec(
    schema_version=1,
    columns=tuple(
        _date_column(name)
        if name in _ESTABELECIMENTO_DATE_COLUMNS
        else _string_column(
            name,
            required=name in _ESTABELECIMENTO_REQUIRED_COLUMNS,
            publication_critical=name in _ESTABELECIMENTO_REQUIRED_COLUMNS,
        )
        for name in ESTABELECIMENTO_COLUMNS
    ),
    primary_key=("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
)


# A ordem declarativa documenta o schema; a estratégia física de carga pode
# usar uma ordem própria em transform.py.
MAIN_TABLES: tuple[TableSpec, ...] = (
    TableSpec(name="empresa", kind="empresas", source=CsvSpec(columns=EMPRESA_COLUMNS)),
    TableSpec(
        name="estabelecimento",
        kind="estabelecimentos",
        source=CsvSpec(columns=ESTABELECIMENTO_COLUMNS),
        canonical=ESTABELECIMENTO_CANONICAL,
    ),
    TableSpec(name="simples", kind="simples", source=CsvSpec(columns=SIMPLES_COLUMNS)),
    TableSpec(name="socio", kind="socios", source=CsvSpec(columns=SOCIO_COLUMNS)),
)


def main_table(name: str) -> TableSpec:
    """Busca um ``TableSpec`` em ``MAIN_TABLES`` pelo nome da tabela."""
    for spec in MAIN_TABLES:
        if spec.name == name:
            return spec
    raise ValueError(f"main_table: nenhuma TableSpec com name={name!r} em MAIN_TABLES")


def quote_identifier(identifier: str) -> str:
    """Escapa um identificador DuckDB com aspas duplas."""
    return '"' + identifier.replace('"', '""') + '"'


def canonical_expression_sql(column: CanonicalColumn, *, source_alias: str = "src") -> str:
    """Compila a expressão de uma coluna canônica contra um alias raw."""
    source = f"{quote_identifier(source_alias)}.{quote_identifier(column.source)}"
    if column.cast_sql is None:
        return source
    return column.cast_sql.format(source=source)


def canonical_projection_sql(spec: ParquetSpec, *, source_alias: str = "src") -> str:
    """Gera a lista ``expr AS coluna`` da projeção canônica, sem linhagem."""
    return ",\n".join(
        f"    {canonical_expression_sql(column, source_alias=source_alias)} "
        f"AS {quote_identifier(column.name)}"
        for column in spec.columns
    )


def csv_columns_clause(columns: tuple[str, ...]) -> str:
    """``{'c1': 'VARCHAR', 'c2': 'VARCHAR'}`` para ``read_csv(columns=...)``."""
    pairs = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
    return "{" + pairs + "}"


def paths_literal(paths: Sequence[Path]) -> str:
    """Lista SQL inline para paths de CSV, com apóstrofos escapados."""
    quoted = ", ".join(f"'{str(path).replace(chr(39), chr(39) * 2)}'" for path in paths)
    return "[" + quoted + "]"


def read_csv_select_sql(
    spec: CsvSpec,
    paths: Sequence[Path],
    *,
    encoding: str,
    ignore_errors: bool,
) -> str:
    """Gera o ``SELECT * FROM read_csv(...)`` semanticamente equivalente ao legado."""
    literal = paths_literal(paths)
    cols_clause = csv_columns_clause(spec.columns)
    header = "true" if spec.header else "false"
    null_padding = "true" if spec.null_padding else "false"
    strict_mode = "true" if spec.strict_mode else "false"
    parallel = "true" if spec.parallel else "false"
    ignore = "true" if ignore_errors else "false"
    return (
        "SELECT * FROM read_csv(\n"
        f"    {literal},\n"
        f"    delim='{spec.delimiter}',\n"
        f"    header={header},\n"
        f"    quote='{spec.quote}',\n"
        f"    encoding='{encoding}',\n"
        f"    columns={cols_clause},\n"
        f"    null_padding={null_padding},\n"
        f"    strict_mode={strict_mode},\n"
        f"    max_line_size={spec.max_line_size},\n"
        f"    parallel={parallel},\n"
        f"    ignore_errors={ignore}\n"
        ")"
    )


def encoding_attempts(sample: bytes) -> tuple[tuple[str, bool], ...]:
    """Determina tentativas ``(encoding, ignore_errors)`` a partir da amostra."""
    try:
        sample.decode("utf-8", errors="strict")
        return (("utf-8", True),)
    except UnicodeDecodeError:
        pass
    attempts: list[tuple[str, bool]] = [("latin-1", False)]
    if attempts[0] != ("utf-8", True):
        attempts.append(("utf-8", True))
    return tuple(attempts)
