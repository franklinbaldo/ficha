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

# `source_cardinality` describes what's known about the RAW input relative to
# `primary_key`, before any collapsing -- not the canonical output (a declared
# `primary_key` always implies "one row per key" *after* processing; these two
# values are about how that's reached:
#
# - "unique": the raw source is expected to already be unique by primary_key.
#   A duplicate found there is a genuine data-integrity failure, not something
#   the writer resolves.
# - "duplicates-expected": the raw source is known NOT to be unique by
#   primary_key. `duplicate_policy` says what the writer does about it.
SourceCardinality = Literal["unique", "duplicates-expected"]
_ALLOWED_SOURCE_CARDINALITIES = frozenset({"unique", "duplicates-expected"})

DuplicatePolicy = Literal["fail", "deterministic-collapse"]
_ALLOWED_DUPLICATE_POLICIES = frozenset({"fail", "deterministic-collapse"})

# Only these two pairings are coherent: "unique" input has nothing to collapse
# (any duplicate is a failure), and "duplicates-expected" input is exactly the
# case `deterministic-collapse` exists for. Declaring "unique" with a collapse
# policy, or "duplicates-expected" with a fail policy, would silently misstate
# either what's known about the source or what the writer actually does.
_VALID_CARDINALITY_POLICY_PAIRS = frozenset(
    {
        ("unique", "fail"),
        ("duplicates-expected", "deterministic-collapse"),
    }
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
    """Contrato lógico do Parquet canônico de uma entidade RFB.

    ``source_cardinality``/``duplicate_policy`` declaram o que se sabe sobre
    a unicidade do RAW por ``primary_key`` e o que o writer faz a respeito --
    ver os comentários nos ``Literal`` correspondentes. Um `primary_key`
    declarado já implica "uma linha por chave" no CANÔNICO; esses dois campos
    são sobre como isso é alcançado a partir de um raw que pode não ser único.
    """

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
    # Default matches estabelecimento's existing fail-on-duplicate shadow
    # writer behavior -- entities that don't override these two fields keep
    # today's semantics with no code change required.
    source_cardinality: SourceCardinality = "unique"
    duplicate_policy: DuplicatePolicy = "fail"

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

        if self.source_cardinality not in _ALLOWED_SOURCE_CARDINALITIES:
            raise ValueError(f"source_cardinality inválida: {self.source_cardinality!r}")
        if self.duplicate_policy not in _ALLOWED_DUPLICATE_POLICIES:
            raise ValueError(f"duplicate_policy inválida: {self.duplicate_policy!r}")
        pair = (self.source_cardinality, self.duplicate_policy)
        if pair not in _VALID_CARDINALITY_POLICY_PAIRS:
            raise ValueError(
                f"combinação inconsistente de source_cardinality={self.source_cardinality!r} "
                f"com duplicate_policy={self.duplicate_policy!r} -- pares válidos: "
                f"{sorted(_VALID_CARDINALITY_POLICY_PAIRS)}"
            )


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


def _decimal_column(name: str) -> CanonicalColumn:
    """DECIMAL(18,2) via TRY_CAST, mesmo padrão comprovado em produção para
    ``capital_social`` (ver ``transform._CAPITAL_SOCIAL_EXPR``, hoje DOUBLE em
    vez de DECIMAL) -- troca só a vírgula decimal do formato RFB pelo ponto
    que o parser DuckDB espera. Branco ou malformado (não-branco que não
    parseia) viram ``NULL`` via ``TRY_CAST`` sem levantar erro; nenhum
    ``NULLIF`` extra é necessário -- confirmado que ``TRY_CAST('' AS
    DECIMAL(18,2))`` já retorna ``NULL``, igual ao comportamento DOUBLE atual.
    """
    return CanonicalColumn(
        name=name,
        duckdb_type="DECIMAL(18,2)",
        source=name,
        nullable=True,
        invalid_policy="null-and-count",
        cast_sql="TRY_CAST(REPLACE({source}, ',', '.') AS DECIMAL(18,2))",
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
    # Explícito, não só o default: o writer shadow (canonical_shadow.py) já
    # falha fechado em chave duplicada para esta entidade -- ver PR #88's
    # "duplicate full-CNPJ excess rows fail closed".
    source_cardinality="unique",
    duplicate_policy="fail",
)

_EMPRESA_REQUIRED_COLUMNS = frozenset({"cnpj_basico"})

EMPRESA_CANONICAL = ParquetSpec(
    schema_version=1,
    columns=tuple(
        _decimal_column(name)
        if name == "capital_social"
        else _string_column(
            name,
            required=name in _EMPRESA_REQUIRED_COLUMNS,
            publication_critical=name in _EMPRESA_REQUIRED_COLUMNS,
        )
        for name in EMPRESA_COLUMNS
    ),
    primary_key=("cnpj_basico",),
    # Unlike estabelecimento, the raw empresa input is NOT guaranteed unique
    # by cnpj_basico -- production's `_dedupe_cnpj_basico_table` (transform.py)
    # already collapses both exact and genuinely conflicting duplicates via a
    # deterministic full-row-order pick. This declaration makes that existing
    # behavior visible in the registry instead of pretending the raw input is
    # already unique; it does not change what the loader does.
    #
    # "deterministic-collapse" is the CURRENT, transitional production policy,
    # not a verified semantic truth -- issue #76 tracks the open decision of
    # whether genuinely conflicting duplicates (different payload, same key)
    # should instead fail or be quarantined. Nothing here resolves #76: no
    # fail-on-conflict behavior, no quarantine writer, no change to
    # `_dedupe_cnpj_basico_table` or the monthly ETL.
    source_cardinality="duplicates-expected",
    duplicate_policy="deterministic-collapse",
)


# A ordem declarativa documenta o schema; a estratégia física de carga pode
# usar uma ordem própria em transform.py.
MAIN_TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="empresa",
        kind="empresas",
        source=CsvSpec(columns=EMPRESA_COLUMNS),
        canonical=EMPRESA_CANONICAL,
    ),
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
    """Lista SQL inline `['a', 'b']` pros paths de CSV.

    Inline em vez de parameter binding — array binding é instável no driver
    DuckDB pra esse caso. Aspas simples no path são escapadas dobrando-as
    (padrão SQL). `as_posix()`, não `str()`: DuckDB aceita `/` em qualquer
    plataforma, e `str(Path(...))` usa separador nativo no Windows -- sem
    isso o SQL gerado varia com o SO de quem rodou, não só com o path em si.
    """
    quoted = ", ".join(f"'{p.as_posix().replace(chr(39), chr(39) * 2)}'" for p in paths)
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
