"""Schema registry — fonte central de metadados das tabelas RFB (Fase 1, RFC 0001).

Este módulo só descreve *o quê* (layout de colunas, opções de leitura CSV) e
*como montar o SQL* correspondente — funções puras, sem I/O e sem side effects.
A orquestração (loop de tentativas de encoding, filtragem de arquivos vazios,
criação de tabela vazia, logging) continua em `transform.py`.

Requisito central (RFC 0001 §8.1): o SQL gerado aqui deve produzir leitura CSV
semanticamente idêntica ao reader legado — nenhuma mudança de comportamento
nesta fase.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .sources import FileKind


@dataclass(frozen=True)
class CsvSpec:
    """Opções de leitura CSV para uma tabela RFB — layout fixo, sem header.

    Os defaults capturam decisões load-bearing do reader legado (ver
    `_create_table_from_csvs` em transform.py), não meras conveniências:

    - `parallel=False`: o scanner paralelo do DuckDB fatia um único arquivo
      grande por offset de bytes; com `null_padding=True` ele não recupera
      linhas ragged cujo campo tem newline entre aspas atravessando um
      corte, e aborta ("parallel scanner does not support null_padding in
      conjunction with quoted new lines"). Isso é dependente da posição dos
      dados, então threads=1 (já a norma aqui) não custa nada e torna a
      carga determinística.
    - `max_line_size=16MiB`: alguns campos (razao_social, nome_fantasia)
      podem ser grandes o bastante pra estourar o default do DuckDB.
    - `null_padding=True`: linhas RFB "ragged" (menos campos que o schema)
      são preenchidas com NULL em vez de falhar o parse inteiro.
    - `strict_mode=False`: RFB não segue estritamente RFC 4180 em todas as
      partições; strict_mode=true rejeitaria linhas que null_padding
      deveria tolerar.
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
class TableSpec:
    """Uma tabela RFB carregável no DuckDB: nome da tabela + FileKind + CsvSpec."""

    name: str
    kind: FileKind
    source: CsvSpec


# Layout RFB CNPJ — colunas em ordem (sem header no CSV). Tipos: tudo VARCHAR
# pra evitar surpresas; conversões acontecem nas SELECTs finais.
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


# Ordem espelha o loop de `load_main_tables_into_duckdb` em transform.py.
MAIN_TABLES: tuple[TableSpec, ...] = (
    TableSpec(name="empresa", kind="empresas", source=CsvSpec(columns=EMPRESA_COLUMNS)),
    TableSpec(
        name="estabelecimento",
        kind="estabelecimentos",
        source=CsvSpec(columns=ESTABELECIMENTO_COLUMNS),
    ),
    TableSpec(name="simples", kind="simples", source=CsvSpec(columns=SIMPLES_COLUMNS)),
    TableSpec(name="socio", kind="socios", source=CsvSpec(columns=SOCIO_COLUMNS)),
)


def csv_columns_clause(columns: tuple[str, ...]) -> str:
    """`{'c1': 'VARCHAR', 'c2': 'VARCHAR'}` para read_csv `columns` arg."""
    pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    return "{" + pairs + "}"


def paths_literal(paths: Sequence[Path]) -> str:
    """Lista SQL inline `['a', 'b']` pros paths de CSV.

    Inline em vez de parameter binding — array binding é instável no driver
    DuckDB pra esse caso. Aspas simples no path são escapadas dobrando-as
    (padrão SQL).
    """
    quoted = ", ".join(f"'{str(p).replace(chr(39), chr(39) * 2)}'" for p in paths)
    return "[" + quoted + "]"


def read_csv_select_sql(
    spec: CsvSpec,
    paths: Sequence[Path],
    *,
    encoding: str,
    ignore_errors: bool,
) -> str:
    """Gera o `SELECT * FROM read_csv(...)` pra ler `paths` com as opções de `spec`.

    Mesmas opções e mesma ordem do reader legado em `_create_table_from_csvs`
    — este SQL deve ser semanticamente idêntico ao anterior (RFC 0001 §8.1).
    `encoding` e `ignore_errors` variam por tentativa (ver `encoding_attempts`)
    e por isso não fazem parte de `CsvSpec`.
    """
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
    """Determina a ordem de tentativas (encoding, ignore_errors) a partir de uma amostra.

    RFB occasionally emits rows that are neither valid latin-1 nor utf-8
    (mixed-encoding garbage from legacy systems). DuckDB's latin-1 mode
    pre-flight-rejects the whole file ("File is not latin-1 encoded"), so
    `ignore_errors` doesn't help that branch. utf-8 mode accepts any bytes
    at parse time and only fails per-row, so `ignore_errors=True` there
    drops the bad rows.

    Se a amostra decodifica em utf-8 estrito, usamos utf-8 direto (com
    ignore_errors=True, pois bytes ruins podem existir no meio do arquivo
    mesmo que a amostra inicial seja válida). Caso contrário, tentamos
    latin-1 primeiro (encoding histórico da RFB, sem ignore_errors — não
    ajudaria de qualquer forma), com utf-8+ignore_errors como safety net.
    """
    try:
        sample.decode("utf-8", errors="strict")
        return (("utf-8", True),)
    except UnicodeDecodeError:
        pass
    attempts: list[tuple[str, bool]] = [("latin-1", False)]
    if attempts[0] != ("utf-8", True):
        attempts.append(("utf-8", True))
    return tuple(attempts)
