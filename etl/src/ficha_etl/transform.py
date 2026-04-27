"""Transform: ZIPs RFB → 3 Parquets + lookups.json.

Pipeline (ADR 0008 + ADR 0009):

    Resolve via fetcher chain  →  Extract ZIPs  →  Load no DuckDB  →
    Write 3 Parquets + lookups.json
"""

from __future__ import annotations

import collections
import json
import logging
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from . import fetcher as fetcher_mod
from .sources import FileKind, canonical_inventory, is_valid_month

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractedFile:
    """Arquivo CSV resultante da extração de um ZIP."""

    kind: FileKind
    zip_name: str
    csv_path: Path


# Tabelas pequenas com formato (codigo, descricao). Encoding ISO-8859-1.
_LOOKUP_KINDS: tuple[FileKind, ...] = (
    "cnaes",
    "motivos",
    "municipios",
    "naturezas",
    "paises",
    "qualificacoes",
)

# Layout RFB CNPJ — colunas em ordem (sem header no CSV). Tipos: tudo VARCHAR
# pra evitar surpresas; conversões acontecem nas SELECTs finais.
_EMPRESA_COLUMNS: tuple[str, ...] = (
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
)
_ESTABELECIMENTO_COLUMNS: tuple[str, ...] = (
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
_SOCIO_COLUMNS: tuple[str, ...] = (
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
_SIMPLES_COLUMNS: tuple[str, ...] = (
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
)


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extrai um ZIP em `dest_dir`. Devolve lista de paths dos arquivos extraídos."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            extracted = zf.extract(member, dest_dir)
            paths.append(Path(extracted))
    return paths


def extract_all(
    month: str,
    chain: fetcher_mod.ChainedFetcher,
    extract_dir: Path,
) -> list[ExtractedFile]:
    """Resolve cada ZIP via chain, extrai pra `extract_dir/{kind}/`.

    RFB publica exatamente 1 CSV por ZIP. A invariante é checada explicitamente
    aqui — se RFB mudar e empacotar arquivos extras (ex.: checksum), falhamos
    loud em vez de pegar silenciosamente o primeiro entry.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    out: list[ExtractedFile] = []
    for spec in canonical_inventory():
        zip_path = chain.get(spec.name)
        kind_dir = extract_dir / spec.kind
        extracted = extract_zip(zip_path, kind_dir)
        files = [p for p in extracted if p.is_file()]
        if not files:
            raise RuntimeError(f"zip {spec.name!r} contained no files")
        if len(files) > 1:
            raise RuntimeError(
                f"zip {spec.name!r} expected exactly 1 CSV, got {len(files)}: "
                f"{[p.name for p in files]}"
            )
        out.append(ExtractedFile(kind=spec.kind, zip_name=spec.name, csv_path=files[0]))
    return out


def _csv_columns_clause(cols: tuple[str, ...]) -> str:
    """`{'c1': 'VARCHAR', 'c2': 'VARCHAR'}` para read_csv `columns` arg."""
    pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in cols)
    return "{" + pairs + "}"


def _create_table_from_csvs(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_paths: Iterable[Path],
    columns: tuple[str, ...],
) -> None:
    """Cria/recria `table` lendo todos os CSVs com layout RFB padrão.

    Layout: ISO-8859-1, sep=`;`, quote=`"`, sem header. Todas as colunas como
    VARCHAR — conversões (decimal, datas) ficam nas SELECTs finais.

    Filtra arquivos vazios pra evitar problemas no sniffer do DuckDB.
    """
    # Pula arquivos zero-byte (alguns ZIPs particionados podem vir vazios).
    paths = [p for p in csv_paths if p.exists() and p.stat().st_size > 0]
    if not paths:
        # Tabela vazia com schema correto, pra que JOINs não quebrem.
        col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
        con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
        return
    # Inline as a SQL list literal — parameter binding for arrays é instável.
    paths_literal = "[" + ", ".join(f"'{str(p)}'" for p in paths) + "]"
    cols_clause = _csv_columns_clause(columns)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv(
            {paths_literal},
            delim=';',
            header=false,
            quote='"',
            encoding='latin-1',
            columns={cols_clause},
            null_padding=true,
            strict_mode=false
        )
        """
    )


def load_main_tables_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
) -> None:
    """Carrega Empresa/Estabelecimento/Socio/Simples no DuckDB."""
    by_kind: dict[FileKind, list[Path]] = collections.defaultdict(list)
    for ef in extracted:
        by_kind[ef.kind].append(ef.csv_path)

    _create_table_from_csvs(con, "empresa", by_kind.get("empresas", []), _EMPRESA_COLUMNS)
    _create_table_from_csvs(
        con, "estabelecimento", by_kind.get("estabelecimentos", []), _ESTABELECIMENTO_COLUMNS
    )
    _create_table_from_csvs(con, "socio", by_kind.get("socios", []), _SOCIO_COLUMNS)
    _create_table_from_csvs(con, "simples", by_kind.get("simples", []), _SIMPLES_COLUMNS)


def load_lookup_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    kind: FileKind,
    csv_path: Path,
) -> None:
    """Carrega uma tabela de lookup (codigo;descricao) numa view DuckDB."""
    table = f"lookup_{kind}"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
            CAST(column0 AS VARCHAR) AS codigo,
            CAST(column1 AS VARCHAR) AS descricao
        FROM read_csv(
            ?,
            delim=';',
            header=false,
            quote='"',
            encoding='latin-1',
            columns={{'column0': 'VARCHAR', 'column1': 'VARCHAR'}}
        )
        """,
        [str(csv_path)],
    )


def lookups_dict(con: duckdb.DuckDBPyConnection, kind: FileKind) -> dict[str, str]:
    """Materializa uma lookup como dict codigo → descricao."""
    table = f"lookup_{kind}"
    rows = con.execute(f"SELECT codigo, descricao FROM {table}").fetchall()
    return {str(c): str(d) for c, d in rows}


def write_lookups_json(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
    *,
    schema_version: str,
    snapshot_date: str,
) -> None:
    """Emite `lookups.json` conforme `web/src/schemas/v1/lookups.ts`."""
    payload = {
        "schema_version": schema_version,
        "snapshot_date": snapshot_date,
        "cnaes": lookups_dict(con, "cnaes"),
        "motivos_situacao_cadastral": lookups_dict(con, "motivos"),
        "municipios": lookups_dict(con, "municipios"),
        "naturezas_juridicas": lookups_dict(con, "naturezas"),
        "paises": lookups_dict(con, "paises"),
        "qualificacoes_socio": lookups_dict(con, "qualificacoes"),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


# -----------------------------------------------------------------------------
# Helpers SQL reutilizados nas queries dos parquets
# -----------------------------------------------------------------------------

# Capital social no RFB usa vírgula como separador decimal. Converte pra DOUBLE.
_CAPITAL_SOCIAL_EXPR = (
    "TRY_CAST(REPLACE(emp.capital_social, ',', '.') AS DOUBLE)"
)

# YYYYMMDD → YYYY-MM-DD. Strings vazias / '0' viram NULL.
def _date_expr(col: str) -> str:
    return (
        f"CASE WHEN {col} IS NULL OR {col} = '' OR {col} = '0' THEN NULL "
        f"ELSE SUBSTR({col}, 1, 4) || '-' || SUBSTR({col}, 5, 2) || '-' || SUBSTR({col}, 7, 2) "
        f"END"
    )


# Mapeamentos pequenos hardcoded — códigos da RFB sem tabela própria de lookup.
_SITUACAO_DESCRICAO_SQL = """
CASE est.situacao_cadastral
    WHEN '01' THEN 'Nula'
    WHEN '02' THEN 'Ativa'
    WHEN '03' THEN 'Suspensa'
    WHEN '04' THEN 'Inapta'
    WHEN '08' THEN 'Baixada'
    ELSE ''
END
"""

_TIPO_SOCIO_DESCRICAO_SQL = """
CASE soc.identificador_socio
    WHEN '1' THEN 'PJ'
    WHEN '2' THEN 'PF'
    WHEN '3' THEN 'estrangeiro'
    ELSE ''
END
"""


def write_cnpjs_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """Produz `cnpjs.parquet`: uma linha por estabelecimento, denormalizado.

    Ver ADR 0008 e schema `web/src/schemas/v1/estabelecimento.ts`.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
                est.cnpj_basico AS cnpj_base,
                est.cnpj_ordem,
                est.cnpj_dv,
                est.identificador_matriz_filial,

                -- Empresa (denormalizado)
                emp.razao_social,
                UPPER(strip_accents(emp.razao_social)) AS razao_social_normalizada,
                emp.natureza_juridica AS natureza_juridica_codigo,
                COALESCE(nj.descricao, '') AS natureza_juridica_descricao,
                emp.qualificacao_responsavel AS qualificacao_responsavel_codigo,
                COALESCE(qr.descricao, '') AS qualificacao_responsavel_descricao,
                {_CAPITAL_SOCIAL_EXPR} AS capital_social,
                emp.porte_empresa,
                emp.ente_federativo_responsavel,

                -- Estabelecimento
                est.nome_fantasia,
                est.situacao_cadastral,
                {_SITUACAO_DESCRICAO_SQL} AS situacao_cadastral_descricao,
                {_date_expr('est.data_situacao_cadastral')} AS data_situacao_cadastral,
                est.motivo_situacao_cadastral AS motivo_situacao_cadastral_codigo,
                COALESCE(mt.descricao, '') AS motivo_situacao_cadastral_descricao,
                {_date_expr('est.data_inicio_atividade')} AS data_inicio_atividade,

                -- CNAE (principal + secundários)
                est.cnae_fiscal_principal AS cnae_principal_codigo,
                COALESCE(cn_p.descricao, '') AS cnae_principal_descricao,
                CASE WHEN est.cnae_fiscal_secundaria IS NULL OR est.cnae_fiscal_secundaria = ''
                     THEN []::VARCHAR[]
                     ELSE str_split(est.cnae_fiscal_secundaria, ',') END
                    AS cnae_secundario_codigos,
                -- TODO: descricoes resolvidas no client via lookups.json (v0.1)
                []::VARCHAR[] AS cnae_secundario_descricoes,

                -- Endereço
                est.tipo_logradouro,
                est.logradouro,
                est.numero,
                est.complemento,
                est.bairro,
                est.cep,
                est.uf,
                est.municipio AS municipio_codigo,
                COALESCE(mn.descricao, '') AS municipio_nome,
                est.pais AS pais_codigo,
                COALESCE(ps.descricao, '') AS pais_nome,
                est.nome_cidade_exterior,

                -- Contato
                est.ddd_1, est.telefone_1, est.ddd_2, est.telefone_2,
                est.ddd_fax, est.fax, est.correio_eletronico,

                -- Estado especial
                est.situacao_especial,
                {_date_expr('est.data_situacao_especial')} AS data_situacao_especial,

                -- Simples / MEI (inline)
                CASE s.opcao_simples WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                    AS opcao_simples,
                {_date_expr('s.data_opcao_simples')} AS data_opcao_simples,
                {_date_expr('s.data_exclusao_simples')} AS data_exclusao_simples,
                CASE s.opcao_mei WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                    AS opcao_mei,
                {_date_expr('s.data_opcao_mei')} AS data_opcao_mei,
                {_date_expr('s.data_exclusao_mei')} AS data_exclusao_mei

            FROM estabelecimento est
            LEFT JOIN empresa emp ON emp.cnpj_basico = est.cnpj_basico
            LEFT JOIN simples s ON s.cnpj_basico = est.cnpj_basico
            LEFT JOIN lookup_naturezas nj ON nj.codigo = emp.natureza_juridica
            LEFT JOIN lookup_qualificacoes qr ON qr.codigo = emp.qualificacao_responsavel
            LEFT JOIN lookup_motivos mt ON mt.codigo = est.motivo_situacao_cadastral
            LEFT JOIN lookup_cnaes cn_p ON cn_p.codigo = est.cnae_fiscal_principal
            LEFT JOIN lookup_municipios mn ON mn.codigo = est.municipio
            LEFT JOIN lookup_paises ps ON ps.codigo = est.pais
            ORDER BY cnpj
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [str(output_path)],
    )


def write_raizes_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """Produz `raizes.parquet`: uma linha por cnpj_base com agregados.

    Ver ADR 0008 e schema `web/src/schemas/v1/raiz.ts`.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            WITH agg AS (
                SELECT
                    est.cnpj_basico AS cnpj_base,
                    COUNT(*)::INTEGER AS qtd_estabelecimentos,
                    COUNT(*) FILTER (WHERE est.situacao_cadastral = '02')::INTEGER
                        AS qtd_estabelecimentos_ativos,
                    LIST_DISTINCT(LIST(est.uf)) AS ufs_atuacao,
                    LIST_DISTINCT(LIST(est.cnae_fiscal_principal)) AS cnaes_principais_distintos
                FROM estabelecimento est
                GROUP BY est.cnpj_basico
            ),
            matriz AS (
                SELECT
                    est.cnpj_basico,
                    {_date_expr('est.data_inicio_atividade')} AS data_inicio_atividade_matriz,
                    est.uf AS uf_matriz,
                    est.municipio AS municipio_matriz_codigo,
                    est.cnae_fiscal_principal AS cnae_principal_matriz_codigo
                FROM estabelecimento est
                WHERE est.identificador_matriz_filial = '1'
            )
            SELECT
                emp.cnpj_basico AS cnpj_base,
                emp.razao_social,
                UPPER(strip_accents(emp.razao_social)) AS razao_social_normalizada,
                emp.natureza_juridica AS natureza_juridica_codigo,
                COALESCE(nj.descricao, '') AS natureza_juridica_descricao,
                {_CAPITAL_SOCIAL_EXPR} AS capital_social,
                emp.porte_empresa,
                emp.ente_federativo_responsavel,
                COALESCE(agg.qtd_estabelecimentos, 0) AS qtd_estabelecimentos,
                COALESCE(agg.qtd_estabelecimentos_ativos, 0) AS qtd_estabelecimentos_ativos,
                COALESCE(agg.ufs_atuacao, []) AS ufs_atuacao,
                COALESCE(agg.cnaes_principais_distintos, []) AS cnaes_principais_distintos,
                matriz.data_inicio_atividade_matriz,
                matriz.uf_matriz,
                matriz.municipio_matriz_codigo,
                COALESCE(mn.descricao, '') AS municipio_matriz_nome,
                matriz.cnae_principal_matriz_codigo,
                COALESCE(cn.descricao, '') AS cnae_principal_matriz_descricao
            FROM empresa emp
            LEFT JOIN agg ON agg.cnpj_base = emp.cnpj_basico
            LEFT JOIN matriz ON matriz.cnpj_basico = emp.cnpj_basico
            LEFT JOIN lookup_naturezas nj ON nj.codigo = emp.natureza_juridica
            LEFT JOIN lookup_municipios mn ON mn.codigo = matriz.municipio_matriz_codigo
            LEFT JOIN lookup_cnaes cn ON cn.codigo = matriz.cnae_principal_matriz_codigo
            ORDER BY razao_social_normalizada
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [str(output_path)],
    )


def write_socios_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """Produz `socios.parquet`: PF + PJ + estrangeiro com flag tipo.

    Ver ADR 0008 e schema `web/src/schemas/v1/socio.ts`.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                soc.cnpj_basico AS cnpj_base,
                soc.identificador_socio AS tipo,
                {_TIPO_SOCIO_DESCRICAO_SQL} AS tipo_descricao,
                soc.nome_socio_razao_social,
                CASE soc.identificador_socio
                    WHEN '2' THEN soc.cnpj_cpf_socio
                    ELSE NULL
                END AS cpf_mascarado,
                CASE soc.identificador_socio
                    WHEN '1' THEN soc.cnpj_cpf_socio
                    ELSE NULL
                END AS cnpj_socio,
                soc.qualificacao_socio AS qualificacao_codigo,
                COALESCE(qs.descricao, '') AS qualificacao_descricao,
                {_date_expr('soc.data_entrada_sociedade')} AS data_entrada_sociedade,
                soc.pais AS pais_codigo,
                COALESCE(ps.descricao, '') AS pais_nome,
                soc.representante_legal AS representante_legal_cpf,
                soc.nome_representante_legal AS representante_legal_nome,
                soc.qualificacao_representante_legal AS representante_legal_qualificacao_codigo,
                COALESCE(qr.descricao, '') AS representante_legal_qualificacao_descricao,
                soc.faixa_etaria
            FROM socio soc
            LEFT JOIN lookup_qualificacoes qs ON qs.codigo = soc.qualificacao_socio
            LEFT JOIN lookup_qualificacoes qr ON qr.codigo = soc.qualificacao_representante_legal
            LEFT JOIN lookup_paises ps ON ps.codigo = soc.pais
            ORDER BY cnpj_base
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [str(output_path)],
    )


def transform_snapshot(
    month: str,
    *,
    cache_dir: Path,
    output_dir: Path,
    chain: fetcher_mod.ChainedFetcher | None = None,
    schema_version: str = "1.0.0",
    skip_unimplemented: bool = True,
) -> None:
    """Orquestrador: resolve → extract → load → write outputs."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    chain = chain or fetcher_mod.default_chain(month, cache_dir=cache_dir)
    extract_dir = cache_dir / month / "extracted"

    log.info("extracting 37 ZIPs for %s into %s", month, extract_dir)
    extracted = extract_all(month, chain, extract_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("loading data into DuckDB")
    con = duckdb.connect()
    try:
        # Lookups primeiro (necessárias pros JOINs dos parquets)
        for ef in extracted:
            if ef.kind in _LOOKUP_KINDS:
                load_lookup_into_duckdb(con, ef.kind, ef.csv_path)

        # Tabelas grandes
        load_main_tables_into_duckdb(con, extracted)

        write_lookups_json(
            con,
            output_dir / "lookups.json",
            schema_version=schema_version,
            snapshot_date=month,
        )
        log.info("wrote %s", output_dir / "lookups.json")

        for name, fn in (
            ("cnpjs", write_cnpjs_parquet),
            ("raizes", write_raizes_parquet),
            ("socios", write_socios_parquet),
        ):
            try:
                fn(con, extracted, output_dir / f"{name}.parquet")
                log.info("wrote %s", output_dir / f"{name}.parquet")
            except NotImplementedError as exc:
                if skip_unimplemented:
                    log.warning("skipping %s.parquet: %s", name, exc)
                else:
                    raise
    finally:
        con.close()
