"""Transform: ZIPs RFB → 3 Parquets + lookups.json.

Pipeline (ADR 0008 + ADR 0009):

    Resolve via fetcher chain  →  Extract ZIPs  →  Load no DuckDB  →
    Write 3 Parquets + lookups.json
"""

from __future__ import annotations

import collections
import json
import logging
import time
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
    inventory = list(canonical_inventory())

    # ── Download em paralelo ────────────────────────────────────────────────
    log.info("downloading %d ZIPs in parallel (4 workers)...", len(inventory))
    t_dl = time.monotonic()
    zip_paths = chain.get_all_parallel(
        [spec.name for spec in inventory],
        workers=4,
    )
    log.info("all ZIPs downloaded in %.0fs", time.monotonic() - t_dl)

    # ── Extração sequencial (I/O local, rápido) ─────────────────────────────
    total = len(inventory)
    out: list[ExtractedFile] = []
    for i, spec in enumerate(inventory, 1):
        t0 = time.monotonic()
        zip_path = zip_paths[spec.name]
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
        csv_path = files[0]
        size_mb = csv_path.stat().st_size / 1024 / 1024
        log.info(
            "[%d/%d] extracted %s → %.1f MB CSV (%.1fs)",
            i,
            total,
            spec.name,
            size_mb,
            time.monotonic() - t0,
        )
        out.append(ExtractedFile(kind=spec.kind, zip_name=spec.name, csv_path=csv_path))
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

    Tenta latin-1 primeiro (encoding histórico da RFB); se falhar por encoding,
    tenta utf-8 (algumas partições da RFB foram publicadas em UTF-8).

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
    # Aspas simples nos paths são escapadas dobrando-as (padrão SQL).
    paths_literal = (
        "[" + ", ".join(f"'{str(p).replace(chr(39), chr(39) * 2)}'" for p in paths) + "]"
    )
    cols_clause = _csv_columns_clause(columns)

    # Each attempt: (encoding, ignore_errors). RFB occasionally emits rows
    # that are neither valid latin-1 nor utf-8 (mixed-encoding garbage from
    # legacy systems). DuckDB's latin-1 mode pre-flight-rejects the whole
    # file ("File is not latin-1 encoded"), so `ignore_errors` doesn't
    # help that branch. utf-8 mode accepts any bytes at parse time and
    # only fails per-row, so `ignore_errors=true` there drops the bad
    # rows. Per ADR 0006, a handful of dropped rows out of 60M+ is
    # preferable to no snapshot. The fallback is logged loudly so we
    # can see if it ever fires in production.
    attempts = [
        ("latin-1", False),
        ("utf-8", False),
        ("utf-8", True),
    ]
    for encoding, ignore_errors in attempts:
        try:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM read_csv(
                    {paths_literal},
                    delim=';',
                    header=false,
                    quote='"',
                    encoding='{encoding}',
                    columns={cols_clause},
                    null_padding=true,
                    strict_mode=false,
                    max_line_size=16777216,
                    ignore_errors={"true" if ignore_errors else "false"}
                )
                """
            )
            if encoding != "latin-1" or ignore_errors:
                log.warning(
                    "tabela '%s' carregada com encoding=%s ignore_errors=%s (fallback)",
                    table,
                    encoding,
                    ignore_errors,
                )
            return
        except Exception as exc:
            msg = str(exc).lower()
            if "not latin-1 encoded" in msg or "not utf-8" in msg or "encoding" in msg:
                log.warning(
                    "encoding=%s ignore_errors=%s falhou para '%s': %s -- tentando proximo",
                    encoding,
                    ignore_errors,
                    table,
                    exc,
                )
                continue
            raise
    raise RuntimeError(
        f"Falha ao carregar tabela '{table}': nenhum encoding funcionou "
        "(latin-1, utf-8, latin-1+ignore_errors)"
    )


def load_main_tables_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
) -> None:
    """Carrega Empresa/Estabelecimento/Socio/Simples no DuckDB."""
    by_kind: dict[FileKind, list[Path]] = collections.defaultdict(list)
    for ef in extracted:
        by_kind[ef.kind].append(ef.csv_path)

    for table, kind, cols in (
        ("empresa", "empresas", _EMPRESA_COLUMNS),
        ("estabelecimento", "estabelecimentos", _ESTABELECIMENTO_COLUMNS),
        ("socio", "socios", _SOCIO_COLUMNS),
        ("simples", "simples", _SIMPLES_COLUMNS),
    ):
        t0 = time.monotonic()
        log.info("loading table '%s' from %d CSV(s)...", table, len(by_kind.get(kind, [])))
        _create_table_from_csvs(con, table, by_kind.get(kind, []), cols)
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(
            "loaded '%s' — %s rows in %.1fs",
            table,
            f"{n:,}",
            time.monotonic() - t0,
        )


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
_CAPITAL_SOCIAL_EXPR = "TRY_CAST(REPLACE(emp.capital_social, ',', '.') AS DOUBLE)"


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
    output_path: Path,
) -> None:
    """Produz `cnpjs.parquet`: uma linha por estabelecimento, denormalizado.

    Requer que `load_main_tables_into_duckdb` e os lookups já estejam carregados em `con`.
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
                {_date_expr("est.data_situacao_cadastral")} AS data_situacao_cadastral,
                est.motivo_situacao_cadastral AS motivo_situacao_cadastral_codigo,
                COALESCE(mt.descricao, '') AS motivo_situacao_cadastral_descricao,
                {_date_expr("est.data_inicio_atividade")} AS data_inicio_atividade,

                -- CNAE (principal + secundários)
                est.cnae_fiscal_principal AS cnae_principal_codigo,
                COALESCE(cn_p.descricao, '') AS cnae_principal_descricao,
                CASE WHEN est.cnae_fiscal_secundaria IS NULL OR est.cnae_fiscal_secundaria = ''
                     THEN []::VARCHAR[]
                     ELSE list_transform(str_split(est.cnae_fiscal_secundaria, ','), x -> trim(x)) END
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
                {_date_expr("est.data_situacao_especial")} AS data_situacao_especial,

                -- Simples / MEI (inline)
                CASE s.opcao_simples WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                    AS opcao_simples,
                {_date_expr("s.data_opcao_simples")} AS data_opcao_simples,
                {_date_expr("s.data_exclusao_simples")} AS data_exclusao_simples,
                CASE s.opcao_mei WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                    AS opcao_mei,
                {_date_expr("s.data_opcao_mei")} AS data_opcao_mei,
                {_date_expr("s.data_exclusao_mei")} AS data_exclusao_mei

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
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
        """,
        [str(output_path)],
    )


def write_raizes_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `raizes.parquet`: uma linha por cnpj_base com agregados.

    Requer que `load_main_tables_into_duckdb` e os lookups já estejam carregados em `con`.
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
                -- QUALIFY garante 1 linha por cnpj_basico mesmo se dados da RFB
                -- tiverem mais de uma entrada com identificador_matriz_filial = '1'.
                SELECT
                    est.cnpj_basico,
                    {_date_expr("est.data_inicio_atividade")} AS data_inicio_atividade_matriz,
                    est.uf AS uf_matriz,
                    est.municipio AS municipio_matriz_codigo,
                    est.cnae_fiscal_principal AS cnae_principal_matriz_codigo
                FROM estabelecimento est
                WHERE est.identificador_matriz_filial = '1'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY est.cnpj_basico ORDER BY est.cnpj_ordem
                ) = 1
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
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
        """,
        [str(output_path)],
    )


def write_socios_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `socios.parquet`: PF + PJ + estrangeiro com flag tipo.

    Requer que `load_main_tables_into_duckdb` e os lookups já estejam carregados em `con`.
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
                {_date_expr("soc.data_entrada_sociedade")} AS data_entrada_sociedade,
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
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
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
    verify: bool = False,
    verify_sample_size: int = 100,
) -> None:
    """Orquestrador: resolve → extract → load → write outputs.

    Se `verify=True`, roda `assert_roundtrip` após escrever `cnpjs.parquet`
    como gate de qualidade (ADR 0009): falha se campos amostrados do
    Parquet não baterem com os dados originais já carregados no DuckDB.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    chain = chain or fetcher_mod.default_chain(month, cache_dir=cache_dir)
    extract_dir = cache_dir / month / "extracted"

    t_total = time.monotonic()
    log.info("=== PHASE 1/4: extract 37 ZIPs for %s ===", month)
    t0 = time.monotonic()
    extracted = extract_all(month, chain, extract_dir)
    log.info("=== PHASE 1/4 done in %.0fs ===", time.monotonic() - t0)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Usa arquivo temporário em vez de in-memory para suportar o dataset real
    # da RFB (~60 M linhas) sem estourar RAM.
    db_path = cache_dir / month / "transform.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("=== PHASE 2/4: load into DuckDB (%s) ===", db_path)
    t0 = time.monotonic()
    con = duckdb.connect(str(db_path))
    # Cap memory and force on-disk spill. GH Actions ubuntu-latest has ~7 GB
    # of RAM; loading 15 GB of estabelecimento CSV with `ignore_errors=true`
    # OOM-killed the runner (PR #24, run 25514278003). DuckDB's default is
    # ~80% of system RAM with limited spill -- explicit limit + dedicated
    # temp dir on the same partition as db_path makes spill behavior
    # predictable.
    con.execute("PRAGMA memory_limit='5GB'")
    con.execute(f"PRAGMA temp_directory='{db_path.parent / 'duckdb_tmp'}'")
    # Reduce per-query memory pressure during the big JOIN at phase 3.
    # DuckDB's default preserves input ordering, which buffers more in
    # memory; we sort by `cnpj` at write time anyway, so insertion order
    # of intermediates doesn't matter. Saves ~30% on temp spill size.
    con.execute("PRAGMA preserve_insertion_order=false")
    # Reduce parallelism. Each thread holds its own working set during
    # the 70M x 67M VARCHAR-keyed hash join in write_cnpjs_parquet --
    # 4 threads (default) blew through 70 GB of temp spill (run
    # 25518175202). Cutting to 2 roughly halves peak temp at the cost
    # of ~2x wall time, which we can afford for the bootstrap.
    con.execute("PRAGMA threads=2")
    try:
        # Lookups primeiro (necessárias pros JOINs dos parquets)
        for ef in extracted:
            if ef.kind in _LOOKUP_KINDS:
                load_lookup_into_duckdb(con, ef.kind, ef.csv_path)
                log.info("  lookup '%s' loaded", ef.kind)

        # Tabelas grandes
        load_main_tables_into_duckdb(con, extracted)
        log.info("=== PHASE 2/4 done in %.0fs ===", time.monotonic() - t0)

        # Reclaim disk before phase 3. Extracted CSVs are now loaded into
        # transform.duckdb; keeping them alongside DuckDB's temp spill
        # exhausts the runner's ~70 GiB filesystem (PR #24, run 25517197692:
        # OOM "70.8 GiB/70.8 GiB used" while writing cnpjs.parquet).
        # Also drop the raw ZIPs -- a retry can re-fetch them from IA
        # via the fetcher chain in <2 min, and keeping them robs phase 3
        # of ~7 GB of join-spill headroom.
        import shutil

        if extract_dir.exists():
            extracted_size_gb = sum(
                p.stat().st_size for p in extract_dir.rglob("*") if p.is_file()
            ) / (1024**3)
            shutil.rmtree(extract_dir)
            log.info("freed %.1f GB by removing %s", extracted_size_gb, extract_dir)
        zips_dir = cache_dir / month
        zip_size_gb = 0.0
        for zp in zips_dir.glob("*.zip"):
            zip_size_gb += zp.stat().st_size / (1024**3)
            zp.unlink()
        if zip_size_gb > 0:
            log.info("freed %.1f GB by removing raw ZIPs in %s", zip_size_gb, zips_dir)

        write_lookups_json(
            con,
            output_dir / "lookups.json",
            schema_version=schema_version,
            snapshot_date=month,
        )
        log.info("wrote lookups.json")

        log.info("=== PHASE 3/4: write parquets ===")
        t0 = time.monotonic()
        for name, fn in (
            ("cnpjs", write_cnpjs_parquet),
            ("raizes", write_raizes_parquet),
            ("socios", write_socios_parquet),
        ):
            try:
                log.info("  writing %s.parquet...", name)
                tp = time.monotonic()
                fn(con, output_dir / f"{name}.parquet")
                size_mb = (output_dir / f"{name}.parquet").stat().st_size / 1024 / 1024
                log.info(
                    "  wrote %s.parquet — %.1f MB in %.0fs",
                    name,
                    size_mb,
                    time.monotonic() - tp,
                )
            except NotImplementedError as exc:
                if skip_unimplemented:
                    log.warning("skipping %s.parquet: %s", name, exc)
                else:
                    raise
        log.info("=== PHASE 3/4 done in %.0fs ===", time.monotonic() - t0)

        if verify:
            cnpjs_parquet = output_dir / "cnpjs.parquet"
            if cnpjs_parquet.exists():
                log.info(
                    "=== PHASE 4/4: roundtrip-equivalence check (sample=%d) ===", verify_sample_size
                )
                t0 = time.monotonic()
                assert_roundtrip(con, cnpjs_parquet, sample_size=verify_sample_size)
                log.info("=== PHASE 4/4 roundtrip OK in %.0fs ===", time.monotonic() - t0)

        log.info("transform_snapshot total: %.0fs", time.monotonic() - t_total)
    finally:
        con.close()
        db_path.unlink(missing_ok=True)


# -----------------------------------------------------------------------------
# Roundtrip-equivalence (ADR 0009)
# -----------------------------------------------------------------------------


# Campos comparados na verificação. Subset "fácil" — pula campos computados
# (razao_social_normalizada, descricoes de lookup) que requerem reaplicar
# transformação. Pra esses, comparações de igualdade dos códigos de origem
# (cnae_principal_codigo, etc.) são proxy suficiente.
_ROUNDTRIP_FIELDS = (
    ("razao_social", "emp.razao_social"),
    ("uf", "est.uf"),
    ("cnae_principal_codigo", "est.cnae_fiscal_principal"),
    ("situacao_cadastral", "est.situacao_cadastral"),
    ("nome_fantasia", "est.nome_fantasia"),
    ("identificador_matriz_filial", "est.identificador_matriz_filial"),
    ("municipio_codigo", "est.municipio"),
)


class RoundtripError(AssertionError):
    """Falha do gate de roundtrip-equivalence (ADR 0009)."""


def assert_roundtrip(
    con: duckdb.DuckDBPyConnection,
    cnpjs_parquet: Path,
    *,
    sample_size: int = 100,
) -> None:
    """Sortea N CNPJs do estabelecimento original e compara com o Parquet.

    Falha (RoundtripError) se a contagem total não bater OU se algum dos
    campos `_ROUNDTRIP_FIELDS` divergir pra qualquer CNPJ amostrado.

    Pré-requisito: tabelas `estabelecimento` + `empresa` carregadas em `con`,
    e `cnpjs_parquet` já escrito.
    """
    # Contagem total
    expected_n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    actual_n = con.execute(f"SELECT COUNT(*) FROM '{cnpjs_parquet}'").fetchone()[0]
    if expected_n != actual_n:
        raise RoundtripError(
            f"row count mismatch: estabelecimento has {expected_n}, cnpjs.parquet has {actual_n}"
        )

    if expected_n == 0:
        return  # nada pra amostrar

    # Sample N CNPJs
    n = min(sample_size, expected_n)
    sample_query = f"""
        SELECT est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
               {", ".join(expr + " AS " + alias for alias, expr in _ROUNDTRIP_FIELDS)}
        FROM estabelecimento est
        LEFT JOIN empresa emp ON emp.cnpj_basico = est.cnpj_basico
        ORDER BY random()
        LIMIT {n}
    """
    sampled = con.execute(sample_query).fetchall()

    field_names = ["cnpj"] + [alias for alias, _ in _ROUNDTRIP_FIELDS]
    parquet_select = ", ".join(field_names)

    divergences: list[str] = []
    for row in sampled:
        cnpj = row[0]
        actual = con.execute(
            f"SELECT {parquet_select} FROM '{cnpjs_parquet}' WHERE cnpj = ?",
            [cnpj],
        ).fetchone()
        if actual is None:
            divergences.append(f"{cnpj}: missing from parquet")
            continue
        for i, (alias, _) in enumerate(_ROUNDTRIP_FIELDS, start=1):
            if row[i] != actual[i]:
                divergences.append(f"{cnpj}.{alias}: source={row[i]!r} parquet={actual[i]!r}")

    if divergences:
        head = divergences[:10]
        more = len(divergences) - len(head)
        msg = "\n  ".join(head)
        if more:
            msg += f"\n  ... and {more} more"
        raise RoundtripError(f"roundtrip mismatch over {len(sampled)} sampled CNPJs:\n  {msg}")
