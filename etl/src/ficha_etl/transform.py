"""Transform: ZIPs RFB → 3 Parquets + lookups.json.

Pipeline (ADR 0008 + ADR 0009):

    Resolve via fetcher chain  →  Extract ZIPs  →  Load no DuckDB  →
    Write 3 Parquets + lookups.json
"""

from __future__ import annotations

import collections
import json
import logging
import os
import shutil
import time
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb
from rich.progress import Progress

from . import fetcher as fetcher_mod
from . import registry
from .progress import make_progress
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
# Fonte de verdade agora é `registry` (Fase 1, RFC 0001 §8.1). Só mantemos
# alias pros dois símbolos com consumidor real remanescente (test_transform.py);
# `_ESTABELECIMENTO_COLUMNS`/`_SOCIO_COLUMNS` foram removidos por não terem
# mais nenhum consumidor após os call sites migrarem pra `registry.main_table`.
_EMPRESA_COLUMNS = registry.EMPRESA_COLUMNS
_SIMPLES_COLUMNS = registry.SIMPLES_COLUMNS


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
    *,
    progress: Progress | None = None,
) -> list[ExtractedFile]:
    """Resolve cada ZIP via chain, extrai pra `extract_dir/{kind}/`.

    RFB publica exatamente 1 CSV por ZIP. A invariante é checada explicitamente
    aqui — se RFB mudar e empacotar arquivos extras (ex.: checksum), falhamos
    loud em vez de pegar silenciosamente o primeiro entry.

    `progress`: barra de progresso já iniciada (rich) pra reusar entre
    chamadas dentro do mesmo `run`; se omitido, cria (e fecha) a sua própria.
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
    owns_progress = progress is None
    progress = progress or make_progress()
    if owns_progress:
        progress.start()
    task_id = progress.add_task("extract ZIPs", total=total)
    try:
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
            progress.update(task_id, description=f"extract {spec.name}", advance=1)
    finally:
        if owns_progress:
            progress.stop()
    return out


def _create_table_from_csvs(
    con: duckdb.DuckDBPyConnection,
    table: str,
    csv_paths: Iterable[Path],
    spec: registry.CsvSpec,
) -> None:
    """Cria/recria `table` lendo todos os CSVs conforme `spec`.

    Tenta latin-1 primeiro (encoding histórico da RFB); se falhar por encoding,
    tenta utf-8 (algumas partições da RFB foram publicadas em UTF-8).

    Filtra arquivos vazios pra evitar problemas no sniffer do DuckDB.

    `spec` vem do registry (chamador decide qual TableSpec/CsvSpec usar) —
    esta função nunca reconstrói um CsvSpec com defaults, senão qualquer
    override futuro (delimiter, quote, parallel, etc.) seria silenciosamente
    ignorado. O SQL de leitura vem de `registry.read_csv_select_sql`; esta
    função só orquestra: filtragem de arquivos vazios, tabela vazia com
    schema correto, sniff de encoding, loop de tentativas, logging e
    tratamento de erro.
    """
    # Pula arquivos zero-byte (alguns ZIPs particionados podem vir vazios).
    paths = [p for p in csv_paths if p.exists() and p.stat().st_size > 0]
    if not paths:
        # Tabela vazia com schema correto, pra que JOINs não quebrem.
        col_defs = ", ".join(f"{c} VARCHAR" for c in spec.columns)
        con.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")
        return

    # Each attempt: (encoding, ignore_errors). RFB occasionally emits rows
    # that are neither valid latin-1 nor utf-8 (mixed-encoding garbage from
    # legacy systems). DuckDB's latin-1 mode pre-flight-rejects the whole
    # file ("File is not latin-1 encoded"), so `ignore_errors` doesn't
    # help that branch. utf-8 mode accepts any bytes at parse time and
    # only fails per-row, so `ignore_errors=true` there drops the bad
    # rows. Per ADR 0006, a handful of dropped rows out of 60M+ is
    # preferable to no snapshot. The fallback is logged loudly so we
    # can see if it ever fires in production.

    # Sniff the first 1 MB of the first non-empty CSV.
    first_path = paths[0]
    with open(first_path, "rb") as f:
        sample = f.read(1024 * 1024)
    attempts = registry.encoding_attempts(sample)

    # parallel=false is load-bearing, not a perf knob (see CsvSpec default —
    # registry.py has the full rationale). DuckDB's parallel CSV scanner
    # range-splits a single large file across byte offsets; with
    # null_padding=true it cannot recover ragged rows whose fields contain a
    # quoted newline that straddles a split boundary, and aborts:
    #   "The parallel scanner does not support null_padding in conjunction
    #    with quoted new lines."
    # This is data-position-dependent, so it hid until the 2026-07 run reached
    # the chunked cnpjs write, where estabelecimento is read one CSV at a time
    # (single-file → intra-file split) instead of as a 10-file list. threads=1
    # is already the norm here (see PRAGMA call site), so disabling the parallel
    # reader costs nothing and makes the load deterministic across both paths.
    for encoding, ignore_errors in attempts:
        select_sql = registry.read_csv_select_sql(
            spec, paths, encoding=encoding, ignore_errors=ignore_errors
        )
        try:
            con.execute(f"CREATE OR REPLACE TABLE {table} AS\n{select_sql}")
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
        "(latin-1, utf-8, utf-8+ignore_errors)"
    )


_OS_HEADROOM_GB = 6  # OS + runner agent + Python heap + tee + DuckDB overshoots
_MEMORY_FRACTION = 0.65  # never exceed this share of total RAM
_MIN_MEMORY_GB = 2


def _total_ram_gb() -> int | None:
    """Read MemTotal from /proc/meminfo, in GB. None if unreadable (non-Linux)."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return kb // (1024 * 1024)
    except (OSError, ValueError):
        return None
    return None


def pick_memory_limit_gb() -> int:
    """Choose a DuckDB memory_limit (GB) for the current runtime.

    Precedence:
        1. `FICHA_MEMORY_LIMIT_GB` env (operator override)
        2. min(total_ram - _OS_HEADROOM_GB, total_ram * _MEMORY_FRACTION),
           clamped to >= _MIN_MEMORY_GB.
        3. 10 GB fallback (GH Actions 16 GB free tier default).

    Rationale lives next to the PRAGMA call site.
    """
    override = os.environ.get("FICHA_MEMORY_LIMIT_GB", "").strip()
    if override:
        try:
            n = int(override)
            if n >= _MIN_MEMORY_GB:
                log.info("FICHA_MEMORY_LIMIT_GB override: %d GB", n)
                return n
            log.warning(
                "FICHA_MEMORY_LIMIT_GB=%s below floor %d — ignoring", override, _MIN_MEMORY_GB
            )
        except ValueError:
            log.warning("FICHA_MEMORY_LIMIT_GB=%r not an int — ignoring", override)

    total = _total_ram_gb()
    if total is None:
        log.info("could not detect total RAM — defaulting memory_limit to 10 GB")
        return 10

    by_headroom = total - _OS_HEADROOM_GB
    by_fraction = int(total * _MEMORY_FRACTION)
    chosen = max(_MIN_MEMORY_GB, min(by_headroom, by_fraction))
    log.info(
        "auto memory_limit: total=%d GB → headroom=%d, fraction=%d → chose %d GB",
        total,
        by_headroom,
        by_fraction,
        chosen,
    )
    return chosen


def pick_threads() -> int:
    """Choose DuckDB thread count. Defaults to 1 per the spillability brake
    (see PRAGMA call site comment). Override with `FICHA_THREADS` env."""
    override = os.environ.get("FICHA_THREADS", "").strip()
    if override:
        try:
            n = int(override)
            if n >= 1:
                log.info("FICHA_THREADS override: %d", n)
                return n
        except ValueError:
            log.warning("FICHA_THREADS=%r not an int — ignoring", override)
    return 1


def load_main_tables_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
) -> None:
    """Carrega Empresa/Estabelecimento/Socio/Simples no DuckDB."""
    by_kind: dict[FileKind, list[Path]] = collections.defaultdict(list)
    for ef in extracted:
        by_kind[ef.kind].append(ef.csv_path)

    for spec in registry.MAIN_TABLES:
        t0 = time.monotonic()
        log.info("loading table '%s' from %d CSV(s)...", spec.name, len(by_kind.get(spec.kind, [])))
        _create_table_from_csvs(con, spec.name, by_kind.get(spec.kind, []), spec.source)
        n = con.execute(f"SELECT COUNT(*) FROM {spec.name}").fetchone()[0]
        log.info(
            "loaded '%s' — %s rows in %.1fs",
            spec.name,
            f"{n:,}",
            time.monotonic() - t0,
        )

    # W13.1a: verify 1:1 cardinality assumption for simples.
    # write_cnpjs_parquet does LEFT JOIN simples ON cnpj_basico — if simples
    # has multiple rows per cnpj_basico the join silently multiplies
    # estabelecimento rows. RFB's intent is 1 row per empresa but this has
    # never been empirically confirmed on a completed bootstrap run.
    dupes = con.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT cnpj_basico FROM simples GROUP BY cnpj_basico HAVING COUNT(*) > 1"
        ")"
    ).fetchone()[0]
    if dupes > 0:
        log.warning(
            "W13.1a: simples has %d cnpj_basico value(s) with multiple rows — "
            "LEFT JOIN in write_cnpjs_parquet may silently multiply rows. "
            "See docs/perf-plan-2026-05.md §13.1 for the fix.",
            dupes,
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


def write_lookup_parquets(con: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Escreve um parquet para cada lookup, para composição SQL.

    A expressão (codigo, descricao, descricao_normalizada) vem de
    `ficha_py.views.lookup_normalized` (ADR 0017/0019): compilada para SQL
    via Ibis e executada aqui com `COPY TO PARQUET`, porque o writer em si
    (compressão, row group size) fica em SQL bruto por design do ADR 0017 —
    Ibis não abstrai esses knobs, e não precisa: a query em si é o único
    vocabulário que ETL e notebooks precisam compartilhar.
    """
    import ibis
    from ficha_py.views import lookup_normalized

    (output_dir / "lookups").mkdir(parents=True, exist_ok=True)
    ibis_con = ibis.duckdb.from_connection(con)
    for kind in _LOOKUP_KINDS:
        select_sql = ibis.to_sql(lookup_normalized(ibis_con, kind), dialect="duckdb")
        con.execute(
            f"COPY ({select_sql}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)",
            [str(output_dir / "lookups" / f"{kind}.parquet")],
        )


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


def _cnpjs_chunk_select_sql(  # noqa: PLR0913
    est_alias: str,
    emp_alias: str,
    smp_alias: str,
    cnae_map_alias: str,
    *,
    order_by: bool = True,
) -> str:
    """Return the SELECT...FROM...JOIN... SQL for the cnpjs denormalized query.

    Takes table/alias names as parameters so it can be reused by both the
    bucket-partitioned write_cnpjs_parquet (which pre-filters into _est_b /
    _emp_b / _smp_b) and the new chunk-per-ZIP write_cnpjs_parquet_chunked
    (which loads one estabelecimento CSV at a time directly).

    The _cnae_map temp table must already exist in `con` before this SQL is
    executed. Pass order_by=False when a global sort at merge time makes the
    per-chunk ORDER BY redundant (avoids fragile rsplit string manipulation).
    """
    est = est_alias
    emp = emp_alias
    smp = smp_alias
    cm = cnae_map_alias
    return f"""
        SELECT
            {est}.cnpj_basico || {est}.cnpj_ordem || {est}.cnpj_dv AS cnpj,
            {est}.cnpj_basico AS cnpj_base,
            {est}.cnpj_ordem,
            {est}.cnpj_dv,
            {est}.identificador_matriz_filial,

            -- Empresa (denormalizado)
            {emp}.razao_social,
            UPPER(strip_accents({emp}.razao_social)) AS razao_social_normalizada,
            {emp}.natureza_juridica AS natureza_juridica_codigo,
            COALESCE(nj.descricao, '') AS natureza_juridica_descricao,
            {emp}.qualificacao_responsavel AS qualificacao_responsavel_codigo,
            COALESCE(qr.descricao, '') AS qualificacao_responsavel_descricao,
            TRY_CAST(REPLACE({emp}.capital_social, ',', '.') AS DOUBLE) AS capital_social,
            {emp}.porte_empresa,
            {emp}.ente_federativo_responsavel,

            -- Estabelecimento
            {est}.nome_fantasia,
            {est}.situacao_cadastral,
            CASE {est}.situacao_cadastral
                WHEN '01' THEN 'Nula'
                WHEN '02' THEN 'Ativa'
                WHEN '03' THEN 'Suspensa'
                WHEN '04' THEN 'Inapta'
                WHEN '08' THEN 'Baixada'
                ELSE ''
            END AS situacao_cadastral_descricao,
            {_date_expr(f"{est}.data_situacao_cadastral")} AS data_situacao_cadastral,
            {est}.motivo_situacao_cadastral AS motivo_situacao_cadastral_codigo,
            COALESCE(mt.descricao, '') AS motivo_situacao_cadastral_descricao,
            {_date_expr(f"{est}.data_inicio_atividade")} AS data_inicio_atividade,

            -- CNAE (principal + secundários)
            {est}.cnae_fiscal_principal AS cnae_principal_codigo,
            COALESCE(cn_p.descricao, '') AS cnae_principal_descricao,
            CASE WHEN {est}.cnae_fiscal_secundaria IS NULL OR {est}.cnae_fiscal_secundaria = ''
                 THEN []::VARCHAR[]
                 ELSE list_transform(str_split({est}.cnae_fiscal_secundaria, ','), x -> trim(x)) END
                AS cnae_secundario_codigos,
            CASE WHEN {est}.cnae_fiscal_secundaria IS NULL OR {est}.cnae_fiscal_secundaria = ''
                 THEN []::VARCHAR[]
                 ELSE list_transform(
                        list_transform(str_split({est}.cnae_fiscal_secundaria, ','), x -> trim(x)),
                        c -> COALESCE({cm}.m[c], '')
                      ) END
                AS cnae_secundario_descricoes,

            -- Endereço
            {est}.tipo_logradouro,
            {est}.logradouro,
            {est}.numero,
            {est}.complemento,
            {est}.bairro,
            {est}.cep,
            {est}.uf,
            {est}.municipio AS municipio_codigo,
            COALESCE(mn.descricao, '') AS municipio_nome,
            {est}.pais AS pais_codigo,
            COALESCE(ps.descricao, '') AS pais_nome,
            {est}.nome_cidade_exterior,

            -- Contato
            {est}.ddd_1, {est}.telefone_1, {est}.ddd_2, {est}.telefone_2,
            {est}.ddd_fax, {est}.fax, {est}.correio_eletronico,

            -- Estado especial
            {est}.situacao_especial,
            {_date_expr(f"{est}.data_situacao_especial")} AS data_situacao_especial,

            -- Simples / MEI (inline)
            CASE {smp}.opcao_simples WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                AS opcao_simples,
            {_date_expr(f"{smp}.data_opcao_simples")} AS data_opcao_simples,
            {_date_expr(f"{smp}.data_exclusao_simples")} AS data_exclusao_simples,
            CASE {smp}.opcao_mei WHEN 'S' THEN TRUE WHEN 'N' THEN FALSE ELSE NULL END
                AS opcao_mei,
            {_date_expr(f"{smp}.data_opcao_mei")} AS data_opcao_mei,
            {_date_expr(f"{smp}.data_exclusao_mei")} AS data_exclusao_mei

        FROM {est}
        CROSS JOIN {cm}
        LEFT JOIN {emp} ON {emp}.cnpj_basico = {est}.cnpj_basico
        LEFT JOIN {smp} ON {smp}.cnpj_basico = {est}.cnpj_basico
        LEFT JOIN lookup_naturezas nj ON nj.codigo = {emp}.natureza_juridica
        LEFT JOIN lookup_qualificacoes qr ON qr.codigo = {emp}.qualificacao_responsavel
        LEFT JOIN lookup_motivos mt ON mt.codigo = {est}.motivo_situacao_cadastral
        LEFT JOIN lookup_cnaes cn_p ON cn_p.codigo = {est}.cnae_fiscal_principal
        LEFT JOIN lookup_municipios mn ON mn.codigo = {est}.municipio
        LEFT JOIN lookup_paises ps ON ps.codigo = {est}.pais
        {"ORDER BY cnpj" if order_by else ""}
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
    # Pre-build a MAP scalar (codigo -> descricao) from lookup_cnaes so the
    # secondary-CNAE descriptions can be resolved order-preservingly inside
    # the SELECT. A correlated JOIN-per-element via list_transform doesn't
    # work because list_transform's lambda can't reference outer tables;
    # an unnest+JOIN+re-aggregate would lose registration order across
    # GROUP BY. Indexing into a precomputed MAP keeps the original
    # str_split order intact. The MAP is small (~1300 cnaes × short
    # descricao ≈ <100 KB) so DuckDB can inline it as a constant.
    # See docs/perf-plan-2026-05.md §9.3.
    #
    # GROUP BY codigo + ANY_VALUE(descricao) defends against duplicate
    # codigos in lookup_cnaes — DuckDB's MAP() throws on duplicate keys
    # ("Map keys must be unique"), which would crash the entire
    # write_cnpjs_parquet step on a single dirty row. The RFB CNAE
    # reference table is small (~1300 rows) and historically clean,
    # but this guards against future drift. Per Kilo PR #28 review.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _cnae_map AS
        SELECT MAP(list(codigo), list(descricao)) AS m FROM (
            SELECT codigo, ANY_VALUE(descricao) AS descricao
            FROM lookup_cnaes
            GROUP BY codigo
        )
        """
    )
    # W1.3 (docs/perf-plan-2026-05.md §1.3): partition the join by
    # `LEFT(cnpj_basico, 1)` digit. The first attempt (run 25859177161)
    # added `WHERE LEFT(est.cnpj_basico, 1) = '0'` to the COPY's main
    # SELECT, but DuckDB's optimizer didn't propagate the predicate
    # across the LEFT JOINs to `empresa` and `simples` — bucket 0 still
    # hashed the full 67M-row empresa and 48M-row simples, hitting the
    # same 95 GiB temp-spill exhaustion.
    #
    # Defensive fix: materialize per-bucket temp tables for *all three*
    # large inputs (estabelecimento, empresa, simples) BEFORE the COPY,
    # so the join's hash inputs are pre-shrunk by ~10×. The COPY then
    # selects from `_est_b` / `_emp_b` / `_smp_b` instead of the full
    # tables. Each bucket's hash-table working set drops to ~700 MB →
    # spill comfortably under the runner's free disk.
    #
    # Each bucket writes to `cnpjs.parquet.parts/part-X.parquet`, then
    # a streaming parquet-to-parquet COPY merges them (no aggregation,
    # no sort → DuckDB row-group-streams it without measurable spill).
    parts_dir = output_path.parent / f"{output_path.stem}.parts"
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)
    try:
        for _bucket in "0123456789":
            _part_path = parts_dir / f"part-{_bucket}.parquet"
            log.info("    writing cnpjs bucket %s/10 → %s", _bucket, _part_path.name)
            # Pre-filter the three big inputs into per-bucket temps so
            # the COPY's joins hash ~1/10 of each table instead of the
            # full ~67M-row empresa / ~48M-row simples (which is what
            # the predicate-in-WHERE attempt failed to achieve — see
            # the W1.3 block above).
            for _tbl, _src in (
                ("_est_b", "estabelecimento"),
                ("_emp_b", "empresa"),
                ("_smp_b", "simples"),
            ):
                con.execute(
                    f"CREATE OR REPLACE TEMP TABLE {_tbl} AS "
                    f"SELECT * FROM {_src} WHERE LEFT(cnpj_basico, 1) = '{_bucket}'"
                )
            # Use the shared helper for the SELECT logic; bucket tables
            # _est_b / _emp_b / _smp_b are the aliases used here.
            _select_sql = _cnpjs_chunk_select_sql("_est_b", "_emp_b", "_smp_b", "_cnae_map")
            con.execute(
                f"""
            COPY (
                {_select_sql}
            ) TO '{_part_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
            """
            )
            # Drop the bucket temps so their working set + on-disk
            # storage is released before the next iteration starts.
            con.execute("DROP TABLE IF EXISTS _est_b")
            con.execute("DROP TABLE IF EXISTS _emp_b")
            con.execute("DROP TABLE IF EXISTS _smp_b")

        # Stream-concat — parquet-to-parquet without aggregation/sort is
        # row-group streaming in DuckDB; peak memory is just one row
        # group's worth, not the full table. Paths inlined for the same
        # DuckDB 1.5.2 quirk noted on the partition loop above.
        log.info("    merging %d bucket parts → %s", 10, output_path.name)
        _parts_glob = parts_dir / "part-*.parquet"
        con.execute(
            f"""
          COPY (SELECT * FROM read_parquet('{_parts_glob}'))
          TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
          """
        )
        shutil.rmtree(parts_dir)
    finally:
        # try/finally guarantees _cnae_map is cleaned up even if COPY
        # raises mid-write — important because the same con is reused
        # across write_raizes_parquet / write_socios_parquet, and a
        # lingering _cnae_map could shadow a future re-run of this
        # function. Per Kilo PR #28 review.
        con.execute("DROP TABLE IF EXISTS _cnae_map")


def write_cnpjs_parquet_chunked(
    con: duckdb.DuckDBPyConnection,
    estabelecimento_csv_paths: list[Path],
    output_path: Path,
) -> None:
    """Write cnpjs.parquet by loading one estabelecimento CSV at a time.

    Pre-condition: empresa, simples, all lookups already in con.
    Each chunk: load CSV → pre-filter empresa/simples → JOIN → write chunk
    parquet → DROP. Final merge: read_parquet(all chunks) ORDER BY cnpj →
    output_path.
    Peak RAM: ~5 GB vs ~70 GB for full-load approach.

    Each chunk pre-filters `empresa`/`simples` down to just the
    `cnpj_basico` values present in that chunk's `estabelecimento` table
    (Ibis `semi_join`, ADR 0017) before joining. Without this, each chunk's
    JOIN hashes the *full* unfiltered empresa (~70M rows) / simples (~50M
    rows) — the same failure mode `write_cnpjs_parquet`'s digit-bucket
    pre-filter exists to avoid (see the W1.3 comment above it: "bucket 0
    still hashed the full 67M-row empresa... hitting the same 95 GiB
    temp-spill exhaustion"). This chunked writer never got that fix; it hit
    the identical OOM at production scale (run 29661697810: 73.9/73.9 GiB
    temp-spill on chunk 0/10) since prod's RFB data volume finally made a
    single unfiltered chunk-join expensive enough to blow the disk. A
    semi-join (not the bucket digit-match) is used here because chunks are
    RFB's own arbitrary ZIP split, not aligned to any `cnpj_basico` prefix.
    """
    import ibis

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Build _cnae_map once — same logic as write_cnpjs_parquet.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _cnae_map AS
        SELECT MAP(list(codigo), list(descricao)) AS m FROM (
            SELECT codigo, ANY_VALUE(descricao) AS descricao
            FROM lookup_cnaes
            GROUP BY codigo
        )
        """
    )
    icon = ibis.duckdb.from_connection(con)

    def _materialize(table: str, expr) -> None:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
        )

    parts_dir = output_path.parent / f"{output_path.stem}.parts"
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)
    try:
        written_parts: list[Path] = []
        for i, csv_path in enumerate(estabelecimento_csv_paths):
            # Skip empty files — _create_table_from_csvs filters them too,
            # but checking here avoids the table CREATE/DROP overhead.
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                log.info("    chunk %d: skipping empty CSV %s", i, csv_path.name)
                continue

            log.info(
                "    chunk %d/%d: loading %s", i, len(estabelecimento_csv_paths), csv_path.name
            )
            _create_table_from_csvs(
                con, "estabelecimento", [csv_path], registry.main_table("estabelecimento").source
            )

            n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
            if n == 0:
                log.info("    chunk %d: 0 rows — skipping", i)
                con.execute("DROP TABLE IF EXISTS estabelecimento")
                continue

            # Pre-filter empresa/simples to this chunk's cnpj_basico values
            # (semi-join: left table's own columns, no fan-out, one row per
            # match) BEFORE the big projection JOIN below.
            est = icon.table("estabelecimento")
            _materialize("_emp_c", icon.table("empresa").semi_join(est, "cnpj_basico"))
            _materialize("_smp_c", icon.table("simples").semi_join(est, "cnpj_basico"))

            part_path = parts_dir / f"chunk-{i}.parquet"
            # order_by=False: the global merge step sorts by cnpj, so
            # per-chunk ORDER BY is unnecessary and wasteful.
            _select_sql = _cnpjs_chunk_select_sql(
                "estabelecimento", "_emp_c", "_smp_c", "_cnae_map", order_by=False
            )
            con.execute(
                f"""
                COPY (
                    {_select_sql}
                ) TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
                """
            )
            log.info("    chunk %d: wrote %s", i, part_path.name)
            written_parts.append(part_path)

            con.execute("DROP TABLE IF EXISTS estabelecimento")
            con.execute("DROP TABLE IF EXISTS _emp_c")
            con.execute("DROP TABLE IF EXISTS _smp_c")

        if not written_parts:
            # No chunks written — produce an empty parquet with the right schema.
            # This shouldn't happen in production but is a safe fallback.
            # Use write_cnpjs_parquet with zero rows rather than read_parquet([])
            # which crashes DuckDB (cannot infer schema from empty list).
            log.warning("write_cnpjs_parquet_chunked: no chunks written; output will be empty")
            write_cnpjs_parquet(con, output_path)
            return

        # Merge all chunk parquets → final output, sorted globally by cnpj.
        log.info(
            "    merging %d chunk parts → %s (ORDER BY cnpj)", len(written_parts), output_path.name
        )
        _parts_glob = parts_dir / "chunk-*.parquet"
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{_parts_glob}') ORDER BY cnpj
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
            """
        )
        shutil.rmtree(parts_dir)
    finally:
        con.execute("DROP TABLE IF EXISTS _cnae_map")
        con.execute("DROP TABLE IF EXISTS estabelecimento")
        con.execute("DROP TABLE IF EXISTS _emp_c")
        con.execute("DROP TABLE IF EXISTS _smp_c")
        # Clean up parts dir on error (success path already removed it above).
        shutil.rmtree(parts_dir, ignore_errors=True)


def write_raizes_parquet_from_cnpjs(
    con: duckdb.DuckDBPyConnection,
    cnpjs_path: Path,
    output_path: Path,
) -> None:
    """Compute raizes.parquet from cnpjs.parquet without empresa/estabelecimento in DuckDB.

    All fields needed for raizes are available in the already-written cnpjs.parquet:
    - Company fields (razao_social, natureza_juridica, etc.) via ANY_VALUE per cnpj_base
    - Counts (qtd_estabelecimentos, qtd_estabelecimentos_ativos) via COUNT
    - ufs_atuacao / cnaes_principais_distintos via two-step pre-dedup (W1.1 pattern)
    - Matriz fields via QUALIFY ROW_NUMBER() OVER (PARTITION BY cnpj_base ORDER BY cnpj) = 1
      WHERE identificador_matriz_filial = '1'

    Output schema matches write_raizes_parquet exactly.

    Queries em Ibis (ADR 0017), compiladas para SQL DuckDB — mesmo padrão de
    `write_socios_parquet`/`write_lookup_parquets`. As FRONTEIRAS de
    materialização (6 TEMP TABLEs na mesma ordem) são preservadas de propósito:
    o perf-plan §1.1 mostra que a forma exata da execução — pre-dedup de dois
    passos, materialização em etapas — é o que segura o OOM histórico do raizes.
    Trocar isso por uma expressão Ibis única (um só SELECT) deixaria o DuckDB
    decidir a materialização e poderia regredir a memória; por isso cada temp
    table é uma expressão Ibis própria, materializada aqui. As listas distintas
    usam `.distinct().collect()` (compila para o two-step seguro), NUNCA
    `collect(distinct=True)` (ver docs/ibis-raizes-benchmark-2026-07-18.md).
    Equivalência bit-a-bit coberta por test_write_raizes_from_cnpjs_matches_original.
    """
    import ibis
    from ibis import _

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cnpjs_glob = str(cnpjs_path)

    # _cnpjs_slim continua em SQL bruto: é I/O (read_parquet + projeção), não
    # vocabulário analítico — mesmo princípio que mantém read_csv/COPY raw.
    # Materialize once — reading cnpjs.parquet multiple times via CTEs causes
    # repeated scans of a potentially large file; temp tables are read once.
    log.info("    materializing _cnpjs_slim from cnpjs.parquet...")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _cnpjs_slim AS
        SELECT
            cnpj,
            cnpj_base,
            identificador_matriz_filial,
            situacao_cadastral,
            data_inicio_atividade,
            cnae_principal_codigo,
            uf,
            municipio_codigo,
            municipio_nome,
            cnae_principal_descricao,
            razao_social,
            razao_social_normalizada,
            natureza_juridica_codigo,
            natureza_juridica_descricao,
            capital_social,
            porte_empresa,
            ente_federativo_responsavel
        FROM read_parquet('{cnpjs_glob}')
        """
    )

    icon = ibis.duckdb.from_connection(con)

    def _materialize(table: str, expr) -> None:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS {ibis.to_sql(expr, dialect='duckdb')}"
        )

    def _dedup(col: str):
        """1º passo do two-step W1.1: SELECT DISTINCT cnpj_base, <col> (não vazio).

        O 2º passo (`.collect().sort()`) roda no agg abaixo e compila para
        ARRAY_AGG + list_sort — a forma spillável de produção, NÃO
        ARRAY_AGG(DISTINCT) (ver docs/ibis-raizes-benchmark-2026-07-18.md).
        """
        slim = icon.table("_cnpjs_slim")
        return (
            slim.filter(slim[col].notnull() & (slim[col] != "")).select("cnpj_base", col).distinct()
        )

    # W1.1 pattern: two-step pre-dedup for list aggregates.
    log.info("    materializing _raizes_ufs / _raizes_cnaes pre-dedup tables...")
    _materialize("_raizes_ufs", _dedup("uf"))
    _materialize(
        "_raizes_ufs_agg",
        icon.table("_raizes_ufs").group_by("cnpj_base").agg(ufs_atuacao=_.uf.collect().sort()),
    )
    _materialize("_raizes_cnaes", _dedup("cnae_principal_codigo"))
    _materialize(
        "_raizes_cnaes_agg",
        icon.table("_raizes_cnaes")
        .group_by("cnpj_base")
        .agg(cnaes_principais_distintos=_.cnae_principal_codigo.collect().sort()),
    )

    log.info("    materializing _raizes_counts...")
    slim = icon.table("_cnpjs_slim")
    _materialize(
        "_raizes_counts",
        slim.group_by("cnpj_base").agg(
            qtd_estabelecimentos=_.count().cast("int32"),
            qtd_estabelecimentos_ativos=_.count(where=_.situacao_cadastral == "02").cast("int32"),
        ),
    )

    log.info("    materializing _raizes_empresa (company fields per cnpj_base)...")
    _empresa_fields = (
        "razao_social",
        "razao_social_normalizada",
        "natureza_juridica_codigo",
        "natureza_juridica_descricao",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    )
    _materialize(
        "_raizes_empresa",
        slim.group_by("cnpj_base").agg(**{f: slim[f].arbitrary() for f in _empresa_fields}),
    )

    log.info("    materializing _raizes_matriz...")
    # QUALIFY ROW_NUMBER() OVER (PARTITION BY cnpj_base ORDER BY cnpj) = 1 →
    # row_number do Ibis é 0-based, então filtramos == 0.
    _mf = slim.filter(slim.identificador_matriz_filial == "1")
    _rn = ibis.row_number().over(group_by=_mf.cnpj_base, order_by=_mf.cnpj)
    _materialize(
        "_raizes_matriz",
        _mf.mutate(_rn=_rn)
        .filter(_._rn == 0)
        .select(
            "cnpj_base",
            data_inicio_atividade_matriz=_.data_inicio_atividade,
            uf_matriz=_.uf,
            municipio_matriz_codigo=_.municipio_codigo,
            municipio_matriz_nome=_.municipio_nome,
            cnae_principal_matriz_codigo=_.cnae_principal_codigo,
            cnae_principal_matriz_descricao=_.cnae_principal_descricao,
        ),
    )

    # Drop the wide slim table now that all temp aggregates are built.
    con.execute("DROP TABLE IF EXISTS _cnpjs_slim")

    log.info("    joining + writing raizes.parquet from cnpjs...")
    emp = icon.table("_raizes_empresa")
    cnt = icon.table("_raizes_counts")
    ufs = icon.table("_raizes_ufs_agg")
    cnaes = icon.table("_raizes_cnaes_agg")
    mat = icon.table("_raizes_matriz")
    _empty = ibis.literal([], type="array<string>")
    raizes_expr = (
        emp.left_join(cnt, emp.cnpj_base == cnt.cnpj_base)
        .left_join(ufs, emp.cnpj_base == ufs.cnpj_base)
        .left_join(cnaes, emp.cnpj_base == cnaes.cnpj_base)
        .left_join(mat, emp.cnpj_base == mat.cnpj_base)
        .select(
            "cnpj_base",
            *_empresa_fields,
            qtd_estabelecimentos=cnt.qtd_estabelecimentos.coalesce(0),
            qtd_estabelecimentos_ativos=cnt.qtd_estabelecimentos_ativos.coalesce(0),
            ufs_atuacao=ufs.ufs_atuacao.coalesce(_empty),
            cnaes_principais_distintos=cnaes.cnaes_principais_distintos.coalesce(_empty),
            data_inicio_atividade_matriz=mat.data_inicio_atividade_matriz,
            uf_matriz=mat.uf_matriz,
            municipio_matriz_codigo=mat.municipio_matriz_codigo,
            municipio_matriz_nome=mat.municipio_matriz_nome,
            cnae_principal_matriz_codigo=mat.cnae_principal_matriz_codigo,
            cnae_principal_matriz_descricao=mat.cnae_principal_matriz_descricao,
        )
    )
    con.execute(
        f"COPY ({ibis.to_sql(raizes_expr, dialect='duckdb')}) "
        f"TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
    )

    # Free all temp tables.
    for tbl in (
        "_raizes_empresa",
        "_raizes_counts",
        "_raizes_ufs",
        "_raizes_ufs_agg",
        "_raizes_cnaes",
        "_raizes_cnaes_agg",
        "_raizes_matriz",
    ):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")


def write_cnpj_contatos_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `cnpj_contatos.parquet`: reverse contact lookup (telefones, fax, email).

    Privacy posture: phones and emails are PII, but RFB publishes them publicly already.
    This parquet is just a re-shape of public data with no enrichment, no new exposure.
    Ver docs/perf-plan-2026-05.md §12.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("    writing cnpj_contatos.parquet...")

    con.execute(
        """
        COPY (
          -- telefone_1
          SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
                 cnpj_basico AS cnpj_base,
                 'telefone' AS tipo,
                 ddd_1 || telefone_1 AS valor,
                 1::INTEGER AS posicao
          FROM estabelecimento
          WHERE telefone_1 IS NOT NULL AND telefone_1 <> ''
            AND ddd_1 IS NOT NULL AND ddd_1 <> ''
          UNION ALL
          -- telefone_2
          SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
                 cnpj_basico,
                 'telefone',
                 ddd_2 || telefone_2,
                 2::INTEGER
          FROM estabelecimento
          WHERE telefone_2 IS NOT NULL AND telefone_2 <> ''
            AND ddd_2 IS NOT NULL AND ddd_2 <> ''
          UNION ALL
          -- fax
          SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
                 cnpj_basico,
                 'fax',
                 ddd_fax || fax,
                 0::INTEGER
          FROM estabelecimento
          WHERE fax IS NOT NULL AND fax <> ''
            AND ddd_fax IS NOT NULL AND ddd_fax <> ''
          UNION ALL
          -- email
          SELECT cnpj_basico || cnpj_ordem || cnpj_dv,
                 cnpj_basico,
                 'email',
                 correio_eletronico,
                 0::INTEGER
          FROM estabelecimento
          WHERE correio_eletronico IS NOT NULL AND correio_eletronico <> ''
          ORDER BY tipo, valor, cnpj
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
        """,
        [str(output_path)],
    )


def write_cnpj_cnaes_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `cnpj_cnaes.parquet`: tabela associativa para buscas reversas por CNAE."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("writing %s", output_path.name)

    con.execute(f"""
        COPY (
          SELECT
            cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
            cnpj_basico AS cnpj_base,
            cnae_fiscal_principal AS cnae_codigo,
            0::INTEGER AS posicao
          FROM estabelecimento
          WHERE cnae_fiscal_principal IS NOT NULL
            AND cnae_fiscal_principal <> ''
          UNION ALL
          SELECT
            cnpj_basico || cnpj_ordem || cnpj_dv,
            cnpj_basico,
            trim(s.value) AS cnae_codigo,
            s.idx::INTEGER AS posicao
          FROM estabelecimento,
               LATERAL (
                 SELECT idx, unnest AS value
                 FROM (
                   SELECT generate_subscripts(arr, 1) AS idx, unnest(arr) AS unnest
                   FROM (SELECT str_split(cnae_fiscal_secundaria, ',') AS arr) t
                 )
               ) s
          WHERE cnae_fiscal_secundaria IS NOT NULL
            AND cnae_fiscal_secundaria <> ''
          ORDER BY cnae_codigo, posicao, cnpj_base
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
    """)


# Top-10 logradouro abbreviations per perf-plan §7.2 (covers ≥90% of variation).
_LOGRADOURO_ABBREVS: dict[str, str] = {
    "R": "RUA",
    "AV": "AVENIDA",
    "TV": "TRAVESSA",
    "AL": "ALAMEDA",
    "PCA": "PRACA",
    "PC": "PRACA",
    "EST": "ESTRADA",
    "ROD": "RODOVIA",
    "VL": "VILA",
    "LG": "LARGO",
}


def write_enderecos_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `enderecos.parquet`: reverse lookup por endereço e município.

    Ordenado por (uf, municipio_codigo, logradouro_normalizado, numero) para que
    buscas por UF+município e logradouro usem min/max row-group pruning.
    Ver docs/perf-plan-2026-05.md §7 e ADR 0023.

    Normalização vetorizada: CTE computa a base normalizada uma vez por linha
    (UPPER + strip_accents + TRIM + whitespace collapse); depois uma única
    extração de prefixo + MAP lookup substitui as 10 chamadas regexp_replace
    anteriores, passando de 11 para 4 operações regex/map por linha.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("    writing enderecos.parquet...")
    # Build MAP literal: MAP {'R': 'RUA ', 'AV': 'AVENIDA ', ...}
    # Trailing space in values avoids re-adding a separator on concatenation.
    abbrev_map = "MAP {" + ", ".join(f"'{k}': '{v} '" for k, v in _LOGRADOURO_ABBREVS.items()) + "}"
    con.execute(
        rf"""
        COPY (
            WITH _base AS (
                SELECT
                    est.uf,
                    est.municipio AS municipio_codigo,
                    UPPER(strip_accents(TRIM(
                        regexp_replace(est.logradouro, '\s+', ' ', 'g')
                    ))) AS _logr,
                    est.numero,
                    est.cep,
                    est.bairro,
                    est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj
                FROM estabelecimento est
                WHERE est.logradouro IS NOT NULL AND est.logradouro <> ''
                  AND est.uf IS NOT NULL AND est.uf <> ''
            )
            SELECT
                uf,
                municipio_codigo,
                COALESCE(
                    {abbrev_map}[regexp_extract(_logr, '^([A-Z]+)\.?\s+', 1)]
                    || regexp_replace(_logr, '^[A-Z]+\.?\s+', ''),
                    _logr
                ) AS logradouro_normalizado,
                numero,
                cep,
                bairro,
                cnpj
            FROM _base
            ORDER BY uf, municipio_codigo, logradouro_normalizado, TRY_CAST(numero AS INTEGER), numero
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
        """
    )


def write_pessoas_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `pessoas.parquet`: reverse lookup PF por CPF mascarado + nome.

    Grain: (cpf_mascarado, nome_normalizado, faixa_etaria, cnpj_base, papel) — uma
    linha por vínculo pessoa×empresa×papel. A mesma pessoa aparece N vezes se for
    sócia em N empresas; o sort por (cpf_mascarado, nome_normalizado) agrupa
    todas as linhas de uma pessoa para leitura eficiente.

    faixa_etaria é atributo da pessoa (não do vínculo) e serve para desambiguar
    homônimos com o mesmo CPF mascarado e nome. NULL para representantes (a RFB
    não publica esse campo em representante_legal_*).

    data_entrada_sociedade é do vínculo e permanece em socios.parquet.

    Ver ADR 0024.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("    writing pessoas.parquet...")
    con.execute(
        """
        COPY (
            -- Sócios PF brasileiros: identificador_socio = '2'
            SELECT
                soc.cnpj_cpf_socio                                      AS cpf_mascarado,
                UPPER(strip_accents(TRIM(soc.nome_socio_razao_social)))  AS nome_normalizado,
                soc.nome_socio_razao_social                             AS nome_original,
                'socio_pf'                                              AS papel,
                soc.cnpj_basico                                         AS cnpj_base,
                soc.qualificacao_socio                                  AS qualificacao_codigo,
                soc.faixa_etaria
            FROM socio soc
            WHERE soc.identificador_socio = '2'
              AND soc.cnpj_cpf_socio IS NOT NULL AND soc.cnpj_cpf_socio <> ''
            UNION ALL
            -- Representantes legais: embutidos como colunas em qualquer linha de socio.
            -- DISTINCT por (cnpj_basico, representante_legal) porque o mesmo representante
            -- pode assinar múltiplos registros da mesma empresa.
            -- faixa_etaria NULL: a RFB não publica esse campo para representante_legal_*.
            SELECT DISTINCT
                soc.representante_legal                                 AS cpf_mascarado,
                UPPER(strip_accents(TRIM(soc.nome_representante_legal))) AS nome_normalizado,
                soc.nome_representante_legal                            AS nome_original,
                'representante'                                         AS papel,
                soc.cnpj_basico                                         AS cnpj_base,
                soc.qualificacao_representante_legal                    AS qualificacao_codigo,
                NULL                                                    AS faixa_etaria
            FROM socio soc
            WHERE soc.representante_legal IS NOT NULL AND soc.representante_legal <> ''
            ORDER BY cpf_mascarado, nome_normalizado
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

    Nota: o pipeline principal (transform_snapshot) usa write_raizes_parquet_from_cnpjs,
    que deriva raizes.parquet do cnpjs.parquet já escrito sem precisar das tabelas brutas.
    Esta função permanece para uso standalone e testes de referência.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # W1.6 (docs/perf-plan-2026-05.md §1.6): project estabelecimento down
    # to only the columns raizes needs *before* the aggregations run.
    # The wide table has 29 VARCHAR columns; raizes only ever reads 8 of
    # them. DuckDB's column store + projection-pushdown already reads the
    # narrow set per-scan, but each scan re-pulls from the underlying
    # column store and competes for buffer-pool slots with the other
    # columns kept warm by `cnpjs.parquet`'s upstream write. Materializing
    # the slim projection once reduces that pressure by ~3.5× during the
    # raizes phase. Released after the temp aggregates are built.
    log.info("    materializing estabelecimento_slim (8 cols of 29)...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _estabelecimento_slim AS
        SELECT
            cnpj_basico,
            cnpj_ordem,
            identificador_matriz_filial,
            situacao_cadastral,
            data_inicio_atividade,
            cnae_fiscal_principal,
            uf,
            municipio
        FROM estabelecimento
    """)

    # W1.1 (docs/perf-plan-2026-05.md §1.1): pre-dedup before list().
    # The previous shape used LIST(DISTINCT est.uf) and
    # LIST(DISTINCT est.cnae_fiscal_principal) inside _raizes_agg, but
    # DuckDB's hash-aggregate cannot spill the per-group hash-set state
    # that DISTINCT-inside-LIST builds — with ~50M cnpj_basico groups
    # and a few distinct UFs each, the in-memory state grew unbounded
    # and OOM'd at 5.5 GiB (PR #24, run 25522678418), even after PR #24
    # split the aggregation into a temp table.
    #
    # Replacing with two-step pre-dedup (SELECT DISTINCT → flat list())
    # uses regular GROUP BYs that DuckDB *can* spill cleanly. Each step
    # is a vanilla aggregate; peak memory drops to ~2 GiB for the
    # 60M-row → 50M-group reduction. See plan §1.1 for the full
    # rationale and an explanation of why splitting alone wasn't
    # sufficient.
    log.info("    materializing _ufs / _cnaes pre-dedup tables...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ufs AS
        SELECT DISTINCT cnpj_basico, uf FROM _estabelecimento_slim
        WHERE uf IS NOT NULL AND uf <> ''
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ufs_agg AS
        SELECT cnpj_basico, list(uf) AS ufs_atuacao
        FROM _ufs GROUP BY cnpj_basico
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _cnaes_principais AS
        SELECT DISTINCT cnpj_basico, cnae_fiscal_principal FROM _estabelecimento_slim
        WHERE cnae_fiscal_principal IS NOT NULL AND cnae_fiscal_principal <> ''
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _cnaes_principais_agg AS
        SELECT cnpj_basico, list(cnae_fiscal_principal) AS cnaes_principais_distintos
        FROM _cnaes_principais GROUP BY cnpj_basico
    """)

    log.info("    materializing raizes_agg (counts only — list aggs split off)...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _raizes_agg AS
        SELECT
            est.cnpj_basico AS cnpj_base,
            COUNT(*)::INTEGER AS qtd_estabelecimentos,
            COUNT(*) FILTER (WHERE est.situacao_cadastral = '02')::INTEGER
                AS qtd_estabelecimentos_ativos
        FROM _estabelecimento_slim est
        GROUP BY est.cnpj_basico
    """)
    log.info("    materializing raizes_matriz...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _raizes_matriz AS
        SELECT
            est.cnpj_basico,
            {_date_expr("est.data_inicio_atividade")} AS data_inicio_atividade_matriz,
            est.uf AS uf_matriz,
            est.municipio AS municipio_matriz_codigo,
            est.cnae_fiscal_principal AS cnae_principal_matriz_codigo
        FROM _estabelecimento_slim est
        WHERE est.identificador_matriz_filial = '1'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY est.cnpj_basico ORDER BY est.cnpj_ordem
        ) = 1
    """)
    # Slim projection no longer needed once both aggregates are built.
    con.execute("DROP TABLE IF EXISTS _estabelecimento_slim")

    log.info("    joining + writing raizes.parquet...")
    con.execute(
        f"""
        COPY (
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
                COALESCE(ufs.ufs_atuacao, []) AS ufs_atuacao,
                COALESCE(cnae_agg.cnaes_principais_distintos, []) AS cnaes_principais_distintos,
                matriz.data_inicio_atividade_matriz,
                matriz.uf_matriz,
                matriz.municipio_matriz_codigo,
                COALESCE(mn.descricao, '') AS municipio_matriz_nome,
                matriz.cnae_principal_matriz_codigo,
                COALESCE(cn.descricao, '') AS cnae_principal_matriz_descricao
            FROM empresa emp
            LEFT JOIN _raizes_agg agg ON agg.cnpj_base = emp.cnpj_basico
            LEFT JOIN _ufs_agg ufs ON ufs.cnpj_basico = emp.cnpj_basico
            LEFT JOIN _cnaes_principais_agg cnae_agg ON cnae_agg.cnpj_basico = emp.cnpj_basico
            LEFT JOIN _raizes_matriz matriz ON matriz.cnpj_basico = emp.cnpj_basico
            LEFT JOIN lookup_naturezas nj ON nj.codigo = emp.natureza_juridica
            LEFT JOIN lookup_municipios mn ON mn.codigo = matriz.municipio_matriz_codigo
            LEFT JOIN lookup_cnaes cn ON cn.codigo = matriz.cnae_principal_matriz_codigo
            -- ORDER BY razao_social_normalizada omitted: forced sort over
            -- ~50M rows alongside the LIST_DISTINCT aggs in the `agg`
            -- CTE OOM'd raizes write (PR #24, run 25520136856 hit
            -- 4.6 GiB / 4.6 GiB memory cap right after cnpjs.parquet
            -- finished). Autocomplete on razao_social can still scan
            -- via row-group bloom on cnpj_base + a sequential filter;
            -- range queries on the normalized name lose pruning, but
            -- typing-prefix queries (the dominant autocomplete pattern)
            -- still work via early-stop. Revisit after bootstrap lands.
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
        """,
        [str(output_path)],
    )
    # Free the temp tables now that raizes.parquet is on disk.
    for tbl in (
        "_raizes_agg",
        "_raizes_matriz",
        "_ufs",
        "_ufs_agg",
        "_cnaes_principais",
        "_cnaes_principais_agg",
    ):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")


def _socios_select_sql(con: duckdb.DuckDBPyConnection) -> str:
    """Compila o SELECT de `socios.parquet` a partir de uma expressão Ibis.

    A tabela `socio` (bruta RFB) + `lookup_qualificacoes`/`lookup_paises` viram
    a expressão denormalizada de `socios.parquet` via Ibis, compilada para SQL
    DuckDB. Mesmo padrão de `write_lookup_parquets` (ADR 0017): a *query* é Ibis,
    o *writer* (`COPY ... PARQUET` com compressão/row-group) fica em SQL bruto.

    A expressão é ETL-local de propósito: ela lê o schema RFB bruto (`socio`),
    que `ficha-py` deliberadamente não conhece — `ficha-py` só fala o shape
    *publicado* (`socios`). Ver o comentário de fronteira em `ficha_py.tables`.
    Equivalência bit-a-bit com o SQL manual anterior verificada em fixtures com
    casos de borda (datas vazias/'0', códigos de lookup ausentes, PF/PJ/
    estrangeiro).
    """
    import ibis

    icon = ibis.duckdb.from_connection(con)
    soc = icon.table("socio")
    qual = icon.table("lookup_qualificacoes")
    pais = icon.table("lookup_paises")
    # Renomeia as colunas dos lookups pra evitar colisão de nome nos LEFT JOINs
    # (qualificacoes entra duas vezes: sócio e representante legal).
    qs = qual.rename(qs_codigo="codigo", qs_descricao="descricao")
    qr = qual.rename(qr_codigo="codigo", qr_descricao="descricao")
    ps = pais.rename(ps_codigo="codigo", ps_descricao="descricao")

    def _date(col):  # YYYYMMDD → YYYY-MM-DD; vazio/'0'/NULL → NULL. Igual a _date_expr.
        return ibis.cases(
            (col.isnull() | (col == "") | (col == "0"), ibis.null("string")),
            else_=col.substr(0, 4) + "-" + col.substr(4, 2) + "-" + col.substr(6, 2),
        )

    ist = soc.identificador_socio
    joined = (
        soc.left_join(qs, soc.qualificacao_socio == qs.qs_codigo)
        .left_join(qr, soc.qualificacao_representante_legal == qr.qr_codigo)
        .left_join(ps, soc.pais == ps.ps_codigo)
    )
    expr = joined.select(
        cnpj_base=soc.cnpj_basico,
        tipo=ist,
        tipo_descricao=ibis.cases(
            (ist == "1", "PJ"), (ist == "2", "PF"), (ist == "3", "estrangeiro"), else_=""
        ),
        nome_socio_razao_social=soc.nome_socio_razao_social,
        cpf_mascarado=ibis.cases((ist == "2", soc.cnpj_cpf_socio), else_=ibis.null("string")),
        cnpj_socio=ibis.cases((ist == "1", soc.cnpj_cpf_socio), else_=ibis.null("string")),
        qualificacao_codigo=soc.qualificacao_socio,
        qualificacao_descricao=qs.qs_descricao.coalesce(""),
        data_entrada_sociedade=_date(soc.data_entrada_sociedade),
        pais_codigo=soc.pais,
        pais_nome=ps.ps_descricao.coalesce(""),
        representante_legal_cpf=soc.representante_legal,
        representante_legal_nome=soc.nome_representante_legal,
        representante_legal_qualificacao_codigo=soc.qualificacao_representante_legal,
        representante_legal_qualificacao_descricao=qr.qr_descricao.coalesce(""),
        faixa_etaria=soc.faixa_etaria,
    )
    return ibis.to_sql(expr, dialect="duckdb")


def write_socios_parquet(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    """Produz `socios.parquet`: PF + PJ + estrangeiro com flag tipo.

    Requer que `load_main_tables_into_duckdb` e os lookups já estejam carregados em `con`.
    Ver ADR 0008 e schema `web/src/schemas/v1/socio.ts`.

    A query vem de `_socios_select_sql` (Ibis → SQL, ADR 0017); o writer fica
    em SQL bruto. ORDER BY cnpj_base é omitido de propósito: o bloom filter em
    cnpj_base resolve "sócios de X" independente da ordem física, e ordenar
    aqui reintroduz o mesmo padrão de OOM que atingiu cnpjs/raizes na PR #24.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({_socios_select_sql(con)}) "
        "TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)",
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
    progress: Progress | None = None,
) -> None:
    """Orquestrador: resolve → extract → load → write outputs.

    Se `verify=True`, roda `assert_roundtrip` após escrever `cnpjs.parquet`
    como gate de qualidade (ADR 0009): falha se campos amostrados do
    Parquet não baterem com os dados originais já carregados no DuckDB.

    `progress`: barra de progresso já iniciada (rich) pra reusar entre
    `_cmd_run`'s outer stages e as 4 fases internas; se omitido, cria (e
    fecha) a sua própria.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    chain = chain or fetcher_mod.default_chain(month, cache_dir=cache_dir)
    extract_dir = cache_dir / month / "extracted"

    owns_progress = progress is None
    progress = progress or make_progress()
    if owns_progress:
        progress.start()
    # 3 macro checkpoints, independent of the internal "PHASE X/4" log
    # labels below (phase 4 there is a verify sub-step nested inside 3,
    # not a fourth top-level stage).
    phase_task = progress.add_task("transform: extract", total=3)

    t_total = time.monotonic()
    log.info("=== PHASE 1/4: extract 37 ZIPs for %s ===", month)
    t0 = time.monotonic()
    extracted = extract_all(month, chain, extract_dir, progress=progress)
    log.info("=== PHASE 1/4 done in %.0fs ===", time.monotonic() - t0)
    progress.update(phase_task, description="transform: load into DuckDB", advance=1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Usa arquivo temporário em vez de in-memory para suportar o dataset real
    # da RFB (~60 M linhas) sem estourar RAM.
    db_path = cache_dir / month / "transform.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("=== PHASE 2/4: load into DuckDB (%s) ===", db_path)
    t0 = time.monotonic()
    con = duckdb.connect(str(db_path))
    # Cap memory and force on-disk spill. Sized at runtime via
    # `pick_memory_limit_gb()` — reads /proc/meminfo, reserves
    # _OS_HEADROOM_GB for OS + runner agent + Python heap + tee buffer
    # + DuckDB brief overshoots, and never exceeds _MEMORY_FRACTION of
    # total. On the 16 GB GH free-tier runner this picks ~10 GB (was
    # hard-coded to that until 2026-05). Self-hosted bigger boxes auto
    # scale up; override via FICHA_MEMORY_LIMIT_GB env. Explicit limit +
    # dedicated temp dir on the same partition as db_path makes spill
    # behavior predictable. Original 6 GB tuning was for legacy 7 GB
    # tier (PR #24, run 25514278003); per Kilo Code Review on PR #27,
    # 12 GB on a 16 GB runner trimmed the safety margin too thin.
    _mem_gb = pick_memory_limit_gb()
    con.execute(f"PRAGMA memory_limit='{_mem_gb}GB'")
    con.execute(f"PRAGMA temp_directory='{db_path.parent / 'duckdb_tmp'}'")
    # Reduce per-query memory pressure during the big JOIN at phase 3.
    # DuckDB's default preserves input ordering, which buffers more in
    # memory; we sort by `cnpj` at write time anyway, so insertion order
    # of intermediates doesn't matter. Saves ~30% on temp spill size.
    con.execute("PRAGMA preserve_insertion_order=false")
    # Reduce parallelism. Each thread holds its own working set during
    # the 70M x 67M VARCHAR-keyed hash join in write_cnpjs_parquet --
    # 4 threads (default) blew through 70 GB of temp spill (run
    # 25518175202). Cutting to 1 sacrifices wall time for peak memory:
    # cnpjs took 32 min with threads=2 in run 25522678418, so threads=1
    # bumps that to ~60 min, fitting in the 350 min runner budget.
    # threads=1 also helps raizes' LIST_DISTINCT(LIST(...)) GROUP BY
    # avoid the 5.5 GB OOM that bit run 25522678418.
    _threads = pick_threads()
    con.execute(f"PRAGMA threads={_threads}")
    try:
        # Lookups primeiro (necessárias pros JOINs dos parquets)
        for ef in extracted:
            if ef.kind in _LOOKUP_KINDS:
                load_lookup_into_duckdb(con, ef.kind, ef.csv_path)
                log.info("  lookup '%s' loaded", ef.kind)

        # Tabelas grandes
        load_main_tables_into_duckdb(con, extracted)
        log.info("=== PHASE 2/4 done in %.0fs ===", time.monotonic() - t0)
        progress.update(phase_task, description="transform: write parquets", advance=1)

        # Collect estabelecimento CSV paths for the chunked cnpjs write.
        # These must be gathered before any CSV deletion below.
        estabelecimento_csv_paths = [
            ef.csv_path for ef in extracted if ef.kind == "estabelecimentos"
        ]

        # Reclaim non-estabelecimento CSV disk before phase 3. Extracted CSVs
        # are loaded into transform.duckdb; keeping them alongside DuckDB's temp
        # spill exhausts the runner's ~70 GiB filesystem (PR #24, run 25517197692:
        # OOM "70.8 GiB/70.8 GiB used" while writing cnpjs.parquet).
        #
        # Chunk-per-ZIP pivot: estabelecimento CSVs are kept on disk so that
        # write_cnpjs_parquet_chunked can load them one at a time. Only
        # non-estabelecimento dirs (empresa, socios, simples, lookups) are
        # removed here. The estabelecimentos/ subdir is cleaned up after
        # write_cnpjs_parquet_chunked completes.
        #
        # Raw ZIP cleanup is opt-in via env var: the bootstrap workflow
        # (one-shot, space-constrained) sets FICHA_DROP_ZIPS_AFTER_LOAD=1 to
        # claim ~7 GB of additional spill headroom, while the monthly cron
        # leaves it unset so its `actions/cache` step finds the ZIPs at
        # post-job and persists them for the next run's cache hit.
        import os
        import shutil

        if extract_dir.exists():
            _kept_size = 0.0
            _freed_size = 0.0
            for _subdir in extract_dir.iterdir():
                if _subdir.is_dir() and _subdir.name == "estabelecimentos":
                    _kept_size += sum(
                        p.stat().st_size for p in _subdir.rglob("*") if p.is_file()
                    ) / (1024**3)
                    continue
                if _subdir.is_dir():
                    _freed_size += sum(
                        p.stat().st_size for p in _subdir.rglob("*") if p.is_file()
                    ) / (1024**3)
                    shutil.rmtree(_subdir)
                elif _subdir.is_file():
                    _freed_size += _subdir.stat().st_size / (1024**3)
                    _subdir.unlink()
            log.info(
                "freed %.1f GB non-estabelecimento CSVs; kept %.1f GB estabelecimento CSVs",
                _freed_size,
                _kept_size,
            )
        if os.environ.get("FICHA_DROP_ZIPS_AFTER_LOAD") == "1":
            zips_dir = cache_dir / month
            zip_size_gb = 0.0
            for zp in zips_dir.glob("*.zip"):
                zip_size_gb += zp.stat().st_size / (1024**3)
                zp.unlink()
            if zip_size_gb > 0:
                log.info(
                    "freed %.1f GB by removing raw ZIPs in %s (FICHA_DROP_ZIPS_AFTER_LOAD=1)",
                    zip_size_gb,
                    zips_dir,
                )

        write_lookups_json(
            con,
            output_dir / "lookups.json",
            schema_version=schema_version,
            snapshot_date=month,
        )
        log.info("wrote lookups.json")

        write_lookup_parquets(con, output_dir)
        log.info("wrote lookup parquets")

        log.info("=== PHASE 3/4: write parquets ===")
        t0 = time.monotonic()

        # --- Step 1: Write parquets that need the full estabelecimento table ---
        # cnpj_contatos, cnpj_cnaes, enderecos all read from the estabelecimento
        # table that is still loaded in DuckDB (loaded by load_main_tables_into_duckdb).
        post_write_drops_step1 = {
            "cnpj_contatos": (),
            "cnpj_cnaes": (),
            "enderecos": (),
        }
        for name, fn in (
            ("cnpj_contatos", write_cnpj_contatos_parquet),
            ("cnpj_cnaes", write_cnpj_cnaes_parquet),
            ("enderecos", write_enderecos_parquet),
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
                for tbl in post_write_drops_step1.get(name, ()):
                    con.execute(f"DROP TABLE IF EXISTS {tbl}")
                    log.info("  dropped table %s (no longer needed)", tbl)
            except NotImplementedError as exc:
                if skip_unimplemented:
                    log.warning("skipping %s.parquet: %s", name, exc)
                else:
                    raise

        # --- Step 2: Drop the estabelecimento table to free ~2 GB ---
        # cnpj_contatos / cnpj_cnaes / enderecos are done; the full table
        # is no longer needed. write_cnpjs_parquet_chunked will reload one
        # CSV at a time into a fresh `estabelecimento` table per chunk.
        con.execute("DROP TABLE IF EXISTS estabelecimento")
        log.info("  dropped table estabelecimento (~2 GB freed)")

        # --- Step 3: Chunk-per-ZIP cnpjs write ---
        # empresa + simples + lookups stay in DuckDB (~3 GB).
        # Each chunk loads one estabelecimento CSV (~2 GB) → JOIN → write → DROP.
        # Peak RAM: ~5 GB instead of ~70 GB.
        log.info("  writing cnpjs.parquet (chunked — %d CSVs)...", len(estabelecimento_csv_paths))
        tp = time.monotonic()
        write_cnpjs_parquet_chunked(con, estabelecimento_csv_paths, output_dir / "cnpjs.parquet")
        size_mb = (output_dir / "cnpjs.parquet").stat().st_size / 1024 / 1024
        log.info("  wrote cnpjs.parquet — %.1f MB in %.0fs", size_mb, time.monotonic() - tp)

        # --- Step 3b: Roundtrip-equivalence verify (ADR 0009) ---
        # Must happen BEFORE deleting estabelecimento CSVs (step 4).
        if verify:
            cnpjs_parquet = output_dir / "cnpjs.parquet"
            if cnpjs_parquet.exists():
                log.info(
                    "=== PHASE 4/4: roundtrip-equivalence check (sample=%d) ===",
                    verify_sample_size,
                )
                t_verify = time.monotonic()
                # Re-load estabelecimento from all CSVs so assert_roundtrip can
                # compare against the original source rows. After write_cnpjs_parquet_chunked
                # each chunk's table was dropped; we reload the full set here.
                if estabelecimento_csv_paths:
                    _create_table_from_csvs(
                        con,
                        "estabelecimento",
                        estabelecimento_csv_paths,
                        registry.main_table("estabelecimento").source,
                    )
                assert_roundtrip(con, cnpjs_parquet, sample_size=verify_sample_size)
                con.execute("DROP TABLE IF EXISTS estabelecimento")
                log.info("=== PHASE 4/4 roundtrip OK in %.0fs ===", time.monotonic() - t_verify)

        # --- Step 4: Delete estabelecimento CSVs ---
        shutil.rmtree(extract_dir / "estabelecimentos", ignore_errors=True)
        log.info("  deleted estabelecimento CSVs")

        # --- Step 5: Write raizes from cnpjs.parquet (no tables needed) ---
        log.info("  writing raizes.parquet (from cnpjs.parquet)...")
        tp = time.monotonic()
        write_raizes_parquet_from_cnpjs(
            con, output_dir / "cnpjs.parquet", output_dir / "raizes.parquet"
        )
        size_mb = (output_dir / "raizes.parquet").stat().st_size / 1024 / 1024
        log.info("  wrote raizes.parquet — %.1f MB in %.0fs", size_mb, time.monotonic() - tp)

        # --- Step 6: Write socios + pessoas (socio table still loaded) ---
        post_write_drops_step6 = {
            "socios": (),
            "pessoas": ("socio",),
        }
        for name, fn in (
            ("socios", write_socios_parquet),
            ("pessoas", write_pessoas_parquet),
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
                for tbl in post_write_drops_step6.get(name, ()):
                    con.execute(f"DROP TABLE IF EXISTS {tbl}")
                    log.info("  dropped table %s (no longer needed)", tbl)
            except NotImplementedError as exc:
                if skip_unimplemented:
                    log.warning("skipping %s.parquet: %s", name, exc)
                else:
                    raise

        log.info("=== PHASE 3/4 done in %.0fs ===", time.monotonic() - t0)
        progress.update(phase_task, description="transform: done", completed=3)

        log.info("transform_snapshot total: %.0fs", time.monotonic() - t_total)
    finally:
        con.close()
        db_path.unlink(missing_ok=True)
        if owns_progress:
            progress.stop()


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
