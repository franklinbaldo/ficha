"""Transform: ZIPs RFB → 3 Parquets + lookups.json.

Pipeline (ADR 0008 + ADR 0009):

    Resolve via fetcher chain  →  Extract ZIPs  →  Load no DuckDB  →
    Write 3 Parquets + lookups.json

Esta versão entrega:
- Extract dos 37 ZIPs.
- `lookups.json` com Cnaes, Motivos, Municipios, Naturezas, Paises, Qualificacoes.

Stubs pendentes (próximas PRs):
- `cnpjs.parquet`  (Empresa + Estabelecimento + Simples + lookups inline)
- `raizes.parquet` (uma linha por raiz com agregados)
- `socios.parquet` (PF/PJ/estrangeiro com flag tipo)
- Roundtrip-equivalence test (ADR 0009).
"""

from __future__ import annotations

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
        # Filtra apenas arquivos (extract_zip já pula dirs, mas defensivo)
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


def load_lookup_into_duckdb(
    con: duckdb.DuckDBPyConnection,
    kind: FileKind,
    csv_path: Path,
) -> None:
    """Carrega uma tabela de lookup (codigo;descricao) numa view DuckDB.

    A tabela é criada como `lookup_{kind}` com colunas `codigo`, `descricao`.
    """
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


def write_cnpjs_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """TODO: produzir cnpjs.parquet (Empresa + Estabelecimento + Simples inline)."""
    raise NotImplementedError("cnpjs.parquet ainda não implementado")


def write_raizes_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """TODO: produzir raizes.parquet (agregados por cnpj_base)."""
    raise NotImplementedError("raizes.parquet ainda não implementado")


def write_socios_parquet(
    con: duckdb.DuckDBPyConnection,
    extracted: Iterable[ExtractedFile],
    output_path: Path,
) -> None:
    """TODO: produzir socios.parquet (PF + PJ com flag tipo)."""
    raise NotImplementedError("socios.parquet ainda não implementado")


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

    log.info("loading lookups into DuckDB")
    con = duckdb.connect()
    try:
        for ef in extracted:
            if ef.kind in _LOOKUP_KINDS:
                load_lookup_into_duckdb(con, ef.kind, ef.csv_path)

        write_lookups_json(
            con,
            output_dir / "lookups.json",
            schema_version=schema_version,
            snapshot_date=month,
        )
        log.info("wrote %s", output_dir / "lookups.json")

        # Os 3 parquets ainda estão como stubs.
        for name, fn in (
            ("cnpjs", write_cnpjs_parquet),
            ("raizes", write_raizes_parquet),
            ("socios", write_socios_parquet),
        ):
            try:
                fn(con, extracted, output_dir / f"{name}.parquet")
            except NotImplementedError as exc:
                if skip_unimplemented:
                    log.warning("skipping %s.parquet: %s", name, exc)
                else:
                    raise
    finally:
        con.close()
