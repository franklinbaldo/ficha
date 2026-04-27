"""Manifest generation: agrega snapshot metadata em `web/public/manifest.json`.

Schema definido em `web/src/schemas/v1/manifest.ts`. Single source of truth do
que está publicado: cada snapshot lista URLs IA, sha256, sizes e row counts
para `cnpjs.parquet`, `raizes.parquet`, `socios.parquet`, `lookups.json`.

Frontend faz **um** fetch do manifest no boot pra descobrir o que carregar.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from . import mirror

log = logging.getLogger(__name__)

_HASH_BUF_SIZE = 1024 * 1024  # 1 MiB


@dataclass(frozen=True)
class FileEntry:
    url: str
    sha256: str
    size: int


@dataclass(frozen=True)
class Snapshot:
    date: str  # YYYY-MM
    schema_version: str
    rfb_layout_date: str | None
    generated_at: str  # ISO 8601 UTC
    generator: str  # ex.: "ficha-etl 0.0.1"
    row_counts: dict[str, int]
    files: dict[str, FileEntry]


def file_entry(path: Path, url: str) -> FileEntry:
    """Calcula sha256 + size de um arquivo local + URL pública."""
    return FileEntry(url=url, sha256=sha256_of(path), size=path.stat().st_size)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(_HASH_BUF_SIZE):
            h.update(chunk)
    return h.hexdigest()


def row_count(parquet_path: Path) -> int:
    """Conta linhas dum Parquet via DuckDB (rápido — só lê metadata)."""
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT COUNT(*) FROM '{parquet_path}'"
        ).fetchone()[0]
    finally:
        con.close()


def build_snapshot_entry(
    month: str,
    output_dir: Path,
    *,
    schema_version: str,
    rfb_layout_date: str | None = None,
    generator: str = "ficha-etl",
) -> Snapshot:
    """Computa a entrada do manifest pra um snapshot pronto em `output_dir`.

    Espera os 4 arquivos canônicos: cnpjs.parquet, raizes.parquet,
    socios.parquet, lookups.json.
    """
    cnpjs = output_dir / "cnpjs.parquet"
    raizes = output_dir / "raizes.parquet"
    socios = output_dir / "socios.parquet"
    lookups = output_dir / "lookups.json"
    for p in (cnpjs, raizes, socios, lookups):
        if not p.exists():
            raise FileNotFoundError(f"expected output missing: {p}")

    return Snapshot(
        date=month,
        schema_version=schema_version,
        rfb_layout_date=rfb_layout_date,
        generated_at=dt.datetime.now(dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        generator=generator,
        row_counts={
            "cnpjs": row_count(cnpjs),
            "raizes": row_count(raizes),
            "socios": row_count(socios),
        },
        files={
            "cnpjs": file_entry(cnpjs, mirror.parquet_url(month, "cnpjs")),
            "raizes": file_entry(raizes, mirror.parquet_url(month, "raizes")),
            "socios": file_entry(socios, mirror.parquet_url(month, "socios")),
            "lookups": file_entry(lookups, mirror.lookups_url(month)),
        },
    )


def update_manifest(manifest_path: Path, snapshot: Snapshot) -> None:
    """Insere/substitui a entrada do `snapshot.date` em `manifest_path`.

    Mantém a ordem cronológica decrescente (mais recente primeiro). Atualiza
    `current` para o mais recente.
    """
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        snapshots = existing.get("snapshots", [])
    else:
        snapshots = []

    # Remove entrada existente pro mesmo mês (re-run idempotente)
    snapshots = [s for s in snapshots if s.get("date") != snapshot.date]
    snapshots.append(_serialize(snapshot))
    snapshots.sort(key=lambda s: s["date"], reverse=True)

    payload = {"current": snapshots[0]["date"], "snapshots": snapshots}
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    )


def _serialize(snapshot: Snapshot) -> dict:
    """Snapshot dataclass → dict pronto pra JSON, com FileEntry expandidos."""
    d = asdict(snapshot)
    d["files"] = {k: asdict(v) for k, v in snapshot.files.items()}
    return d
