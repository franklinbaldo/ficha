"""Geração e atualização de web/public/manifest.json.

O manifest é o contrato entre o ETL e o frontend:
  - lista todos os snapshots disponíveis no Internet Archive
  - aponta qual é o mais recente (`current`)
  - traz URLs, hashes SHA-256 e row counts de cada arquivo

Schema: web/src/schemas/v1/manifest.ts (ManifestSchema / SnapshotEntrySchema).
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
from pathlib import Path

import duckdb

from .mirror import lookups_url, parquet_url

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"
GENERATOR = "ficha-etl"


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    """SHA-256 hex de um arquivo local (leitura em blocos de 64 KB)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def _row_count(parquet_path: Path) -> int:
    """Conta linhas de um Parquet via DuckDB (leitura local, sem copiar)."""
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
        ).fetchone()[0]
    finally:
        con.close()


def _file_entry(path: Path, url: str) -> dict:
    return {
        "url": url,
        "sha256": _sha256(path),
        "size": path.stat().st_size,
    }


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def build_snapshot_entry(month: str, output_dir: Path) -> dict:
    """Constrói um SnapshotEntry conforme ManifestSchema.

    Args:
        month: snapshot no formato YYYY-MM.
        output_dir: diretório com cnpjs.parquet, raizes.parquet,
                    socios.parquet e lookups.json.

    Returns:
        dict pronto para ser inserido em manifest["snapshots"].
    """
    cnpjs = output_dir / "cnpjs.parquet"
    raizes = output_dir / "raizes.parquet"
    socios = output_dir / "socios.parquet"
    lookups = output_dir / "lookups.json"

    for path in (cnpjs, raizes, socios, lookups):
        if not path.exists():
            raise FileNotFoundError(f"arquivo ausente para manifest: {path}")

    log.info("computing row counts for %s", month)
    row_counts = {
        "cnpjs": _row_count(cnpjs),
        "raizes": _row_count(raizes),
        "socios": _row_count(socios),
    }
    log.info("row counts: %s", row_counts)

    log.info("computing SHA-256 hashes")
    return {
        "date": month,
        "schema_version": SCHEMA_VERSION,
        "rfb_layout_date": None,
        "generated_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": GENERATOR,
        "row_counts": row_counts,
        "files": {
            "cnpjs": _file_entry(cnpjs, parquet_url(month, "cnpjs")),
            "raizes": _file_entry(raizes, parquet_url(month, "raizes")),
            "socios": _file_entry(socios, parquet_url(month, "socios")),
            "lookups": _file_entry(lookups, lookups_url(month)),
        },
    }


def update_manifest(manifest_path: Path, snapshot_entry: dict) -> None:
    """Upserta um snapshot no manifest.json (cria do zero se não existir).

    - Remove entrada prévia do mesmo mês (se houver).
    - Ordena snapshots por data decrescente.
    - Atualiza `current` para o snapshot mais recente.

    Args:
        manifest_path: caminho para web/public/manifest.json.
        snapshot_entry: dict produzido por build_snapshot_entry().
    """
    month = snapshot_entry["date"]

    if manifest_path.exists():
        manifest: dict = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        log.info("manifest.json não existe — criando do zero")
        manifest = {"current": month, "snapshots": []}

    # Upsert: descarta entrada antiga do mesmo mês
    manifest["snapshots"] = [
        s for s in manifest["snapshots"] if s["date"] != month
    ]
    manifest["snapshots"].append(snapshot_entry)

    # Mais recente primeiro; current aponta pro topo
    manifest["snapshots"].sort(key=lambda s: s["date"], reverse=True)
    manifest["current"] = manifest["snapshots"][0]["date"]

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    log.info(
        "manifest atualizado: current=%s, %d snapshot(s)",
        manifest["current"],
        len(manifest["snapshots"]),
    )
