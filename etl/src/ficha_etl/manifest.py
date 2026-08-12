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
from collections.abc import Iterable
from pathlib import Path

import duckdb
import httpx

from .mirror import lookup_parquet_url, lookups_url, parquet_url, raw_file_url
from .shard_remote import PUBLIC_COMPANIES_GEOMETRY
from .shard_sidecar import ShardSidecar
from .transform import _LOOKUP_KINDS

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)

SCHEMA_VERSION = "1.0.0"
GENERATOR = "ficha-etl"


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
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    finally:
        con.close()


def _file_entry(path: Path, url: str) -> dict:
    return {
        "url": url,
        "sha256": _sha256(path),
        "size": path.stat().st_size,
    }


def _is_lower_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value)


def _companies_sharded_entry(month: str, sidecars: Iterable[ShardSidecar]) -> dict:
    """Converte 100 sidecars confirmadas no contrato público de companies."""
    geometry = PUBLIC_COMPANIES_GEOMETRY
    by_prefix: dict[str, ShardSidecar] = {}
    for sidecar in sidecars:
        prefix = geometry.validate_prefix(sidecar.shard)
        if prefix in by_prefix:
            raise ValueError(f"duplicate companies shard sidecar: {prefix}")
        expected_name = geometry.shard_name(prefix)
        if sidecar.snapshot != month:
            raise ValueError(
                f"companies shard {prefix}: sidecar snapshot {sidecar.snapshot!r} != {month!r}"
            )
        if sidecar.artifact_name != expected_name:
            raise ValueError(
                f"companies shard {prefix}: artifact name {sidecar.artifact_name!r} "
                f"!= {expected_name!r}"
            )
        if sidecar.artifact.size <= 0:
            raise ValueError(f"companies shard {prefix}: artifact size must be positive")
        if not _is_lower_hex(sidecar.artifact.sha256, 64):
            raise ValueError(f"companies shard {prefix}: invalid sha256")
        by_prefix[prefix] = sidecar

    expected = list(geometry.prefixes())
    missing = [prefix for prefix in expected if prefix not in by_prefix]
    extra = sorted(set(by_prefix) - set(expected))
    if missing or extra:
        raise ValueError(f"incomplete companies shards: missing={missing} extra={extra}")

    return {
        "shard_by": "cnpj_base_prefix_2",
        "shards": [
            {
                "shard": prefix,
                "url": raw_file_url(month, by_prefix[prefix].artifact_name),
                "sha256": by_prefix[prefix].artifact.sha256,
                "size": by_prefix[prefix].artifact.size,
            }
            for prefix in expected
        ],
    }


def build_snapshot_entry(
    month: str,
    output_dir: Path,
    *,
    company_sidecars: Iterable[ShardSidecar] | None = None,
) -> dict:
    """Constrói um SnapshotEntry conforme ManifestSchema.

    Args:
        month: snapshot no formato YYYY-MM.
        output_dir: diretório com todos os parquets produzidos pelo transform
                    e lookups.json + lookups/*.parquet.
        company_sidecars: quando fornecidas, substituem o ``companies.zip``
                    monolítico pelo conjunto completo de shards 00..99.
    """
    cnpjs = output_dir / "cnpjs.parquet"
    cnpj_contatos = output_dir / "cnpj_contatos.parquet"
    cnpj_cnaes = output_dir / "cnpj_cnaes.parquet"
    raizes = output_dir / "raizes.parquet"
    socios = output_dir / "socios.parquet"
    enderecos = output_dir / "enderecos.parquet"
    pessoas = output_dir / "pessoas.parquet"
    lookups = output_dir / "lookups.json"
    companies_zip = output_dir / "companies.zip"

    required = (
        cnpjs,
        cnpj_contatos,
        cnpj_cnaes,
        raizes,
        socios,
        enderecos,
        pessoas,
        lookups,
    )
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"arquivo ausente para manifest: {path}")

    if company_sidecars is None and not companies_zip.exists():
        raise FileNotFoundError(f"arquivo ausente para manifest: {companies_zip}")

    for kind in _LOOKUP_KINDS:
        parquet_path = output_dir / "lookups" / f"{kind}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"arquivo ausente para manifest: {parquet_path}")

    log.info("computing row counts for %s", month)
    row_counts = {
        "cnpjs": _row_count(cnpjs),
        "cnpj_contatos": _row_count(cnpj_contatos),
        "cnpj_cnaes": _row_count(cnpj_cnaes),
        "raizes": _row_count(raizes),
        "socios": _row_count(socios),
        "enderecos": _row_count(enderecos),
        "pessoas": _row_count(pessoas),
    }
    log.info("row counts: %s", row_counts)

    log.info("computing SHA-256 hashes")
    files: dict[str, object] = {
        "cnpjs": _file_entry(cnpjs, parquet_url(month, "cnpjs")),
        "cnpj_contatos": _file_entry(cnpj_contatos, parquet_url(month, "cnpj_contatos")),
        "cnpj_cnaes": {
            **_file_entry(cnpj_cnaes, parquet_url(month, "cnpj_cnaes")),
            "sort": ["cnae_codigo", "posicao", "cnpj_base"],
        },
        "raizes": _file_entry(raizes, parquet_url(month, "raizes")),
        "socios": _file_entry(socios, parquet_url(month, "socios")),
        "enderecos": {
            **_file_entry(enderecos, parquet_url(month, "enderecos")),
            "sort": ["uf", "municipio_codigo", "logradouro_normalizado", "numero"],
        },
        "pessoas": {
            **_file_entry(pessoas, parquet_url(month, "pessoas")),
            "sort": ["cpf_mascarado", "nome_normalizado"],
        },
        "lookups": _file_entry(lookups, lookups_url(month)),
    }
    if company_sidecars is None:
        files["companies_zip"] = _file_entry(companies_zip, raw_file_url(month, "companies.zip"))
    else:
        files["companies"] = _companies_sharded_entry(month, company_sidecars)

    return {
        "date": month,
        "schema_version": SCHEMA_VERSION,
        "rfb_layout_date": None,
        "generated_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": GENERATOR,
        "row_counts": row_counts,
        "files": files,
        "lookups": {
            kind: {"url": lookup_parquet_url(month, kind)} for kind in _LOOKUP_KINDS
        },
    }


def verify_snapshot_files(snapshot_entry: dict) -> list[str]:
    """HEAD em toda URL declarada no snapshot, incluindo shards de companies."""
    checks: list[tuple[str, int | None]] = []
    for name, entry in snapshot_entry["files"].items():
        if name == "companies":
            shards = entry.get("shards") if isinstance(entry, dict) else None
            if not isinstance(shards, list):
                log.warning("verify_snapshot_files: companies sem lista shards")
                return ["<manifest:companies>"]
            checks.extend((shard["url"], shard.get("size")) for shard in shards)
            continue
        checks.append((entry["url"], entry.get("size")))

    checks += [
        (entry["url"], entry.get("size")) for entry in snapshot_entry.get("lookups", {}).values()
    ]

    broken: list[str] = []
    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        for url, expected_size in checks:
            try:
                r = client.head(url)
            except httpx.HTTPError as exc:
                log.warning("verify_snapshot_files: %s -> %s", url, exc)
                broken.append(url)
                continue
            if r.status_code != 200:
                log.warning("verify_snapshot_files: %s -> HTTP %d", url, r.status_code)
                broken.append(url)
                continue
            if expected_size is not None:
                remote_len = r.headers.get("content-length")
                if remote_len is None:
                    log.warning(
                        "verify_snapshot_files: %s -> sem Content-Length; size não verificado", url
                    )
                elif int(remote_len) != expected_size:
                    log.warning(
                        "verify_snapshot_files: %s -> size remoto %s != manifest %d",
                        url,
                        remote_len,
                        expected_size,
                    )
                    broken.append(url)
    return broken


def update_manifest(manifest_path: Path, snapshot_entry: dict) -> None:
    """Upserta um snapshot no manifest.json (cria do zero se não existir)."""
    month = snapshot_entry["date"]

    if manifest_path.exists():
        manifest: dict = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        log.info("manifest.json não existe — criando do zero")
        manifest = {"current": month, "snapshots": []}

    manifest["snapshots"] = [s for s in manifest["snapshots"] if s["date"] != month]
    manifest["snapshots"].append(snapshot_entry)
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
