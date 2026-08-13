"""Geração do descriptor de produção e do manifest público.

A identidade de um snapshot nasce enquanto os bytes ainda são locais. O
``production descriptor`` registra ``size + sha1 + sha256`` e row counts antes
de qualquer upload. A promoção posterior só acrescenta os shards de
``companies`` já produzidos/verificados e publica exatamente essa identidade.

O SHA-1 é o checksum operacional comparável diretamente com o catálogo do
Internet Archive; o SHA-256 continua disponível como digest forte para
consumidores. A identidade semântica dos shards segue validada separadamente
pelo ``MaterializationSpec``.

Schema público: web/src/schemas/v1/manifest.ts.
"""

from __future__ import annotations

import copy
import datetime
import hashlib
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import httpx

from .mirror import (
    companies_shard_url,
    lookup_parquet_url,
    lookups_url,
    parquet_url,
    raw_file_url,
)
from .shard_remote import PUBLIC_COMPANIES_GEOMETRY
from .transform import _LOOKUP_KINDS

log = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)

SCHEMA_VERSION = "1.0.0"
GENERATOR = "ficha-etl"


@dataclass(frozen=True)
class CompanyShardIdentity:
    """Identidade de bytes já fechada de um ``companies-NN.zip``."""

    shard: str
    name: str
    size: int
    sha1: str


def _sha1(path: Path) -> str:
    """SHA-1 hex operacional de um arquivo local (leitura em blocos de 64 KB)."""
    h = hashlib.sha1(usedforsecurity=False)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


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
        "sha1": _sha1(path),
        "sha256": _sha256(path),
        "size": path.stat().st_size,
    }


def _is_lower_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value)


def _companies_sharded_entry(month: str, shards: Iterable[CompanyShardIdentity]) -> dict:
    """Converte 100 identidades fechadas no contrato público."""
    geometry = PUBLIC_COMPANIES_GEOMETRY
    by_prefix: dict[str, CompanyShardIdentity] = {}
    for shard in shards:
        prefix = geometry.validate_prefix(shard.shard)
        if prefix in by_prefix:
            raise ValueError(f"duplicate companies shard: {prefix}")
        expected_name = geometry.shard_name(prefix)
        if shard.name != expected_name:
            raise ValueError(
                f"companies shard {prefix}: artifact name {shard.name!r} != {expected_name!r}"
            )
        if shard.size <= 0:
            raise ValueError(f"companies shard {prefix}: artifact size must be positive")
        if not _is_lower_hex(shard.sha1, 40):
            raise ValueError(f"companies shard {prefix}: invalid sha1")
        by_prefix[prefix] = shard

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
                "url": companies_shard_url(month, by_prefix[prefix].name),
                "sha1": by_prefix[prefix].sha1,
                "size": by_prefix[prefix].size,
            }
            for prefix in expected
        ],
    }


def build_production_descriptor(month: str, output_dir: Path) -> dict:
    """Fecha a identidade dos outputs locais antes de qualquer upload.

    O resultado já contém tudo que é imutável no snapshot exceto os shards de
    ``companies``, cuja identidade nasce individualmente quando cada ZIP é
    materializado. Ele deve ser persistido imediatamente pela orquestração.
    Nenhuma etapa posterior deve recalcular o SHA esperado a partir do remoto.
    """
    cnpjs = output_dir / "cnpjs.parquet"
    cnpj_contatos = output_dir / "cnpj_contatos.parquet"
    cnpj_cnaes = output_dir / "cnpj_cnaes.parquet"
    raizes = output_dir / "raizes.parquet"
    socios = output_dir / "socios.parquet"
    enderecos = output_dir / "enderecos.parquet"
    pessoas = output_dir / "pessoas.parquet"
    lookups = output_dir / "lookups.json"

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
            raise FileNotFoundError(f"arquivo ausente para descriptor: {path}")

    for kind in _LOOKUP_KINDS:
        parquet_path = output_dir / "lookups" / f"{kind}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"arquivo ausente para descriptor: {parquet_path}")

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

    log.info("computing SHA-1 + SHA-256 hashes for local snapshot files")
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
    lookup_files = {
        kind: _file_entry(
            output_dir / "lookups" / f"{kind}.parquet",
            lookup_parquet_url(month, kind),
        )
        for kind in _LOOKUP_KINDS
    }

    return {
        "date": month,
        "schema_version": SCHEMA_VERSION,
        "rfb_layout_date": None,
        "generated_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": GENERATOR,
        "row_counts": row_counts,
        "files": files,
        "lookups": lookup_files,
    }


def finalize_sharded_snapshot_entry(
    production_descriptor: dict,
    company_shards: Iterable[CompanyShardIdentity],
) -> dict:
    """Acrescenta shards ao descriptor sem recalcular identidade já produzida."""
    entry = copy.deepcopy(production_descriptor)
    month = entry.get("date")
    if not isinstance(month, str):
        raise ValueError("production descriptor missing date")
    files = entry.get("files")
    if not isinstance(files, dict):
        raise ValueError("production descriptor missing files")
    if "companies" in files or "companies_zip" in files:
        raise ValueError("production descriptor already contains companies artifact")
    files["companies"] = _companies_sharded_entry(month, company_shards)
    return entry


def write_production_descriptor(path: Path, descriptor: dict) -> None:
    """Persiste o recibo de produção em JSON canônico legível."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(descriptor, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_snapshot_entry(
    month: str,
    output_dir: Path,
    *,
    company_shards: Iterable[CompanyShardIdentity] | None = None,
) -> dict:
    """Constrói um SnapshotEntry conforme ManifestSchema.

    Mantém o caminho monolítico apenas para compatibilidade. Publicações novas
    devem usar ``build_production_descriptor`` durante a produção e
    ``finalize_sharded_snapshot_entry`` depois da verificação dos shards.
    """
    descriptor = build_production_descriptor(month, output_dir)
    if company_shards is not None:
        return finalize_sharded_snapshot_entry(descriptor, company_shards)

    companies_zip = output_dir / "companies.zip"
    if not companies_zip.exists():
        raise FileNotFoundError(f"arquivo ausente para manifest: {companies_zip}")
    descriptor["files"]["companies_zip"] = _file_entry(
        companies_zip, raw_file_url(month, "companies.zip")
    )
    return descriptor


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
