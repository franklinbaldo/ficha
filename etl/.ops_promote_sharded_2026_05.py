"""Gera candidato de manifesto para 2026-05 sem recomputar ETL (#110).

A promoção só usa:
- os derivados já duráveis em ``ia:ficha-2026-05``;
- os 100 ``companies-NN.zip`` previamente publicados;
- as APIs canônicas ``verify_all_shards``, ``build_snapshot_entry`` e
  ``verify_snapshot_files``.

Nenhuma operação deste script escreve no Internet Archive. ``--verify-shards``
fecha primeiro a identidade de todos os shards em uma única leitura do catálogo
do IA, cotejando ``size + sha1`` e o pequeno ``_meta.json`` de cada ZIP.
``--promote`` só aceita essa evidência completa, calcula SHA-256/row counts dos
arquivos padrão a partir dos bytes locais e então atualiza uma cópia de
``web/public/manifest.json`` no workspace do runner.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from ficha_etl.manifest import (
    CompanyShardIdentity,
    build_snapshot_entry,
    update_manifest,
    verify_snapshot_files,
)
from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import pin_materialization_inputs
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, fetch_remote_shard_meta
from ficha_etl.shard_transfer import ShardTransferAction, verify_all_shards
from ficha_etl.sharded_pack import ShardPackSession

MONTH = "2026-05"
OUTPUT_DIR = Path(".cache") / MONTH / "output"
MANIFEST_PATH = Path("..") / "web" / "public" / "manifest.json"
SHARDS_PATH = Path("company-shards-verified.json")
SUMMARY_PATH = Path("promotion-summary.json")


def _metadata() -> dict | None:
    return fetch_item_metadata(MONTH)


def _fetch_meta_once(name: str) -> object | None:
    # O batch é uma leitura instantânea do estado público: sem retry serial por
    # shard. Se um _meta.json ainda não estiver observável, a promoção falha e
    # pode ser reexecutada integralmente sem qualquer capacidade de escrita.
    return fetch_remote_shard_meta(MONTH, name, attempts=1)


def _session() -> ShardPackSession:
    # VERIFY usa apenas materialization_spec(); não abre DuckDB nem baixa lookups.
    return ShardPackSession(MONTH, PUBLIC_COMPANIES_GEOMETRY)


def verify_shards() -> None:
    """Fecha 100/100 checkpoints read-only a partir do estado público do IA."""
    metadata = _metadata()
    if metadata is None:
        raise RuntimeError("metadata do Internet Archive indisponível")

    # A mesma resposta de /metadata fornece os nove inputs semânticos e as
    # identidades size+sha1 dos 100 objetos. Não fazemos uma segunda leitura do
    # catálogo dentro deste comando.
    pinned = pin_materialization_inputs(lambda: metadata)
    results = verify_all_shards(
        _session(),
        pinned_inputs=pinned,
        fetch_metadata=lambda: metadata,
        fetch_meta=_fetch_meta_once,
    )

    rows: list[dict] = []
    for result in results:
        if result.action is not ShardTransferAction.VERIFIED:
            raise RuntimeError(f"{result.name} não final: {result.action}")
        rows.append(
            {
                "shard": result.prefix,
                "name": result.name,
                "materialization_id": result.materialization_id,
                "size": result.size,
                "sha1": result.sha1,
            }
        )
        print(
            f"VERIFIED {result.name} size={result.size} sha1={result.sha1}",
            flush=True,
        )

    if len(rows) != PUBLIC_COMPANIES_GEOMETRY.count:
        raise RuntimeError(f"expected 100 verified shards, got {len(rows)}")

    payload = {
        "month": MONTH,
        "pinned_inputs": dict(sorted(pinned.items())),
        "shards": rows,
    }
    SHARDS_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"100/100 shards VERIFIED -> {SHARDS_PATH}", flush=True)


def _load_shards() -> list[CompanyShardIdentity]:
    payload = json.loads(SHARDS_PATH.read_text(encoding="utf-8"))
    if payload.get("month") != MONTH:
        raise ValueError(f"shard evidence month != {MONTH}")
    raw = payload.get("shards")
    if not isinstance(raw, list) or len(raw) != PUBLIC_COMPANIES_GEOMETRY.count:
        raise ValueError("shard evidence must contain exactly 100 entries")
    return [
        CompanyShardIdentity(
            shard=row["shard"],
            name=row["name"],
            size=int(row["size"]),
            sha1=row["sha1"],
        )
        for row in raw
    ]


def promote() -> None:
    before = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    before_dates = {snapshot["date"] for snapshot in before.get("snapshots", [])}
    if "2026-04" not in before_dates:
        raise RuntimeError("manifest base perdeu o snapshot público 2026-04")

    identities = _load_shards()
    print("[manifest 1/3] build_snapshot_entry com 100 company shards", flush=True)
    entry = build_snapshot_entry(MONTH, OUTPUT_DIR, company_shards=identities)

    companies = entry["files"].get("companies")
    if not isinstance(companies, dict) or len(companies.get("shards", [])) != 100:
        raise RuntimeError("entry novo não contém exatamente 100 companies shards")
    if "companies_zip" in entry["files"]:
        raise RuntimeError("entry shardado não pode declarar companies_zip monolítico")

    print("[manifest 2/3] verify_snapshot_files — HEAD + tamanho", flush=True)
    broken = verify_snapshot_files(entry)
    if broken:
        raise RuntimeError(
            "manifest não promovido; URLs declaradas não verificaram:\n"
            + "\n".join(f"  {url}" for url in broken)
        )

    print("[manifest 3/3] update_manifest", flush=True)
    update_manifest(MANIFEST_PATH, entry)

    after = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    after_dates = {snapshot["date"] for snapshot in after.get("snapshots", [])}
    if after.get("current") != MONTH:
        raise RuntimeError(f"manifest current != {MONTH}: {after.get('current')!r}")
    if not {"2026-04", MONTH}.issubset(after_dates):
        raise RuntimeError(f"manifest não preservou 2026-04 + {MONTH}: {sorted(after_dates)}")

    cnpjs = entry["files"]["cnpjs"]
    if int(cnpjs["size"]) <= 0 or len(cnpjs["sha256"]) != 64:
        raise RuntimeError("files.cnpjs não preservou contrato size + sha256")

    summary = {
        "month": MONTH,
        "current": after["current"],
        "snapshot_dates": sorted(after_dates),
        "companies_shards": len(companies["shards"]),
        "cnpjs": cnpjs,
        "row_counts": entry["row_counts"],
    }
    SUMMARY_PATH.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--verify-shards", action="store_true")
    mode.add_argument("--promote", action="store_true")
    args = parser.parse_args()

    if args.verify_shards:
        verify_shards()
    else:
        promote()
    return 0


if __name__ == "__main__":
    sys.exit(main())
