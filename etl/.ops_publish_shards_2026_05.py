"""Publicação operacional retomável de companies shards para 2026-05 (#171).

Não roda transform, não baixa os Parquets inteiros e nunca constrói
``companies.zip`` monolítico. Cada processo lê os inputs já duráveis diretamente
do IA via httpfs e publica apenas os prefixos pedidos.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

import internetarchive as ia

from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import (
    ShardPublishAction,
    pin_materialization_inputs,
    publish_one_shard,
    publish_shards,
)
from ficha_etl.shard_remote import (
    PUBLIC_COMPANIES_GEOMETRY,
    fetch_remote_shard_meta,
)
from ficha_etl.shard_sidecar import fetch_remote_sidecar, hash_remote_artifact
from ficha_etl.sharded_pack import ShardPackSession

MONTH = "2026-05"
IDENTIFIER = f"ficha-{MONTH}"
OUTPUT_DIR = Path(".ops-shards-2026-05")


def _require_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _metadata() -> dict | None:
    return fetch_item_metadata(MONTH)


def _fetch_meta(name: str) -> object | None:
    return fetch_remote_shard_meta(MONTH, name)


def _fetch_sidecar(name: str) -> object | None:
    return fetch_remote_sidecar(MONTH, name, attempts=6, backoff_s=5.0)


def _hash_remote(name: str, expected_size: int, expected_sha1: str):
    return hash_remote_artifact(
        MONTH,
        name,
        expected_size=expected_size,
        expected_sha1=expected_sha1,
    )


def _uploader(access_key: str, secret_key: str):
    def upload(name: str, path: Path) -> None:
        if not path.exists() or path.stat().st_size <= 0:
            raise RuntimeError(f"refusing to upload missing/empty local file: {path}")
        print(f"UPLOAD {name} bytes={path.stat().st_size}", flush=True)
        responses = ia.upload(
            IDENTIFIER,
            files={name: str(path)},
            access_key=access_key,
            secret_key=secret_key,
            retries=5,
            retries_sleep=30,
            verbose=True,
        )
        bad: list[tuple[int | None, str]] = []
        for response in responses:
            if response is None:
                continue
            status = getattr(response, "status_code", None)
            if status not in (200, 201):
                bad.append((status, getattr(response, "text", "")[:200]))
        if bad:
            raise RuntimeError(f"IA rejected {name}: {bad}")

    return upload


def _session() -> ShardPackSession:
    # Default parquets_base resolves directly to ia:ficha-2026-05. That is
    # deliberate: four matrix workers must not each download ~9.5 GiB first.
    return ShardPackSession(
        MONTH,
        PUBLIC_COMPANIES_GEOMETRY,
        batch_size=10_000,
        memory_limit_gb=12.0,
    )


def _serialize(result) -> dict:
    return {
        "prefix": result.prefix,
        "name": result.name,
        "action": str(result.action),
        "materialization_id": result.materialization_id,
        "size": result.size,
        "sha1": result.sha1,
        "sha256": result.sha256,
    }


def _write_summary(mode: str, results: list, *, proof_second_action: str | None = None) -> None:
    counts = Counter(str(result.action) for result in results)
    payload = {
        "month": MONTH,
        "mode": mode,
        "results": [_serialize(result) for result in results],
        "action_counts": dict(sorted(counts.items())),
        "proof_second_action": proof_second_action,
    }
    Path("shard-publication-summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


def run_proof(upload) -> None:
    """Publica/garante 99 e prova resume numa sessão DuckDB nova."""
    pinned = pin_materialization_inputs(_metadata)
    print(f"PINNED {len(pinned)} semantic inputs", flush=True)

    with _session() as session:
        first = publish_one_shard(
            session,
            "99",
            OUTPUT_DIR,
            pinned_inputs=pinned,
            fetch_metadata=_metadata,
            fetch_meta=_fetch_meta,
            fetch_sidecar=_fetch_sidecar,
            hash_remote=_hash_remote,
            upload=upload,
        )
    print(f"PROOF first session: 99 -> {first.action}", flush=True)

    # Nova conexão DuckDB e nenhuma memória de estado local do primeiro pack.
    with _session() as session:
        second = publish_one_shard(
            session,
            "99",
            OUTPUT_DIR,
            pinned_inputs=pinned,
            fetch_metadata=_metadata,
            fetch_meta=_fetch_meta,
            fetch_sidecar=_fetch_sidecar,
            hash_remote=_hash_remote,
            upload=upload,
        )
    print(f"PROOF second session: 99 -> {second.action}", flush=True)
    if second.action is not ShardPublishAction.SKIPPED:
        raise RuntimeError(
            f"resume proof failed: second fresh session returned {second.action}, expected skipped"
        )
    _write_summary("proof", [first, second], proof_second_action=str(second.action))


def _parse_prefixes(raw: str) -> list[str]:
    values = [value.strip() for value in raw.split(",") if value.strip()]
    if not values:
        raise ValueError("--prefixes must contain at least one prefix")
    validated = [PUBLIC_COMPANIES_GEOMETRY.validate_prefix(value) for value in values]
    if "99" in validated:
        raise ValueError("batch mode must not include 99; it belongs to the resume proof")
    if len(set(validated)) != len(validated):
        raise ValueError("duplicate prefix in --prefixes")
    return validated


def run_batch(upload, raw_prefixes: str) -> None:
    prefixes = _parse_prefixes(raw_prefixes)
    pinned = pin_materialization_inputs(_metadata)
    print(
        f"BATCH {prefixes[0]}..{prefixes[-1]} count={len(prefixes)} "
        f"pinned_inputs={len(pinned)}",
        flush=True,
    )
    with _session() as session:
        results = publish_shards(
            session,
            OUTPUT_DIR,
            pinned_inputs=pinned,
            fetch_metadata=_metadata,
            fetch_meta=_fetch_meta,
            fetch_sidecar=_fetch_sidecar,
            hash_remote=_hash_remote,
            upload=upload,
            prefixes=prefixes,
        )
    _write_summary("batch", results)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proof", action="store_true")
    parser.add_argument("--prefixes")
    args = parser.parse_args()
    if args.proof == bool(args.prefixes):
        parser.error("choose exactly one of --proof or --prefixes")

    access_key = _require_secret("IA_ACCESS_KEY")
    secret_key = _require_secret("IA_SECRET_KEY")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    upload = _uploader(access_key, secret_key)

    if args.proof:
        run_proof(upload)
    else:
        run_batch(upload, args.prefixes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
