"""Publicação operacional retomável de companies shards para 2026-05 (#171).

Não roda transform, não baixa os Parquets inteiros e nunca constrói
``companies.zip`` monolítico. Cada processo lê os inputs já duráveis diretamente
do IA via httpfs e publica apenas os prefixos pedidos.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
import internetarchive as ia

from ficha_etl.mirror import raw_file_url
from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import (
    ShardPublishAction,
    pin_materialization_inputs,
    publish_one_shard,
)
from ficha_etl.shard_remote import (
    PUBLIC_COMPANIES_GEOMETRY,
    fetch_remote_shard_meta,
)
from ficha_etl.shard_sidecar import fetch_remote_sidecar, hash_remote_artifact, sidecar_name
from ficha_etl.sharded_pack import ShardPackSession
from ficha_etl.upload_identity import files_list

MONTH = "2026-05"
IDENTIFIER = f"ficha-{MONTH}"
OUTPUT_DIR = Path(".ops-shards-2026-05")
CONFIRM_ATTEMPTS = 20


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


def _direct_sidecar_entry(prefix: str) -> dict | None:
    """Observa a sidecar no endpoint público sem depender de metadata.files.

    404 é ausência observada. Qualquer outro estado não-200 é ambíguo e aborta,
    porque não pode autorizar um PUT que talvez sobrescreva uma sidecar existente.
    """
    name = sidecar_name(PUBLIC_COMPANIES_GEOMETRY, prefix)
    url = raw_file_url(MONTH, name)
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(url)
    except httpx.HTTPError as exc:
        raise RuntimeError(f"cannot observe {name} directly: {exc}") from exc
    if response.status_code == 404:
        return None
    if response.status_code != 200 or not response.content:
        raise RuntimeError(
            f"cannot classify {name} directly: HTTP {response.status_code}, "
            f"bytes={len(response.content)}"
        )
    digest = hashlib.sha1(response.content, usedforsecurity=False).hexdigest()
    return {"name": name, "size": str(len(response.content)), "sha1": digest}


def _metadata_for(prefix: str) -> dict | None:
    """Metadata do item enriquecido com sidecar diretamente observável.

    O IA pode servir o objeto muito antes de incluí-lo em ``metadata.files``.
    Para a sidecar pequena, GET + bytes exatos são prova mais direta do que a
    indexação eventual do catálogo. O ZIP continua dependendo do metadata real.
    """
    metadata = _metadata()
    if metadata is None:
        return None
    files = files_list(metadata)
    if files is None:
        return metadata
    identity_name = sidecar_name(PUBLIC_COMPANIES_GEOMETRY, prefix)
    if any(entry.get("name") == identity_name for entry in files):
        return metadata
    direct = _direct_sidecar_entry(prefix)
    if direct is None:
        return metadata
    enriched = dict(metadata)
    enriched["files"] = [*files, direct]
    print(f"DIRECT {identity_name}: readable before metadata index", flush=True)
    return enriched


def _listed(name: str) -> bool | None:
    metadata = _metadata()
    if metadata is None:
        return None
    files = files_list(metadata)
    if files is None:
        return None
    return any(entry.get("name") == name for entry in files)


def _reconcile_previous_attempt(prefix: str) -> None:
    name = PUBLIC_COMPANIES_GEOMETRY.shard_name(prefix)
    for attempt in range(1, 13):
        state = _listed(name)
        if state is True:
            print(f"RECONCILED {name}: now listed in metadata", flush=True)
            return
        if state is None:
            print(f"RECONCILE {name}: metadata ambiguous ({attempt}/12)", flush=True)
        else:
            print(f"RECONCILE {name}: still absent ({attempt}/12)", flush=True)
        if attempt < 12:
            time.sleep(30)

    url = raw_file_url(MONTH, name)
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.head(url)
    except httpx.HTTPError as exc:
        raise RuntimeError(f"cannot prove {name} absent after reconciliation: {exc}") from exc
    if response.status_code == 200:
        raise RuntimeError(
            f"{name} is directly readable but missing from metadata; refusing another PUT"
        )
    if response.status_code != 404:
        raise RuntimeError(
            f"cannot prove {name} absent after reconciliation: HEAD returned {response.status_code}"
        )
    print(f"RECONCILED {name}: metadata absent and direct HEAD 404", flush=True)


def _publish_one(session, prefix: str, pinned, upload):
    return publish_one_shard(
        session,
        prefix,
        OUTPUT_DIR,
        pinned_inputs=pinned,
        fetch_metadata=lambda: _metadata_for(prefix),
        fetch_meta=_fetch_meta,
        fetch_sidecar=_fetch_sidecar,
        hash_remote=_hash_remote,
        upload=upload,
        confirm_attempts=CONFIRM_ATTEMPTS,
    )


def run_proof(upload) -> None:
    pinned = pin_materialization_inputs(_metadata)
    print(f"PINNED {len(pinned)} semantic inputs", flush=True)
    _reconcile_previous_attempt("99")

    with _session() as session:
        first = _publish_one(session, "99", pinned, upload)
    print(f"PROOF first session: 99 -> {first.action}", flush=True)

    with _session() as session:
        second = _publish_one(session, "99", pinned, upload)
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
    results = []
    with _session() as session:
        for prefix in prefixes:
            results.append(_publish_one(session, prefix, pinned, upload))
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
