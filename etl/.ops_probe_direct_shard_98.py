"""Probe real: durabilidade direta de companies-98.zip sem metadata.files como gate."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import httpx
import internetarchive as ia

from ficha_etl import mirror
from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import pin_materialization_inputs
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, materialization_input_sha1s
from ficha_etl.shard_sidecar import (
    ShardSidecar,
    artifact_identity,
    hash_remote_artifact,
    sidecar_name,
    write_sidecar,
)
from ficha_etl.sharded_pack import ShardPackSession
from ficha_etl.upload_identity import files_list

MONTH = "2026-05"
PREFIX = "98"
IDENTIFIER = f"ficha-{MONTH}"
OUT = Path(".ops-direct-probe-98")


def _secret(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _upload(name: str, path: Path, access_key: str, secret_key: str) -> None:
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
    for response in responses:
        if response is None:
            continue
        status = getattr(response, "status_code", None)
        if status not in (200, 201):
            raise RuntimeError(f"IA rejected {name}: HTTP {status}")


def _listed(name: str) -> bool | None:
    metadata = fetch_item_metadata(MONTH)
    if metadata is None:
        return None
    files = files_list(metadata)
    if files is None:
        return None
    return any(entry.get("name") == name for entry in files)


def _head(client: httpx.Client, url: str) -> tuple[int, int | None]:
    response = client.head(url)
    raw = response.headers.get("content-length")
    try:
        length = int(raw) if raw is not None else None
    except ValueError:
        length = None
    return response.status_code, length


def _meta(client: httpx.Client, url: str, expected_id: str, expected_doc: dict) -> tuple[int, bool]:
    response = client.get(url)
    if response.status_code != 200:
        return response.status_code, False
    try:
        payload = response.json()
    except ValueError:
        return response.status_code, False
    materialization = payload.get("materialization") if isinstance(payload, dict) else None
    return response.status_code, bool(
        isinstance(materialization, dict)
        and materialization.get("id") == expected_id
        and materialization.get("spec") == expected_doc
    )


def main() -> int:
    access_key = _secret("IA_ACCESS_KEY")
    secret_key = _secret("IA_SECRET_KEY")
    metadata = fetch_item_metadata(MONTH)
    pinned = pin_materialization_inputs(lambda: metadata)
    print(f"PINNED {len(pinned)} semantic inputs", flush=True)

    name = PUBLIC_COMPANIES_GEOMETRY.shard_name(PREFIX)
    zip_url = mirror.raw_file_url(MONTH, name)
    meta_url = f"{mirror.item_root(MONTH)}/{name}/_meta.json"

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        status, length = _head(client, zip_url)
    print(f"PRECHECK direct_head={status} length={length} metadata_listed={_listed(name)}", flush=True)
    if status != 404 or _listed(name) is True:
        raise RuntimeError(f"{name} is not safely absent before probe")

    OUT.mkdir(parents=True, exist_ok=True)
    with ShardPackSession(
        MONTH,
        PUBLIC_COMPANIES_GEOMETRY,
        batch_size=10_000,
        memory_limit_gb=12.0,
    ) as session:
        spec = session.materialization_spec(PREFIX, input_sha1s=pinned)
        artifact = session.pack(PREFIX, OUT, materialization=spec)

    local = artifact_identity(artifact.path)
    expected_id = spec.materialization_id()
    print(
        f"LOCAL size={local.size} sha1={local.sha1} sha256={local.sha256} "
        f"materialization_id={expected_id}",
        flush=True,
    )

    # Revalida os nove inputs imediatamente antes do único PUT do probe.
    current = materialization_input_sha1s(fetch_item_metadata(MONTH))
    if current != pinned:
        raise RuntimeError("semantic inputs changed before probe PUT")

    started = time.monotonic()
    _upload(name, artifact.path, access_key, secret_key)
    put_done = time.monotonic()
    print(f"PUT_DONE t={put_done - started:.3f}s", flush=True)

    direct_identity = None
    observations: list[dict] = []
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for attempt in range(1, 41):
            elapsed = time.monotonic() - put_done
            try:
                head_status, head_size = _head(client, zip_url)
            except httpx.HTTPError as exc:
                head_status, head_size = -1, None
                head_detail = type(exc).__name__
            else:
                head_detail = None
            try:
                meta_status, meta_matches = _meta(
                    client,
                    meta_url,
                    expected_id,
                    spec.as_document(),
                )
            except httpx.HTTPError as exc:
                meta_status, meta_matches = -1, False
                meta_detail = type(exc).__name__
            else:
                meta_detail = None
            listed = _listed(name)
            observation = {
                "attempt": attempt,
                "elapsed_s": round(elapsed, 3),
                "head_status": head_status,
                "head_size": head_size,
                "head_detail": head_detail,
                "meta_status": meta_status,
                "meta_matches": meta_matches,
                "meta_detail": meta_detail,
                "metadata_listed": listed,
            }
            observations.append(observation)
            print("OBS " + json.dumps(observation, sort_keys=True), flush=True)

            if head_status == 200 and head_size == local.size and meta_matches:
                try:
                    candidate = hash_remote_artifact(
                        MONTH,
                        name,
                        expected_size=local.size,
                        expected_sha1=local.sha1,
                    )
                except (httpx.HTTPError, RuntimeError) as exc:
                    print(f"DIRECT_HASH not ready: {type(exc).__name__}: {exc}", flush=True)
                else:
                    if candidate.sha256 != local.sha256:
                        raise RuntimeError("direct remote sha256 differs from local artifact")
                    direct_identity = candidate
                    print(
                        f"DIRECT_CONFIRMED elapsed={time.monotonic() - put_done:.3f}s "
                        f"metadata_listed={listed}",
                        flush=True,
                    )
                    break
            if attempt < 40:
                time.sleep(10)

    if direct_identity is None:
        raise RuntimeError("direct ZIP identity + materialization did not confirm")

    sidecar = ShardSidecar(
        snapshot=MONTH,
        shard=PREFIX,
        materialization_id=expected_id,
        artifact_name=name,
        artifact=direct_identity,
    )
    side_name = sidecar_name(PUBLIC_COMPANIES_GEOMETRY, PREFIX)
    side_path = OUT / side_name
    write_sidecar(side_path, sidecar)
    _upload(side_name, side_path, access_key, secret_key)

    side_url = mirror.raw_file_url(MONTH, side_name)
    side_confirmed = False
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for attempt in range(1, 41):
            try:
                response = client.get(side_url)
            except httpx.HTTPError as exc:
                print(f"SIDECAR_OBS {attempt}: {type(exc).__name__}", flush=True)
            else:
                print(
                    f"SIDECAR_OBS {attempt}: HTTP {response.status_code} bytes={len(response.content)}",
                    flush=True,
                )
                if response.status_code == 200:
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = None
                    if payload == sidecar.as_document():
                        side_confirmed = True
                        break
                elif response.status_code != 404:
                    raise RuntimeError(f"ambiguous sidecar response HTTP {response.status_code}")
            if attempt < 40:
                time.sleep(5)
    if not side_confirmed:
        raise RuntimeError("sidecar direct GET did not confirm")

    summary = {
        "prefix": PREFIX,
        "artifact": direct_identity.as_document(),
        "materialization_id": expected_id,
        "metadata_listed_at_direct_confirmation": _listed(name),
        "observations": observations,
        "sidecar": side_name,
    }
    Path("direct-shard-probe-98.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
