"""Transferência operacional de companies shards para 2026-05 (#110/#171/#189).

Não roda transform e nunca constrói ``companies.zip`` monolítico. Cada job
SUBMIT trata exatamente um prefixo e pode fazer no máximo um PUT. O retorno
``SUBMITTED`` é deliberadamente provisório: não significa checkpoint completo.

VERIFY é uma fase separada e estritamente read-only. Só aceita um shard quando
``size + sha1`` dos bytes remotos e o ``MaterializationSpec`` do ``_meta.json``
concordam exatamente. Apenas resultados VERIFIED podem alimentar o manifesto.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import internetarchive as ia

from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import ShardPublishError, pin_materialization_inputs
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, fetch_remote_shard_meta
from ficha_etl.shard_transfer import ShardTransferAction, submit_one_shard, verify_one_shard
from ficha_etl.sharded_pack import ShardPackSession

MONTH = "2026-05"
IDENTIFIER = f"ficha-{MONTH}"
OUTPUT_DIR = Path(".ops-shards-2026-05")
SUMMARY_PATH = Path("shard-transfer-summary.json")


def _require_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _metadata() -> dict | None:
    return fetch_item_metadata(MONTH)


def _fetch_meta(name: str) -> object | None:
    return fetch_remote_shard_meta(MONTH, name)


def _uploader(access_key: str, secret_key: str):
    def upload(name: str, path: Path) -> None:
        if not path.exists() or path.stat().st_size <= 0:
            raise RuntimeError(f"refusing missing/empty local file: {path}")
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
    }


def _write_summary(mode: str, prefix: str, *, result=None, error: str | None = None) -> None:
    payload = {
        "month": MONTH,
        "mode": mode,
        "prefix": prefix,
        "result": _serialize(result) if result is not None else None,
        "error": error,
    }
    SUMMARY_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


def _prefix(raw: str) -> str:
    return PUBLIC_COMPANIES_GEOMETRY.validate_prefix(raw)


def run_submit(prefix: str) -> None:
    """Faz no máximo um PUT e nunca espera consistência read-after-write."""
    access_key = _require_secret("IA_ACCESS_KEY")
    secret_key = _require_secret("IA_SECRET_KEY")
    upload = _uploader(access_key, secret_key)
    pinned = pin_materialization_inputs(_metadata)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        with _session() as session:
            result = submit_one_shard(
                session,
                prefix,
                OUTPUT_DIR,
                pinned_inputs=pinned,
                fetch_metadata=_metadata,
                fetch_meta=_fetch_meta,
                upload=upload,
            )
    except Exception as exc:
        _write_summary("submit", prefix, error=f"{type(exc).__name__}: {exc}")
        raise

    if result.action not in (ShardTransferAction.SUBMITTED, ShardTransferAction.VERIFIED):
        raise RuntimeError(f"unexpected submit action: {result.action}")
    _write_summary("submit", prefix, result=result)


def _retryable_verify_error(exc: ShardPublishError) -> bool:
    text = str(exc).lower()
    # Mismatch é evidência positiva de divergência e não melhora esperando.
    if "mismatch" in text or "diverge" in text:
        return False
    # ABSENT/UNKNOWN/timeout de observação são exatamente o lag eventual que a
    # fase read-only pode esperar sem criar risco de nova escrita.
    return True


def run_verify(prefix: str, *, attempts: int, interval_s: float) -> None:
    """Verifica read-only com retry apenas de estados ainda não observáveis."""
    if attempts < 1:
        raise ValueError("--attempts must be >= 1")
    if interval_s < 0:
        raise ValueError("--interval-seconds must be >= 0")

    pinned = pin_materialization_inputs(_metadata)
    session = _session()  # materialization_spec não requer abrir DuckDB/lookups.
    last_error: ShardPublishError | None = None

    for attempt in range(1, attempts + 1):
        try:
            result = verify_one_shard(
                session,
                prefix,
                pinned_inputs=pinned,
                fetch_metadata=_metadata,
                fetch_meta=_fetch_meta,
            )
        except ShardPublishError as exc:
            last_error = exc
            if not _retryable_verify_error(exc) or attempt == attempts:
                _write_summary("verify", prefix, error=f"{type(exc).__name__}: {exc}")
                raise
            print(
                f"VERIFY {prefix} attempt {attempt}/{attempts} not ready: {exc}; "
                f"retrying in {interval_s:.0f}s",
                flush=True,
            )
            time.sleep(interval_s)
            continue

        if result.action is not ShardTransferAction.VERIFIED:
            raise RuntimeError(f"verify returned non-final action: {result.action}")
        _write_summary("verify", prefix, result=result)
        return

    assert last_error is not None
    raise last_error


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--submit")
    mode.add_argument("--verify")
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    args = parser.parse_args()

    if args.submit is not None:
        run_submit(_prefix(args.submit))
    else:
        run_verify(
            _prefix(args.verify),
            attempts=args.attempts,
            interval_s=args.interval_seconds,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
