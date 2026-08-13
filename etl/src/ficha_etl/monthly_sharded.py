"""Orquestração permanente do snapshot mensal shardado.

Princípio central: identidade é produzida, não descoberta depois do upload.

1. ``produce`` transforma os dados e persiste o descriptor com hashes duplos;
2. ``prepare-shard`` materializa um ZIP e grava seu recibo antes de qualquer PUT;
3. ``submit-shard`` só pode enviar exatamente os bytes daquele recibo;
4. ``finalize`` compara IA ``size + sha1`` com descriptor/recibos e só então
   promove o snapshot no manifest público.

O módulo é intencionalmente invocável por ``python -m ficha_etl.monthly_sharded``
para que GitHub Actions consiga colocar ``actions/upload-artifact`` entre
produção e submissão, tornando o recibo durável antes da escrita remota.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import internetarchive as ia

from . import transform, upload
from .manifest import (
    CompanyShardIdentity,
    build_production_descriptor,
    finalize_sharded_snapshot_entry,
    update_manifest,
    write_production_descriptor,
)
from .publication_upload import submit_outputs_fail_closed
from .remote_reuse import fetch_item_metadata
from .shard_publish import ShardPublishError
from .shard_remote import (
    PUBLIC_COMPANIES_GEOMETRY,
    SHARD_INPUT_NAMES,
    fetch_remote_shard_meta,
)
from .shard_transfer import (
    PreparedShard,
    ShardTransferAction,
    prepare_one_shard,
    submit_prepared_shard,
    verify_all_shards,
)
from .sharded_pack import ShardPackSession
from .sources import is_valid_month

_STANDARD_FILE_NAMES = {
    "cnpjs": "cnpjs.parquet",
    "cnpj_contatos": "cnpj_contatos.parquet",
    "cnpj_cnaes": "cnpj_cnaes.parquet",
    "raizes": "raizes.parquet",
    "socios": "socios.parquet",
    "enderecos": "enderecos.parquet",
    "pessoas": "pessoas.parquet",
    "lookups": "lookups.json",
}


def _require_month(month: str) -> str:
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return month


def _require_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _valid_hex(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and value == value.lower()
        and all(char in "0123456789abcdef" for char in value)
    )


def _load_descriptor(path: Path, month: str) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("date") != month:
        raise ValueError(f"descriptor {path} does not describe {month}")
    files = payload.get("files")
    lookups = payload.get("lookups")
    if not isinstance(files, dict) or not isinstance(lookups, dict):
        raise ValueError("descriptor missing files/lookups")

    for key in _STANDARD_FILE_NAMES:
        entry = files.get(key)
        _validate_identity(f"files.{key}", entry)
    for kind in ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"):
        _validate_identity(f"lookups.{kind}", lookups.get(kind))
    return payload


def _validate_identity(label: str, entry: object) -> None:
    if not isinstance(entry, dict):
        raise ValueError(f"{label}: identity entry missing")
    try:
        size = int(entry.get("size"))
    except (TypeError, ValueError):
        size = 0
    if size <= 0:
        raise ValueError(f"{label}: invalid size")
    if not _valid_hex(entry.get("sha1"), 40):
        raise ValueError(f"{label}: invalid sha1")
    if not _valid_hex(entry.get("sha256"), 64):
        raise ValueError(f"{label}: invalid sha256")


def _expected_remote_files(descriptor: dict) -> dict[str, tuple[int, str]]:
    expected: dict[str, tuple[int, str]] = {}
    files = descriptor["files"]
    for key, name in _STANDARD_FILE_NAMES.items():
        entry = files[key]
        expected[name] = (int(entry["size"]), entry["sha1"])
    for kind, entry in descriptor["lookups"].items():
        expected[f"lookups/{kind}.parquet"] = (int(entry["size"]), entry["sha1"])
    return expected


def materialization_inputs_from_descriptor(descriptor: dict) -> dict[str, str]:
    """Extrai os nove SHA-1 semânticos do recibo local, nunca do remoto."""
    files = descriptor["files"]
    lookups = descriptor["lookups"]
    result = {
        "cnpjs.parquet": files["cnpjs"]["sha1"],
        "raizes.parquet": files["raizes"]["sha1"],
        "socios.parquet": files["socios"]["sha1"],
    }
    for name in SHARD_INPUT_NAMES:
        if name.startswith("lookups/"):
            kind = name.removeprefix("lookups/").removesuffix(".parquet")
            result[name] = lookups[kind]["sha1"]
    if tuple(result) != SHARD_INPUT_NAMES:
        raise ValueError(
            f"descriptor materialization inputs differ from canonical order: {tuple(result)!r}"
        )
    return result


def _remote_descriptor_state(
    descriptor: dict,
    metadata: dict | None,
) -> tuple[list[str], list[str]]:
    """Retorna (pending, mismatches) para os outputs-base no catálogo IA."""
    if metadata is None or not isinstance(metadata.get("files"), list):
        return ["<metadata>"], []
    by_name = {
        entry.get("name"): entry
        for entry in metadata["files"]
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }
    pending: list[str] = []
    mismatches: list[str] = []
    for name, (expected_size, expected_sha1) in _expected_remote_files(descriptor).items():
        entry = by_name.get(name)
        if entry is None:
            pending.append(name)
            continue
        try:
            actual_size = int(entry.get("size"))
        except (TypeError, ValueError):
            actual_size = 0
        actual_sha1 = entry.get("sha1")
        if actual_size <= 0 or not _valid_hex(actual_sha1, 40):
            pending.append(name)
            continue
        if actual_size != expected_size or actual_sha1 != expected_sha1:
            mismatches.append(
                f"{name}: size={actual_size}/{expected_size} sha1={actual_sha1}/{expected_sha1}"
            )
    return pending, mismatches


def wait_for_descriptor_remote(
    month: str,
    descriptor: dict,
    *,
    attempts: int,
    interval_s: float,
) -> dict:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        metadata = fetch_item_metadata(month)
        pending, mismatches = _remote_descriptor_state(descriptor, metadata)
        if mismatches:
            raise RuntimeError(
                "remote bytes diverge from production descriptor:\n" + "\n".join(mismatches)
            )
        if not pending:
            assert metadata is not None
            print(
                f"base descriptor verified in IA: {len(_expected_remote_files(descriptor))} files"
            )
            return metadata
        if attempt == attempts:
            raise RuntimeError(
                f"base descriptor still not observable after {attempts} attempts: {pending}"
            )
        print(
            f"base descriptor attempt {attempt}/{attempts}: pending={pending}; "
            f"retrying in {interval_s:.0f}s",
            flush=True,
        )
        time.sleep(interval_s)
    raise AssertionError("unreachable")


def _shard_uploader(month: str):
    access_key = _require_secret("IA_ACCESS_KEY")
    secret_key = _require_secret("IA_SECRET_KEY")
    identifier = f"ficha-{month}"

    def upload_one(name: str, path: Path) -> None:
        responses = ia.upload(
            identifier,
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

    return upload_one


def _receipt_payload(month: str, prepared: PreparedShard) -> dict:
    return {
        "month": month,
        "prefix": prepared.prefix,
        "name": prepared.name,
        "materialization_id": prepared.materialization_id,
        "size": prepared.size,
        "sha1": prepared.sha1,
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_receipt(path: Path, month: str, prefix: str) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_name = PUBLIC_COMPANIES_GEOMETRY.shard_name(prefix)
    if (
        not isinstance(payload, dict)
        or payload.get("month") != month
        or payload.get("prefix") != prefix
        or payload.get("name") != expected_name
    ):
        raise ValueError(f"invalid receipt for {month}/{prefix}: {path}")
    if int(payload.get("size", 0)) <= 0 or not _valid_hex(payload.get("sha1"), 40):
        raise ValueError(f"invalid byte identity in receipt: {path}")
    if not _valid_hex(payload.get("materialization_id"), 64):
        raise ValueError(f"invalid materialization_id in receipt: {path}")
    return payload


def command_produce(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    descriptor_path = Path(args.descriptor)

    if not args.skip_raw_upload:
        upload.stream_raw_zips_to_ia(
            month,
            access_key=_require_secret("IA_ACCESS_KEY"),
            secret_key=_require_secret("IA_SECRET_KEY"),
        )

    transform.transform_snapshot(
        month,
        cache_dir=cache_dir,
        output_dir=output_dir,
        skip_unimplemented=False,
        verify=not args.no_verify,
        verify_sample_size=args.verify_sample_size,
    )
    descriptor = build_production_descriptor(month, output_dir)
    write_production_descriptor(descriptor_path, descriptor)
    print(f"production descriptor written before output upload: {descriptor_path}")


def command_upload_outputs(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    descriptor = _load_descriptor(Path(args.descriptor), month)
    plan = submit_outputs_fail_closed(
        month,
        Path(args.output_dir),
        descriptor,
        access_key=_require_secret("IA_ACCESS_KEY"),
        secret_key=_require_secret("IA_SECRET_KEY"),
    )
    print(f"base outputs: upload={len(plan.upload)} reuse={len(plan.reuse)}")


def command_wait_inputs(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    descriptor = _load_descriptor(Path(args.descriptor), month)
    wait_for_descriptor_remote(
        month,
        descriptor,
        attempts=args.attempts,
        interval_s=args.interval_seconds,
    )


def command_prepare_shard(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    prefix = PUBLIC_COMPANIES_GEOMETRY.validate_prefix(args.prefix)
    descriptor = _load_descriptor(Path(args.descriptor), month)
    pinned = materialization_inputs_from_descriptor(descriptor)
    output_dir = Path(args.output_dir)
    with ShardPackSession(
        month,
        PUBLIC_COMPANIES_GEOMETRY,
        batch_size=10_000,
        memory_limit_gb=12.0,
    ) as session:
        prepared = prepare_one_shard(
            session,
            prefix,
            output_dir,
            pinned_inputs=pinned,
        )
    _write_json(Path(args.receipt), _receipt_payload(month, prepared))
    print(
        f"prepared {prepared.name}: size={prepared.size} sha1={prepared.sha1} "
        f"materialization_id={prepared.materialization_id}"
    )


def command_submit_shard(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    prefix = PUBLIC_COMPANIES_GEOMETRY.validate_prefix(args.prefix)
    descriptor = _load_descriptor(Path(args.descriptor), month)
    pinned = materialization_inputs_from_descriptor(descriptor)
    receipt = _load_receipt(Path(args.receipt), month, prefix)
    shard_path = Path(args.shard_path)
    prepared = PreparedShard(
        prefix=prefix,
        name=receipt["name"],
        path=shard_path,
        materialization_id=receipt["materialization_id"],
        size=int(receipt["size"]),
        sha1=receipt["sha1"],
    )
    session = ShardPackSession(month, PUBLIC_COMPANIES_GEOMETRY)
    result = submit_prepared_shard(
        session,
        prepared,
        pinned_inputs=pinned,
        fetch_metadata=lambda: fetch_item_metadata(month),
        fetch_meta=lambda name: fetch_remote_shard_meta(month, name),
        upload=_shard_uploader(month),
    )
    _write_json(
        Path(args.summary),
        {
            "month": month,
            "prefix": prefix,
            "action": str(result.action),
            "name": result.name,
            "materialization_id": result.materialization_id,
            "size": result.size,
            "sha1": result.sha1,
        },
    )
    print(f"{prefix}: {result.action} size={result.size} sha1={result.sha1}")


def _load_all_receipts(receipts_dir: Path, month: str) -> dict[str, dict]:
    receipts: dict[str, dict] = {}
    for prefix in PUBLIC_COMPANIES_GEOMETRY.prefixes():
        path = receipts_dir / f"receipt-{prefix}.json"
        receipts[prefix] = _load_receipt(path, month, prefix)
    extras = [p.name for p in receipts_dir.glob("receipt-*.json") if p.stem[8:] not in receipts]
    if extras:
        raise ValueError(f"unexpected shard receipts: {extras}")
    return receipts


def _retryable_shard_error(exc: ShardPublishError) -> bool:
    text = str(exc).lower()
    return "mismatch" not in text and "diverge" not in text


def command_finalize(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    descriptor = _load_descriptor(Path(args.descriptor), month)
    pinned = materialization_inputs_from_descriptor(descriptor)
    receipts = _load_all_receipts(Path(args.receipts_dir), month)

    # Primeiro fixa que os outputs-base servidos pelo IA são exatamente os
    # bytes do descriptor produzido localmente.
    wait_for_descriptor_remote(
        month,
        descriptor,
        attempts=args.attempts,
        interval_s=args.interval_seconds,
    )

    session = ShardPackSession(month, PUBLIC_COMPANIES_GEOMETRY)
    verified = None
    for attempt in range(1, args.attempts + 1):
        try:
            verified = verify_all_shards(
                session,
                pinned_inputs=pinned,
                fetch_metadata=lambda: fetch_item_metadata(month),
                fetch_meta=lambda name: fetch_remote_shard_meta(month, name),
            )
        except ShardPublishError as exc:
            if not _retryable_shard_error(exc) or attempt == args.attempts:
                raise
            print(
                f"shard verification attempt {attempt}/{args.attempts}: {exc}; "
                f"retrying in {args.interval_seconds:.0f}s",
                flush=True,
            )
            time.sleep(args.interval_seconds)
            continue
        break
    assert verified is not None

    by_prefix = {result.prefix: result for result in verified}
    identities: list[CompanyShardIdentity] = []
    for prefix in PUBLIC_COMPANIES_GEOMETRY.prefixes():
        receipt = receipts[prefix]
        result = by_prefix[prefix]
        if result.action is not ShardTransferAction.VERIFIED:
            raise RuntimeError(f"{prefix}: verify returned {result.action}")
        if (
            result.name != receipt["name"]
            or result.materialization_id != receipt["materialization_id"]
            or result.size != int(receipt["size"])
            or result.sha1 != receipt["sha1"]
        ):
            raise RuntimeError(
                f"{prefix}: remote verified identity diverges from production receipt"
            )
        identities.append(
            CompanyShardIdentity(
                shard=prefix,
                name=result.name,
                size=result.size,
                sha1=result.sha1,
            )
        )

    entry = finalize_sharded_snapshot_entry(descriptor, identities)
    _write_json(Path(args.snapshot_entry), entry)
    update_manifest(Path(args.manifest), entry)
    _write_json(
        Path(args.verification),
        {
            "month": month,
            "base_files_verified": len(_expected_remote_files(descriptor)),
            "company_shards_verified": len(identities),
            "identity_source": "production-descriptor-and-pre-upload-shard-receipts",
        },
    )
    print(f"snapshot {month} finalized from production identities: 100/100 shards")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("produce")
    p.add_argument("--month", required=True)
    p.add_argument("--cache-dir", default=".cache")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--descriptor", required=True)
    p.add_argument("--no-verify", action="store_true")
    p.add_argument("--verify-sample-size", type=int, default=1000)
    p.add_argument("--skip-raw-upload", action="store_true")
    p.set_defaults(func=command_produce)

    p = sub.add_parser("upload-outputs")
    p.add_argument("--month", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--descriptor", required=True)
    p.set_defaults(func=command_upload_outputs)

    p = sub.add_parser("wait-inputs")
    p.add_argument("--month", required=True)
    p.add_argument("--descriptor", required=True)
    p.add_argument("--attempts", type=int, default=30)
    p.add_argument("--interval-seconds", type=float, default=20.0)
    p.set_defaults(func=command_wait_inputs)

    p = sub.add_parser("prepare-shard")
    p.add_argument("--month", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--descriptor", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--receipt", required=True)
    p.set_defaults(func=command_prepare_shard)

    p = sub.add_parser("submit-shard")
    p.add_argument("--month", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--descriptor", required=True)
    p.add_argument("--shard-path", required=True)
    p.add_argument("--receipt", required=True)
    p.add_argument("--summary", required=True)
    p.set_defaults(func=command_submit_shard)

    p = sub.add_parser("finalize")
    p.add_argument("--month", required=True)
    p.add_argument("--descriptor", required=True)
    p.add_argument("--receipts-dir", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--snapshot-entry", required=True)
    p.add_argument("--verification", required=True)
    p.add_argument("--attempts", type=int, default=30)
    p.add_argument("--interval-seconds", type=float, default=30.0)
    p.set_defaults(func=command_finalize)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
