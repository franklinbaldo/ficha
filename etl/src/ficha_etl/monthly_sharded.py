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
from typing import BinaryIO

import internetarchive as ia

from . import transform, upload
from .monthly_source_bag_gate import _write_receipt
from .monthly_source_authorization import (
    _SOURCE_SCHEMA,
    _bind_source_receipt,
    _require_source_attestation,
    _source_receipt_path,
)
from .cnpj import SHARD_BY as COMPANIES_SHARD_BY
from .pack import SCHEMA_VERSION as COMPANIES_WIRE_VERSION
from .manifest import (
    CompanyShardIdentity,
    build_production_descriptor,
    finalize_sharded_snapshot_entry,
    update_manifest,
    write_production_descriptor,
)
from .monthly_estabelecimento_gate import run_monthly_gate
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
    # A local production descriptor must obey the same strict byte-size
    # contract as the remote IA catalog; int(11.9) and int(True) are unsafe.
    size = _remote_positive_size(entry.get("size"))
    if size is None:
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


def _remote_positive_size(value: object) -> int | None:
    """Parse IA byte sizes without accepting fractional or boolean coercion."""
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        try:
            parsed = int(value)
        except ValueError:  # Python's integer-string digit limit
            return None
        return parsed if parsed > 0 else None
    return None


def _remote_descriptor_state(
    descriptor: dict,
    metadata: dict | None,
) -> tuple[list[str], list[str]]:
    """Retorna (pending, mismatches) para os outputs-base no catálogo IA."""
    if metadata is None or not isinstance(metadata.get("files"), list):
        return ["<metadata>"], []
    expected = _expected_remote_files(descriptor)
    by_name: dict[str, dict] = {}
    duplicates: set[str] = set()
    for entry in metadata["files"]:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or name not in expected:
            continue
        if name in by_name:
            duplicates.add(name)
        else:
            by_name[name] = entry

    pending: list[str] = []
    mismatches: list[str] = []
    for name, (expected_size, expected_sha1) in expected.items():
        if name in duplicates:
            pending.append(f"{name} (duplicado)")
            continue
        entry = by_name.get(name)
        if entry is None:
            pending.append(name)
            continue
        actual_size = _remote_positive_size(entry.get("size"))
        actual_sha1 = entry.get("sha1")
        if actual_size is None or not _valid_hex(actual_sha1, 40):
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

    def upload_one(name: str, source: Path | BinaryIO) -> None:
        # Prepared shards supply an already verified, anonymous open handle.
        # The legacy one-step publisher still passes a pathname.
        file_source = str(source) if isinstance(source, Path) else source
        responses = ia.upload(
            identifier,
            files={name: file_source},
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
    size = _remote_positive_size(payload.get("size"))
    if size is None or not _valid_hex(payload.get("sha1"), 40):
        raise ValueError(f"invalid byte identity in receipt: {path}")
    if not _valid_hex(payload.get("materialization_id"), 64):
        raise ValueError(f"invalid materialization_id in receipt: {path}")
    return payload


def command_produce(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    # Reject an incompatible wire contract before the first remote write.
    # The strict local transform must also finish before any raw ZIP PUT.
    if not args.skip_raw_upload:
        _require_safe_legacy_shard_upload()
    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    descriptor_path = Path(args.descriptor)

    # Even fetch/extract errors must invalidate a previous successful receipt.
    _write_receipt(
        _source_receipt_path(output_dir),
        {
            "schema": _SOURCE_SCHEMA,
            "month": month,
            "status": "failed",
            "identity_level": "parsed-duckdb-source-not-zip-custody",
            "reason": "produce has not completed source validation",
            "parts": [],
        },
    )

    transform.transform_snapshot(
        month,
        cache_dir=cache_dir,
        output_dir=output_dir,
        skip_unimplemented=False,
        verify=not args.no_verify,
        verify_sample_size=args.verify_sample_size,
        reject_empty_estabelecimentos=True,
    )

    # Fail closed on duplicate keys and invalid CNPJ check digits before
    # *any* raw ZIP PUT. Evidence survives gate failures as a job artifact.
    run_monthly_gate(
        month,
        output_dir / "cnpjs.parquet",
        output_dir.parent / "evidence" / "monthly-estabelecimento-key-gate.json",
        output_dir.parent / "monthly-estabelecimento-key-gate",
    )

    # Bind the parsed-source witness to the descriptor before ANY first PUT.
    # ZIP custody and retry pinning remain separate requirements under #281.
    descriptor = build_production_descriptor(month, output_dir)
    _bind_source_receipt(month, output_dir, descriptor)
    write_production_descriptor(descriptor_path, descriptor)

    if not args.skip_raw_upload:
        try:
            upload.stream_raw_zips_to_ia(
                month,
                access_key=_require_secret("IA_ACCESS_KEY"),
                secret_key=_require_secret("IA_SECRET_KEY"),
            )
        except Exception:
            # The descriptor may survive a partial remote raw upload, but may
            # never authorize independent derived uploads after that failure.
            descriptor["source_attestation"]["raw_upload_status"] = "failed"
            write_production_descriptor(descriptor_path, descriptor)
            raise
        descriptor["source_attestation"]["raw_upload_status"] = "complete"
        write_production_descriptor(descriptor_path, descriptor)
    # --skip-raw-upload deliberately leaves the descriptor pending. A future
    # publication requires a separately verified raw completion contract.
    print(f"production descriptor written before output upload: {descriptor_path}")


def command_upload_outputs(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    # A separately produced snapshot must not bypass the early guard.
    _require_safe_legacy_shard_upload()
    descriptor = _load_descriptor(Path(args.descriptor), month)
    _require_source_attestation(month, Path(args.output_dir), descriptor)
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


def _require_safe_legacy_shard_upload() -> None:
    """Do not publish 1.1 wire under ZIP names discoverable by v1 decoders.

    The monthly submit path still targets unversioned companies-NN.zip. An
    updated manifest gate runs only *after* submit, too late to protect v1
    consumers from a newly created 1.1 ZIP. Block before any remote access
    until a separate, versioned artifact namespace is implemented.
    """
    if COMPANIES_WIRE_VERSION != "1.0.0" or COMPANIES_SHARD_BY != "cnpj_base_prefix_2":
        raise ShardPublishError(
            "refusing unversioned companies-NN.zip upload of Protobuf "
            f"{COMPANIES_WIRE_VERSION} with shard mapping {COMPANIES_SHARD_BY}; "
            "separate versioned artifact URLs and opt-in discovery are required"
        )


def command_submit_shard(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    prefix = PUBLIC_COMPANIES_GEOMETRY.validate_prefix(args.prefix)
    _require_safe_legacy_shard_upload()
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


def _require_distinct_finalize_outputs(
    snapshot_entry_path: Path,
    verification_path: Path,
    manifest_path: Path,
    descriptor_path: Path,
    receipts_dir: Path,
    source_receipt: Path | None = None,
) -> None:
    """Reject aliasing of outputs with each other or immutable inputs.

    The descriptor and all 100 input shard receipts are protected before any
    descriptor read, receipt load, or remote verification starts.
    """
    paths = (
        ("descriptor", descriptor_path),
        ("snapshot-entry", snapshot_entry_path),
        ("verification", verification_path),
        ("manifest", manifest_path),
    )
    for index, (name, path) in enumerate(paths):
        for other_name, other in paths[index + 1 :]:
            same_path = path.resolve() == other.resolve()
            same_inode = path.exists() and other.exists() and path.samefile(other)
            if same_path or same_inode:
                raise ValueError(f"finalize output paths must be distinct: {name} and {other_name}")

    if source_receipt is not None:
        # Separate immutable input artifact. Do not let a symlink or hardlink
        # turn any output or the descriptor into this authority.
        for name, path in paths:
            if path.resolve() == source_receipt.resolve() or (
                path.exists() and source_receipt.exists() and path.samefile(source_receipt)
            ):
                raise ValueError(f"source receipt aliases finalize {name}")
        for prefix in PUBLIC_COMPANIES_GEOMETRY.prefixes():
            shard_receipt = receipts_dir / f"receipt-{prefix}.json"
            if shard_receipt.resolve() == source_receipt.resolve() or (
                shard_receipt.exists()
                and source_receipt.exists()
                and shard_receipt.samefile(source_receipt)
            ):
                raise ValueError(f"source receipt aliases shard receipt-{prefix}")

    # A receipt is read-only input. Guard it before _load_all_receipts so that
    # a successful finalize cannot overwrite its own production checkpoint.
    for prefix in PUBLIC_COMPANIES_GEOMETRY.prefixes():
        receipt = receipts_dir / f"receipt-{prefix}.json"
        for name, output in paths[1:]:
            same_path = output.resolve() == receipt.resolve()
            same_inode = output.exists() and receipt.exists() and output.samefile(receipt)
            if same_path or same_inode:
                raise ValueError(
                    f"finalize output paths must be distinct: {name} and receipt-{prefix}"
                )


def command_finalize(args: argparse.Namespace) -> None:
    month = _require_month(args.month)
    source_receipt = getattr(args, "source_receipt", None)
    if not source_receipt:
        raise ValueError("finalize requires --source-receipt from the producer artifact")
    source_receipt_path = Path(source_receipt)
    _require_distinct_finalize_outputs(
        Path(args.snapshot_entry),
        Path(args.verification),
        Path(args.manifest),
        Path(args.descriptor),
        Path(args.receipts_dir),
        source_receipt_path,
    )
    descriptor = _load_descriptor(Path(args.descriptor), month)
    # Must be a separately transported producer receipt, validated before GET/HEAD
    # and before building a candidate. The 100-shard gate still follows.
    _require_source_attestation(
        month,
        Path(args.descriptor).parent / "output",
        descriptor,
        receipt_path=source_receipt_path,
        require_run_id=True,
    )
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

    # Detect changes to the external artifact while 100 remote shards were
    # being verified. This narrows, but does not eliminate, TOCTOU if a local
    # adversary can replace evidence immediately after the second read.
    _require_source_attestation(
        month,
        Path(args.descriptor).parent / "output",
        descriptor,
        receipt_path=source_receipt_path,
        require_run_id=True,
    )
    entry = finalize_sharded_snapshot_entry(descriptor, identities)
    snapshot_entry_path = Path(args.snapshot_entry)
    verification_path = Path(args.verification)
    manifest_path = Path(args.manifest)
    # The manifest is the discovery commit point. Both local receipts must
    # exist before it changes; a failed receipt/manifest write rolls back
    # previous bytes instead of exposing an apparently finalized snapshot.
    previous_documents = {
        path: path.read_bytes() if path.exists() else None
        for path in (snapshot_entry_path, verification_path)
    }
    try:
        _write_json(snapshot_entry_path, entry)
        _write_json(
            verification_path,
            {
                "month": month,
                "base_files_verified": len(_expected_remote_files(descriptor)),
                "company_shards_verified": len(identities),
                "identity_source": "production-descriptor-and-pre-upload-shard-receipts",
            },
        )
        update_manifest(manifest_path, entry)
    except Exception:
        # Local compensation only. This is not a crash-safe transaction across
        # three files or a remote IA promotion.
        for path, previous in previous_documents.items():
            if previous is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(previous)
        raise
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
    p.add_argument("--source-receipt", required=True)
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
