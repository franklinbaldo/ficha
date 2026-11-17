"""P0-D: fail-closed offline tests for no promotion of partial monthly shards.

Every path is a tmp_path fixture. No ETL, HTTP, Internet Archive or live manifest.
This file is independent of the implementation on the Castor #246 branch.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl import manifest as manifest_mod
from ficha_etl import monthly_sharded, shard_transfer
from ficha_etl.shard_publish import ShardPublishError, publish_one_shard
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY
from ficha_etl.shard_transfer import ShardTransferAction

MONTH = "2026-08"


def _identity(seed: str) -> dict:
    return {"size": 11, "sha1": seed * 40, "sha256": seed * 64}


def _descriptor() -> dict:
    return {
        "date": MONTH,
        "files": {
            name: _identity(str(i))
            for i, name in enumerate(
                (
                    "cnpjs",
                    "cnpj_contatos",
                    "cnpj_cnaes",
                    "raizes",
                    "socios",
                    "enderecos",
                    "pessoas",
                    "lookups",
                ),
                start=1,
            )
        },
        "lookups": {
            name: _identity(letter)
            for name, letter in zip(
                (
                    "cnaes",
                    "motivos",
                    "municipios",
                    "naturezas",
                    "paises",
                    "qualificacoes",
                ),
                "abcdef",
                strict=True,
            )
        },
    }


def _receipt(prefix: str) -> dict:
    return {
        "month": MONTH,
        "prefix": prefix,
        "name": f"companies-{prefix}.zip",
        "materialization_id": "f" * 64,
        "size": 42,
        "sha1": "d" * 40,
    }


def _setup(tmp_path: Path, *, missing_receipt: str | None = None):
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    for i in range(100):
        prefix = f"{i:02d}"
        if prefix != missing_receipt:
            (receipts / f"receipt-{prefix}.json").write_text(
                json.dumps(_receipt(prefix)), encoding="utf-8"
            )

    descriptor = tmp_path / "descriptor.json"
    descriptor.write_text(json.dumps(_descriptor()), encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    # Noncanonical bytes expose rewrites even if JSON semantics stay unchanged.
    before = b'{"current":"2026-07","snapshots":[{"date":"2026-07"}]}\n'
    manifest.write_bytes(before)
    args = argparse.Namespace(
        month=MONTH,
        descriptor=descriptor,
        receipts_dir=receipts,
        attempts=1,
        interval_seconds=0,
        snapshot_entry=tmp_path / "snapshot-entry.json",
        manifest=manifest,
        verification=tmp_path / "verification.json",
    )
    return args, before


def _assert_no_promotion(args: argparse.Namespace, before: bytes) -> None:
    assert args.manifest.read_bytes() == before
    assert not args.snapshot_entry.exists()
    assert not args.verification.exists()


def _verified(prefix: str):
    return SimpleNamespace(
        prefix=prefix,
        name=f"companies-{prefix}.zip",
        materialization_id="f" * 64,
        size=42,
        sha1="d" * 40,
        action=ShardTransferAction.VERIFIED,
    )


def test_missing_receipt_never_accesses_network_or_promotes(monkeypatch, tmp_path: Path):
    args, before = _setup(tmp_path, missing_receipt="42")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network verification must not run with missing receipt")

    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", forbidden)
    monkeypatch.setattr(monthly_sharded, "verify_all_shards", forbidden)
    with pytest.raises(FileNotFoundError):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


def test_partial_remote_verification_never_promotes(monkeypatch, tmp_path: Path):
    args, before = _setup(tmp_path)
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        monthly_sharded,
        "verify_all_shards",
        lambda *_a, **_kw: [_verified(f"{i:02d}") for i in range(100) if i != 42],
    )
    with pytest.raises((KeyError, RuntimeError, ValueError)):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


def test_remote_receipt_mismatch_never_promotes(monkeypatch, tmp_path: Path):
    args, before = _setup(tmp_path)
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    results = [_verified(f"{i:02d}") for i in range(100)]
    results[42].sha1 = "e" * 40
    monkeypatch.setattr(monthly_sharded, "verify_all_shards", lambda *_a, **_kw: results)
    with pytest.raises(RuntimeError, match="diverges from production receipt"):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


def test_remote_descriptor_sha_mismatch_blocks_promotion(monkeypatch, tmp_path: Path):
    """A base-file hash divergence must fail before shard verification/promotion."""
    args, before = _setup(tmp_path)
    expected = monthly_sharded._expected_remote_files(_descriptor())
    remote_files = [
        {"name": name, "size": size, "sha1": sha1} for name, (size, sha1) in expected.items()
    ]
    for entry in remote_files:
        if entry["name"] == "cnpjs.parquet":
            entry["sha1"] = "e" * 40
    monkeypatch.setattr(
        monthly_sharded,
        "fetch_item_metadata",
        lambda *_args, **_kwargs: {"files": remote_files},
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("shards must not be checked after descriptor mismatch")

    monkeypatch.setattr(monthly_sharded, "verify_all_shards", forbidden)
    with pytest.raises(RuntimeError, match="remote bytes diverge from production descriptor"):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


def test_submitted_but_unverified_shard_never_promotes(monkeypatch, tmp_path: Path):
    """All 100 names being present is insufficient without VERIFIED action."""
    args, before = _setup(tmp_path)
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    results = [_verified(f"{i:02d}") for i in range(100)]
    results[42].action = ShardTransferAction.SUBMITTED
    monkeypatch.setattr(monthly_sharded, "verify_all_shards", lambda *_a, **_kw: results)
    with pytest.raises(RuntimeError, match="42: verify returned submitted"):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


@pytest.mark.parametrize(
    "previous_entry",
    [None, b'{ "previous" : "preserve exact bytes" }\\n'],
)
def test_manifest_refusal_does_not_leave_orphan_snapshot_entry(
    monkeypatch, tmp_path: Path, previous_entry: bytes | None
):
    """Rejected publication must restore an old entry or leave it absent.

    The 100 shard inputs and failing manifest gate are synthetic and offline.
    """
    args, before = _setup(tmp_path)
    if previous_entry is not None:
        args.snapshot_entry.write_bytes(previous_entry)
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        monthly_sharded,
        "verify_all_shards",
        lambda *_a, **_kw: [_verified(f"{i:02d}") for i in range(100)],
    )

    def reject_legacy_manifest(*_args, **_kwargs):
        raise ValueError("legacy manifest.json rejects Protobuf 1.1")

    monkeypatch.setattr(monthly_sharded, "update_manifest", reject_legacy_manifest)
    with pytest.raises(ValueError, match="legacy manifest.json"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == before
    assert (
        args.snapshot_entry.read_bytes() if args.snapshot_entry.exists() else None
    ) == previous_entry
    assert not args.verification.exists()


@pytest.mark.parametrize("entrypoint", ("submit-one", "submit-prepared", "publish-one"))
def test_direct_transfer_api_must_refuse_unversioned_v11_wire(
    tmp_path: Path, entrypoint: str
) -> None:
    """All three public APIs must reject wire 1.1 before local/remote IO.

    No preflight CLI, network, pack, metadata, filesystem write or IA PUT.
    This tests the shared guard on the currently legacy companies-NN.zip API,
    NOT safe publication/discovery of a separate 1.1 namespace.
    """

    class V11Session:
        geometry = PUBLIC_COMPANIES_GEOMETRY
        month = MONTH
        wire_version = "1.1.0"
        shard_by = "cnpj_base_prefix_or_base36_mod_100_v1"

        def materialization_spec(self, *_args, **_kwargs):
            pytest.fail("materialization must not start before v1.1 rejection")

    def forbidden(*_args, **_kwargs):
        pytest.fail("I/O must not start before v1.1 rejection")

    kwargs = {
        "pinned_inputs": {},
        "fetch_metadata": forbidden,
        "fetch_meta": forbidden,
        "fetch_direct": forbidden,
        "upload": forbidden,
    }
    session = V11Session()
    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        if entrypoint == "submit-one":
            shard_transfer.submit_one_shard(session, "00", tmp_path, **kwargs)
        elif entrypoint == "submit-prepared":
            prepared = shard_transfer.PreparedShard(
                prefix="00",
                name="companies-00.zip",
                path=tmp_path / "must-not-read.zip",
                materialization_id="synthetic",
                size=12,
                sha1="a" * 40,
            )
            shard_transfer.submit_prepared_shard(session, prepared, **kwargs)
        else:
            publish_one_shard(session, "00", tmp_path, **kwargs)

    assert list(tmp_path.iterdir()) == []


def _prepare_legacy_finalize(monkeypatch: pytest.MonkeyPatch, args: argparse.Namespace) -> None:
    """Reach the real legacy manifest writer using 100 offline verified shards."""
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        monthly_sharded,
        "verify_all_shards",
        lambda *_a, **_kw: [_verified(f"{i:02d}") for i in range(100)],
    )
    # Keep the real writer reachable; production wire 1.1 is deliberately
    # rejected by the legacy gate and is not published by these tests.
    monkeypatch.setattr(
        monthly_sharded,
        "finalize_sharded_snapshot_entry",
        lambda *_a, **_kw: {
            "date": MONTH,
            "schema_version": "1.0.0",
            "files": {"companies": {"shard_by": "cnpj_base_prefix_2"}},
        },
    )


def test_manifest_replace_failure_preserves_bytes_and_no_snapshot_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exercise the new staged writer at os.replace through command_finalize.

    Unlike the old Path.write_text injection, this reaches the Castor #246
    implementation. The test fails if the staging/rollback is bypassed.
    """
    args, before = _setup(tmp_path)
    _prepare_legacy_finalize(monkeypatch, args)

    def interrupted_replace(src: str | Path, dst: str | Path) -> None:
        assert Path(src).parent == args.manifest.parent
        assert Path(src).is_file(), "staged manifest must exist before replacement"
        assert Path(dst) == args.manifest
        raise OSError("injected manifest os.replace failure")

    monkeypatch.setattr(manifest_mod.os, "replace", interrupted_replace)
    with pytest.raises(OSError, match="injected manifest os.replace failure"):
        monthly_sharded.command_finalize(args)

    _assert_no_promotion(args, before)
    assert list(args.manifest.parent.glob(".manifest.json.*.tmp")) == []


@pytest.mark.xfail(
    strict=True,
    reason="P0-D: verification.json fails after manifest commit; transaction boundary unresolved",
)
def test_verification_receipt_failure_must_not_promote_partial_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Known gap: a failed verification receipt follows a successful commit.

    The fault is injected only into the receipt writer after 100 synthetic
    verified shards, with no network or live publication. This is a contract
    counterexample, not proof of multi-file rollback.
    """
    args, before = _setup(tmp_path)
    _prepare_legacy_finalize(monkeypatch, args)
    original_write_json = monthly_sharded._write_json

    def fail_verification(path: Path, payload: object) -> None:
        if Path(path) == args.verification:
            raise OSError("injected verification.json write failure")
        original_write_json(path, payload)

    monkeypatch.setattr(monthly_sharded, "_write_json", fail_verification)
    with pytest.raises(OSError, match="injected verification.json write failure"):
        monthly_sharded.command_finalize(args)

    _assert_no_promotion(args, before)
