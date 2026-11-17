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

from ficha_etl import monthly_sharded, shard_transfer
from ficha_etl.materialization import MaterializationSpec, ShardRange
from ficha_etl.shard_publish import ShardPublishError
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, ShardReuseState
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


@pytest.mark.xfail(
    strict=True,
    reason="P0-A: direct shard_transfer API still lacks a universal v1.1 pre-PUT guard",
)
def test_direct_transfer_api_must_refuse_unversioned_v11_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Known gap: bypassing the monthly CLI must never PUT v1.1 to a v1 ZIP name.

    Purely offline transfer simulation. XFAIL documents the missing boundary;
    it is NOT evidence that the unversioned publish path is safe.
    """

    class V11Session:
        geometry = PUBLIC_COMPANIES_GEOMETRY
        month = MONTH
        schema_version = "1.1.0"
        shard_by = "cnpj_base_prefix_or_base36_mod_100_v1"

        def materialization_spec(self, prefix: str, *, input_sha1s: dict):
            return MaterializationSpec(
                snapshot=self.month,
                shard_range=ShardRange(prefix),
                inputs=input_sha1s,
                descriptor_sha256="f" * 64,
            )

        def pack(self, prefix: str, output_dir: Path, *, materialization):
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / self.geometry.shard_name(prefix)
            path.write_bytes(b"synthetic-protobuf-v11-wire-only")
            return SimpleNamespace(path=path)

    transfer = shard_transfer

    def always_absent(*_args, **_kwargs):
        return SimpleNamespace(state=ShardReuseState.ABSENT, name="companies-00.zip", detail="")

    monkeypatch.setattr(transfer, "_metadata_still_matches", lambda *_a, **_kw: {})
    monkeypatch.setattr(transfer, "_classify", always_absent)
    monkeypatch.setattr(transfer, "_reconcile_catalog_absent", lambda *_a, **_kw: None)

    uploads: list[str] = []

    def record_fake_upload(name: str, _path: Path) -> None:
        uploads.append(name)

    with pytest.raises(ShardPublishError, match=r"1\.1|wire|unversioned|namespace"):
        shard_transfer.submit_one_shard(
            V11Session(),
            "00",
            tmp_path / "shards",
            pinned_inputs={},
            fetch_metadata=lambda: {},
            fetch_meta=lambda _name: None,
            fetch_direct=lambda _name: None,
            upload=record_fake_upload,
        )

    assert uploads == []
