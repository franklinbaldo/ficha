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

from ficha_etl import monthly_sharded
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
                ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"),
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
        {"name": name, "size": size, "sha1": sha1}
        for name, (size, sha1) in expected.items()
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
    with pytest.raises(
        RuntimeError, match="remote bytes diverge from production descriptor"
    ):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)


def test_submitted_but_unverified_shard_never_promotes(monkeypatch, tmp_path: Path):
    """All 100 names being present is insufficient without VERIFIED action."""
    args, before = _setup(tmp_path)
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    results = [_verified(f"{i:02d}") for i in range(100)]
    results[42].action = ShardTransferAction.SUBMITTED
    monkeypatch.setattr(
        monthly_sharded, "verify_all_shards", lambda *_a, **_kw: results
    )
    with pytest.raises(RuntimeError, match="42: verify returned submitted"):
        monthly_sharded.command_finalize(args)
    _assert_no_promotion(args, before)

