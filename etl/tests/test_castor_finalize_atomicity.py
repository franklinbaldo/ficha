"""P0-A: rejected manifest cannot leave a misleading local snapshot-entry.

Synthetic, fully verified 100-shard inputs; no HTTP, IA or production ETL.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl import monthly_sharded
from ficha_etl.shard_transfer import ShardTransferAction


def _args(tmp_path: Path) -> argparse.Namespace:
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"current":"2026-07","snapshots":[{"date":"2026-07"}]}\n')
    return argparse.Namespace(
        month="2026-08",
        descriptor=tmp_path / "descriptor.json",
        receipts_dir=tmp_path / "receipts",
        attempts=1,
        interval_seconds=0,
        manifest=manifest,
        snapshot_entry=tmp_path / "snapshot-entry.json",
        verification=tmp_path / "verification.json",
    )


def _mock_offline_inputs(monkeypatch: pytest.MonkeyPatch, *, alpha: bool) -> None:
    month = "2026-08"
    receipts = {
        f"{i:02d}": {
            "name": f"companies-{i:02d}.zip",
            "materialization_id": "f" * 64,
            "size": 42,
            "sha1": "d" * 40,
        }
        for i in range(100)
    }
    verified = [
        SimpleNamespace(
            prefix=prefix,
            name=receipt["name"],
            materialization_id=receipt["materialization_id"],
            size=receipt["size"],
            sha1=receipt["sha1"],
            action=ShardTransferAction.VERIFIED,
        )
        for prefix, receipt in receipts.items()
    ]
    layer = {
        "shard_by": ("cnpj_base_prefix_or_base36_mod_100_v1" if alpha else "cnpj_base_prefix_2"),
        "schema_version": "1.1.0" if alpha else "1.0.0",
    }
    entry = {"date": month, "files": {"companies": layer}}

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network access is forbidden in fixture")

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", lambda *_: {"date": month})
    monkeypatch.setattr(monthly_sharded, "materialization_inputs_from_descriptor", lambda _: {})
    monkeypatch.setattr(monthly_sharded, "_load_all_receipts", lambda *_: receipts)
    monkeypatch.setattr(monthly_sharded, "ShardPackSession", lambda *_a: object())
    monkeypatch.setattr(monthly_sharded, "wait_for_descriptor_remote", lambda *_a, **_kw: {})
    monkeypatch.setattr(monthly_sharded, "verify_all_shards", lambda *_a, **_kw: verified)
    monkeypatch.setattr(monthly_sharded, "fetch_item_metadata", forbidden)
    monkeypatch.setattr(monthly_sharded, "fetch_remote_shard_meta", forbidden)
    monkeypatch.setattr(monthly_sharded, "finalize_sharded_snapshot_entry", lambda *_a: entry)
    monkeypatch.setattr(monthly_sharded, "_expected_remote_files", lambda _: {})


@pytest.mark.parametrize("preexisting", [False, True])
def test_legacy_gate_refusal_rolls_back_snapshot_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, preexisting: bool
) -> None:
    args = _args(tmp_path)
    _mock_offline_inputs(monkeypatch, alpha=True)
    original_manifest = args.manifest.read_bytes()
    original_entry = b'{"old":"keep byte-for-byte"}\n' if preexisting else None
    if original_entry is not None:
        args.snapshot_entry.write_bytes(original_entry)

    with pytest.raises(ValueError, match="legacy manifest.json"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == original_manifest
    assert (
        args.snapshot_entry.read_bytes() if args.snapshot_entry.exists() else None
    ) == original_entry
    assert not args.verification.exists()


def test_simulated_gate_failure_restores_preexisting_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    _mock_offline_inputs(monkeypatch, alpha=False)
    previous = b'{"existing":"unformatted"}\n'
    args.snapshot_entry.write_bytes(previous)
    original_manifest = args.manifest.read_bytes()

    def refuse(*_args):
        assert args.snapshot_entry.exists()
        assert args.snapshot_entry.read_bytes() != previous
        raise ValueError("synthetic publication refusal")

    monkeypatch.setattr(monthly_sharded, "update_manifest", refuse)
    with pytest.raises(ValueError, match="synthetic publication refusal"):
        monthly_sharded.command_finalize(args)

    assert args.snapshot_entry.read_bytes() == previous
    assert args.manifest.read_bytes() == original_manifest
    assert not args.verification.exists()


def test_legacy_success_persists_entry_and_verification(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    _mock_offline_inputs(monkeypatch, alpha=False)
    monthly_sharded.command_finalize(args)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    entry = json.loads(args.snapshot_entry.read_text(encoding="utf-8"))
    verification = json.loads(args.verification.read_text(encoding="utf-8"))
    assert manifest["current"] == args.month
    assert entry["files"]["companies"]["schema_version"] == "1.0.0"
    assert verification["company_shards_verified"] == 100
