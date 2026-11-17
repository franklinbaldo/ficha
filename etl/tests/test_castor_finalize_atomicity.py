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


@pytest.mark.parametrize("preexisting", [False, True])
@pytest.mark.parametrize("partial_write", [False, True])
def test_verification_write_failure_never_promotes_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    preexisting: bool,
    partial_write: bool,
) -> None:
    args = _args(tmp_path)
    _mock_offline_inputs(monkeypatch, alpha=False)
    previous_manifest = args.manifest.read_bytes()
    previous_entry = b'{"old":"entry"}\n' if preexisting else None
    previous_verification = b'{"old":"verified"}\n' if preexisting else None
    if preexisting:
        args.snapshot_entry.write_bytes(previous_entry)
        args.verification.write_bytes(previous_verification)

    original_write = monthly_sharded._write_json

    def interrupted(path: Path, payload: object) -> None:
        if Path(path) == args.verification:
            if partial_write:
                Path(path).write_bytes(b'{"incomplete":')
            raise OSError("injected verification write failure")
        original_write(path, payload)

    monkeypatch.setattr(monthly_sharded, "_write_json", interrupted)
    with pytest.raises(OSError, match="injected verification write failure"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == previous_manifest
    assert (
        args.snapshot_entry.read_bytes() if args.snapshot_entry.exists() else None
    ) == previous_entry
    assert (
        args.verification.read_bytes() if args.verification.exists() else None
    ) == previous_verification


def test_manifest_refusal_restores_previous_verification_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    _mock_offline_inputs(monkeypatch, alpha=True)
    previous_manifest = args.manifest.read_bytes()
    previous_entry = b'{"old":"entry"}\n'
    previous_verification = b'{"old":"verified"}\n'
    args.snapshot_entry.write_bytes(previous_entry)
    args.verification.write_bytes(previous_verification)

    with pytest.raises(ValueError, match="legacy manifest.json"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == previous_manifest
    assert args.snapshot_entry.read_bytes() == previous_entry
    assert args.verification.read_bytes() == previous_verification


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


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("snapshot_entry", "verification"),
        ("snapshot_entry", "manifest"),
        ("verification", "manifest"),
    ],
)
def test_finalize_rejects_identical_output_paths_before_remote_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    left: str,
    right: str,
) -> None:
    args = _args(tmp_path)
    setattr(args, right, getattr(args, left))
    old_manifest = (tmp_path / "manifest.json").read_bytes()
    monkeypatch.setattr(
        monthly_sharded,
        "_load_descriptor",
        lambda *_: pytest.fail("alias must be rejected before descriptor access"),
    )

    with pytest.raises(ValueError, match="finalize output paths must be distinct"):
        monthly_sharded.command_finalize(args)

    assert (tmp_path / "manifest.json").read_bytes() == old_manifest
    assert not (tmp_path / "snapshot-entry.json").exists()
    assert not (tmp_path / "verification.json").exists()


@pytest.mark.parametrize("link_type", ["symlink", "hardlink"])
def test_finalize_rejects_linked_output_paths_before_remote_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    link_type: str,
) -> None:
    args = _args(tmp_path)
    old_manifest = args.manifest.read_bytes()
    try:
        if link_type == "symlink":
            args.verification.symlink_to(args.manifest)
        else:
            args.verification.hardlink_to(args.manifest)
    except (OSError, NotImplementedError):
        pytest.skip(f"{link_type} unavailable on this filesystem")
    monkeypatch.setattr(
        monthly_sharded,
        "_load_descriptor",
        lambda *_: pytest.fail("alias must be rejected before descriptor access"),
    )

    with pytest.raises(ValueError, match="finalize output paths must be distinct"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == old_manifest
    assert args.verification.read_bytes() == old_manifest
    assert not args.snapshot_entry.exists()


def test_finalize_rejects_lexical_alias_before_remote_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)
    args.verification = tmp_path / "uncreated-child" / ".." / "manifest.json"
    old_manifest = args.manifest.read_bytes()
    monkeypatch.setattr(
        monthly_sharded,
        "_load_descriptor",
        lambda *_: pytest.fail("alias must be rejected before descriptor access"),
    )

    with pytest.raises(ValueError, match="finalize output paths must be distinct"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == old_manifest
    assert not args.snapshot_entry.exists()


@pytest.mark.parametrize("output", ["snapshot_entry", "verification", "manifest"])
def test_finalize_rejects_descriptor_as_output_before_io(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, output: str
) -> None:
    args = _args(tmp_path)
    args.descriptor = getattr(args, output)
    previous_manifest = args.manifest.read_bytes()
    monkeypatch.setattr(
        monthly_sharded,
        "_load_descriptor",
        lambda *_: pytest.fail("descriptor alias must be rejected before reading"),
    )

    with pytest.raises(ValueError, match="finalize output paths must be distinct"):
        monthly_sharded.command_finalize(args)

    assert args.manifest.read_bytes() == previous_manifest
    assert not args.snapshot_entry.exists()
    assert not args.verification.exists()


@pytest.mark.parametrize("link_type", ["symlink", "hardlink"])
def test_finalize_rejects_descriptor_linked_to_output_before_io(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, link_type: str
) -> None:
    args = _args(tmp_path)
    original_descriptor = b'{"date":"2026-08","source":"preserve"}\n'
    args.descriptor.write_bytes(original_descriptor)
    try:
        if link_type == "symlink":
            args.verification.symlink_to(args.descriptor)
        else:
            args.verification.hardlink_to(args.descriptor)
    except (OSError, NotImplementedError):
        pytest.skip(f"{link_type} unavailable on this filesystem")
    previous_manifest = args.manifest.read_bytes()
    monkeypatch.setattr(
        monthly_sharded,
        "_load_descriptor",
        lambda *_: pytest.fail("descriptor alias must be rejected before reading"),
    )

    with pytest.raises(ValueError, match="finalize output paths must be distinct"):
        monthly_sharded.command_finalize(args)

    assert args.descriptor.read_bytes() == original_descriptor
    assert args.verification.read_bytes() == original_descriptor
    assert args.manifest.read_bytes() == previous_manifest
    assert not args.snapshot_entry.exists()
