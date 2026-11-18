"""P0-A: a manifest-only guard is too late to prevent uploading 1.1 wire.

All inputs are tiny offline fixtures; no credentials, network or IA requests.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from ficha_etl import monthly_sharded
from ficha_etl.shard_publish import ShardPublishError


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        month="2026-08",
        prefix="00",
        descriptor=tmp_path / "descriptor-never-read.json",
        receipt=tmp_path / "receipt-never-read.json",
        shard_path=tmp_path / "shard-never-read.zip",
        summary=tmp_path / "summary-never-created.json",
    )


@pytest.mark.parametrize(
    ("wire_version", "shard_mapping"),
    [
        ("1.1.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
    ],
)
def test_submit_refuses_incompatible_wire_before_any_remote_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wire_version: str,
    shard_mapping: str,
) -> None:
    args = _args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", wire_version)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", shard_mapping)

    def no_io(*_args: object, **_kwargs: object) -> None:
        pytest.fail("unversioned 1.1 submission must fail before IO/network")

    for name in (
        "_load_descriptor",
        "_load_receipt",
        "submit_prepared_shard",
        "fetch_item_metadata",
        "fetch_remote_shard_meta",
        "_shard_uploader",
    ):
        monkeypatch.setattr(monthly_sharded, name, no_io)

    with pytest.raises(ShardPublishError, match="unversioned companies-NN.zip"):
        monthly_sharded.command_submit_shard(args)

    assert not args.summary.exists()


def test_unchanged_legacy_wire_passes_only_the_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    monkeypatch.setattr(monthly_sharded, "COMPANIES_WIRE_VERSION", "1.0.0")
    monkeypatch.setattr(monthly_sharded, "COMPANIES_SHARD_BY", "cnpj_base_prefix_2")

    class ReachedDescriptor(Exception):
        pass

    def stop_before_io(*_args: object, **_kwargs: object) -> None:
        raise ReachedDescriptor

    monkeypatch.setattr(monthly_sharded, "_load_descriptor", stop_before_io)
    with pytest.raises(ReachedDescriptor):
        monthly_sharded.command_submit_shard(args)

    assert not args.summary.exists()
