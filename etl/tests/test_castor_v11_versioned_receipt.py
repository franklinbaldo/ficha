"""Offline adversarial tests for Protobuf 1.1 local shard identity planning.

No ETL, network request, remote upload or public discovery is exercised.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl.shard_publish import ShardPublishError
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY
from ficha_etl.shard_transfer import (
    PreparedShard,
    plan_opt_in_v11_shard,
    submit_prepared_shard,
)
from ficha_etl.sharded_pack import ShardGeometry


class _Spec:
    def materialization_id(self) -> str:
        return "a" * 64


def _session(
    *,
    wire: str = "1.1.0",
    mapping: str = "cnpj_base_prefix_or_base36_mod_100_v1",
    geometry: ShardGeometry = PUBLIC_COMPANIES_GEOMETRY,
):
    return SimpleNamespace(
        geometry=geometry,
        wire_version=wire,
        shard_by=mapping,
        materialization_spec=lambda prefix, *, input_sha1s: _Spec(),
    )


def _prepared(tmp_path: Path) -> PreparedShard:
    path = tmp_path / "companies-42.zip"
    payload = b"synthetic Protobuf 1.1 ZIP identity: no production bytes"
    path.write_bytes(payload)
    return PreparedShard(
        prefix="42",
        name="companies-42.zip",
        path=path,
        materialization_id="a" * 64,
        size=len(payload),
        sha1=hashlib.sha1(payload, usedforsecurity=False).hexdigest(),
    )


def test_local_versioned_plan_keeps_identity_and_separates_legacy_name(tmp_path: Path) -> None:
    original = _prepared(tmp_path)
    before = original.path.read_bytes()
    result = plan_opt_in_v11_shard(_session(), original, pinned_inputs={})

    assert result.name == "wire-1.1/companies-42.zip"
    assert result.prefix == original.prefix
    assert result.path == original.path
    assert result.size == original.size
    assert result.sha1 == original.sha1
    assert result.materialization_id == original.materialization_id
    assert original.name == "companies-42.zip"
    assert original.path.read_bytes() == before
    assert not (tmp_path / "wire-1.1").exists()


@pytest.mark.parametrize(
    ("wire", "mapping"),
    [
        ("1.0.0", "cnpj_base_prefix_2"),
        ("1.0.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("1.1.0", "cnpj_base_prefix_2"),
        ("1.2.0", "cnpj_base_prefix_or_base36_mod_100_v1"),
        ("unknown", "unknown"),
    ],
)
def test_unknown_or_legacy_contract_fails_before_local_io(
    tmp_path: Path, wire: str, mapping: str
) -> None:
    missing = PreparedShard(
        prefix="42",
        name="companies-42.zip",
        path=tmp_path / "not-created.zip",
        materialization_id="a" * 64,
        size=1,
        sha1="0" * 40,
    )
    with pytest.raises(ShardPublishError, match="opt-in wire-1.1"):
        plan_opt_in_v11_shard(_session(wire=wire, mapping=mapping), missing, pinned_inputs={})
    assert not missing.path.exists()


@pytest.mark.parametrize(
    ("field", "replacement", "pattern"),
    [
        ("name", "wire-1.1/companies-42.zip", "prepared name"),
        ("name", "companies-43.zip", "prepared name"),
        ("materialization_id", "b" * 64, "materialization_id"),
        ("size", -1, "bytes locais mudaram"),
        ("sha1", "0" * 40, "bytes locais mudaram"),
        ("prefix", "AA", "ASCII digits"),
    ],
)
def test_tampered_receipt_rejected_without_moving_bytes(
    tmp_path: Path, field: str, replacement: object, pattern: str
) -> None:
    original = _prepared(tmp_path)
    before = original.path.read_bytes()
    modified = replace(original, **{field: replacement})
    with pytest.raises((ValueError, ShardPublishError), match=pattern):
        plan_opt_in_v11_shard(_session(), modified, pinned_inputs={})
    assert original.path.read_bytes() == before
    assert not (tmp_path / "wire-1.1").exists()


def test_missing_or_modified_local_bytes_rejected(tmp_path: Path) -> None:
    original = _prepared(tmp_path)
    original.path.write_bytes(b"changed after receipt")
    with pytest.raises(ShardPublishError, match="bytes locais mudaram"):
        plan_opt_in_v11_shard(_session(), original, pinned_inputs={})
    original.path.unlink()
    with pytest.raises(ShardPublishError, match="prepared file ausente"):
        plan_opt_in_v11_shard(_session(), original, pinned_inputs={})


def test_wrong_geometry_rejected(tmp_path: Path) -> None:
    with pytest.raises(ShardPublishError, match="geometria de publicação"):
        plan_opt_in_v11_shard(
            _session(geometry=ShardGeometry(3)), _prepared(tmp_path), pinned_inputs={}
        )


def test_legacy_submitter_rejects_1_1_plan_before_network_or_put(tmp_path: Path) -> None:
    session = _session()
    versioned = plan_opt_in_v11_shard(session, _prepared(tmp_path), pinned_inputs={})
    called: list[str] = []

    def forbidden(*args, **kwargs):
        called.append("I/O")
        raise AssertionError("legacy submitter tried remote I/O")

    with pytest.raises(ShardPublishError, match="refusing unversioned"):
        submit_prepared_shard(
            session,
            versioned,
            pinned_inputs={},
            fetch_metadata=forbidden,
            fetch_meta=forbidden,
            upload=forbidden,
            fetch_direct=forbidden,
        )
    assert called == []
