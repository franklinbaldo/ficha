from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pytest

from ficha_etl.materialization import MaterializationSpec, ShardRange
from ficha_etl.shard_publish import ShardPublishError, pin_materialization_inputs
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, SHARD_INPUT_NAMES
from ficha_etl.shard_transfer import (
    ShardTransferAction,
    submit_one_shard,
    verify_all_shards,
    verify_one_shard,
)
from ficha_etl.upload_identity import LocalIdentity


def _sha1(data: bytes) -> str:
    return hashlib.sha1(data, usedforsecurity=False).hexdigest()


def _inputs() -> list[dict]:
    return [
        {"name": name, "size": str(index), "sha1": f"{index:040x}"}
        for index, name in enumerate(SHARD_INPUT_NAMES, start=1)
    ]


@dataclass
class _Artifact:
    path: Path


class _Session:
    geometry = PUBLIC_COMPANIES_GEOMETRY
    month = "2026-05"

    def __init__(self) -> None:
        self.pack_calls: list[str] = []

    def materialization_spec(self, prefix: str, *, input_sha1s) -> MaterializationSpec:
        return MaterializationSpec(
            snapshot=self.month,
            shard_range=ShardRange(prefix),
            inputs=input_sha1s,
            descriptor_sha256="d" * 64,
        )

    def pack(self, prefix: str, output_dir: Path, *, materialization) -> _Artifact:
        self.pack_calls.append(prefix)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / self.geometry.shard_name(prefix)
        path.write_bytes((prefix + materialization.materialization_id()).encode())
        return _Artifact(path)


class _Remote:
    def __init__(self) -> None:
        self.visible: dict[str, bytes] = {}
        self.accepted: dict[str, bytes] = {}
        self.uploads: list[str] = []

    def metadata(self) -> dict:
        files = _inputs()
        for name, data in self.visible.items():
            files.append({"name": name, "size": str(len(data)), "sha1": _sha1(data)})
        return {"files": files}

    def direct(self, name: str) -> LocalIdentity | None:
        data = self.visible.get(name)
        if data is None:
            return None
        return LocalIdentity(size=len(data), sha1=_sha1(data))

    def upload_without_visibility(self, name: str, path: Path) -> None:
        self.accepted[name] = path.read_bytes()
        self.uploads.append(name)


def _spec(session: _Session, pinned: dict[str, str], prefix: str = "07") -> MaterializationSpec:
    return session.materialization_spec(prefix, input_sha1s=pinned)


def _meta(spec: MaterializationSpec) -> dict:
    return {
        "materialization": {
            "id": spec.materialization_id(),
            "spec": spec.as_document(),
        }
    }


def _prefix_from_name(name: str) -> str:
    return name.removeprefix("companies-").removesuffix(".zip")


def test_submit_returns_submitted_without_waiting_for_remote_visibility(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)

    result = submit_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=lambda _: None,
        upload=remote.upload_without_visibility,
        fetch_direct=remote.direct,
    )

    data = remote.accepted["companies-07.zip"]
    assert result.action is ShardTransferAction.SUBMITTED
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)
    assert remote.uploads == ["companies-07.zip"]
    assert session.pack_calls == ["07"]
    assert not (tmp_path / "out" / "companies-07.zip").exists()


def test_submit_existing_verified_shard_never_uploads(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _spec(session, pinned)
    remote.visible["companies-07.zip"] = b"already durable"

    result = submit_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=lambda _: _meta(spec),
        upload=lambda name, path: pytest.fail("verified shard cannot be overwritten"),
        fetch_direct=remote.direct,
    )

    assert result.action is ShardTransferAction.VERIFIED
    assert session.pack_calls == []
    assert remote.uploads == []


def test_submit_unknown_remote_never_packs_or_uploads(tmp_path):
    session = _Session()
    remote = _Remote()
    remote.visible["companies-07.zip"] = b"unknown materialization"
    pinned = pin_materialization_inputs(remote.metadata)

    with pytest.raises(ShardPublishError):
        submit_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: None,
            upload=lambda name, path: pytest.fail("unknown remote cannot be overwritten"),
            fetch_direct=remote.direct,
        )

    assert session.pack_calls == []
    assert remote.uploads == []


def test_verify_uses_ia_catalog_identity_without_direct_download():
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _spec(session, pinned)
    data = b"durable bytes represented by IA metadata"
    remote.visible["companies-07.zip"] = data

    result = verify_one_shard(
        session,
        "07",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=lambda _: _meta(spec),
    )

    assert result.action is ShardTransferAction.VERIFIED
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)


def test_verify_absent_is_read_only_failure():
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)

    with pytest.raises(ShardPublishError, match="metadata do Internet Archive"):
        verify_one_shard(
            session,
            "07",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: None,
        )

    assert session.pack_calls == []
    assert remote.uploads == []


def test_verify_mismatching_materialization_fails_closed():
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    remote.visible["companies-07.zip"] = b"wrong semantic identity"

    with pytest.raises(ShardPublishError):
        verify_one_shard(
            session,
            "07",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: {"materialization": {"id": "f" * 64, "spec": {}}},
        )


def test_verify_all_shards_reads_ia_catalog_exactly_once():
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)

    for prefix in session.geometry.prefixes():
        remote.visible[session.geometry.shard_name(prefix)] = f"durable-{prefix}".encode()

    metadata_calls = 0

    def metadata() -> dict:
        nonlocal metadata_calls
        metadata_calls += 1
        return remote.metadata()

    def meta(name: str) -> dict:
        return _meta(_spec(session, pinned, _prefix_from_name(name)))

    results = verify_all_shards(
        session,
        pinned_inputs=pinned,
        fetch_metadata=metadata,
        fetch_meta=meta,
    )

    assert metadata_calls == 1
    assert len(results) == 100
    assert [result.prefix for result in results] == list(session.geometry.prefixes())
    assert all(result.action is ShardTransferAction.VERIFIED for result in results)


def test_verify_all_shards_reports_incomplete_catalog_without_downloading():
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)

    for prefix in session.geometry.prefixes():
        if prefix != "42":
            remote.visible[session.geometry.shard_name(prefix)] = f"durable-{prefix}".encode()

    def meta(name: str) -> dict:
        return _meta(_spec(session, pinned, _prefix_from_name(name)))

    with pytest.raises(ShardPublishError, match=r"1/100 shards.*companies-42\.zip"):
        verify_all_shards(
            session,
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=meta,
        )
