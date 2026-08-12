from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from ficha_etl.materialization import MaterializationSpec, ShardRange
from ficha_etl.shard_publish import (
    ShardPublishAction,
    ShardPublishError,
    pin_materialization_inputs,
    publish_one_shard,
)
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, SHARD_INPUT_NAMES
from ficha_etl.shard_sidecar import (
    ArtifactIdentity,
    ShardSidecar,
    SidecarObservationError,
    sidecar_name,
)


def _input_entries() -> list[dict]:
    return [
        {"name": name, "size": str(index), "sha1": f"{index:040x}"}
        for index, name in enumerate(SHARD_INPUT_NAMES, start=1)
    ]


def _identity(data: bytes) -> ArtifactIdentity:
    return ArtifactIdentity(
        size=len(data),
        sha1=hashlib.sha1(data, usedforsecurity=False).hexdigest(),
        sha256=hashlib.sha256(data).hexdigest(),
    )


@dataclass
class _Artifact:
    path: Path


class _Session:
    geometry = PUBLIC_COMPANIES_GEOMETRY
    month = "2026-05"

    def __init__(self):
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
        path.write_bytes((f"shard:{prefix}:" + materialization.materialization_id()).encode())
        return _Artifact(path)


class _Remote:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.uploads: list[str] = []

    def metadata(self) -> dict:
        entries = _input_entries()
        for name, data in self.files.items():
            identity = _identity(data)
            entries.append({"name": name, "size": str(identity.size), "sha1": identity.sha1})
        return {"files": entries, "pending_tasks": True}

    def upload(self, name: str, path: Path) -> None:
        self.files[name] = path.read_bytes()
        self.uploads.append(name)

    def fetch_sidecar(self, name: str):
        data = self.files.get(name)
        return json.loads(data) if data is not None else None

    def hash_remote(self, name: str, expected_size: int, expected_sha1: str) -> ArtifactIdentity:
        identity = _identity(self.files[name])
        assert identity.size == expected_size
        assert identity.sha1 == expected_sha1
        return identity


def _metadata_without_sidecars(remote: _Remote) -> dict:
    metadata = remote.metadata()
    metadata["files"] = [
        entry
        for entry in metadata["files"]
        if not str(entry.get("name", "")).endswith(".identity.json")
    ]
    return metadata


def _expected(session: _Session, prefix: str, pinned: dict[str, str]) -> MaterializationSpec:
    return session.materialization_spec(prefix, input_sha1s=pinned)


def _meta_payload(spec: MaterializationSpec) -> dict:
    return {
        "materialization": {
            "id": spec.materialization_id(),
            "spec": spec.as_document(),
        }
    }


def _fetch_meta(remote: _Remote, spec: MaterializationSpec):
    def fetch(name: str):
        return _meta_payload(spec) if name in remote.files else None

    return fetch


def _install_reusable_zip_and_sidecar(
    remote: _Remote,
    session: _Session,
    prefix: str,
    pinned: dict[str, str],
    data: bytes = b"remote shard bytes",
) -> ArtifactIdentity:
    spec = _expected(session, prefix, pinned)
    zip_name = session.geometry.shard_name(prefix)
    identity = _identity(data)
    remote.files[zip_name] = data
    sidecar = ShardSidecar(
        snapshot=session.month,
        shard=prefix,
        materialization_id=spec.materialization_id(),
        artifact_name=zip_name,
        artifact=identity,
    )
    remote.files[sidecar_name(session.geometry, prefix)] = sidecar.canonical_bytes()
    return identity


def _publish(session, remote, tmp_path, prefix="07"):
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, prefix, pinned)
    return publish_one_shard(
        session,
        prefix,
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        fetch_sidecar=remote.fetch_sidecar,
        hash_remote=remote.hash_remote,
        upload=remote.upload,
        sleep=lambda _: None,
    )


def test_reusable_shard_with_sidecar_skips_without_pack_or_upload(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    expected_identity = _install_reusable_zip_and_sidecar(remote, session, "07", pinned)

    result = _publish(session, remote, tmp_path)

    assert result.action is ShardPublishAction.SKIPPED
    assert result.sha256 == expected_identity.sha256
    assert session.pack_calls == []
    assert remote.uploads == []


def test_direct_sidecar_skips_even_before_metadata_index(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    expected_identity = _install_reusable_zip_and_sidecar(remote, session, "07", pinned)
    spec = _expected(session, "07", pinned)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=lambda: _metadata_without_sidecars(remote),
        fetch_meta=_fetch_meta(remote, spec),
        fetch_sidecar=remote.fetch_sidecar,
        hash_remote=remote.hash_remote,
        upload=remote.upload,
        sleep=lambda _: None,
    )

    assert result.action is ShardPublishAction.SKIPPED
    assert result.sha256 == expected_identity.sha256
    assert session.pack_calls == []
    assert remote.uploads == []


def test_reusable_zip_without_sidecar_stream_hashes_once_and_repairs_checkpoint(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    remote.files["companies-07.zip"] = b"already durable zip"

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        fetch_sidecar=remote.fetch_sidecar,
        hash_remote=remote.hash_remote,
        upload=remote.upload,
        sleep=lambda _: None,
    )

    assert result.action is ShardPublishAction.SIDECAR_REPAIRED
    assert session.pack_calls == []
    assert remote.uploads == ["companies-07.identity.json"]
    assert result.sha256 == _identity(b"already durable zip").sha256


def test_repaired_sidecar_confirms_directly_before_metadata_index(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    remote.files["companies-07.zip"] = b"already durable zip"

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=lambda: _metadata_without_sidecars(remote),
        fetch_meta=_fetch_meta(remote, spec),
        fetch_sidecar=remote.fetch_sidecar,
        hash_remote=remote.hash_remote,
        upload=remote.upload,
        sleep=lambda _: None,
    )

    assert result.action is ShardPublishAction.SIDECAR_REPAIRED
    assert remote.uploads == ["companies-07.identity.json"]
    assert result.sha256 == _identity(b"already durable zip").sha256


def test_ambiguous_sidecar_observation_never_authorizes_repair(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    remote.files["companies-07.zip"] = b"already durable zip"

    def unavailable(_name: str):
        raise SidecarObservationError("HTTP 503")

    with pytest.raises(SidecarObservationError, match="503"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=_fetch_meta(remote, spec),
            fetch_sidecar=unavailable,
            hash_remote=remote.hash_remote,
            upload=remote.upload,
            sleep=lambda _: None,
        )

    assert remote.uploads == []
    assert session.pack_calls == []


@pytest.mark.parametrize(
    "remote_meta",
    [
        None,
        {"materialization": {"id": "x" * 64, "spec": {}}},
    ],
)
def test_unknown_or_mismatch_never_writes(tmp_path, remote_meta):
    session = _Session()
    remote = _Remote()
    remote.files["companies-07.zip"] = b"other"
    pinned = pin_materialization_inputs(remote.metadata)

    with pytest.raises(ShardPublishError):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: remote_meta,
            fetch_sidecar=remote.fetch_sidecar,
            hash_remote=remote.hash_remote,
            upload=lambda name, path: pytest.fail("não pode escrever estado ambíguo/divergente"),
        )

    assert session.pack_calls == []


def test_absent_shard_uploads_zip_then_sidecar_and_only_then_cleans_local(tmp_path):
    session = _Session()
    remote = _Remote()

    result = _publish(session, remote, tmp_path)

    assert result.action is ShardPublishAction.UPLOADED
    assert remote.uploads == ["companies-07.zip", "companies-07.identity.json"]
    assert session.pack_calls == ["07"]
    assert result.sha256 == _identity(remote.files["companies-07.zip"]).sha256
    assert not (tmp_path / "out" / "companies-07.zip").exists()
    assert not (tmp_path / "out" / "companies-07.identity.json").exists()


def test_unconfirmed_zip_upload_fails_before_sidecar_and_keeps_local_shard(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)

    with pytest.raises(ShardPublishError, match="size\\+sha1 remoto não confirmou"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=_fetch_meta(remote, spec),
            fetch_sidecar=remote.fetch_sidecar,
            hash_remote=remote.hash_remote,
            upload=lambda name, path: None,
            confirm_attempts=1,
            sleep=lambda _: None,
        )

    assert (tmp_path / "out" / "companies-07.zip").exists()
    assert not (tmp_path / "out" / "companies-07.identity.json").exists()


def test_existing_but_divergent_sidecar_fails_closed(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    remote.files["companies-07.zip"] = b"good zip"
    wrong = ShardSidecar(
        snapshot=session.month,
        shard="07",
        materialization_id="f" * 64,
        artifact_name="companies-07.zip",
        artifact=_identity(b"good zip"),
    )
    remote.files["companies-07.identity.json"] = wrong.canonical_bytes()

    with pytest.raises(ShardPublishError, match="sidecar ausente, inválida ou divergente"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=_fetch_meta(remote, spec),
            fetch_sidecar=remote.fetch_sidecar,
            hash_remote=remote.hash_remote,
            upload=remote.upload,
        )

    assert remote.uploads == []
    assert session.pack_calls == []


def test_input_change_aborts_before_mixing_materializations(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    changed = remote.metadata()
    changed["files"][0]["sha1"] = "f" * 40

    with pytest.raises(ShardPublishError, match="inputs remotos mudaram"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=lambda: changed,
            fetch_meta=lambda _: None,
            fetch_sidecar=remote.fetch_sidecar,
            hash_remote=remote.hash_remote,
            upload=lambda name, path: pytest.fail("não pode escrever com inputs mudados"),
        )

    assert session.pack_calls == []
