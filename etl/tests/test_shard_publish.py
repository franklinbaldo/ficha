from __future__ import annotations

import hashlib
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
from ficha_etl.upload_identity import LocalIdentity


def _input_entries() -> list[dict]:
    return [
        {"name": name, "size": str(index), "sha1": f"{index:040x}"}
        for index, name in enumerate(SHARD_INPUT_NAMES, start=1)
    ]


def _sha1(data: bytes) -> str:
    return hashlib.sha1(data, usedforsecurity=False).hexdigest()


def _identity(data: bytes) -> LocalIdentity:
    return LocalIdentity(size=len(data), sha1=_sha1(data))


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
            entries.append({"name": name, "size": str(len(data)), "sha1": _sha1(data)})
        return {"files": entries, "pending_tasks": True}

    def upload(self, name: str, path: Path) -> None:
        self.files[name] = path.read_bytes()
        self.uploads.append(name)

    def direct(self, name: str) -> LocalIdentity | None:
        data = self.files.get(name)
        return _identity(data) if data is not None else None


class _StaleCatalogRemote(_Remote):
    """Bytes reais existem, mas o catálogo nunca mostra companies-NN.zip."""

    def metadata(self) -> dict:
        return {"files": _input_entries(), "pending_tasks": True}


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


def _install_reusable_zip(
    remote: _Remote,
    session: _Session,
    prefix: str,
    data: bytes = b"remote shard bytes",
) -> None:
    remote.files[session.geometry.shard_name(prefix)] = data


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
        upload=remote.upload,
        fetch_direct=remote.direct,
        sleep=lambda _: None,
    )


def test_reusable_shard_skips_without_pack_or_upload(tmp_path):
    session = _Session()
    remote = _Remote()
    pinned = pin_materialization_inputs(remote.metadata)
    _install_reusable_zip(remote, session, "07")

    result = _publish(session, remote, tmp_path)

    assert result.action is ShardPublishAction.SKIPPED
    assert result.size == len(b"remote shard bytes")
    assert result.sha1 == _sha1(b"remote shard bytes")
    assert result.materialization_id == _expected(session, "07", pinned).materialization_id()
    assert session.pack_calls == []
    assert remote.uploads == []


def test_catalog_absent_direct_identity_and_exact_meta_skips_without_pack(tmp_path):
    session = _Session()
    remote = _StaleCatalogRemote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    data = b"already durable despite stale catalog"
    _install_reusable_zip(remote, session, "07", data)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        upload=lambda name, path: pytest.fail("não pode sobrescrever objeto reconciliado"),
        fetch_direct=remote.direct,
    )

    assert result.action is ShardPublishAction.SKIPPED
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)
    assert session.pack_calls == []
    assert remote.uploads == []


@pytest.mark.parametrize(
    "remote_meta",
    [
        None,
        {"materialization": {"id": "x" * 64, "spec": {}}},
    ],
)
def test_direct_object_with_unknown_or_mismatching_meta_never_writes(tmp_path, remote_meta):
    session = _Session()
    remote = _StaleCatalogRemote()
    remote.files["companies-07.zip"] = b"other"
    pinned = pin_materialization_inputs(remote.metadata)

    with pytest.raises(ShardPublishError, match="objeto direto existe mas não é reutilizável"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: remote_meta,
            upload=lambda name, path: pytest.fail("não pode escrever estado ambíguo/divergente"),
            fetch_direct=remote.direct,
        )

    assert session.pack_calls == []


def test_direct_observation_failure_never_packs_or_uploads(tmp_path):
    session = _Session()
    remote = _StaleCatalogRemote()
    pinned = pin_materialization_inputs(remote.metadata)

    def fail_direct(name: str):
        raise ShardPublishError(f"{name}: GET direto transitório: HTTP 503")

    with pytest.raises(ShardPublishError, match="HTTP 503"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=remote.metadata,
            fetch_meta=lambda _: None,
            upload=lambda name, path: pytest.fail("não pode escrever sob falha de observação"),
            fetch_direct=fail_direct,
        )

    assert session.pack_calls == []


def test_direct_404_allows_pack_and_catalog_confirmation(tmp_path):
    session = _Session()
    remote = _Remote()
    calls: list[str] = []
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)

    def direct_404(name: str):
        calls.append(name)
        return None

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        upload=remote.upload,
        fetch_direct=direct_404,
        post_put_direct_attempts=1,
        sleep=lambda _: None,
    )

    assert result.action is ShardPublishAction.UPLOADED
    assert calls == ["companies-07.zip", "companies-07.zip", "companies-07.zip"]
    assert session.pack_calls == ["07"]
    assert remote.uploads == ["companies-07.zip"]


def test_direct_race_after_pack_skips_before_put_and_cleans_local(tmp_path):
    session = _Session()
    remote = _StaleCatalogRemote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    race_data = b"published concurrently"
    direct_calls = 0

    def direct(name: str):
        nonlocal direct_calls
        direct_calls += 1
        if direct_calls == 1:
            return None
        remote.files[name] = race_data
        return _identity(race_data)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=lambda name: _meta_payload(spec) if name in remote.files else None,
        upload=lambda name, path: pytest.fail("corrida reconciliada não pode fazer PUT"),
        fetch_direct=direct,
    )

    assert result.action is ShardPublishAction.SKIPPED
    assert result.size == len(race_data)
    assert session.pack_calls == ["07"]
    assert not (tmp_path / "out" / "companies-07.zip").exists()


def test_post_put_stale_catalog_confirms_directly_before_metadata_poll(tmp_path):
    session = _Session()
    remote = _StaleCatalogRemote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        upload=remote.upload,
        fetch_direct=remote.direct,
        sleep=lambda _: pytest.fail("objeto direto imediato não deve esperar catálogo"),
    )

    data = remote.files["companies-07.zip"]
    assert result.action is ShardPublishAction.UPLOADED
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)
    assert remote.uploads == ["companies-07.zip"]
    assert session.pack_calls == ["07"]
    assert not (tmp_path / "out" / "companies-07.zip").exists()


def test_post_put_direct_retries_404_then_confirms(tmp_path):
    session = _Session()
    remote = _StaleCatalogRemote()
    pinned = pin_materialization_inputs(remote.metadata)
    spec = _expected(session, "07", pinned)
    direct_calls = 0
    sleeps: list[float] = []

    def delayed_direct(name: str):
        nonlocal direct_calls
        direct_calls += 1
        # 1 = antes do pack; 2 = antes do PUT; 3 = primeira observação pós-PUT.
        if direct_calls <= 3:
            return None
        return remote.direct(name)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=remote.metadata,
        fetch_meta=_fetch_meta(remote, spec),
        upload=remote.upload,
        fetch_direct=delayed_direct,
        confirm_attempts=1,
        post_put_direct_attempts=2,
        sleep=sleeps.append,
    )

    data = remote.files["companies-07.zip"]
    assert result.action is ShardPublishAction.UPLOADED
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)
    assert direct_calls == 4
    assert sleeps == [5.0]
    assert remote.uploads == ["companies-07.zip"]
    assert not (tmp_path / "out" / "companies-07.zip").exists()


def test_unknown_or_mismatch_from_catalog_never_writes(tmp_path):
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
            fetch_meta=lambda _: None,
            upload=lambda name, path: pytest.fail("não pode escrever estado ambíguo/divergente"),
            fetch_direct=remote.direct,
        )

    assert session.pack_calls == []


def test_absent_shard_uploads_only_zip_then_cleans_local(tmp_path):
    session = _Session()
    remote = _Remote()

    result = _publish(session, remote, tmp_path)

    data = remote.files["companies-07.zip"]
    assert result.action is ShardPublishAction.UPLOADED
    assert remote.uploads == ["companies-07.zip"]
    assert session.pack_calls == ["07"]
    assert result.size == len(data)
    assert result.sha1 == _sha1(data)
    assert not (tmp_path / "out" / "companies-07.zip").exists()
    assert "companies-07.identity.json" not in remote.files


def test_unconfirmed_zip_upload_fails_and_keeps_local_shard(tmp_path):
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
            upload=lambda name, path: None,
            fetch_direct=lambda name: None,
            confirm_attempts=1,
            post_put_direct_attempts=1,
            sleep=lambda _: None,
        )

    assert (tmp_path / "out" / "companies-07.zip").exists()


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
            upload=lambda name, path: pytest.fail("não pode escrever com inputs mudados"),
            fetch_direct=lambda name: pytest.fail("não deve observar shard com inputs mudados"),
        )

    assert session.pack_calls == []
