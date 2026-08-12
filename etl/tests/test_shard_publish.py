from __future__ import annotations

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
from ficha_etl.upload_identity import local_identity


def _input_entries() -> list[dict]:
    return [
        {"name": name, "size": str(index), "sha1": f"{index:040x}"}
        for index, name in enumerate(SHARD_INPUT_NAMES, start=1)
    ]


def _metadata(*extra: dict) -> dict:
    return {"files": [*_input_entries(), *extra], "pending_tasks": True}


@dataclass
class _Artifact:
    path: Path


class _Session:
    geometry = PUBLIC_COMPANIES_GEOMETRY

    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.pack_calls: list[str] = []

    def materialization_spec(self, prefix: str, *, input_sha1s) -> MaterializationSpec:
        return MaterializationSpec(
            snapshot="2026-05",
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


def _expected(session: _Session, prefix: str, pinned: dict[str, str]) -> MaterializationSpec:
    return session.materialization_spec(prefix, input_sha1s=pinned)


def _meta_payload(spec: MaterializationSpec) -> dict:
    return {
        "materialization": {
            "id": spec.materialization_id(),
            "spec": spec.as_document(),
        }
    }


def test_pin_inputs_reads_all_semantic_inputs_once():
    pinned = pin_materialization_inputs(lambda: _metadata())
    assert tuple(pinned) == SHARD_INPUT_NAMES


def test_reusable_shard_skips_without_pack_or_upload(tmp_path):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())
    spec = _expected(session, "07", pinned)
    entry = {"name": "companies-07.zip", "size": "123", "sha1": "a" * 40}
    uploads: list[str] = []

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=lambda: _metadata(entry),
        fetch_meta=lambda _: _meta_payload(spec),
        upload=lambda name, path: uploads.append(name),
    )

    assert result.action is ShardPublishAction.SKIPPED
    assert session.pack_calls == []
    assert uploads == []


@pytest.mark.parametrize(
    "remote_meta",
    [
        None,
        {"materialization": {"id": "x" * 64, "spec": {}}},
    ],
)
def test_unknown_or_mismatch_never_writes(tmp_path, remote_meta):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())
    entry = {"name": "companies-07.zip", "size": "123", "sha1": "a" * 40}

    with pytest.raises(ShardPublishError):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=lambda: _metadata(entry),
            fetch_meta=lambda _: remote_meta,
            upload=lambda name, path: pytest.fail("não pode escrever estado ambíguo/divergente"),
        )

    assert session.pack_calls == []


def test_absent_shard_uploads_then_requires_both_postconditions(tmp_path):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())
    spec = _expected(session, "07", pinned)
    remote_entry: dict | None = None
    uploads: list[str] = []

    def fetch_metadata():
        return _metadata(*([remote_entry] if remote_entry else []))

    def upload(name: str, path: Path):
        nonlocal remote_entry
        identity = local_identity(path)
        remote_entry = {"name": name, "size": str(identity.size), "sha1": identity.sha1}
        uploads.append(name)

    result = publish_one_shard(
        session,
        "07",
        tmp_path / "out",
        pinned_inputs=pinned,
        fetch_metadata=fetch_metadata,
        fetch_meta=lambda _: _meta_payload(spec) if remote_entry else None,
        upload=upload,
        sleep=lambda _: None,
    )

    assert result.action is ShardPublishAction.UPLOADED
    assert uploads == ["companies-07.zip"]
    assert session.pack_calls == ["07"]
    assert not (tmp_path / "out" / "companies-07.zip").exists()


def test_unconfirmed_upload_fails_and_keeps_local_shard(tmp_path):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())

    with pytest.raises(ShardPublishError, match="size\+sha1 remoto não confirmou"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=lambda: _metadata(),
            fetch_meta=lambda _: None,
            upload=lambda name, path: None,
            confirm_attempts=1,
            sleep=lambda _: None,
        )

    assert (tmp_path / "out" / "companies-07.zip").exists()


def test_input_change_aborts_before_mixing_materializations(tmp_path):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())
    changed = _metadata()
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
        )

    assert session.pack_calls == []


def test_remote_appearing_after_pack_must_match_semantics_or_abort(tmp_path):
    session = _Session(tmp_path)
    pinned = pin_materialization_inputs(lambda: _metadata())
    calls = 0
    mismatched_entry = {"name": "companies-07.zip", "size": "123", "sha1": "a" * 40}

    def fetch_metadata():
        nonlocal calls
        calls += 1
        return _metadata() if calls == 1 else _metadata(mismatched_entry)

    with pytest.raises(ShardPublishError, match="estado mudou antes do PUT"):
        publish_one_shard(
            session,
            "07",
            tmp_path / "out",
            pinned_inputs=pinned,
            fetch_metadata=fetch_metadata,
            fetch_meta=lambda _: {"materialization": {"id": "x" * 64, "spec": {}}},
            upload=lambda name, path: pytest.fail(
                "mismatch surgido após pack não pode ser substituído"
            ),
        )

    assert session.pack_calls == ["07"]
    assert (tmp_path / "out" / "companies-07.zip").exists()
