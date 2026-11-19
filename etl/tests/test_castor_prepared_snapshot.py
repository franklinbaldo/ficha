"""Adversarial offline contract: prepared shard PUT reads a verified anonymous snapshot.

No credentials, RFB downloads, IA PUT, manifest write or production artifact.
The real transfer/uploader functions run; only remote observations are doubled.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl import monthly_sharded, shard_transfer
from ficha_etl.shard_publish import ShardPublishError
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, ShardReuseState
from ficha_etl.shard_transfer import PreparedShard, ShardTransferAction


def _sha1(data: bytes) -> str:
    return hashlib.sha1(data, usedforsecurity=False).hexdigest()


@dataclass
class _Spec:
    def materialization_id(self) -> str:
        return "a" * 64


class _Session:
    geometry = PUBLIC_COMPANIES_GEOMETRY
    wire_version = "1.0.0"
    shard_by = "cnpj_base_prefix_2"

    def materialization_spec(
        self, prefix: str, *, input_sha1s: dict[str, str]
    ) -> _Spec:
        assert prefix == "07"
        return _Spec()


def _prepared(tmp_path: Path) -> tuple[PreparedShard, bytes, bytes]:
    good = b"healthy-ZIP-payload-111"
    bad = b"tamperd-ZIP-payload-222"
    assert len(good) == len(bad) and _sha1(good) != _sha1(bad)
    path = tmp_path / "companies-07.zip"
    path.write_bytes(good)
    return (
        PreparedShard("07", path.name, path, "a" * 64, len(good), _sha1(good)),
        good,
        bad,
    )


def _replace_source(path: Path, bad: bytes) -> None:
    replacement = path.with_suffix(".replacement")
    replacement.write_bytes(bad)
    replacement.replace(path)


def _setup_remote(monkeypatch: pytest.MonkeyPatch, events: list[str], mutate) -> None:
    def tick(event: str) -> None:
        events.append(event)
        mutate(event)

    def catalog(_pinned, fetch):
        tick("before_metadata")
        return fetch()

    def classify(_session, _prefix, _expected, _metadata, _fetch_meta):
        tick("classify")
        return SimpleNamespace(
            state=ShardReuseState.ABSENT,
            name="companies-07.zip",
            detail="",
        )

    def direct(*_args, **_kwargs):
        tick("direct")
        return None

    monkeypatch.setattr(shard_transfer, "_metadata_still_matches", catalog)
    monkeypatch.setattr(shard_transfer, "_classify", classify)
    monkeypatch.setattr(shard_transfer, "_reconcile_catalog_absent", direct)


def _submit(prepared: PreparedShard, uploader):
    return shard_transfer.submit_prepared_shard(
        _Session(),
        prepared,
        pinned_inputs={},
        fetch_metadata=lambda: {"files": []},
        fetch_meta=lambda _name: None,
        fetch_direct=lambda _name: None,
        upload=uploader,
    )


@pytest.mark.parametrize("mutation", ["rewrite", "replace"])
@pytest.mark.parametrize("trigger", [1, 2, 3, 4])
def test_mutation_after_each_remote_observation_refuses_put(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, trigger: int, mutation: str
) -> None:
    prepared, _good, bad = _prepared(tmp_path)
    events: list[str] = []
    remote_count = 0
    uploads: list[bytes] = []

    def mutate(event: str) -> None:
        nonlocal remote_count
        if event not in ("before_metadata", "direct"):
            return
        remote_count += 1
        if remote_count != trigger:
            return
        if mutation == "rewrite":
            prepared.path.write_bytes(bad)
        else:
            _replace_source(prepared.path, bad)

    _setup_remote(monkeypatch, events, mutate)
    with pytest.raises(ShardPublishError, match="durante snapshot anterior ao PUT"):
        _submit(prepared, lambda _name, source: uploads.append(source.read()))
    assert remote_count == 4
    assert uploads == []
    assert events.count("direct") == 2


@pytest.mark.parametrize("mutation", ["rewrite", "replace"])
def test_source_mutation_after_freeze_cannot_change_sent_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutation: str
) -> None:
    prepared, good, bad = _prepared(tmp_path)
    _setup_remote(monkeypatch, [], lambda _event: None)
    sent: list[bytes] = []

    def upload(name: str, source) -> None:
        assert name == prepared.name
        assert not isinstance(source, Path)
        assert source.seekable()
        # Called after snapshot verification; the path's inode/content is now unrelated.
        if mutation == "rewrite":
            prepared.path.write_bytes(bad)
        else:
            _replace_source(prepared.path, bad)
        sent.append(source.read())

    result = _submit(prepared, upload)
    assert result.action is ShardTransferAction.SUBMITTED
    assert sent == [good]
    assert result.sha1 == _sha1(sent[0]) == prepared.sha1
    assert _sha1(prepared.path.read_bytes()) != result.sha1


def test_unmodified_snapshot_yields_submitted_not_verified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prepared, good, _bad = _prepared(tmp_path)
    _setup_remote(monkeypatch, [], lambda _event: None)
    sent: list[bytes] = []
    result = _submit(prepared, lambda _name, stream: sent.append(stream.read()))
    assert sent == [good]
    assert result.action is ShardTransferAction.SUBMITTED
    assert (result.size, result.sha1) == (len(good), _sha1(good))


def test_invalid_initial_receipt_stops_before_remote_or_put(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prepared, _good, bad = _prepared(tmp_path)
    prepared.path.write_bytes(bad)
    events: list[str] = []
    _setup_remote(monkeypatch, events, lambda _event: None)
    with pytest.raises(ShardPublishError, match="bytes locais mudaram após o recibo"):
        _submit(prepared, lambda _name, _stream: pytest.fail("unexpected PUT"))
    assert events == []


def test_ia_uploader_passes_binary_handle_without_reopening_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(monthly_sharded, "_require_secret", lambda _name: "fake-only")
    observations: list[tuple[str, bytes]] = []

    def ia_upload(identifier: str, *, files, **_kwargs):
        assert identifier == "ficha-2026-08"
        handle = files["companies-07.zip"]
        assert handle is stream
        observations.append((identifier, handle.read()))
        return [SimpleNamespace(status_code=200)]

    monkeypatch.setattr(monthly_sharded.ia, "upload", ia_upload)
    stream = BytesIO(b"frozen")
    monthly_sharded._shard_uploader("2026-08")("companies-07.zip", stream)
    assert observations == [("ficha-2026-08", b"frozen")]
