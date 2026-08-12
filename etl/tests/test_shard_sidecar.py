from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from ficha_etl.shard_sidecar import (
    ArtifactIdentity,
    ShardSidecar,
    SidecarObservationError,
    artifact_identity,
    fetch_remote_sidecar,
    parse_sidecar,
    sidecar_matches,
    sidecar_name,
    write_sidecar,
)
from ficha_etl.sharded_pack import ShardGeometry


def test_artifact_identity_streams_both_hashes(tmp_path):
    path = tmp_path / "artifact.zip"
    path.write_bytes(b"abc" * 1000)

    identity = artifact_identity(path)

    assert identity.size == 3000
    assert identity.sha1 == hashlib.sha1(path.read_bytes(), usedforsecurity=False).hexdigest()
    assert identity.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_sidecar_is_canonical_roundtrippable_and_atomic(tmp_path):
    identity = ArtifactIdentity(size=123, sha1="a" * 40, sha256="b" * 64)
    sidecar = ShardSidecar(
        snapshot="2026-05",
        shard="07",
        materialization_id="c" * 64,
        artifact_name="companies-07.zip",
        artifact=identity,
    )
    path = tmp_path / sidecar_name(ShardGeometry(2), "07")

    write_sidecar(path, sidecar)
    parsed = parse_sidecar(json.loads(path.read_bytes()))

    assert parsed == sidecar
    assert path.read_bytes() == sidecar.canonical_bytes()
    assert not path.with_name(path.name + ".part").exists()
    assert sidecar_matches(
        parsed,
        snapshot="2026-05",
        prefix="07",
        materialization_id="c" * 64,
        artifact_name="companies-07.zip",
        remote_size=123,
        remote_sha1="a" * 40,
    )


def test_sidecar_rejects_bad_artifact_hashes():
    payload = ShardSidecar(
        snapshot="2026-05",
        shard="07",
        materialization_id="c" * 64,
        artifact_name="companies-07.zip",
        artifact=ArtifactIdentity(size=123, sha1="a" * 40, sha256="b" * 64),
    ).as_document()
    payload["artifact"]["sha256"] = "not-a-hash"
    assert parse_sidecar(payload) is None


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_remote_sidecar_returns_none_only_for_observed_404():
    client = _client(lambda request: httpx.Response(404, request=request))
    try:
        assert (
            fetch_remote_sidecar(
                "2026-05",
                "companies-07.identity.json",
                attempts=2,
                sleep=lambda _: None,
                client=client,
            )
            is None
        )
    finally:
        client.close()


def test_fetch_remote_sidecar_transient_then_404_is_still_unknown():
    statuses = iter((503, 404))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(next(statuses), request=request)

    client = _client(handler)
    try:
        with pytest.raises(SidecarObservationError, match="permaneceu ambígua"):
            fetch_remote_sidecar(
                "2026-05",
                "companies-07.identity.json",
                attempts=2,
                sleep=lambda _: None,
                client=client,
            )
    finally:
        client.close()


def test_fetch_remote_sidecar_invalid_json_fails_closed():
    client = _client(
        lambda request: httpx.Response(200, content=b"not-json", request=request)
    )
    try:
        with pytest.raises(SidecarObservationError, match="JSON remoto inválido"):
            fetch_remote_sidecar(
                "2026-05",
                "companies-07.identity.json",
                client=client,
            )
    finally:
        client.close()


def test_fetch_remote_sidecar_returns_direct_payload_even_without_metadata():
    payload = ShardSidecar(
        snapshot="2026-05",
        shard="07",
        materialization_id="c" * 64,
        artifact_name="companies-07.zip",
        artifact=ArtifactIdentity(size=123, sha1="a" * 40, sha256="b" * 64),
    ).as_document()
    client = _client(lambda request: httpx.Response(200, json=payload, request=request))
    try:
        assert (
            fetch_remote_sidecar(
                "2026-05",
                "companies-07.identity.json",
                client=client,
            )
            == payload
        )
    finally:
        client.close()
