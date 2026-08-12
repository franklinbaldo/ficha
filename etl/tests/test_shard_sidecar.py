from __future__ import annotations

import hashlib
import json

from ficha_etl.shard_sidecar import (
    ArtifactIdentity,
    ShardSidecar,
    artifact_identity,
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
