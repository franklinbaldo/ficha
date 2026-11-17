"""P0-A: local manifest discovery stays intact on interrupted writes.

Only synthetic temporary files; no network, real ETL, IA or promotion.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ficha_etl import manifest as manifest_mod


def _entry() -> dict:
    return {
        "date": "2026-08",
        "files": {
            "companies": {
                "shard_by": "cnpj_base_prefix_2",
                "schema_version": "1.0.0",
            }
        },
    }


@pytest.mark.parametrize("preexisting", [False, True])
@pytest.mark.parametrize("failure", ["fsync", "replace"])
def test_failed_stage_or_replace_preserves_manifest_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    preexisting: bool,
    failure: str,
) -> None:
    manifest = tmp_path / "public" / "manifest.json"
    manifest.parent.mkdir()
    original = b'{"current":"2026-07","snapshots":[{"date":"2026-07"}]}\n'
    if preexisting:
        manifest.write_bytes(original)
    before = manifest.read_bytes() if manifest.exists() else None

    def interrupted(*_args: object, **_kwargs: object) -> None:
        raise OSError(f"injected {failure} failure")

    monkeypatch.setattr(manifest_mod.os, failure, interrupted)
    with pytest.raises(OSError, match=f"injected {failure} failure"):
        manifest_mod.update_manifest(manifest, _entry())

    assert (manifest.read_bytes() if manifest.exists() else None) == before
    assert list(manifest.parent.glob(".manifest.json.*.tmp")) == []


@pytest.mark.parametrize("preexisting", [False, True])
def test_successful_atomic_replace_writes_complete_legacy_index(
    tmp_path: Path,
    preexisting: bool,
) -> None:
    manifest = tmp_path / "public" / "manifest.json"
    manifest.parent.mkdir()
    if preexisting:
        manifest.write_bytes(b'{"current":"2026-07","snapshots":[{"date":"2026-07"}]}\n')

    manifest_mod.update_manifest(manifest, _entry())

    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert document["current"] == "2026-08"
    assert document["snapshots"][0] == _entry()
    assert len(document["snapshots"]) == (2 if preexisting else 1)
    assert list(manifest.parent.glob(".manifest.json.*.tmp")) == []
