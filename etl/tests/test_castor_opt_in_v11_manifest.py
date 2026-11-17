"""Offline contract for separate Protobuf 1.1 archive names and discovery.

No network, production ETL, Internet Archive PUT, or manifest promotion.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ficha_etl.manifest import (
    CompanyShardIdentity,
    finalize_sharded_snapshot_entry,
    update_manifest,
)


def _shards(*, namespace: str = "wire-1.1/") -> list[CompanyShardIdentity]:
    return [
        CompanyShardIdentity(
            shard=f"{number:02d}",
            name=f"{namespace}companies-{number:02d}.zip",
            size=number + 100,
            sha1=f"{number + 1:040x}",
        )
        for number in range(100)
    ]


def _entry() -> dict:
    return finalize_sharded_snapshot_entry(
        {"date": "2026-08", "files": {}},
        _shards(),
        namespace="wire-1.1/",
    )


def test_opt_in_index_uses_100_disjoint_versioned_urls_and_leaves_v1_untouched(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "manifest.json"
    previous = b'{"current":"2026-07","snapshots":[]}\n'
    legacy.write_bytes(previous)
    versioned = tmp_path / "manifest-atomic-v1.1.json"
    entry = _entry()

    update_manifest(versioned, entry, opt_in_v11=True)

    assert legacy.read_bytes() == previous
    assert not (tmp_path / "wire-1.1").exists()
    new_index = json.loads(versioned.read_text(encoding="utf-8"))
    assert new_index["current"] == "2026-08"
    urls = [shard["url"] for shard in new_index["snapshots"][0]["files"]["companies"]["shards"]]
    assert len(urls) == len(set(urls)) == 100
    for number, url in enumerate(urls):
        assert url.endswith(f"/ficha-2026-08/wire-1.1/companies-{number:02d}.zip")


@pytest.mark.parametrize(
    "namespace",
    ["", "wire-1.0/", "../wire-1.1/", "wire-1.1/../"],
)
def test_v11_builder_rejects_legacy_or_ambiguous_names(namespace: str) -> None:
    with pytest.raises(ValueError, match="artifact name"):
        finalize_sharded_snapshot_entry(
            {"date": "2026-08", "files": {}},
            _shards(namespace=namespace),
            namespace="wire-1.1/",
        )


@pytest.mark.parametrize("name", ["manifest.json", "other.json", "manifest-atomic-v1.1.json"])
def test_opt_in_refuses_wrong_destination_or_legacy_symlink(tmp_path: Path, name: str) -> None:
    original = tmp_path / "manifest.json"
    original.write_bytes(b"legacy index stays untouched\n")
    destination = tmp_path / name
    if name == "manifest-atomic-v1.1.json":
        destination.symlink_to(original)
    before = original.read_bytes()
    with pytest.raises(ValueError, match="separate manifest-atomic-v1.1.json"):
        update_manifest(destination, _entry(), opt_in_v11=True)
    assert original.read_bytes() == before


@pytest.mark.parametrize(
    "mutation",
    [
        lambda entry: entry["files"]["companies"]["shards"][0].update(
            url="https://archive.org/download/ficha-2026-08/companies-00.zip"
        ),
        lambda entry: entry["files"]["companies"]["shards"][0].update(
            url="https://archive.org/download/ficha-2026-08/wire-1.1/companies-01.zip"
        ),
        lambda entry: entry["files"]["companies"]["shards"][0].update(size=0),
        lambda entry: entry["files"]["companies"].update(schema_version="1.0.0"),
        lambda entry: entry["files"]["companies"]["shards"].pop(),
    ],
)
def test_invalid_v11_opt_in_never_mutates_an_index(tmp_path: Path, mutation) -> None:
    path = tmp_path / "manifest-atomic-v1.1.json"
    before = b'{"current":"2026-06","snapshots":[]}\n'
    path.write_bytes(before)
    entry = _entry()
    mutation(entry)
    with pytest.raises(ValueError, match="opt-in 1.1"):
        update_manifest(path, entry, opt_in_v11=True)
    assert path.read_bytes() == before


def test_v1_discovery_refuses_versioned_entry_even_under_legacy_filename(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "manifest.json"
    with pytest.raises(ValueError, match="legacy manifest.json"):
        update_manifest(legacy, _entry())
    assert not legacy.exists()


def test_opt_in_refuses_mixed_existing_v1_index_without_rewriting(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manifest-atomic-v1.1.json"
    prior = (
        json.dumps(
            {
                "current": "2026-07",
                "snapshots": [
                    {"date": "2026-07", "files": {"companies": {"shard_by": "cnpj_base_prefix_2"}}}
                ],
            }
        ).encode()
        + b"\n"
    )
    path.write_bytes(prior)
    with pytest.raises(ValueError, match="opt-in 1.1"):
        update_manifest(path, _entry(), opt_in_v11=True)
    assert path.read_bytes() == prior
