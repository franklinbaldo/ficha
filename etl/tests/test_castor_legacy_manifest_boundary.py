"""P0-A: no Protobuf 1.1 layer can silently enter the shared v1 discovery.

All indices here are local fixtures. No HTTP/IA traffic or manifest promotion.
"""

from __future__ import annotations

import json

import pytest

from ficha_etl.manifest import update_manifest


@pytest.mark.parametrize(
    "companies",
    [
        {"shard_by": "cnpj_base_prefix_or_base36_mod_100_v1", "schema_version": "1.1.0"},
        {"shard_by": "cnpj_base_prefix_or_base36_mod_100_v1"},
        {"shard_by": "cnpj_base_prefix_2", "schema_version": "1.1.0"},
        {"shard_by": "unknown", "schema_version": "1.0.0"},
    ],
)
@pytest.mark.parametrize("existing", [True, False])
def test_v1_discovery_rejects_new_wire_before_any_file_mutation(tmp_path, companies, existing):
    path = tmp_path / "public" / "manifest.json"
    if existing:
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps({"current": "2026-07", "snapshots": [{"date": "2026-07"}]}) + "\n",
            encoding="utf-8",
        )
    before = path.read_bytes() if path.exists() else None

    with pytest.raises(ValueError, match="legacy manifest.json"):
        update_manifest(path, {"date": "2026-08", "files": {"companies": companies}})

    assert (path.read_bytes() if path.exists() else None) == before


@pytest.mark.parametrize("schema_version", [None, "1.0.0"])
def test_numeric_legacy_discovery_remains_writable(tmp_path, schema_version):
    path = tmp_path / "manifest.json"
    layer = {"shard_by": "cnpj_base_prefix_2"}
    if schema_version is not None:
        layer["schema_version"] = schema_version

    update_manifest(path, {"date": "2026-07", "files": {"companies": layer}})
    contents = json.loads(path.read_text(encoding="utf-8"))
    assert contents["current"] == "2026-07"
    assert contents["snapshots"][0]["files"]["companies"] == layer
