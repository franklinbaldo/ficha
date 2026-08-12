from __future__ import annotations

from pathlib import Path

import duckdb
import httpx
import pytest

from ficha_etl import manifest as manifest_mod
from ficha_etl.shard_sidecar import ArtifactIdentity, ShardSidecar
from ficha_etl.transform import _LOOKUP_KINDS


def _write_parquet(path: Path, n_rows: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT range AS id FROM range({n_rows})) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _output_dir(tmp_path: Path) -> Path:
    root = tmp_path / "output"
    for name in (
        "cnpjs",
        "cnpj_contatos",
        "cnpj_cnaes",
        "raizes",
        "socios",
        "enderecos",
        "pessoas",
    ):
        _write_parquet(root / f"{name}.parquet")
    (root / "lookups.json").write_text("{}", encoding="utf-8")
    for kind in _LOOKUP_KINDS:
        _write_parquet(root / "lookups" / f"{kind}.parquet")
    return root


def _sidecars(month: str = "2026-05") -> list[ShardSidecar]:
    return [
        ShardSidecar(
            snapshot=month,
            shard=f"{value:02d}",
            materialization_id=f"{value + 1:064x}",
            artifact_name=f"companies-{value:02d}.zip",
            artifact=ArtifactIdentity(
                size=value + 100,
                sha1=f"{value + 1:040x}",
                sha256=f"{value + 1:064x}",
            ),
        )
        for value in range(100)
    ]


def _patch_client(monkeypatch, handler) -> None:
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched)


def test_build_snapshot_entry_accepts_complete_sharded_layer_without_monolith(tmp_path):
    output = _output_dir(tmp_path)
    entry = manifest_mod.build_snapshot_entry(
        "2026-05", output, company_sidecars=_sidecars()
    )

    assert "companies_zip" not in entry["files"]
    companies = entry["files"]["companies"]
    assert companies["shard_by"] == "cnpj_base_prefix_2"
    assert [shard["shard"] for shard in companies["shards"]] == [
        f"{value:02d}" for value in range(100)
    ]
    assert companies["shards"][7]["url"].endswith("/companies-07.zip")
    assert companies["shards"][7]["size"] == 107
    assert companies["shards"][7]["sha256"] == f"{8:064x}"


def test_build_snapshot_entry_rejects_incomplete_or_duplicate_shards(tmp_path):
    output = _output_dir(tmp_path)
    incomplete = _sidecars()[:-1]
    with pytest.raises(ValueError, match="incomplete companies shards"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_sidecars=incomplete)

    duplicate = _sidecars()
    duplicate[-1] = duplicate[-2]
    with pytest.raises(ValueError, match="duplicate companies shard sidecar"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_sidecars=duplicate)


def test_build_snapshot_entry_rejects_sidecar_for_wrong_snapshot_or_name(tmp_path):
    output = _output_dir(tmp_path)
    wrong_month = _sidecars()
    wrong_month[0] = ShardSidecar(
        snapshot="2026-06",
        shard="00",
        materialization_id="a" * 64,
        artifact_name="companies-00.zip",
        artifact=ArtifactIdentity(size=1, sha1="b" * 40, sha256="c" * 64),
    )
    with pytest.raises(ValueError, match="sidecar snapshot"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_sidecars=wrong_month)

    wrong_name = _sidecars()
    wrong_name[0] = ShardSidecar(
        snapshot="2026-05",
        shard="00",
        materialization_id="a" * 64,
        artifact_name="companies-01.zip",
        artifact=ArtifactIdentity(size=1, sha1="b" * 40, sha256="c" * 64),
    )
    with pytest.raises(ValueError, match="artifact name"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_sidecars=wrong_name)


def test_verify_snapshot_files_heads_all_100_shards_and_checks_size(monkeypatch, tmp_path):
    output = _output_dir(tmp_path)
    entry = manifest_mod.build_snapshot_entry(
        "2026-05", output, company_sidecars=_sidecars()
    )
    shard_entries = entry["files"]["companies"]["shards"]
    by_url = {shard["url"]: shard for shard in shard_entries}
    seen: set[str] = set()

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in by_url:
            seen.add(url)
            return httpx.Response(
                200,
                headers={"content-length": str(by_url[url]["size"])},
            )
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    assert manifest_mod.verify_snapshot_files(entry) == []
    assert seen == set(by_url)


def test_verify_snapshot_files_reports_one_bad_shard_size(monkeypatch, tmp_path):
    output = _output_dir(tmp_path)
    entry = manifest_mod.build_snapshot_entry(
        "2026-05", output, company_sidecars=_sidecars()
    )
    shard_entries = entry["files"]["companies"]["shards"]
    by_url = {shard["url"]: shard for shard in shard_entries}
    bad_url = shard_entries[42]["url"]

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in by_url:
            size = by_url[url]["size"] + (1 if url == bad_url else 0)
            return httpx.Response(200, headers={"content-length": str(size)})
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    assert manifest_mod.verify_snapshot_files(entry) == [bad_url]
