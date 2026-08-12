from __future__ import annotations

from pathlib import Path

import duckdb
import httpx
import pytest

from ficha_etl import manifest as manifest_mod
from ficha_etl.manifest import CompanyShardIdentity
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


def _shards() -> list[CompanyShardIdentity]:
    return [
        CompanyShardIdentity(
            shard=f"{value:02d}",
            name=f"companies-{value:02d}.zip",
            size=value + 100,
            sha1=f"{value + 1:040x}",
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
    entry = manifest_mod.build_snapshot_entry("2026-05", output, company_shards=_shards())

    assert "companies_zip" not in entry["files"]
    companies = entry["files"]["companies"]
    assert companies["shard_by"] == "cnpj_base_prefix_2"
    assert [shard["shard"] for shard in companies["shards"]] == [
        f"{value:02d}" for value in range(100)
    ]
    assert companies["shards"][7]["url"].endswith("/companies-07.zip")
    assert companies["shards"][7]["size"] == 107
    assert companies["shards"][7]["sha1"] == f"{8:040x}"
    assert "sha256" not in companies["shards"][7]


def test_build_snapshot_entry_rejects_incomplete_or_duplicate_shards(tmp_path):
    output = _output_dir(tmp_path)
    incomplete = _shards()[:-1]
    with pytest.raises(ValueError, match="incomplete companies shards"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_shards=incomplete)

    duplicate = _shards()
    duplicate[-1] = duplicate[-2]
    with pytest.raises(ValueError, match="duplicate companies shard"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_shards=duplicate)


def test_build_snapshot_entry_rejects_wrong_name_or_sha1(tmp_path):
    output = _output_dir(tmp_path)
    wrong_name = _shards()
    wrong_name[0] = CompanyShardIdentity(
        shard="00",
        name="companies-01.zip",
        size=1,
        sha1="b" * 40,
    )
    with pytest.raises(ValueError, match="artifact name"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_shards=wrong_name)

    wrong_sha1 = _shards()
    wrong_sha1[0] = CompanyShardIdentity(
        shard="00",
        name="companies-00.zip",
        size=1,
        sha1="NOT-SHA1",
    )
    with pytest.raises(ValueError, match="invalid sha1"):
        manifest_mod.build_snapshot_entry("2026-05", output, company_shards=wrong_sha1)


def test_verify_snapshot_files_heads_all_100_shards_and_checks_size(monkeypatch, tmp_path):
    output = _output_dir(tmp_path)
    entry = manifest_mod.build_snapshot_entry("2026-05", output, company_shards=_shards())
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
    entry = manifest_mod.build_snapshot_entry("2026-05", output, company_shards=_shards())
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
