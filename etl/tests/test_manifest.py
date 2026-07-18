"""Testes para ficha_etl.manifest."""

import hashlib
import json
from pathlib import Path

import duckdb
import httpx
import pytest

from ficha_etl import manifest as manifest_mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_parquet(path: Path, n_rows: int) -> None:
    """Cria um Parquet mínimo com `n_rows` linhas (coluna `id` INTEGER)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT range AS id FROM range({n_rows})) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    from ficha_etl.transform import _LOOKUP_KINDS

    d = tmp_path / "output"
    _write_parquet(d / "cnpjs.parquet", 10)
    _write_parquet(d / "cnpj_cnaes.parquet", 15)
    _write_parquet(d / "raizes.parquet", 3)
    _write_parquet(d / "socios.parquet", 7)
    _write_parquet(d / "cnpj_contatos.parquet", 5)
    _write_parquet(d / "enderecos.parquet", 8)
    _write_parquet(d / "pessoas.parquet", 12)
    (d / "lookups.json").write_text('{"schema_version":"1.0.0"}', encoding="utf-8")
    # Camada atômica — parte do contrato do snapshot (build_snapshot_entry a exige).
    (d / "companies.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)

    (d / "lookups").mkdir(parents=True, exist_ok=True)
    for kind in _LOOKUP_KINDS:
        _write_parquet(d / "lookups" / f"{kind}.parquet", 5)
    return d


# ---------------------------------------------------------------------------
# build_snapshot_entry
# ---------------------------------------------------------------------------


def test_build_snapshot_entry_shape(output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    assert entry["date"] == "2026-04"
    assert entry["schema_version"] == "1.0.0"
    assert entry["generator"] == "ficha-etl"
    assert entry["rfb_layout_date"] is None
    assert "generated_at" in entry


def test_build_snapshot_entry_row_counts(output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    assert entry["row_counts"] == {
        "cnpjs": 10,
        "cnpj_contatos": 5,
        "cnpj_cnaes": 15,
        "raizes": 3,
        "socios": 7,
        "enderecos": 8,
        "pessoas": 12,
    }


def test_build_snapshot_entry_file_hashes(output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    cnpjs_path = output_dir / "cnpjs.parquet"
    assert entry["files"]["cnpjs"]["sha256"] == _sha256(cnpjs_path)
    assert entry["files"]["cnpjs"]["size"] == cnpjs_path.stat().st_size
    assert "ficha-2026-04" in entry["files"]["cnpjs"]["url"]
    assert "cnpjs.parquet" in entry["files"]["cnpjs"]["url"]

    lookups_path = output_dir / "lookups.json"
    assert entry["files"]["lookups"]["sha256"] == _sha256(lookups_path)
    assert "lookups.json" in entry["files"]["lookups"]["url"]


def test_build_snapshot_entry_includes_companies_zip(output_dir: Path) -> None:
    # Camada atômica faz parte do contrato do snapshot (não pode sumir em silêncio).
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    zip_path = output_dir / "companies.zip"
    assert "companies_zip" in entry["files"]
    assert entry["files"]["companies_zip"]["sha256"] == _sha256(zip_path)
    assert entry["files"]["companies_zip"]["size"] == zip_path.stat().st_size
    assert "companies.zip" in entry["files"]["companies_zip"]["url"]


def test_build_snapshot_entry_missing_companies_zip_raises(output_dir: Path) -> None:
    (output_dir / "companies.zip").unlink()
    with pytest.raises(FileNotFoundError, match="companies.zip"):
        manifest_mod.build_snapshot_entry("2026-04", output_dir)


def test_build_snapshot_entry_missing_file_raises(tmp_path: Path) -> None:
    d = tmp_path / "empty"
    d.mkdir()
    with pytest.raises(FileNotFoundError, match="ausente"):
        manifest_mod.build_snapshot_entry("2026-04", d)


# ---------------------------------------------------------------------------
# update_manifest
# ---------------------------------------------------------------------------


def test_update_manifest_creates_from_scratch(tmp_path: Path, output_dir: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    manifest_mod.update_manifest(manifest_path, entry)

    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert data["current"] == "2026-04"
    assert len(data["snapshots"]) == 1
    assert data["snapshots"][0]["date"] == "2026-04"


def test_update_manifest_upserts_same_month(tmp_path: Path, output_dir: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    # Insere duas vezes o mesmo mês
    manifest_mod.update_manifest(manifest_path, entry)
    manifest_mod.update_manifest(manifest_path, entry)

    data = json.loads(manifest_path.read_text())
    assert len(data["snapshots"]) == 1  # não duplica


def test_update_manifest_keeps_older_snapshots(tmp_path: Path, output_dir: Path) -> None:
    manifest_path = tmp_path / "manifest.json"

    # Snapshot antigo pré-existente
    old_entry = {
        "date": "2026-03",
        "schema_version": "1.0.0",
        "rfb_layout_date": None,
        "generated_at": "2026-03-05T03:00:00Z",
        "generator": "ficha-etl",
        "row_counts": {"cnpjs": 1, "raizes": 1, "socios": 1},
        "files": {
            "cnpjs": {"url": "https://example.com/c.parquet", "sha256": "aa", "size": 1},
            "raizes": {"url": "https://example.com/r.parquet", "sha256": "bb", "size": 1},
            "socios": {"url": "https://example.com/s.parquet", "sha256": "cc", "size": 1},
            "lookups": {"url": "https://example.com/l.json", "sha256": "dd", "size": 1},
        },
    }
    manifest_path.write_text(
        json.dumps({"current": "2026-03", "snapshots": [old_entry]}, indent=2),
        encoding="utf-8",
    )

    new_entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    manifest_mod.update_manifest(manifest_path, new_entry)

    data = json.loads(manifest_path.read_text())
    assert len(data["snapshots"]) == 2
    assert data["current"] == "2026-04"
    dates = [s["date"] for s in data["snapshots"]]
    assert dates == ["2026-04", "2026-03"]  # ordenado decrescente


def test_update_manifest_creates_parent_dirs(tmp_path: Path, output_dir: Path) -> None:
    manifest_path = tmp_path / "nested" / "dir" / "manifest.json"
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    manifest_mod.update_manifest(manifest_path, entry)
    assert manifest_path.exists()


# ---------------------------------------------------------------------------
# verify_snapshot_files
# ---------------------------------------------------------------------------


def _patch_client(monkeypatch, handler) -> None:
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched)


def test_verify_snapshot_files_all_ok(monkeypatch, output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    assert manifest_mod.verify_snapshot_files(entry) == []


def test_verify_snapshot_files_reports_404(monkeypatch, output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    broken_url = entry["files"]["cnpj_contatos"]["url"]

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == broken_url:
            return httpx.Response(404)
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    broken = manifest_mod.verify_snapshot_files(entry)
    assert broken == [broken_url]


def test_verify_snapshot_files_reports_size_mismatch(monkeypatch, output_dir: Path) -> None:
    # Content-Length remoto != size gravado no manifest → upload truncado/trocado.
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)
    bad_url = entry["files"]["cnpjs"]["url"]
    real_size = entry["files"]["cnpjs"]["size"]

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == bad_url:
            return httpx.Response(200, headers={"content-length": str(real_size + 999)})
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    assert manifest_mod.verify_snapshot_files(entry) == [bad_url]


def test_verify_snapshot_files_size_match_ok(monkeypatch, output_dir: Path) -> None:
    # Content-Length correto → não reprova.
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    def handler(request: httpx.Request) -> httpx.Response:
        for f in entry["files"].values():
            if str(request.url) == f["url"]:
                return httpx.Response(200, headers={"content-length": str(f["size"])})
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    assert manifest_mod.verify_snapshot_files(entry) == []


def test_verify_snapshot_files_reports_network_error(monkeypatch, output_dir: Path) -> None:
    entry = manifest_mod.build_snapshot_entry("2026-04", output_dir)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    _patch_client(monkeypatch, handler)
    broken = manifest_mod.verify_snapshot_files(entry)
    # todas as URLs (files + lookups) falham com erro de conexão
    expected_count = len(entry["files"]) + len(entry.get("lookups", {}))
    assert len(broken) == expected_count
