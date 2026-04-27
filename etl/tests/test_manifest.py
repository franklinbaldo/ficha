import json
from pathlib import Path

import duckdb
import pytest

from ficha_etl import manifest


def _write_parquet(path: Path, n_rows: int) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT range AS id FROM range({n_rows})) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def test_sha256_of(tmp_path):
    p = tmp_path / "f.txt"
    p.write_bytes(b"hello")
    # sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    assert (
        manifest.sha256_of(p)
        == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )


def test_row_count(tmp_path):
    p = tmp_path / "x.parquet"
    _write_parquet(p, 42)
    assert manifest.row_count(p) == 42


def test_file_entry(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"abc")
    fe = manifest.file_entry(p, "https://example.test/f.bin")
    assert fe.url == "https://example.test/f.bin"
    assert fe.size == 3
    assert len(fe.sha256) == 64


def _build_outputs(output_dir: Path) -> None:
    """Cria os 4 arquivos canônicos com tamanhos diferentes pra distinguir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(output_dir / "cnpjs.parquet", 100)
    _write_parquet(output_dir / "raizes.parquet", 30)
    _write_parquet(output_dir / "socios.parquet", 50)
    (output_dir / "lookups.json").write_text(
        json.dumps({"schema_version": "1.0.0", "snapshot_date": "2026-04"})
    )


def test_build_snapshot_entry(tmp_path):
    out = tmp_path / "out"
    _build_outputs(out)
    snap = manifest.build_snapshot_entry(
        "2026-04", out, schema_version="1.0.0"
    )
    assert snap.date == "2026-04"
    assert snap.schema_version == "1.0.0"
    assert snap.row_counts == {"cnpjs": 100, "raizes": 30, "socios": 50}
    assert set(snap.files.keys()) == {"cnpjs", "raizes", "socios", "lookups"}
    assert snap.files["cnpjs"].url.endswith("/ficha-2026-04/cnpjs.parquet")
    assert snap.files["lookups"].url.endswith("/ficha-2026-04/lookups.json")
    assert snap.files["cnpjs"].size > 0


def test_build_snapshot_entry_missing_file(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    # cria só 3 dos 4 arquivos
    _write_parquet(out / "cnpjs.parquet", 1)
    _write_parquet(out / "raizes.parquet", 1)
    _write_parquet(out / "socios.parquet", 1)
    with pytest.raises(FileNotFoundError, match="lookups.json"):
        manifest.build_snapshot_entry("2026-04", out, schema_version="1.0.0")


def test_update_manifest_creates_when_missing(tmp_path):
    out = tmp_path / "out"
    _build_outputs(out)
    snap = manifest.build_snapshot_entry("2026-04", out, schema_version="1.0.0")

    mp = tmp_path / "manifest.json"
    manifest.update_manifest(mp, snap)
    data = json.loads(mp.read_text())
    assert data["current"] == "2026-04"
    assert len(data["snapshots"]) == 1
    assert data["snapshots"][0]["date"] == "2026-04"


def test_update_manifest_appends_and_sorts(tmp_path):
    out = tmp_path / "out"
    _build_outputs(out)
    s_apr = manifest.build_snapshot_entry("2026-04", out, schema_version="1.0.0")
    s_mar = manifest.build_snapshot_entry("2026-03", out, schema_version="1.0.0")
    s_feb = manifest.build_snapshot_entry("2026-02", out, schema_version="1.0.0")

    mp = tmp_path / "manifest.json"
    manifest.update_manifest(mp, s_mar)
    manifest.update_manifest(mp, s_feb)
    manifest.update_manifest(mp, s_apr)

    data = json.loads(mp.read_text())
    dates = [s["date"] for s in data["snapshots"]]
    assert dates == ["2026-04", "2026-03", "2026-02"]
    assert data["current"] == "2026-04"


def test_update_manifest_replaces_existing_month(tmp_path):
    """Re-rodar o ETL pro mesmo mês deve substituir, não duplicar."""
    out = tmp_path / "out"
    _build_outputs(out)
    s1 = manifest.build_snapshot_entry("2026-04", out, schema_version="1.0.0")

    mp = tmp_path / "manifest.json"
    manifest.update_manifest(mp, s1)

    # Modifica um arquivo e re-roda
    _write_parquet(out / "cnpjs.parquet", 999)  # tamanho diferente
    s2 = manifest.build_snapshot_entry("2026-04", out, schema_version="1.0.0")
    manifest.update_manifest(mp, s2)

    data = json.loads(mp.read_text())
    assert len(data["snapshots"]) == 1
    assert data["snapshots"][0]["row_counts"]["cnpjs"] == 999
