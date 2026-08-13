"""Contrato de identidade dos lookups parquet no manifest."""

import hashlib
from pathlib import Path

import duckdb

from ficha_etl import manifest as manifest_mod
from ficha_etl.transform import _LOOKUP_KINDS


def _write_parquet(path: Path, n_rows: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT range AS id FROM range({n_rows})) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _sha1(path: Path) -> str:
    return hashlib.sha1(path.read_bytes(), usedforsecurity=False).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_lookup_parquets_have_dual_hash_identity(tmp_path: Path) -> None:
    out = tmp_path / "output"
    for name in (
        "cnpjs",
        "cnpj_contatos",
        "cnpj_cnaes",
        "raizes",
        "socios",
        "enderecos",
        "pessoas",
    ):
        _write_parquet(out / f"{name}.parquet")
    (out / "lookups.json").write_text("{}", encoding="utf-8")
    (out / "companies.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)

    for kind in _LOOKUP_KINDS:
        _write_parquet(out / "lookups" / f"{kind}.parquet")

    entry = manifest_mod.build_snapshot_entry("2026-05", out)

    assert set(entry["lookups"]) == set(_LOOKUP_KINDS)
    for kind in _LOOKUP_KINDS:
        path = out / "lookups" / f"{kind}.parquet"
        lookup = entry["lookups"][kind]
        assert lookup["size"] == path.stat().st_size
        assert lookup["sha1"] == _sha1(path)
        assert lookup["sha256"] == _sha256(path)
        assert lookup["url"].endswith(f"/lookups/{kind}.parquet")
