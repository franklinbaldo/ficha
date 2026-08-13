from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb

from ficha_etl import fetcher, transform


def _transform_fixture_module():
    path = Path(__file__).with_name("test_transform.py")
    spec = importlib.util.spec_from_file_location("ficha_test_transform_fixture", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _declared_schema(path: Path, table: str) -> list[tuple[str, str]]:
    con = duckdb.connect()
    try:
        con.execute(path.read_text(encoding="utf-8"))
        return [
            (name, data_type)
            for name, data_type in con.execute(
                "SELECT column_name, data_type FROM duckdb_columns() "
                "WHERE table_name = ? ORDER BY column_index",
                [table],
            ).fetchall()
        ]
    finally:
        con.close()


def _parquet_schema(path: Path) -> list[tuple[str, str]]:
    con = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        return [
            (name, data_type)
            for name, data_type, *_ in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
        ]
    finally:
        con.close()


def test_okf_declared_schemas_match_published_parquet_shapes(tmp_path: Path) -> None:
    fixtures = _transform_fixture_module()
    zips = tmp_path / "zips"
    fixtures._build_full_fixture_zips(zips)

    output = tmp_path / "output"
    transform.transform_snapshot(
        "2026-04",
        cache_dir=tmp_path / "cache",
        output_dir=output,
        chain=fetcher.ChainedFetcher(fetchers=[fixtures._ZipDirFetcher(zips)]),
        skip_unimplemented=False,
    )

    repo = Path(__file__).resolve().parents[2]
    cases = {
        "Cnpj": (repo / "knowledge/types/cnpj.schema.sql", output / "cnpjs.parquet"),
        "Raiz": (repo / "knowledge/types/raiz.schema.sql", output / "raizes.parquet"),
        "Socio": (repo / "knowledge/types/socio.schema.sql", output / "socios.parquet"),
    }

    for table, (declared, parquet) in cases.items():
        assert _declared_schema(declared, table) == _parquet_schema(parquet), table
