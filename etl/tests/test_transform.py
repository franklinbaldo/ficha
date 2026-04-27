import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import fetcher, transform


# Fixtures pequenas, encoding ISO-8859-1, formato (codigo;descricao).
LOOKUP_FIXTURES: dict[str, list[tuple[str, str]]] = {
    "cnaes": [
        ("0111301", "Cultivo de arroz"),
        ("4711301", "Comércio varejista de mercadorias em supermercados"),
    ],
    "motivos": [
        ("00", "Sem motivo"),
        ("01", "Extinção por encerramento liquidação voluntária"),
    ],
    "municipios": [
        ("3550308", "São Paulo"),
        ("3304557", "Rio de Janeiro"),
    ],
    "naturezas": [
        ("2062", "Sociedade Empresária Limitada"),
        ("2135", "Empresário Individual"),
    ],
    "paises": [
        ("105", "Brasil"),
        ("249", "Estados Unidos"),
    ],
    "qualificacoes": [
        ("05", "Administrador"),
        ("49", "Sócio"),
    ],
}


def _write_csv_iso(path: Path, rows: list[tuple[str, str]]) -> None:
    """Escreve CSV no formato RFB: ISO-8859-1, sep=';', quote='"', no header."""
    body = "\n".join(f'"{c}";"{d}"' for c, d in rows) + "\n"
    path.write_bytes(body.encode("latin-1"))


def _zip_with_csv(zip_path: Path, csv_name: str, rows: list[tuple[str, str]]) -> None:
    """Cria um ZIP contendo um único CSV ISO-8859-1 com as rows."""
    body = ("\n".join(f'"{c}";"{d}"' for c, d in rows) + "\n").encode("latin-1")
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, body)


# -----------------------------------------------------------------------------
# extract_zip
# -----------------------------------------------------------------------------


def test_extract_zip_single_file(tmp_path):
    zp = tmp_path / "Cnaes.zip"
    _zip_with_csv(zp, "F.K03200$Z.D40410.CNAECSV", LOOKUP_FIXTURES["cnaes"])
    dest = tmp_path / "out"
    paths = transform.extract_zip(zp, dest)
    assert len(paths) == 1
    assert paths[0].exists()
    # ISO-8859-1 round-trip preserved
    text = paths[0].read_bytes().decode("latin-1")
    assert "Cultivo de arroz" in text


def test_extract_zip_skips_directories(tmp_path):
    zp = tmp_path / "x.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("data/", "")  # dir entry
        zf.writestr("data/file.csv", "content")
    dest = tmp_path / "out"
    paths = transform.extract_zip(zp, dest)
    # Apenas o arquivo, não a pasta
    files_only = [p for p in paths if p.is_file()]
    assert len(files_only) == 1


# -----------------------------------------------------------------------------
# load_lookup_into_duckdb + lookups_dict
# -----------------------------------------------------------------------------


def test_load_lookup(tmp_path):
    csv = tmp_path / "cnaes.csv"
    _write_csv_iso(csv, LOOKUP_FIXTURES["cnaes"])
    con = duckdb.connect()
    try:
        transform.load_lookup_into_duckdb(con, "cnaes", csv)
        result = transform.lookups_dict(con, "cnaes")
        assert result == dict(LOOKUP_FIXTURES["cnaes"])
    finally:
        con.close()


def test_load_lookup_preserves_iso_encoding(tmp_path):
    """Valida que acentos do português sobrevivem ao read."""
    csv = tmp_path / "muni.csv"
    _write_csv_iso(csv, [("0001", "Águas de São Pedro"), ("0002", "Mauá")])
    con = duckdb.connect()
    try:
        transform.load_lookup_into_duckdb(con, "municipios", csv)
        d = transform.lookups_dict(con, "municipios")
        assert d["0001"] == "Águas de São Pedro"
        assert d["0002"] == "Mauá"
    finally:
        con.close()


# -----------------------------------------------------------------------------
# write_lookups_json
# -----------------------------------------------------------------------------


def test_write_lookups_json_full_shape(tmp_path):
    con = duckdb.connect()
    try:
        for kind, rows in LOOKUP_FIXTURES.items():
            csv = tmp_path / f"{kind}.csv"
            _write_csv_iso(csv, rows)
            transform.load_lookup_into_duckdb(con, kind, csv)

        out = tmp_path / "lookups.json"
        transform.write_lookups_json(
            con, out, schema_version="1.0.0", snapshot_date="2026-04"
        )
        data = json.loads(out.read_text())

        assert data["schema_version"] == "1.0.0"
        assert data["snapshot_date"] == "2026-04"
        # Chaves canônicas conforme web/src/schemas/v1/lookups.ts
        assert set(data.keys()) == {
            "schema_version",
            "snapshot_date",
            "cnaes",
            "motivos_situacao_cadastral",
            "municipios",
            "naturezas_juridicas",
            "paises",
            "qualificacoes_socio",
        }
        assert data["cnaes"]["0111301"] == "Cultivo de arroz"
        assert data["paises"]["105"] == "Brasil"
    finally:
        con.close()


# -----------------------------------------------------------------------------
# Stubs ainda não implementados
# -----------------------------------------------------------------------------


def test_write_cnpjs_parquet_raises(tmp_path):
    con = duckdb.connect()
    try:
        with pytest.raises(NotImplementedError):
            transform.write_cnpjs_parquet(con, [], tmp_path / "x.parquet")
    finally:
        con.close()


def test_write_raizes_parquet_raises(tmp_path):
    con = duckdb.connect()
    try:
        with pytest.raises(NotImplementedError):
            transform.write_raizes_parquet(con, [], tmp_path / "x.parquet")
    finally:
        con.close()


def test_write_socios_parquet_raises(tmp_path):
    con = duckdb.connect()
    try:
        with pytest.raises(NotImplementedError):
            transform.write_socios_parquet(con, [], tmp_path / "x.parquet")
    finally:
        con.close()


# -----------------------------------------------------------------------------
# transform_snapshot end-to-end (com chain stub)
# -----------------------------------------------------------------------------


class _ZipDirFetcher:
    """Stub fetcher que serve ZIPs de um diretório local."""

    name = "stub"

    def __init__(self, zips_dir: Path):
        self.zips_dir = zips_dir

    def get(self, filename: str):
        path = self.zips_dir / filename
        return path if path.exists() else None


@pytest.fixture
def all_zips_dir(tmp_path):
    """Cria os 37 ZIPs canônicos com fixture pra cada um.

    Os 6 lookups recebem dados reais; os outros 31 ficam vazios (CSV vazio).
    """
    zips = tmp_path / "zips"
    zips.mkdir()
    from ficha_etl.sources import canonical_inventory

    csv_name_for_kind = {
        "cnaes": "F.K03200$Z.D40410.CNAECSV",
        "motivos": "F.K03200$Z.D40410.MOTICSV",
        "municipios": "F.K03200$Z.D40410.MUNICCSV",
        "naturezas": "F.K03200$Z.D40410.NATJUCSV",
        "paises": "F.K03200$Z.D40410.PAISCSV",
        "qualificacoes": "F.K03200$Z.D40410.QUALSCSV",
    }
    for spec in canonical_inventory():
        zip_path = zips / spec.name
        rows = LOOKUP_FIXTURES.get(spec.kind, [])
        csv_inside = csv_name_for_kind.get(spec.kind, f"{spec.name}.csv")
        _zip_with_csv(zip_path, csv_inside, rows)
    return zips


def test_transform_snapshot_writes_lookups(tmp_path, all_zips_dir):
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        schema_version="1.0.0",
    )

    lookups_path = output_dir / "lookups.json"
    assert lookups_path.exists()
    data = json.loads(lookups_path.read_text())
    assert data["snapshot_date"] == "2026-04"
    assert data["cnaes"]["0111301"] == "Cultivo de arroz"

    # Os 3 parquets stubs foram pulados (skip_unimplemented=True default).
    assert not (output_dir / "cnpjs.parquet").exists()
    assert not (output_dir / "raizes.parquet").exists()
    assert not (output_dir / "socios.parquet").exists()


def test_transform_snapshot_propagates_unimplemented_when_strict(tmp_path, all_zips_dir):
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    with pytest.raises(NotImplementedError):
        transform.transform_snapshot(
            "2026-04",
            cache_dir=cache_dir,
            output_dir=output_dir,
            chain=chain,
            skip_unimplemented=False,
        )


def test_transform_snapshot_invalid_month():
    with pytest.raises(ValueError):
        transform.transform_snapshot("bad", cache_dir=Path("."), output_dir=Path("."))
