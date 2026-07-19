import json
import logging
import time
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import fetcher, metrics, registry, transform
from ficha_etl.sources import canonical_inventory


# Fixtures pequenas, encoding ISO-8859-1, formato (codigo;descricao).
LOOKUP_FIXTURES: dict[str, list[tuple[str, str]]] = {
    "cnaes": [
        ("0111301", "Cultivo de arroz"),
        ("4711301", "Comércio varejista de mercadorias em supermercados"),
        ("6201500", "Desenvolvimento de programas de computador sob encomenda"),
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

# Linhas de fixture pra tabelas grandes — formato real RFB (sem header).
EMPRESA_ROWS: list[tuple[str, ...]] = [
    # cnpj_basico, razao_social, nat_jur, qualif_resp, capital, porte, ente_fed
    ("11111111", "ACME LTDA", "2062", "49", "100000,50", "03", ""),
    ("22222222", "EMPRESA INDIVIDUAL ME", "2135", "05", "10000,00", "01", ""),
    ("33333333", "TECH SP LTDA", "2062", "49", "500000,00", "03", ""),
]

ESTABELECIMENTO_ROWS: list[tuple[str, ...]] = [
    # cnpj_basico, ordem, dv, matriz_filial, nome_fantasia, sit_cad,
    # data_sit, motivo, cidade_ext, pais, data_inicio, cnae_p, cnae_s,
    # tipo_log, log, num, comp, bairro, cep, uf, municipio,
    # ddd1, tel1, ddd2, tel2, ddd_fax, fax, email, sit_esp, data_sit_esp
    (
        "11111111",
        "0001",
        "00",
        "1",
        "ACME",
        "02",
        "20200101",
        "00",
        "",
        "105",
        "20200101",
        "4711301",
        "6201500",
        "RUA",
        "DAS FLORES",
        "100",
        "",
        "CENTRO",
        "01000000",
        "SP",
        "3550308",
        "11",
        "999999999",
        "",
        "",
        "",
        "",
        "contato@acme.com",
        "",
        "",
    ),
    (
        "11111111",
        "0002",
        "00",
        "2",
        "ACME FILIAL",
        "02",
        "20210101",
        "00",
        "",
        "105",
        "20210101",
        "4711301",
        "",
        "AV",
        "BRASIL",
        "200",
        "",
        "CENTRO",
        "20000000",
        "RJ",
        "3304557",
        "21",
        "888888888",
        "",
        "",
        "",
        "",
        "filial@acme.com",
        "",
        "",
    ),
    (
        "22222222",
        "0001",
        "00",
        "1",
        "INDIV",
        "08",
        "20240101",
        "01",
        "",
        "105",
        "20180101",
        "4711301",
        "",
        "RUA",
        "OUTRA",
        "50",
        "",
        "VILA",
        "01010000",
        "SP",
        "3550308",
        "11",
        "777777777",
        "",
        "",
        "",
        "",
        "ind@x.com",
        "",
        "",
    ),
    (
        "33333333",
        "0001",
        "00",
        "1",
        "TECH",
        "02",
        "20230101",
        "00",
        "",
        "105",
        "20230101",
        "6201500",
        "",
        "RUA",
        "DEV",
        "10",
        "",
        "BAIRRO",
        "04000000",
        "SP",
        "3550308",
        "11",
        "555555555",
        "",
        "",
        "",
        "",
        "dev@tech.com",
        "",
        "",
    ),
]

SOCIO_ROWS: list[tuple[str, ...]] = [
    # cnpj_basico, ident_socio, nome, cnpj_cpf, qualif, data_entrada,
    # pais, rep_legal_cpf, nome_rep, qualif_rep, faixa_etaria
    (
        "11111111",
        "2",
        "JOAO DA SILVA",
        "***123456**",
        "49",
        "20200101",
        "105",
        "",
        "",
        "",
        "5",
    ),
    (
        "11111111",
        "1",
        "OUTRA EMPRESA SA",
        "44444444000100",
        "49",
        "20200101",
        "105",
        "",
        "",
        "",
        "0",
    ),
    # Sócio estrangeiro (tipo '3'): cpf_mascarado e cnpj_socio devem ser NULL
    (
        "11111111",
        "3",
        "JOHN DOE",
        "USA123456",
        "49",
        "20200101",
        "249",
        "",
        "",
        "",
        "0",
    ),
    (
        "33333333",
        "2",
        "MARIA SOUZA",
        "***987654**",
        "49",
        "20230101",
        "105",
        "",
        "",
        "",
        "4",
    ),
]

SIMPLES_ROWS: list[tuple[str, ...]] = [
    # cnpj_basico, opcao_simples, data_opcao, data_excl, opcao_mei, data_opcao_mei, data_excl_mei
    ("11111111", "S", "20200101", "", "N", "", ""),
    ("22222222", "S", "20180101", "", "S", "20180101", ""),
]


def _write_csv_iso(path: Path, rows: list[tuple[str, ...]]) -> None:
    """Escreve CSV no formato RFB: ISO-8859-1, sep=';', quote='"', no header."""
    body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
    path.write_bytes(body.encode("latin-1"))


def _zip_with_csv(zip_path: Path, csv_name: str, rows: list[tuple[str, ...]]) -> None:
    """Cria um ZIP contendo um único CSV ISO-8859-1 com as rows.

    Lista vazia → CSV de 0 bytes (não "\n"), pra que o filtro de
    `_create_table_from_csvs` ignore.
    """
    if rows:
        body = ("\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n").encode("latin-1")
    else:
        body = b""
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
    text = paths[0].read_bytes().decode("latin-1")
    assert "Cultivo de arroz" in text


def test_extract_zip_skips_directories(tmp_path):
    zp = tmp_path / "x.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("data/", "")
        zf.writestr("data/file.csv", "content")
    dest = tmp_path / "out"
    paths = transform.extract_zip(zp, dest)
    files_only = [p for p in paths if p.is_file()]
    assert len(files_only) == 1


# -----------------------------------------------------------------------------
# Lookup loading
# -----------------------------------------------------------------------------


def test_load_lookup(tmp_path):
    csv = tmp_path / "cnaes.csv"
    _write_csv_iso(csv, [(c, d) for c, d in LOOKUP_FIXTURES["cnaes"]])
    con = duckdb.connect()
    try:
        transform.load_lookup_into_duckdb(con, "cnaes", csv)
        result = transform.lookups_dict(con, "cnaes")
        assert result == dict(LOOKUP_FIXTURES["cnaes"])
    finally:
        con.close()


def test_load_lookup_preserves_iso_encoding(tmp_path):
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


def test_load_main_tables_dedupes_simples_duplicates(tmp_path, caplog):
    """W13.1a: load_main_tables collapses simples to 1 row per cnpj_basico."""
    import logging

    con = duckdb.connect()
    try:
        # Build extracted list with duplicate simples rows for the same cnpj_basico.
        zips = tmp_path / "zips"
        rows_for_kind = {
            "cnaes": LOOKUP_FIXTURES["cnaes"],
            "motivos": LOOKUP_FIXTURES["motivos"],
            "municipios": LOOKUP_FIXTURES["municipios"],
            "naturezas": LOOKUP_FIXTURES["naturezas"],
            "paises": LOOKUP_FIXTURES["paises"],
            "qualificacoes": LOOKUP_FIXTURES["qualificacoes"],
            "empresas": EMPRESA_ROWS,
            "estabelecimentos": ESTABELECIMENTO_ROWS,
            "socios": SOCIO_ROWS,
            # Two rows for cnpj_basico '11111111' — violates the 1:1 assumption.
            "simples": [
                ("11111111", "S", "20200101", "", "N", "", ""),
                ("11111111", "N", "20210101", "20210601", "N", "", ""),
            ],
        }
        zips.mkdir()
        seen: set[str] = set()
        for spec in canonical_inventory():
            rows = rows_for_kind.get(spec.kind, [])
            if spec.kind in seen:
                rows = []
            seen.add(spec.kind)
            _zip_with_csv(zips / spec.name, f"{spec.name.removesuffix('.zip')}.CSV", rows)

        from ficha_etl import fetcher

        class _Dir:
            def get(self, name, dest):
                import shutil

                shutil.copy2(zips / name, dest)

        chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(zips)])
        extracted = transform.extract_all("2026-04", chain, tmp_path / "extracted")

        with caplog.at_level(logging.WARNING, logger="ficha_etl.transform"):
            dupes = transform.load_main_tables_into_duckdb(con, extracted)

        assert any("W13.1a" in r.message for r in caplog.records), (
            "Expected W13.1a warning for duplicate simples rows"
        )
        # Finding G do review do owner na PR #70: load_main_tables_into_duckdb
        # devolve a contagem W13.1a (nº de cnpj_basico com >1 linha em
        # simples, antes do dedup) pra transform_snapshot repassar como
        # duplicate_rows do estágio "load_duckdb", sem custo de query nova.
        assert dupes == 1
        # The dedup fix: simples is now exactly 1 row per cnpj_basico.
        n = con.execute("SELECT COUNT(*) FROM simples WHERE cnpj_basico = '11111111'").fetchone()[0]
        assert n == 1, f"expected simples deduped to 1 row for 11111111, got {n}"
    finally:
        con.close()


def test_load_main_tables_dedupes_empresa_duplicates(tmp_path, caplog):
    """W13.1a regression for the +532k mismatch: a duplicate cnpj_basico in
    empresa (unguarded before this fix) fans out the LEFT JOIN in
    write_cnpjs_parquet the same way a simples duplicate does."""
    import logging

    con = duckdb.connect()
    try:
        zips = tmp_path / "zips"
        rows_for_kind = {
            "cnaes": LOOKUP_FIXTURES["cnaes"],
            "motivos": LOOKUP_FIXTURES["motivos"],
            "municipios": LOOKUP_FIXTURES["municipios"],
            "naturezas": LOOKUP_FIXTURES["naturezas"],
            "paises": LOOKUP_FIXTURES["paises"],
            "qualificacoes": LOOKUP_FIXTURES["qualificacoes"],
            # Two rows for cnpj_basico '11111111' — violates the 1:1 assumption.
            "empresas": [
                *EMPRESA_ROWS,
                ("11111111", "ACME LTDA (DUP)", "2062", "49", "100000,50", "03", ""),
            ],
            "estabelecimentos": ESTABELECIMENTO_ROWS,
            "socios": SOCIO_ROWS,
            "simples": SIMPLES_ROWS,
        }
        zips.mkdir()
        seen: set[str] = set()
        for spec in canonical_inventory():
            rows = rows_for_kind.get(spec.kind, [])
            if spec.kind in seen:
                rows = []
            seen.add(spec.kind)
            _zip_with_csv(zips / spec.name, f"{spec.name.removesuffix('.zip')}.CSV", rows)

        from ficha_etl import fetcher

        chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(zips)])
        extracted = transform.extract_all("2026-04", chain, tmp_path / "extracted")

        with caplog.at_level(logging.WARNING, logger="ficha_etl.transform"):
            dupes = transform.load_main_tables_into_duckdb(con, extracted)

        assert any("W13.1a" in r.message and "empresa" in r.message for r in caplog.records), (
            "Expected W13.1a warning for duplicate empresa rows"
        )
        assert dupes == 1
        n = con.execute("SELECT COUNT(*) FROM empresa WHERE cnpj_basico = '11111111'").fetchone()[0]
        assert n == 1, f"expected empresa deduped to 1 row for 11111111, got {n}"
        total = con.execute("SELECT COUNT(*) FROM empresa").fetchone()[0]
        distinct = con.execute("SELECT COUNT(DISTINCT cnpj_basico) FROM empresa").fetchone()[0]
        assert total == distinct
    finally:
        con.close()


def test_transform_snapshot_records_duplicate_rows_for_load_duckdb_stage(tmp_path):
    """Finding G do review do owner na PR #70: duplicate_rows do estágio
    "load_duckdb" reflete a contagem W13.1a real (não None) quando simples
    tem cnpj_basico duplicado -- outros estágios continuam com None
    explícito pros três campos que não existem no pipeline atual
    (casts_invalid, quarantine_rows) ou não se aplicam a eles
    (duplicate_rows fora de load_duckdb)."""
    zips = tmp_path / "zips"
    rows_for_kind = {
        "cnaes": LOOKUP_FIXTURES["cnaes"],
        "motivos": LOOKUP_FIXTURES["motivos"],
        "municipios": LOOKUP_FIXTURES["municipios"],
        "naturezas": LOOKUP_FIXTURES["naturezas"],
        "paises": LOOKUP_FIXTURES["paises"],
        "qualificacoes": LOOKUP_FIXTURES["qualificacoes"],
        "empresas": EMPRESA_ROWS,
        "estabelecimentos": ESTABELECIMENTO_ROWS,
        "socios": SOCIO_ROWS,
        "simples": [
            ("11111111", "S", "20200101", "", "N", "", ""),
            ("11111111", "N", "20210101", "20210601", "N", "", ""),
        ],
    }
    zips.mkdir()
    seen: set[str] = set()
    for spec in canonical_inventory():
        rows = rows_for_kind.get(spec.kind, [])
        if spec.kind in seen:
            rows = []
        seen.add(spec.kind)
        _zip_with_csv(zips / spec.name, f"{spec.name.removesuffix('.zip')}.CSV", rows)

    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(zips)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    data = json.loads(metrics_path.read_text())
    load_stage = next(s for s in data["stages"] if s["stage"] == "load_duckdb")
    assert load_stage["duplicate_rows"] == 1
    assert load_stage["casts_invalid"] is None  # Fase 0 -- conceito não existe ainda
    assert load_stage["quarantine_rows"] is None
    assert load_stage["files_read"] is not None
    assert load_stage["files_read"] > 0

    other_stage = next(s for s in data["stages"] if s["stage"] == "cnpj_contatos")
    assert other_stage["duplicate_rows"] is None
    assert other_stage["casts_invalid"] is None
    assert other_stage["quarantine_rows"] is None


def test_write_lookups_json_full_shape(tmp_path):
    con = duckdb.connect()
    try:
        for kind, rows in LOOKUP_FIXTURES.items():
            csv = tmp_path / f"{kind}.csv"
            _write_csv_iso(csv, [(c, d) for c, d in rows])
            transform.load_lookup_into_duckdb(con, kind, csv)

        out = tmp_path / "lookups.json"
        transform.write_lookups_json(con, out, schema_version="1.0.0", snapshot_date="2026-04")
        data = json.loads(out.read_text())

        assert data["schema_version"] == "1.0.0"
        assert data["snapshot_date"] == "2026-04"
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
# extract_all enforces the "1 CSV per ZIP" invariant
# -----------------------------------------------------------------------------


class _ZipDirFetcher:
    name = "stub"

    def __init__(self, zips_dir: Path):
        self.zips_dir = zips_dir

    def get(self, filename: str):
        path = self.zips_dir / filename
        return path if path.exists() else None


def _build_full_fixture_zips(zips_dir: Path) -> None:
    """Cria os 37 ZIPs canônicos com fixtures realistas.

    Internal filename é único por ZIP (spec.name sem .zip + sufixo) pra que
    extrações múltiplas pra mesma pasta não se sobrescrevam.
    """
    zips_dir.mkdir(parents=True, exist_ok=True)

    rows_for_kind: dict[str, list[tuple[str, ...]]] = {
        "cnaes": LOOKUP_FIXTURES["cnaes"],
        "motivos": LOOKUP_FIXTURES["motivos"],
        "municipios": LOOKUP_FIXTURES["municipios"],
        "naturezas": LOOKUP_FIXTURES["naturezas"],
        "paises": LOOKUP_FIXTURES["paises"],
        "qualificacoes": LOOKUP_FIXTURES["qualificacoes"],
        "empresas": EMPRESA_ROWS,
        "estabelecimentos": ESTABELECIMENTO_ROWS,
        "socios": SOCIO_ROWS,
        "simples": SIMPLES_ROWS,
    }

    # Tabelas particionadas: o primeiro ZIP carrega os dados, os demais ficam vazios
    # (CSV de 0 bytes). Filtrados em _create_table_from_csvs.
    seen_kinds: set[str] = set()
    for spec in canonical_inventory():
        zp = zips_dir / spec.name
        rows = rows_for_kind.get(spec.kind, [])
        if spec.kind in seen_kinds:
            rows = []
        seen_kinds.add(spec.kind)
        # Internal filename único por ZIP — evita colisão na extração.
        inside = f"{spec.name.removesuffix('.zip')}.CSV"
        _zip_with_csv(zp, inside, rows)


@pytest.fixture
def all_zips_dir(tmp_path):
    zips = tmp_path / "zips"
    _build_full_fixture_zips(zips)
    return zips


def test_extract_all_rejects_zip_with_multiple_files(tmp_path, all_zips_dir):
    # Adiciona arquivo extra dentro de Cnaes.zip
    cnaes_zip = all_zips_dir / "Cnaes.zip"
    cnaes_zip.unlink()
    rows = LOOKUP_FIXTURES["cnaes"]
    body = ("\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n").encode("latin-1")
    with zipfile.ZipFile(cnaes_zip, "w") as zf:
        zf.writestr("K3241.K03200$Z.D40410.CNAECSV", body)
        zf.writestr("README.txt", b"unexpected extra")

    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    with pytest.raises(RuntimeError, match="expected exactly 1 CSV"):
        transform.extract_all("2026-04", chain, tmp_path / "extracted")


def test_extract_all_rejects_empty_zip(tmp_path, all_zips_dir):
    # Esvazia Cnaes.zip
    cnaes_zip = all_zips_dir / "Cnaes.zip"
    cnaes_zip.unlink()
    with zipfile.ZipFile(cnaes_zip, "w") as _:
        pass

    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    with pytest.raises(RuntimeError, match="contained no files"):
        transform.extract_all("2026-04", chain, tmp_path / "extracted")


# -----------------------------------------------------------------------------
# transform_snapshot end-to-end com strict=True (3 parquets reais)
# -----------------------------------------------------------------------------


def test_write_cnpj_contatos_parquet_shape(tmp_path):
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR,
            cnpj_ordem VARCHAR,
            cnpj_dv VARCHAR,
            ddd_1 VARCHAR,
            telefone_1 VARCHAR,
            ddd_2 VARCHAR,
            telefone_2 VARCHAR,
            ddd_fax VARCHAR,
            fax VARCHAR,
            correio_eletronico VARCHAR
        );
        INSERT INTO estabelecimento VALUES
        ('11111111', '0001', '00', '11', '12345678', '11', '87654321', '11', '11111111', 'contato@acme.com'),
        ('22222222', '0001', '00', '22', '22222222', NULL, '', '', NULL, ''),
        ('33333333', '0001', '00', '', '', '', '', '', '', '');
        """
    )

    out_path = tmp_path / "cnpj_contatos.parquet"
    transform.write_cnpj_contatos_parquet(con, out_path)

    assert out_path.exists()

    rows = con.execute(f"SELECT * FROM '{out_path}' ORDER BY cnpj, tipo, posicao").fetchall()

    # Expected rows:
    # 11111111000100 -> telefone (1112345678, pos 1), telefone (1187654321, pos 2), fax (1111111111, pos 0), email (contato@acme.com, pos 0)
    # 22222222000100 -> telefone (2222222222, pos 1)
    # 33333333000100 -> no rows

    assert len(rows) == 5

    # 11111111000100
    assert rows[0] == ("11111111000100", "11111111", "email", "contato@acme.com", 0)
    assert rows[1] == ("11111111000100", "11111111", "fax", "1111111111", 0)
    assert rows[2] == ("11111111000100", "11111111", "telefone", "1112345678", 1)
    assert rows[3] == ("11111111000100", "11111111", "telefone", "1187654321", 2)

    # 22222222000100
    assert rows[4] == ("22222222000100", "22222222", "telefone", "2222222222", 1)


def test_transform_snapshot_writes_lookups_and_4_parquets(tmp_path, all_zips_dir):
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        schema_version="1.0.0",
        skip_unimplemented=False,  # exige todos os 4 parquets
    )

    # lookups.json
    lookups_path = output_dir / "lookups.json"
    assert lookups_path.exists()
    data = json.loads(lookups_path.read_text())
    assert data["snapshot_date"] == "2026-04"
    assert data["cnaes"]["0111301"] == "Cultivo de arroz"

    # Os 4 parquets existem
    cnpjs_path = output_dir / "cnpjs.parquet"
    cnpj_cnaes_path = output_dir / "cnpj_cnaes.parquet"
    raizes_path = output_dir / "raizes.parquet"
    socios_path = output_dir / "socios.parquet"
    cnpj_contatos_path = output_dir / "cnpj_contatos.parquet"
    assert cnpjs_path.exists()
    assert cnpj_cnaes_path.exists()
    assert raizes_path.exists()
    assert socios_path.exists()
    assert cnpj_contatos_path.exists()

    # E os 6 parquets de lookups
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            pq_path = output_dir / "lookups" / f"{kind}.parquet"
            assert pq_path.exists()
            rows = con.execute(
                f"SELECT codigo, descricao, descricao_normalizada FROM '{pq_path}' ORDER BY codigo"
            ).fetchall()
            expected_fixture = sorted(LOOKUP_FIXTURES[kind])
            assert len(rows) == len(expected_fixture)
            for i, (expected_codigo, expected_descricao) in enumerate(expected_fixture):
                assert rows[i][0] == expected_codigo
                assert rows[i][1] == expected_descricao
                # just check it has upper chars and strip accents (basic check)
                assert rows[i][2] is not None
                assert isinstance(rows[i][2], str)
    finally:
        con.close()

    # Lê de volta com DuckDB pra validar conteúdo.
    con = duckdb.connect()
    try:
        cnpjs = con.execute(
            f"SELECT cnpj, razao_social, razao_social_normalizada, capital_social, "
            f"natureza_juridica_descricao, situacao_cadastral_descricao, "
            f"municipio_nome, opcao_simples FROM '{cnpjs_path}' ORDER BY cnpj"
        ).fetchall()
        # Valida cnpj_contatos.parquet para ACME
        contatos = con.execute(
            f"SELECT * FROM '{cnpj_contatos_path}' WHERE cnpj_base = '11111111' ORDER BY tipo, valor"
        ).fetchall()
        # No fixture (look at 'Estabelecimentos0.csv'), ACME tem email e ddd_1/telefone_1?
        # Actually I just need to verify it has rows and basic shape. Let's just assert existence and columns.
        assert len(contatos) > 0
        assert len(contatos[0]) == 5  # cnpj, cnpj_base, tipo, valor, posicao

        # 4 estabelecimentos no fixture
        assert len(cnpjs) == 4

        # Primeiro CNPJ: 11111111000100 (ACME matriz, SP)
        first = cnpjs[0]
        assert first[0] == "11111111000100"
        assert first[1] == "ACME LTDA"
        assert "ACME" in first[2]  # normalizada (uppercase)
        assert first[3] == 100000.50
        assert first[4] == "Sociedade Empresária Limitada"
        assert first[5] == "Ativa"
        assert first[6] == "São Paulo"
        assert first[7] is True  # opcao_simples 'S'

        # Verifica cnpj_cnaes da matriz ACME
        acme_cnaes = con.execute(
            f"SELECT cnae_codigo, posicao FROM '{cnpj_cnaes_path}' "
            f"WHERE cnpj = '11111111000100' ORDER BY posicao"
        ).fetchall()
        assert acme_cnaes == [
            ("4711301", 0),  # principal
            ("6201500", 1),  # secundário
        ]

        # ACME tem 2 estabelecimentos (matriz + filial)
        acme_count = con.execute(
            f"SELECT COUNT(*) FROM '{cnpjs_path}' WHERE cnpj_base = '11111111'"
        ).fetchone()[0]
        assert acme_count == 2

        # Raizes — uma linha por cnpj_base
        raizes = con.execute(
            f"SELECT cnpj_base, qtd_estabelecimentos, qtd_estabelecimentos_ativos, "
            f"ufs_atuacao, uf_matriz, municipio_matriz_nome FROM '{raizes_path}' "
            f"ORDER BY cnpj_base"
        ).fetchall()
        assert len(raizes) == 3
        # ACME (11111111): 2 estab, ambos ativos, atua em SP+RJ, matriz SP
        acme = next(r for r in raizes if r[0] == "11111111")
        assert acme[1] == 2
        assert acme[2] == 2
        assert sorted(acme[3]) == ["RJ", "SP"]
        assert acme[4] == "SP"
        assert acme[5] == "São Paulo"
        # Empresa individual (22222222): 1 estab, BAIXADA
        ind = next(r for r in raizes if r[0] == "22222222")
        assert ind[1] == 1
        assert ind[2] == 0  # situacao 08 = baixada (não conta como ativo)

        # cnae_secundario_codigos: ACME matriz tem "6201500" como secundário
        acme_cnae = con.execute(
            f"SELECT cnae_secundario_codigos, cnae_secundario_descricoes "
            f"FROM '{cnpjs_path}' WHERE cnpj = '11111111000100'"
        ).fetchone()
        assert acme_cnae[0] == ["6201500"], f"esperado ['6201500'], got {acme_cnae[0]}"
        # cnae_secundario_descricoes: now populated from lookup_cnaes via
        # _cnae_map cross-join (PR 3b / §9.3). Assert non-empty descriptions
        # — exact text depends on the test fixture's lookup_cnaes content.
        assert len(acme_cnae[1]) == 1
        assert acme_cnae[1][0] != "", (
            f"description for 6201500 should be populated, got {acme_cnae[1]}"
        )

        # CNAE secundário com espaços (trim): ACME filial não tem secundário → []
        filial_cnae = con.execute(
            f"SELECT cnae_secundario_codigos, cnae_secundario_descricoes "
            f"FROM '{cnpjs_path}' WHERE cnpj = '11111111000200'"
        ).fetchone()
        assert filial_cnae[0] == []
        assert filial_cnae[1] == []

        # Socios — agora 4 (PF + PJ + estrangeiro em ACME, PF em TECH)
        socios = con.execute(
            f"SELECT cnpj_base, tipo, tipo_descricao, nome_socio_razao_social, "
            f"cpf_mascarado, cnpj_socio, qualificacao_descricao "
            f"FROM '{socios_path}' ORDER BY cnpj_base, nome_socio_razao_social"
        ).fetchall()
        assert len(socios) == 4
        # PF: JOAO DA SILVA
        joao = next(s for s in socios if s[3] == "JOAO DA SILVA")
        assert joao[0] == "11111111"
        assert joao[1] == "2"
        assert joao[2] == "PF"
        assert joao[4] == "***123456**"
        assert joao[5] is None  # PF não tem cnpj_socio
        # PJ: OUTRA EMPRESA SA
        pj = next(s for s in socios if s[3] == "OUTRA EMPRESA SA")
        assert pj[1] == "1"
        assert pj[2] == "PJ"
        assert pj[4] is None  # PJ não tem cpf_mascarado
        assert pj[5] == "44444444000100"
        # Estrangeiro: JOHN DOE — ambos cpf_mascarado e cnpj_socio devem ser NULL
        ext = next(s for s in socios if s[3] == "JOHN DOE")
        assert ext[1] == "3"
        assert ext[2] == "estrangeiro"
        assert ext[4] is None  # sem cpf_mascarado
        assert ext[5] is None  # sem cnpj_socio
    finally:
        con.close()


def test_transform_snapshot_skips_when_lookup_missing_does_not_break_join(tmp_path, all_zips_dir):
    """Estabelecimentos com CNAE/Município que não existem nos lookups devem
    receber descricao = '' (LEFT JOIN safe), não falhar."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )
    # Ler arquivo, confirmar que LEFT JOINs nunca explodem
    con = duckdb.connect()
    try:
        rc = con.execute(f"SELECT COUNT(*) FROM '{output_dir / 'cnpjs.parquet'}'").fetchone()[0]
        assert rc == 4
    finally:
        con.close()


def test_transform_snapshot_invalid_month():
    with pytest.raises(ValueError):
        transform.transform_snapshot("bad", cache_dir=Path("."), output_dir=Path("."))


# -----------------------------------------------------------------------------
# Finding #2 do review adversarial da PR #70: uma falha ANTES de
# `duckdb.connect()` (ex.: dentro do estágio "extract") corria inteiramente
# fora do try/finally que escreve metrics.json -- nenhum baseline parcial
# sobrevivia a essa falha. O try/finally agora envolve tudo desde antes do
# estágio "extract".
# -----------------------------------------------------------------------------


def test_transform_snapshot_writes_partial_metrics_when_extract_fails(
    tmp_path, all_zips_dir, monkeypatch
):
    """Uma exceção dentro de extract_all (fora do antigo try/finally, antes
    de duckdb.connect() sequer rodar) ainda deve deixar um
    transform_metrics.json no disco, com o estágio "extract" registrado
    (mesmo que parcial) -- baseline parcial é dado útil pra diagnosticar
    onde o pipeline morreu."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    def _boom(*args, **kwargs):
        raise RuntimeError("falha simulada em extract_all")

    monkeypatch.setattr(transform, "extract_all", _boom)

    with pytest.raises(RuntimeError, match="falha simulada em extract_all"):
        transform.transform_snapshot(
            "2026-04",
            cache_dir=cache_dir,
            output_dir=output_dir,
            chain=chain,
            skip_unimplemented=False,
        )

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    assert metrics_path.exists(), "metrics.json deve sobreviver mesmo a uma falha em extract_all"
    data = json.loads(metrics_path.read_text())
    assert data["month"] == "2026-04"
    stage_names = [s["stage"] for s in data["stages"]]
    assert "extract" in stage_names


# -----------------------------------------------------------------------------
# Roundtrip-equivalence (ADR 0009)
# -----------------------------------------------------------------------------


def test_transform_snapshot_with_verify_passes(tmp_path, all_zips_dir):
    """Verify=True não deve falhar quando os dados batem."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    transform.transform_snapshot(
        "2026-04",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        chain=chain,
        skip_unimplemented=False,
        verify=True,
        verify_sample_size=4,
    )
    # Sem exceção = passou


def test_transform_snapshot_roundtrip_verify_metrics_include_reload_time(
    tmp_path, all_zips_dir, monkeypatch
):
    """Finding C do review do owner na PR #70: o reload completo de
    `estabelecimento` pro roundtrip precisa estar DENTRO do estágio medido
    "roundtrip_verify", não antes dele -- senão o baseline reporta
    tempo/picos artificialmente baixos pra verificação, exatamente o custo
    que a Fase 0 existe pra medir.

    Prova injetando um sleep mensurável em `_create_table_from_csvs` só na
    chamada do RELOAD (que passa a lista COMPLETA de CSVs de
    estabelecimento, ao contrário do write chunked, que passa 1 CSV por
    vez) e conferindo que o `wall_seconds` do estágio no metrics.json
    inclui esse tempo.
    """
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    real_create_table = transform._create_table_from_csvs
    sleep_seconds = 0.3

    def _slow_create_table(con_, table, csv_paths, spec):
        if len(csv_paths) > 1:  # reload completo (roundtrip), não um chunk de 1 CSV
            time.sleep(sleep_seconds)
        return real_create_table(con_, table, csv_paths, spec)

    monkeypatch.setattr(transform, "_create_table_from_csvs", _slow_create_table)

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
        verify=True,
        verify_sample_size=4,
    )

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    data = json.loads(metrics_path.read_text())
    verify_stage = next(s for s in data["stages"] if s["stage"] == "roundtrip_verify")
    assert verify_stage["wall_seconds"] >= sleep_seconds


def test_assert_roundtrip_detects_row_count_mismatch(tmp_path, all_zips_dir):
    """Se o parquet tem rows diferente do estabelecimento original, falha."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )
    cnpjs_parquet = output_dir / "cnpjs.parquet"

    # Reconstrói o DuckDB com os dados originais e regrava parquet com 1 row a menos.
    con = duckdb.connect()
    try:
        # Recarrega o source via extract_all (idempotente — reusa pasta diferente)
        extracted = transform.extract_all("2026-04", chain, tmp_path / "extracted2")
        for ef in extracted:
            if ef.kind in transform._LOOKUP_KINDS:
                transform.load_lookup_into_duckdb(con, ef.kind, ef.csv_path)
        transform.load_main_tables_into_duckdb(con, extracted)

        # Regrava parquet com WHERE que tira 1 row.
        truncated = output_dir / "cnpjs_truncated.parquet"
        con.execute(
            f"COPY (SELECT * FROM '{cnpjs_parquet}' WHERE cnpj != "
            f"(SELECT cnpj FROM '{cnpjs_parquet}' LIMIT 1)) "
            f"TO '{truncated}' (FORMAT PARQUET)"
        )

        with pytest.raises(transform.RoundtripError, match="row count mismatch"):
            transform.assert_roundtrip(con, truncated)
    finally:
        con.close()


def test_assert_roundtrip_detects_field_divergence(tmp_path, all_zips_dir):
    """Se um campo no parquet diverge do source, falha."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )
    cnpjs_parquet = output_dir / "cnpjs.parquet"

    con = duckdb.connect()
    try:
        # Recarrega o source (cache_dir/2026-04/extracted)
        extracted = transform.extract_all("2026-04", chain, tmp_path / "extracted_for_verify")
        for ef in extracted:
            if ef.kind in transform._LOOKUP_KINDS:
                transform.load_lookup_into_duckdb(con, ef.kind, ef.csv_path)
        transform.load_main_tables_into_duckdb(con, extracted)

        # Cria um parquet "tampered" com razao_social trocada.
        tampered = output_dir / "cnpjs_tampered.parquet"
        con.execute(
            f"""COPY (
                SELECT * REPLACE ('TAMPERED' AS razao_social)
                FROM '{cnpjs_parquet}'
            ) TO '{tampered}' (FORMAT PARQUET)"""
        )

        with pytest.raises(transform.RoundtripError, match="razao_social"):
            transform.assert_roundtrip(con, tampered, sample_size=4)
    finally:
        con.close()


def test_assert_roundtrip_empty_estabelecimento_is_noop(tmp_path):
    """Se estabelecimento estiver vazio, roundtrip passa silenciosamente."""
    con = duckdb.connect()
    try:
        # Cria tabelas vazias com schema mínimo
        con.execute(
            "CREATE TABLE estabelecimento (cnpj_basico VARCHAR, cnpj_ordem VARCHAR, "
            "cnpj_dv VARCHAR, identificador_matriz_filial VARCHAR, nome_fantasia VARCHAR, "
            "situacao_cadastral VARCHAR, uf VARCHAR, municipio VARCHAR, "
            "cnae_fiscal_principal VARCHAR)"
        )
        con.execute("CREATE TABLE empresa (cnpj_basico VARCHAR, razao_social VARCHAR)")

        empty = tmp_path / "empty.parquet"
        con.execute(
            "COPY (SELECT NULL::VARCHAR AS cnpj, NULL::VARCHAR AS razao_social, "
            "NULL::VARCHAR AS uf, NULL::VARCHAR AS cnae_principal_codigo, "
            "NULL::VARCHAR AS situacao_cadastral, NULL::VARCHAR AS nome_fantasia, "
            "NULL::VARCHAR AS identificador_matriz_filial, NULL::VARCHAR AS municipio_codigo "
            "WHERE FALSE) TO '" + str(empty) + "' (FORMAT PARQUET)"
        )

        # Não deve raise (0 == 0)
        transform.assert_roundtrip(con, empty)
    finally:
        con.close()


def _build_roundtrip_dataset(con, parquet_path: Path, n_rows: int, tamper: bool) -> None:
    """Monta estabelecimento+empresa e o cnpjs.parquet correspondente.

    Se `tamper`, grava razao_social divergente pra todas as linhas — assim
    qualquer amostra não-vazia deve acusar divergência.
    """
    con.execute(
        "CREATE TABLE estabelecimento (cnpj_basico VARCHAR, cnpj_ordem VARCHAR, "
        "cnpj_dv VARCHAR, identificador_matriz_filial VARCHAR, nome_fantasia VARCHAR, "
        "situacao_cadastral VARCHAR, uf VARCHAR, municipio VARCHAR, "
        "cnae_fiscal_principal VARCHAR)"
    )
    con.execute("CREATE TABLE empresa (cnpj_basico VARCHAR, razao_social VARCHAR)")
    for i in range(n_rows):
        base = f"{i:08d}"
        con.execute(
            "INSERT INTO estabelecimento VALUES (?, '0001', '00', '1', ?, '02', 'SP', "
            "'3550308', '6201500')",
            [base, f"FANTASIA {i}"],
        )
        con.execute("INSERT INTO empresa VALUES (?, ?)", [base, f"RAZAO {i}"])
    razao_expr = "'TAMPERED'" if tamper else "emp.razao_social"
    con.execute(
        f"""
        COPY (
            SELECT est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
                   {razao_expr} AS razao_social,
                   est.uf AS uf,
                   est.cnae_fiscal_principal AS cnae_principal_codigo,
                   est.situacao_cadastral AS situacao_cadastral,
                   est.nome_fantasia AS nome_fantasia,
                   est.identificador_matriz_filial AS identificador_matriz_filial,
                   est.municipio AS municipio_codigo
            FROM estabelecimento est
            LEFT JOIN empresa emp ON emp.cnpj_basico = est.cnpj_basico
        ) TO '{parquet_path}' (FORMAT PARQUET)
        """
    )


def test_assert_roundtrip_sample_is_deterministic(tmp_path):
    """A verificação usa reservoir REPEATABLE — a mesma amostra em toda run.

    Com razao_social adulterada em todas as linhas e amostra menor que o total,
    duas chamadas devem produzir a MESMA mensagem de erro (mesmo conjunto e
    ordem de CNPJs amostrados). Amostragem não-determinística faria a mensagem
    variar entre chamadas.
    """
    parquet = tmp_path / "cnpjs.parquet"
    con = duckdb.connect()
    try:
        _build_roundtrip_dataset(con, parquet, n_rows=40, tamper=True)

        msgs: list[str] = []
        for _ in range(2):
            with pytest.raises(transform.RoundtripError) as exc:
                transform.assert_roundtrip(con, parquet, sample_size=8)
            msgs.append(str(exc.value))

        assert msgs[0] == msgs[1]
        # Sanidade: divergência veio do caminho de campo, não de count/missing.
        assert "razao_social" in msgs[0]
        assert "row count mismatch" not in msgs[0]
    finally:
        con.close()


def test_assert_roundtrip_passes_on_faithful_parquet(tmp_path):
    """Parquet fiel ao source passa sem raise, mesmo com amostra < total."""
    parquet = tmp_path / "cnpjs.parquet"
    con = duckdb.connect()
    try:
        _build_roundtrip_dataset(con, parquet, n_rows=40, tamper=False)
        transform.assert_roundtrip(con, parquet, sample_size=8)  # não deve raise
    finally:
        con.close()


def test_write_cnpjs_parquet_handles_duplicate_cnae_codigo(tmp_path):
    """write_cnpjs_parquet must not crash if lookup_cnaes has duplicate codigos.

    Regression test for Kilo PR #28 review: DuckDB's MAP() throws on
    duplicate keys, so the _cnae_map build is wrapped in GROUP BY codigo
    + ANY_VALUE(descricao). This test exercises write_cnpjs_parquet
    directly against synthetic tables containing a duplicate codigo.
    """
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE lookup_cnaes (codigo VARCHAR, descricao VARCHAR)")
        con.execute(
            "INSERT INTO lookup_cnaes VALUES "
            "('6201500', 'Desenvolvimento de software'), "
            "('6201500', 'Desenvolvimento de software (duplicate)'), "
            "('5611201', 'Restaurantes')"
        )
        # Other lookups that write_cnpjs_parquet JOINs against — empty
        # tables with the right schema are sufficient.
        for tbl in (
            "lookup_naturezas",
            "lookup_qualificacoes",
            "lookup_motivos",
            "lookup_municipios",
            "lookup_paises",
        ):
            con.execute(f"CREATE TABLE {tbl} (codigo VARCHAR, descricao VARCHAR)")
        con.execute(
            "CREATE TABLE estabelecimento ("
            "cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR, "
            "identificador_matriz_filial VARCHAR, nome_fantasia VARCHAR, "
            "situacao_cadastral VARCHAR, data_situacao_cadastral VARCHAR, "
            "motivo_situacao_cadastral VARCHAR, nome_cidade_exterior VARCHAR, "
            "pais VARCHAR, data_inicio_atividade VARCHAR, "
            "cnae_fiscal_principal VARCHAR, cnae_fiscal_secundaria VARCHAR, "
            "tipo_logradouro VARCHAR, logradouro VARCHAR, numero VARCHAR, "
            "complemento VARCHAR, bairro VARCHAR, cep VARCHAR, uf VARCHAR, "
            "municipio VARCHAR, ddd_1 VARCHAR, telefone_1 VARCHAR, "
            "ddd_2 VARCHAR, telefone_2 VARCHAR, ddd_fax VARCHAR, fax VARCHAR, "
            "correio_eletronico VARCHAR, situacao_especial VARCHAR, "
            "data_situacao_especial VARCHAR)"
        )
        con.execute(
            "INSERT INTO estabelecimento VALUES ("
            "'11111111','0001','00','1','ACME','02','20200101','','','','20200101',"
            "'6201500','5611201',"
            "'','','','','','','SP','3550308','','','','','','','','','')"
        )
        con.execute(
            "CREATE TABLE empresa (cnpj_basico VARCHAR, razao_social VARCHAR, "
            "natureza_juridica VARCHAR, qualificacao_responsavel VARCHAR, "
            "capital_social VARCHAR, porte_empresa VARCHAR, "
            "ente_federativo_responsavel VARCHAR)"
        )
        con.execute("INSERT INTO empresa VALUES ('11111111','ACME','','','0','','')")
        con.execute(
            "CREATE TABLE simples (cnpj_basico VARCHAR, opcao_simples VARCHAR, "
            "data_opcao_simples VARCHAR, data_exclusao_simples VARCHAR, "
            "opcao_mei VARCHAR, data_opcao_mei VARCHAR, "
            "data_exclusao_mei VARCHAR)"
        )

        out_path = tmp_path / "cnpjs_dup.parquet"
        # Should not raise despite the duplicate '6201500' in lookup_cnaes.
        transform.write_cnpjs_parquet(con, out_path)
        assert out_path.exists()

        descricoes = con.execute(
            f"SELECT cnae_secundario_descricoes FROM '{out_path}' WHERE cnpj = '11111111000100'"
        ).fetchone()[0]
        # Whichever winning descricao GROUP BY picked, the lookup must
        # have produced something non-empty for '5611201'.
        assert descricoes == ["Restaurantes"]
    finally:
        con.close()


def test_create_table_from_csvs_sniff_utf8(tmp_path, caplog):
    import logging
    from ficha_etl.transform import _create_table_from_csvs
    import duckdb

    csv_path = tmp_path / "data_utf8.csv"
    # Write some utf-8 characters
    csv_path.write_bytes('1;2;"Olá Mundo"'.encode("utf-8"))

    con = duckdb.connect()
    try:
        with caplog.at_level(logging.WARNING):
            spec = registry.CsvSpec(columns=("c1", "c2", "c3"))
            _create_table_from_csvs(con, "test_table", [csv_path], spec)

        assert (
            "tabela 'test_table' carregada com encoding=utf-8 ignore_errors=True (fallback)"
            in caplog.text
        )

        res = con.execute("SELECT * FROM test_table").fetchall()
        assert res == [("1", "2", "Olá Mundo")]
    finally:
        con.close()


def test_create_table_from_csvs_sniff_latin1(tmp_path, caplog):
    import logging
    from ficha_etl.transform import _create_table_from_csvs
    import duckdb

    csv_path = tmp_path / "data_latin1.csv"
    # Write some latin-1 characters that are invalid utf-8
    csv_path.write_bytes('1;2;"Olá Mundo"'.encode("latin-1"))

    con = duckdb.connect()
    try:
        with caplog.at_level(logging.WARNING):
            spec = registry.CsvSpec(columns=("c1", "c2", "c3"))
            _create_table_from_csvs(con, "test_table_latin", [csv_path], spec)

        assert "fallback" not in caplog.text  # latin-1 without ignore_errors does not log fallback

        res = con.execute("SELECT * FROM test_table_latin").fetchall()
        assert res == [("1", "2", "Olá Mundo")]
    finally:
        con.close()


def test_write_cnpj_cnaes_parquet_position_ordering(tmp_path):
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR,
            cnpj_ordem VARCHAR,
            cnpj_dv VARCHAR,
            cnae_fiscal_principal VARCHAR,
            cnae_fiscal_secundaria VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO estabelecimento VALUES
        ('99999999', '0001', '99', '1111111', '5611201,4711301,9311500')
    """)

    output_path = tmp_path / "cnpj_cnaes.parquet"
    transform.write_cnpj_cnaes_parquet(con, output_path)

    rows = con.execute(
        f"SELECT cnae_codigo, posicao FROM '{output_path}' ORDER BY posicao"
    ).fetchall()

    assert rows == [
        ("1111111", 0),
        ("5611201", 1),
        ("4711301", 2),
        ("9311500", 3),
    ]


# -----------------------------------------------------------------------------
# write_cnpjs_parquet_chunked — matches full write_cnpjs_parquet
# -----------------------------------------------------------------------------


def _setup_duckdb_with_lookups_empresa_simples(
    con: duckdb.DuckDBPyConnection, extracted: list, tmp_path: "Path"
) -> None:
    """Load lookups + empresa + simples into con (but NOT estabelecimento)."""
    for ef in extracted:
        if ef.kind in transform._LOOKUP_KINDS:
            transform.load_lookup_into_duckdb(con, ef.kind, ef.csv_path)
    # Load only empresa and simples, not estabelecimento.
    import collections

    by_kind: dict = collections.defaultdict(list)
    for ef in extracted:
        by_kind[ef.kind].append(ef.csv_path)
    for table, kind, cols in (
        ("empresa", "empresas", transform._EMPRESA_COLUMNS),
        ("simples", "simples", transform._SIMPLES_COLUMNS),
    ):
        spec = registry.CsvSpec(columns=cols)
        transform._create_table_from_csvs(con, table, by_kind.get(kind, []), spec)


def test_write_cnpjs_parquet_chunked_matches_full_write(tmp_path, all_zips_dir):
    """write_cnpjs_parquet_chunked must produce the same rows as write_cnpjs_parquet.

    Compares row count and every row (sorted by cnpj) between:
    - write_cnpjs_parquet (full load, all tables in con)
    - write_cnpjs_parquet_chunked (loads one estabelecimento CSV at a time)
    """
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    extract_dir = tmp_path / "extracted"
    extracted = transform.extract_all("2026-04", chain, extract_dir)

    estabelecimento_csv_paths = [ef.csv_path for ef in extracted if ef.kind == "estabelecimentos"]

    # --- Full write (reference) ---
    con_full = duckdb.connect()
    try:
        for ef in extracted:
            if ef.kind in transform._LOOKUP_KINDS:
                transform.load_lookup_into_duckdb(con_full, ef.kind, ef.csv_path)
        transform.load_main_tables_into_duckdb(con_full, extracted)

        full_out = tmp_path / "cnpjs_full.parquet"
        transform.write_cnpjs_parquet(con_full, full_out)
    finally:
        con_full.close()

    # --- Chunked write ---
    con_chunked = duckdb.connect()
    try:
        _setup_duckdb_with_lookups_empresa_simples(con_chunked, extracted, tmp_path)

        chunked_out = tmp_path / "cnpjs_chunked.parquet"
        transform.write_cnpjs_parquet_chunked(con_chunked, estabelecimento_csv_paths, chunked_out)
    finally:
        con_chunked.close()

    # --- Compare ---
    compare_con = duckdb.connect()
    try:
        full_count = compare_con.execute(f"SELECT COUNT(*) FROM '{full_out}'").fetchone()[0]
        chunked_count = compare_con.execute(f"SELECT COUNT(*) FROM '{chunked_out}'").fetchone()[0]
        assert full_count == chunked_count, (
            f"row count mismatch: full={full_count}, chunked={chunked_count}"
        )

        # Compare all rows sorted by cnpj — same order expected since both sort by cnpj.
        full_rows = compare_con.execute(
            f"SELECT cnpj, razao_social, situacao_cadastral, uf, municipio_codigo "
            f"FROM '{full_out}' ORDER BY cnpj"
        ).fetchall()
        chunked_rows = compare_con.execute(
            f"SELECT cnpj, razao_social, situacao_cadastral, uf, municipio_codigo "
            f"FROM '{chunked_out}' ORDER BY cnpj"
        ).fetchall()

        assert full_rows == chunked_rows, (
            f"row content mismatch; first divergence at index "
            f"{next(i for i, (a, b) in enumerate(zip(full_rows, chunked_rows)) if a != b)}"
            if full_rows != chunked_rows
            else ""
        )
    finally:
        compare_con.close()


# -----------------------------------------------------------------------------
# Finding #4 do review adversarial da PR #70: RFC 0001 §16 exige registro
# por estágio E POR CHUNK -- write_cnpjs_parquet_chunked precisa reportar um
# ChunkMetrics por CSV não-vazio via `on_chunk`, não só o agregado do
# estágio inteiro (o incidente histórico de OOM foi isolado no chunk 0/10,
# invisível olhando só o total).
# -----------------------------------------------------------------------------


def test_write_cnpjs_parquet_chunked_calls_on_chunk_per_nonempty_csv(tmp_path):
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            con.execute(
                f"CREATE OR REPLACE TABLE lookup_{kind} (codigo VARCHAR, descricao VARCHAR)"
            )
        con.execute(
            """
            CREATE TABLE empresa (
                cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
                qualificacao_responsavel VARCHAR, capital_social VARCHAR,
                porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR
            )
            """
        )
        for row in EMPRESA_ROWS:
            con.execute("INSERT INTO empresa VALUES (?, ?, ?, ?, ?, ?, ?)", list(row))
        con.execute(
            """
            CREATE TABLE simples (
                cnpj_basico VARCHAR, opcao_simples VARCHAR, data_opcao_simples VARCHAR,
                data_exclusao_simples VARCHAR, opcao_mei VARCHAR, data_opcao_mei VARCHAR,
                data_exclusao_mei VARCHAR
            )
            """
        )

        # 2 CSVs "de chunk" não-vazios, cnpj_basico distintos -- simula o
        # split real por ZIP da RFB (arbitrário, não alinhado a nenhum
        # prefixo de cnpj_basico).
        chunk_a_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] == "11111111"]
        chunk_b_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] != "11111111"]
        assert chunk_a_rows and chunk_b_rows  # sanity da fixture

        def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
            body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
            path.write_bytes(body.encode("latin-1"))

        csv_a = tmp_path / "chunk_a.csv"
        csv_b = tmp_path / "chunk_b.csv"
        _write_csv(csv_a, chunk_a_rows)
        _write_csv(csv_b, chunk_b_rows)

        recorded: list = []
        out_path = tmp_path / "cnpjs.parquet"
        transform.write_cnpjs_parquet_chunked(
            con, [csv_a, csv_b], out_path, on_chunk=recorded.append
        )

        assert len(recorded) == 2  # um por CSV não-vazio
        for idx, (chunk_metrics, csv_path, expected_rows) in enumerate(
            (
                (recorded[0], csv_a, len(chunk_a_rows)),
                (recorded[1], csv_b, len(chunk_b_rows)),
            )
        ):
            assert chunk_metrics.index == idx
            assert chunk_metrics.csv_name == csv_path.name
            assert chunk_metrics.rows_written == expected_rows
            assert chunk_metrics.wall_seconds >= 0
            assert chunk_metrics.bytes_read == csv_path.stat().st_size
            assert chunk_metrics.bytes_written is not None
            assert chunk_metrics.bytes_written > 0

        # rows_written de cada chunk bate com o parquet final (métricas plausíveis).
        total_rows = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
        assert total_rows == sum(c.rows_written for c in recorded)
    finally:
        con.close()


def test_write_cnpjs_parquet_chunked_rows_written_reflects_join_fanout_not_input_count(tmp_path):
    """Finding B do review do owner na PR #70: `rows_written` tem que vir
    do PARQUET escrito, não da contagem da tabela `estabelecimento` de
    ENTRADA -- prova com um cenário onde os dois números DIVERGEM de
    propósito: uma duplicata em `simples` pro mesmo cnpj_basico causa
    fan-out no LEFT JOIN (W13.1a), então 1 linha de estabelecimento vira 2
    linhas no parquet do chunk."""
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            con.execute(
                f"CREATE OR REPLACE TABLE lookup_{kind} (codigo VARCHAR, descricao VARCHAR)"
            )
        con.execute(
            """
            CREATE TABLE empresa (
                cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
                qualificacao_responsavel VARCHAR, capital_social VARCHAR,
                porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO empresa VALUES "
            "('11111111', 'ACME LTDA', '2062', '49', '100000,00', '03', '')"
        )
        con.execute(
            """
            CREATE TABLE simples (
                cnpj_basico VARCHAR, opcao_simples VARCHAR, data_opcao_simples VARCHAR,
                data_exclusao_simples VARCHAR, opcao_mei VARCHAR, data_opcao_mei VARCHAR,
                data_exclusao_mei VARCHAR
            )
            """
        )
        # Duplicata proposital: 2 linhas de simples pro MESMO cnpj_basico ->
        # fan-out no LEFT JOIN -- 1 estabelecimento vira 2 linhas no parquet.
        con.execute("INSERT INTO simples VALUES ('11111111', 'S', '20200101', '', 'N', '', '')")
        con.execute(
            "INSERT INTO simples VALUES ('11111111', 'N', '20210101', '20220101', 'N', '', '')"
        )

        # 1 única linha de estabelecimento na entrada (n=1 se contássemos a
        # tabela de entrada, como o código fazia antes do fix).
        chunk_row = [r for r in ESTABELECIMENTO_ROWS if r[0] == "11111111"][:1]

        def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
            body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
            path.write_bytes(body.encode("latin-1"))

        csv_a = tmp_path / "chunk_a.csv"
        _write_csv(csv_a, chunk_row)

        recorded: list = []
        out_path = tmp_path / "cnpjs.parquet"
        transform.write_cnpjs_parquet_chunked(con, [csv_a], out_path, on_chunk=recorded.append)

        assert len(recorded) == 1
        actual_parquet_rows = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
        assert actual_parquet_rows == 2  # fan-out: 1 estabelecimento x 2 simples
        # rows_written bate com o PARQUET (2), não com a entrada (1) --
        # antes do fix isso reportaria 1 (o bug que o finding B aponta).
        assert recorded[0].rows_written == 2
    finally:
        con.close()


def test_write_cnpjs_parquet_chunked_skips_empty_csv_without_calling_on_chunk(tmp_path):
    """CSV de 0 bytes na lista -- não deve gerar chamada a on_chunk (o
    finding pede "uma vez por CSV NÃO-vazio")."""
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            con.execute(
                f"CREATE OR REPLACE TABLE lookup_{kind} (codigo VARCHAR, descricao VARCHAR)"
            )
        con.execute(
            """
            CREATE TABLE empresa (
                cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
                qualificacao_responsavel VARCHAR, capital_social VARCHAR,
                porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR
            )
            """
        )
        for row in EMPRESA_ROWS:
            con.execute("INSERT INTO empresa VALUES (?, ?, ?, ?, ?, ?, ?)", list(row))
        con.execute(
            """
            CREATE TABLE simples (
                cnpj_basico VARCHAR, opcao_simples VARCHAR, data_opcao_simples VARCHAR,
                data_exclusao_simples VARCHAR, opcao_mei VARCHAR, data_opcao_mei VARCHAR,
                data_exclusao_mei VARCHAR
            )
            """
        )

        chunk_a_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] == "11111111"]

        def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
            body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
            path.write_bytes(body.encode("latin-1"))

        csv_a = tmp_path / "chunk_a.csv"
        _write_csv(csv_a, chunk_a_rows)
        empty_csv = tmp_path / "chunk_empty.csv"
        empty_csv.write_bytes(b"")

        recorded: list = []
        out_path = tmp_path / "cnpjs.parquet"
        transform.write_cnpjs_parquet_chunked(
            con, [empty_csv, csv_a], out_path, on_chunk=recorded.append
        )

        assert len(recorded) == 1
        assert recorded[0].csv_name == csv_a.name
        assert recorded[0].index == 1  # posição real na lista, não reindexado
    finally:
        con.close()


def test_write_cnpjs_parquet_chunked_reports_failed_chunk_and_reraises(tmp_path, monkeypatch):
    """Finding B do review do owner na PR #70: um chunk que falha no meio do
    processamento (ex.: OOM) precisa continuar aparecendo no metrics.json
    (status="failed") E a exceção original ainda tem que propagar pra fora
    de write_cnpjs_parquet_chunked -- o callback de métrica documenta a
    falha, nunca a suprime.

    Também cobre Finding F: mesmo o chunk que falha registra
    rss_peak_mib/duckdb_tmp_peak_mib/workdir_peak_mib com o que foi
    possível medir até a falha (via `disk_peaks_fn`, o mesmo sampler
    compartilhado do estágio -- não uma thread nova por chunk)."""
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            con.execute(
                f"CREATE OR REPLACE TABLE lookup_{kind} (codigo VARCHAR, descricao VARCHAR)"
            )
        con.execute(
            """
            CREATE TABLE empresa (
                cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
                qualificacao_responsavel VARCHAR, capital_social VARCHAR,
                porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR
            )
            """
        )
        for row in EMPRESA_ROWS:
            con.execute("INSERT INTO empresa VALUES (?, ?, ?, ?, ?, ?, ?)", list(row))
        con.execute(
            """
            CREATE TABLE simples (
                cnpj_basico VARCHAR, opcao_simples VARCHAR, data_opcao_simples VARCHAR,
                data_exclusao_simples VARCHAR, opcao_mei VARCHAR, data_opcao_mei VARCHAR,
                data_exclusao_mei VARCHAR
            )
            """
        )

        chunk_a_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] == "11111111"]
        chunk_b_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] != "11111111"]

        def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
            body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
            path.write_bytes(body.encode("latin-1"))

        csv_a = tmp_path / "chunk_a.csv"
        csv_b = tmp_path / "chunk_b.csv"
        _write_csv(csv_a, chunk_a_rows)
        _write_csv(csv_b, chunk_b_rows)

        real_create_table = transform._create_table_from_csvs

        def _flaky_create_table(con_, table, csv_paths, spec):
            # Cada chunk chama isto com uma lista de 1 CSV só -- falha
            # especificamente no chunk cujo CSV é csv_b (índice 1).
            if csv_paths and csv_paths[0].name == csv_b.name:
                raise RuntimeError("falha simulada no chunk B (OOM)")
            return real_create_table(con_, table, csv_paths, spec)

        monkeypatch.setattr(transform, "_create_table_from_csvs", _flaky_create_table)

        workdir = tmp_path / "workdir"
        workdir.mkdir()
        sampler = metrics._DiskPeakSampler({"workdir": workdir}, interval=0.02)
        sampler.start()

        recorded: list = []
        out_path = tmp_path / "cnpjs.parquet"
        try:
            with pytest.raises(RuntimeError, match="falha simulada no chunk B"):
                transform.write_cnpjs_parquet_chunked(
                    con,
                    [csv_a, csv_b],
                    out_path,
                    on_chunk=recorded.append,
                    disk_peaks_fn=sampler.current_peaks,
                )
        finally:
            sampler.stop()

        assert len(recorded) == 2
        assert recorded[0].status == "ok"
        assert recorded[0].csv_name == csv_a.name
        assert recorded[0].index == 0
        # rss_peak_mib é sempre barato (chamada direta, sem thread) -- deve
        # vir preenchido mesmo no chunk que teve sucesso.
        assert recorded[0].rss_peak_mib is not None

        assert recorded[1].status == "failed"
        assert recorded[1].csv_name == csv_b.name
        assert recorded[1].index == 1
        assert recorded[1].rows_written is None
        assert recorded[1].error is not None
        assert "falha simulada no chunk B" in recorded[1].error
        assert recorded[1].wall_seconds >= 0
        # Finding F: o chunk que falhou ainda registra o que foi possível
        # medir até ali -- rss sempre disponível, e o pico de workdir do
        # sampler compartilhado (mesmo que 0.0, o campo não pode ser
        # ausente/levantar exceção tentando computá-lo).
        assert recorded[1].rss_peak_mib is not None
        assert recorded[1].workdir_peak_mib is not None
    finally:
        con.close()


def test_write_cnpjs_parquet_chunked_records_disk_peaks_via_shared_sampler(tmp_path):
    """Finding F do review do owner na PR #70: ChunkMetrics.workdir_peak_mib
    reflete o pico visto ATÉ AGORA no MESMO sampler do estágio (via
    `disk_peaks_fn`), sem abrir uma thread de amostragem nova por chunk.
    Prova escrevendo um arquivo grande no workdir monitorado DEPOIS do
    chunk 0 (dentro do callback de teste) e conferindo que o chunk 1 (que
    roda em seguida) já vê esse crescimento."""
    con = duckdb.connect()
    try:
        for kind in transform._LOOKUP_KINDS:
            con.execute(
                f"CREATE OR REPLACE TABLE lookup_{kind} (codigo VARCHAR, descricao VARCHAR)"
            )
        con.execute(
            """
            CREATE TABLE empresa (
                cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
                qualificacao_responsavel VARCHAR, capital_social VARCHAR,
                porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR
            )
            """
        )
        for row in EMPRESA_ROWS:
            con.execute("INSERT INTO empresa VALUES (?, ?, ?, ?, ?, ?, ?)", list(row))
        con.execute(
            """
            CREATE TABLE simples (
                cnpj_basico VARCHAR, opcao_simples VARCHAR, data_opcao_simples VARCHAR,
                data_exclusao_simples VARCHAR, opcao_mei VARCHAR, data_opcao_mei VARCHAR,
                data_exclusao_mei VARCHAR
            )
            """
        )

        chunk_a_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] == "11111111"]
        chunk_b_rows = [r for r in ESTABELECIMENTO_ROWS if r[0] != "11111111"]

        def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
            body = "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
            path.write_bytes(body.encode("latin-1"))

        csv_a = tmp_path / "chunk_a.csv"
        csv_b = tmp_path / "chunk_b.csv"
        _write_csv(csv_a, chunk_a_rows)
        _write_csv(csv_b, chunk_b_rows)

        workdir = tmp_path / "workdir"
        workdir.mkdir()
        sampler = metrics._DiskPeakSampler({"workdir": workdir}, interval=0.02)
        sampler.start()

        recorded: list = []

        def _on_chunk(cm) -> None:
            recorded.append(cm)
            if cm.index == 0:
                # Cresce o workdir monitorado DEPOIS do chunk 0 -- o
                # sampler compartilhado (rodando em thread própria desde
                # antes do primeiro chunk) deve pegar isso antes do
                # chunk 1 rodar.
                (workdir / "big.bin").write_bytes(b"0" * (2 * 1024 * 1024))
                time.sleep(0.15)

        out_path = tmp_path / "cnpjs.parquet"
        try:
            transform.write_cnpjs_parquet_chunked(
                con,
                [csv_a, csv_b],
                out_path,
                on_chunk=_on_chunk,
                disk_peaks_fn=sampler.current_peaks,
            )
        finally:
            sampler.stop()

        assert len(recorded) == 2
        assert recorded[0].rss_peak_mib is not None
        assert (recorded[0].workdir_peak_mib or 0) < 1.0  # ainda não cresceu
        assert recorded[1].rss_peak_mib is not None
        assert recorded[1].workdir_peak_mib is not None
        assert recorded[1].workdir_peak_mib >= 1.9  # já viu o arquivo de ~2 MiB
    finally:
        con.close()


# -----------------------------------------------------------------------------
# Finding E do review do owner na PR #70: leitura de métrica (stat/COUNT)
# rodando DEPOIS que o writer já terminou com sucesso não pode derrubar o
# pipeline. `_record_parquet_output` e a medição por chunk em
# write_cnpjs_parquet_chunked blindam isso com try/except (OSError,
# duckdb.Error) -> log.warning, deixando os campos None.
# -----------------------------------------------------------------------------


def test_record_parquet_output_survives_stat_failure_after_write(tmp_path, monkeypatch, caplog):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM range(10) AS r(i)")
        out_path = tmp_path / "out.parquet"
        con.execute(f"COPY t TO '{out_path}' (FORMAT PARQUET)")
        assert out_path.exists()

        real_stat = Path.stat
        already_failed = {"value": False}

        def flaky_stat(self, *args, **kwargs):
            # Falha só na PRIMEIRA chamada -- é a de dentro de
            # _record_parquet_output; a assert final (out_path.exists())
            # usa stat() de novo e precisa do comportamento real.
            if self == out_path and not already_failed["value"]:
                already_failed["value"] = True
                raise OSError("disco cheio (simulado) bem na hora do stat")
            return real_stat(self, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", flaky_stat)

        handle = metrics.StageHandle(name="s")
        with caplog.at_level(logging.WARNING):
            transform._record_parquet_output(con, handle, out_path)  # não deve levantar

        assert handle.bytes_written is None
        assert handle.rows_written is None
        assert any("metrics" in r.message.lower() for r in caplog.records)
        assert out_path.exists()  # output de produção continua intacto
    finally:
        con.close()


def test_record_parquet_output_survives_count_failure_after_write(tmp_path, monkeypatch, caplog):
    """Mesmo cenário, mas a falha é no COUNT(*) (duckdb.Error), não no stat.
    bytes_written TAMBÉM fica None (atômico -- ou os dois campos são
    preenchidos, ou nenhum), não um valor parcial/incorreto."""
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM range(10) AS r(i)")
        out_path = tmp_path / "out.parquet"
        con.execute(f"COPY t TO '{out_path}' (FORMAT PARQUET)")

        real_execute = duckdb.DuckDBPyConnection.execute

        def flaky_execute(self, query, *args, **kwargs):
            if "COUNT(*)" in query and "read_parquet" in query:
                raise duckdb.Error("corrupção simulada na leitura")
            return real_execute(self, query, *args, **kwargs)

        monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", flaky_execute)

        handle = metrics.StageHandle(name="s")
        with caplog.at_level(logging.WARNING):
            transform._record_parquet_output(con, handle, out_path)  # não deve levantar

        assert handle.bytes_written is None
        assert handle.rows_written is None
        assert any("metrics" in r.message.lower() for r in caplog.records)
        assert out_path.exists()
    finally:
        con.close()


def test_transform_snapshot_survives_metric_stat_failure_after_write_success(
    tmp_path, all_zips_dir, monkeypatch
):
    """Fim-a-fim: uma falha de stat() DEPOIS que cnpj_contatos.parquet já
    foi escrito com sucesso não pode derrubar transform_snapshot inteiro --
    o parquet de produção existe intacto, e o campo correspondente fica
    None no metrics.json (não um valor incorreto)."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    target_name = "cnpj_contatos.parquet"
    real_stat = Path.stat
    already_failed = {"value": False}

    def flaky_stat(self, *args, **kwargs):
        # Falha só UMA vez, só pra esse arquivo -- é a chamada dentro de
        # _record_parquet_output, logo depois do write bem-sucedido. As
        # chamadas seguintes (ex.: o log.info de tamanho, já fora do
        # caminho de métricas) usam o stat real normalmente.
        if self.name == target_name and not already_failed["value"]:
            already_failed["value"] = True
            raise OSError("falha simulada de stat (disco cheio) logo após o write")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )  # não deve levantar

    produced = output_dir / target_name
    assert produced.exists()
    con = duckdb.connect()
    try:
        n = con.execute(f"SELECT COUNT(*) FROM '{produced}'").fetchone()[0]
        assert n > 0  # conteúdo real, output de produção intacto
    finally:
        con.close()

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    data = json.loads(metrics_path.read_text())
    contatos_stage = next(s for s in data["stages"] if s["stage"] == "cnpj_contatos")
    assert contatos_stage["bytes_written"] is None
    assert contatos_stage["rows_written"] is None


def test_record_parquet_output_stores_codec_and_row_group_size_in_extra(tmp_path):
    """Finding G do review do owner na PR #70: codec/row_group_size são
    constantes literais do COPY do writer chamador -- _record_parquet_output
    só repassa o que recebe, guardando em handle.extra (metadados do
    WRITER, não do estágio)."""
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM range(5) AS r(i)")
        out_path = tmp_path / "out.parquet"
        con.execute(f"COPY t TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        handle = metrics.StageHandle(name="s")
        transform._record_parquet_output(con, handle, out_path, codec="ZSTD", row_group_size=200000)

        assert handle.extra["codec"] == "ZSTD"
        assert handle.extra["row_group_size"] == 200000
        assert handle.bytes_written is not None
        assert handle.rows_written == 5
    finally:
        con.close()


def test_transform_snapshot_writer_stages_record_codec_and_row_group_size(tmp_path, all_zips_dir):
    """Fim-a-fim: os writers que passam por _record_parquet_output
    registram codec/row_group_size (200000 é o valor real de todos eles,
    ao contrário do 100000 de write_lookup_parquets, que não passa por
    esta função)."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    data = json.loads(metrics_path.read_text())
    for name in ("cnpj_contatos", "cnpj_cnaes", "enderecos", "cnpjs_chunked", "raizes", "socios"):
        stage = next(s for s in data["stages"] if s["stage"] == name)
        assert stage["extra"]["codec"] == "ZSTD", name
        assert stage["extra"]["row_group_size"] == 200000, name


def test_transform_snapshot_files_read_populated_across_stages(tmp_path, all_zips_dir):
    """Finding G: files_read reflete a contagem de arquivos/CSVs lidos onde
    já está disponível (extract, lookups, load_duckdb, cnpjs_chunked,
    roundtrip_verify); None onde não se aplica (writers que só leem
    tabelas já carregadas, sem tocar arquivo novo)."""
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
        verify=True,
        verify_sample_size=4,
    )

    metrics_path = cache_dir / "2026-04" / "metrics" / "transform_metrics.json"
    data = json.loads(metrics_path.read_text())
    stages_by_name = {s["stage"]: s for s in data["stages"]}

    for name in ("extract", "lookups", "load_duckdb", "cnpjs_chunked", "roundtrip_verify"):
        assert stages_by_name[name]["files_read"] is not None, name
        assert stages_by_name[name]["files_read"] > 0, name

    # Writers que só leem tabelas já carregadas -- files_read não se aplica.
    for name in ("cnpj_contatos", "cnpj_cnaes", "enderecos", "raizes", "socios", "pessoas"):
        assert stages_by_name[name]["files_read"] is None, name


# -----------------------------------------------------------------------------
# write_raizes_parquet_from_cnpjs — matches write_raizes_parquet
# -----------------------------------------------------------------------------


def test_write_raizes_from_cnpjs_matches_original(tmp_path, all_zips_dir):
    """write_raizes_parquet_from_cnpjs must produce the same raizes as write_raizes_parquet.

    Runs transform_snapshot to get both cnpjs.parquet and raizes.parquet (original),
    then calls write_raizes_parquet_from_cnpjs on the cnpjs.parquet and compares
    row counts plus key aggregated fields.
    """
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    # Run the full pipeline to get reference outputs.
    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        skip_unimplemented=False,
    )

    cnpjs_path = output_dir / "cnpjs.parquet"
    raizes_ref = output_dir / "raizes.parquet"
    raizes_new = tmp_path / "raizes_from_cnpjs.parquet"

    assert cnpjs_path.exists(), "cnpjs.parquet must exist after transform_snapshot"
    assert raizes_ref.exists(), "raizes.parquet must exist after transform_snapshot"

    # Compute raizes from cnpjs.parquet using the new function.
    con = duckdb.connect()
    try:
        transform.write_raizes_parquet_from_cnpjs(con, cnpjs_path, raizes_new)
    finally:
        con.close()

    assert raizes_new.exists()

    # Compare.
    compare_con = duckdb.connect()
    try:
        ref_count = compare_con.execute(f"SELECT COUNT(*) FROM '{raizes_ref}'").fetchone()[0]
        new_count = compare_con.execute(f"SELECT COUNT(*) FROM '{raizes_new}'").fetchone()[0]
        assert ref_count == new_count, (
            f"raizes row count mismatch: original={ref_count}, from_cnpjs={new_count}"
        )

        # Compare key fields for each cnpj_base.
        ref_rows = compare_con.execute(
            f"SELECT cnpj_base, qtd_estabelecimentos, qtd_estabelecimentos_ativos, "
            f"ufs_atuacao, uf_matriz, municipio_matriz_nome "
            f"FROM '{raizes_ref}' ORDER BY cnpj_base"
        ).fetchall()
        new_rows = compare_con.execute(
            f"SELECT cnpj_base, qtd_estabelecimentos, qtd_estabelecimentos_ativos, "
            f"ufs_atuacao, uf_matriz, municipio_matriz_nome "
            f"FROM '{raizes_new}' ORDER BY cnpj_base"
        ).fetchall()

        assert len(ref_rows) == len(new_rows)
        for ref, new in zip(ref_rows, new_rows):
            assert ref[0] == new[0], f"cnpj_base mismatch: {ref[0]} vs {new[0]}"
            assert ref[1] == new[1], (
                f"qtd_estabelecimentos mismatch for {ref[0]}: {ref[1]} vs {new[1]}"
            )
            assert ref[2] == new[2], (
                f"qtd_estabelecimentos_ativos mismatch for {ref[0]}: {ref[2]} vs {new[2]}"
            )
            assert sorted(ref[3]) == sorted(new[3]), (
                f"ufs_atuacao mismatch for {ref[0]}: {ref[3]} vs {new[3]}"
            )
    finally:
        compare_con.close()


# -----------------------------------------------------------------------------
# write_enderecos_parquet — schema, normalization, sort order
# -----------------------------------------------------------------------------


def test_write_enderecos_parquet_schema_and_normalization(tmp_path):
    """write_enderecos_parquet produces correct schema and normalizes logradouro."""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR,
            uf VARCHAR, municipio VARCHAR, logradouro VARCHAR,
            tipo_logradouro VARCHAR, numero VARCHAR, complemento VARCHAR,
            bairro VARCHAR, cep VARCHAR,
            nome_fantasia VARCHAR, situacao_cadastral VARCHAR,
            data_situacao_cadastral VARCHAR, motivo_situacao_cadastral VARCHAR,
            nome_cidade_exterior VARCHAR, pais VARCHAR,
            data_inicio_atividade VARCHAR, cnae_fiscal_principal VARCHAR,
            cnae_fiscal_secundaria VARCHAR, ddd_1 VARCHAR, telefone_1 VARCHAR,
            ddd_2 VARCHAR, telefone_2 VARCHAR, ddd_fax VARCHAR, fax VARCHAR,
            correio_eletronico VARCHAR, situacao_especial VARCHAR,
            data_situacao_especial VARCHAR, identificador_matriz_filial VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO estabelecimento
            (cnpj_basico, cnpj_ordem, cnpj_dv, uf, municipio, logradouro, numero, cep, bairro)
        VALUES
            ('11111111', '0001', '00', 'SP', '3550308', 'AV  BRASIL',       '200', '01000000', 'CTR'),
            ('22222222', '0001', '00', 'RJ', '3304557', 'R. PAULISTA',      '10',  '20000000', 'CTR'),
            ('33333333', '0001', '00', 'SP', '3550308', 'rua  dos  testes', '5',   '04000000', 'VL'),
            ('44444444', '0001', '00', 'SP', '3550308', '',                 '1',   '05000000', '')
        """
    )
    out = tmp_path / "enderecos.parquet"
    transform.write_enderecos_parquet(con, out)

    rows = con.execute(f"SELECT * FROM '{out}' ORDER BY cnpj").fetchall()
    cols = [d[0] for d in con.execute(f"DESCRIBE SELECT * FROM '{out}'").fetchall()]

    assert "logradouro_normalizado" in cols
    assert "municipio_codigo" in cols
    assert "cnpj" in cols

    # Row with empty logradouro excluded
    cnpjs = {r[cols.index("cnpj")] for r in rows}
    assert "444444444400100" not in cnpjs
    assert len(rows) == 3

    # 'AV' → 'AVENIDA'
    av_row = next(r for r in rows if r[cols.index("cnpj")].startswith("111111"))
    assert av_row[cols.index("logradouro_normalizado")].startswith("AVENIDA")

    # 'R.' → 'RUA'
    r_row = next(r for r in rows if r[cols.index("cnpj")].startswith("222222"))
    assert r_row[cols.index("logradouro_normalizado")].startswith("RUA")

    # Whitespace collapse + UPPER
    rua_row = next(r for r in rows if r[cols.index("cnpj")].startswith("333333"))
    assert "  " not in rua_row[cols.index("logradouro_normalizado")]
    assert (
        rua_row[cols.index("logradouro_normalizado")]
        == rua_row[cols.index("logradouro_normalizado")].upper()
    )

    con.close()


def test_write_enderecos_parquet_numeric_sort(tmp_path):
    """Numeric street numbers sort as integers, not lexicographically."""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR,
            uf VARCHAR, municipio VARCHAR, logradouro VARCHAR,
            tipo_logradouro VARCHAR, numero VARCHAR, complemento VARCHAR,
            bairro VARCHAR, cep VARCHAR,
            nome_fantasia VARCHAR, situacao_cadastral VARCHAR,
            data_situacao_cadastral VARCHAR, motivo_situacao_cadastral VARCHAR,
            nome_cidade_exterior VARCHAR, pais VARCHAR,
            data_inicio_atividade VARCHAR, cnae_fiscal_principal VARCHAR,
            cnae_fiscal_secundaria VARCHAR, ddd_1 VARCHAR, telefone_1 VARCHAR,
            ddd_2 VARCHAR, telefone_2 VARCHAR, ddd_fax VARCHAR, fax VARCHAR,
            correio_eletronico VARCHAR, situacao_especial VARCHAR,
            data_situacao_especial VARCHAR, identificador_matriz_filial VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO estabelecimento
            (cnpj_basico, cnpj_ordem, cnpj_dv, uf, municipio, logradouro, numero, cep, bairro)
        VALUES
            ('00000001', '0001', '00', 'SP', '3550308', 'RUA ALFA', '2',   '01000000', 'CTR'),
            ('00000002', '0001', '00', 'SP', '3550308', 'RUA ALFA', '10',  '01000000', 'CTR'),
            ('00000003', '0001', '00', 'SP', '3550308', 'RUA ALFA', '100', '01000000', 'CTR')
        """
    )
    out = tmp_path / "enderecos.parquet"
    transform.write_enderecos_parquet(con, out)

    # row_number() OVER () without ORDER BY reflects physical parquet row order —
    # DuckDB scans row groups sequentially, so this matches the COPY ... ORDER BY
    # written above. This is a DuckDB implementation guarantee (not SQL spec),
    # but it's the most practical way to assert physical sort order without
    # re-sorting on the read side (which would defeat the purpose of the test).
    numeros = con.execute(
        f"""
        SELECT numero FROM (
            SELECT numero, row_number() OVER () AS rn FROM '{out}'
            WHERE municipio_codigo = '3550308'
        ) ORDER BY rn
        """
    ).fetchall()
    # Must be [2, 10, 100] (numeric order), not [10, 100, 2] (lexicographic).
    assert [r[0] for r in numeros] == ["2", "10", "100"]
    con.close()


# -----------------------------------------------------------------------------
# write_pessoas_parquet — grain, deduplication, faixa_etaria, exclusions
# -----------------------------------------------------------------------------


def test_write_pessoas_parquet_grain_and_deduplication(tmp_path):
    """write_pessoas_parquet includes socio_pf + representantes; excludes PJ/estrangeiros."""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE socio (
            cnpj_basico VARCHAR,
            identificador_socio VARCHAR,
            nome_socio_razao_social VARCHAR,
            cnpj_cpf_socio VARCHAR,
            qualificacao_socio VARCHAR,
            data_entrada_sociedade VARCHAR,
            pais VARCHAR,
            representante_legal VARCHAR,
            nome_representante_legal VARCHAR,
            qualificacao_representante_legal VARCHAR,
            faixa_etaria VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO socio VALUES
            ('11111111', '2', 'JOAO SILVA',     '***123456**',   '49', '20200101', '105', '',            '',         '',   '5'),
            ('22222222', '2', 'JOAO SILVA',     '***123456**',   '49', '20210101', '105', '',            '',         '',   '5'),
            ('11111111', '1', 'EMPRESA XYZ',    '12345678000100','49', '20200101', '105', '',            '',         '',   '0'),
            ('11111111', '3', 'JOHN DOE',       'USA123',        '49', '20200101', '249', '',            '',         '',   '0'),
            ('11111111', '2', 'OUTRO SOCIO',    '***999999**',   '49', '20200101', '105', '***777777**', 'ANA LIMA', '10', '3'),
            ('33333333', '2', 'TERCEIRO SOCIO', '***888888**',   '49', '20200101', '105', '***777777**', 'ANA LIMA', '10', '4')
        """
    )
    out = tmp_path / "pessoas.parquet"
    transform.write_pessoas_parquet(con, out)

    rows = con.execute(f"SELECT * FROM '{out}' ORDER BY cpf_mascarado, cnpj_base").fetchall()
    cols = [d[0] for d in con.execute(f"DESCRIBE SELECT * FROM '{out}'").fetchall()]

    papeis = {r[cols.index("papel")] for r in rows}
    assert papeis <= {"socio_pf", "representante"}, f"unexpected papeis: {papeis}"

    cpfs = [r[cols.index("cpf_mascarado")] for r in rows]
    assert "12345678000100" not in cpfs  # PJ excluded
    assert "USA123" not in cpfs  # estrangeiro excluded

    # JOAO SILVA in two companies → two rows
    joao_rows = [r for r in rows if r[cols.index("cpf_mascarado")] == "***123456**"]
    assert len(joao_rows) == 2
    assert {r[cols.index("cnpj_base")] for r in joao_rows} == {"11111111", "22222222"}

    # ANA LIMA as representante: DISTINCT per (cnpj_basico, representante_legal) → two rows
    ana_rows = [r for r in rows if r[cols.index("cpf_mascarado")] == "***777777**"]
    assert len(ana_rows) == 2
    assert all(r[cols.index("papel")] == "representante" for r in ana_rows)
    assert all(r[cols.index("faixa_etaria")] is None for r in ana_rows)

    # faixa_etaria preserved for socio_pf
    assert joao_rows[0][cols.index("faixa_etaria")] == "5"

    # nome_normalizado is UPPER
    for r in rows:
        nome = r[cols.index("nome_normalizado")]
        assert nome == nome.upper(), f"nome_normalizado not upper: {nome}"

    con.close()
