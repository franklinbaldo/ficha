import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import fetcher, transform
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
        "11111111", "0001", "00", "1", "ACME", "02", "20200101", "00", "", "105",
        "20200101", "4711301", "6201500", "RUA", "DAS FLORES", "100", "", "CENTRO",
        "01000000", "SP", "3550308", "11", "999999999", "", "", "", "",
        "contato@acme.com", "", "",
    ),
    (
        "11111111", "0002", "00", "2", "ACME FILIAL", "02", "20210101", "00", "", "105",
        "20210101", "4711301", "", "AV", "BRASIL", "200", "", "CENTRO",
        "20000000", "RJ", "3304557", "21", "888888888", "", "", "", "",
        "filial@acme.com", "", "",
    ),
    (
        "22222222", "0001", "00", "1", "INDIV", "08", "20240101", "01", "", "105",
        "20180101", "4711301", "", "RUA", "OUTRA", "50", "", "VILA",
        "01010000", "SP", "3550308", "11", "777777777", "", "", "", "",
        "ind@x.com", "", "",
    ),
    (
        "33333333", "0001", "00", "1", "TECH", "02", "20230101", "00", "", "105",
        "20230101", "6201500", "", "RUA", "DEV", "10", "", "BAIRRO",
        "04000000", "SP", "3550308", "11", "555555555", "", "", "", "",
        "dev@tech.com", "", "",
    ),
]

SOCIO_ROWS: list[tuple[str, ...]] = [
    # cnpj_basico, ident_socio, nome, cnpj_cpf, qualif, data_entrada,
    # pais, rep_legal_cpf, nome_rep, qualif_rep, faixa_etaria
    (
        "11111111", "2", "JOAO DA SILVA", "***123456**", "49", "20200101",
        "105", "", "", "", "5",
    ),
    (
        "11111111", "1", "OUTRA EMPRESA SA", "44444444000100", "49", "20200101",
        "105", "", "", "", "0",
    ),
    # Sócio estrangeiro (tipo '3'): cpf_mascarado e cnpj_socio devem ser NULL
    (
        "11111111", "3", "JOHN DOE", "USA123456", "49", "20200101",
        "249", "", "", "", "0",
    ),
    (
        "33333333", "2", "MARIA SOUZA", "***987654**", "49", "20230101",
        "105", "", "", "", "4",
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
        body = (
            "\n".join(";".join(f'"{c}"' for c in row) for row in rows) + "\n"
        ).encode("latin-1")
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


def test_write_lookups_json_full_shape(tmp_path):
    con = duckdb.connect()
    try:
        for kind, rows in LOOKUP_FIXTURES.items():
            csv = tmp_path / f"{kind}.csv"
            _write_csv_iso(csv, [(c, d) for c, d in rows])
            transform.load_lookup_into_duckdb(con, kind, csv)

        out = tmp_path / "lookups.json"
        transform.write_lookups_json(
            con, out, schema_version="1.0.0", snapshot_date="2026-04"
        )
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


def test_transform_snapshot_writes_lookups_and_3_parquets(tmp_path, all_zips_dir):
    chain = fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(all_zips_dir)])
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"

    transform.transform_snapshot(
        "2026-04",
        cache_dir=cache_dir,
        output_dir=output_dir,
        chain=chain,
        schema_version="1.0.0",
        skip_unimplemented=False,  # exige todos os 3 parquets
    )

    # lookups.json
    lookups_path = output_dir / "lookups.json"
    assert lookups_path.exists()
    data = json.loads(lookups_path.read_text())
    assert data["snapshot_date"] == "2026-04"
    assert data["cnaes"]["0111301"] == "Cultivo de arroz"

    # Os 3 parquets existem
    cnpjs_path = output_dir / "cnpjs.parquet"
    raizes_path = output_dir / "raizes.parquet"
    socios_path = output_dir / "socios.parquet"
    assert cnpjs_path.exists()
    assert raizes_path.exists()
    assert socios_path.exists()

    # Lê de volta com DuckDB pra validar conteúdo.
    con = duckdb.connect()
    try:
        cnpjs = con.execute(
            f"SELECT cnpj, razao_social, razao_social_normalizada, capital_social, "
            f"natureza_juridica_descricao, situacao_cadastral_descricao, "
            f"municipio_nome, opcao_simples FROM '{cnpjs_path}' ORDER BY cnpj"
        ).fetchall()
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
            f"SELECT cnae_secundario_codigos FROM '{cnpjs_path}' WHERE cnpj = '11111111000100'"
        ).fetchone()[0]
        assert acme_cnae == ["6201500"], f"esperado ['6201500'], got {acme_cnae}"

        # CNAE secundário com espaços (trim): ACME filial não tem secundário → []
        filial_cnae = con.execute(
            f"SELECT cnae_secundario_codigos FROM '{cnpjs_path}' WHERE cnpj = '11111111000200'"
        ).fetchone()[0]
        assert filial_cnae == []

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
