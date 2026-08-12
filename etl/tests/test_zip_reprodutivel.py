"""Reprodutibilidade byte a byte do ZIP produzido por `pack_companies` (#151).

Antes desta correção, `zipfile.writestr(str, ...)` construía cada `ZipInfo` a
partir de `time.localtime()`, então dois packs dos mesmos dados produziam
artefatos com `sha256` diferente — em todos os ~68 milhões de membros.

A propriedade de regressão **forte** é uma só:

    mesmos inputs → SHA-256 do ZIP idêntico

Ela é o que protege o artefato, porque cobre qualquer campo do `ZipInfo` e
qualquer outra fonte de variação que apareça no futuro. Os testes de
`date_time`, `compress_type` e `external_attr` são **complementares**: eles
documentam a causa específica corrigida aqui e localizam a regressão quando o
teste forte falha. Nenhum deles substitui o teste de SHA-256 — verificar só
`date_time` deixaria passar uma regressão em qualquer outro campo.

O escopo do que estes testes provam é estreito de propósito: **mesmo ambiente,
mesma stack de dependências**. Eles não afirmam nada sobre reprodutibilidade
entre versões de Python, zlib ou protobuf, nem entre plataformas — todos
continuam sendo determinantes não medidos dos bytes de saída.
"""

from __future__ import annotations

import hashlib
import time
import zipfile

import pytest

from ficha_etl.pack import LOOKUP_KINDS, ZIP_EPOCA, pack_companies


def _lookups():
    return {k: [{"codigo": "1", "descricao": "DESC"}] for k in LOOKUP_KINDS}


def _rows():
    return iter(
        [
            {"cnpj_base": "00000001", "razao_social": "ALFA", "estabelecimentos": [], "socios": []},
            {"cnpj_base": "00000002", "razao_social": "BETA", "estabelecimentos": [], "socios": []},
        ]
    )


def _pack(path):
    pack_companies(_rows(), _lookups(), path, snapshot_month="2026-05")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _feito(tmp_path):
    p = tmp_path / "c.zip"
    _pack(p)
    return p


# --- TESTE FORTE: a propriedade de regressão que protege o artefato ---------


def test_dois_packs_dos_mesmos_dados_sao_byte_identicos(tmp_path):
    """Cobre qualquer campo do ZipInfo e qualquer fonte futura de variação."""
    a = _pack(tmp_path / "a.zip")
    time.sleep(1.1)  # garante que o relógio mudou entre as duas execuções
    b = _pack(tmp_path / "b.zip")
    assert a == b


# --- complementares: localizam a causa quando o teste forte falha -----------


def test_todo_membro_grava_a_epoca_canonica(tmp_path):
    with zipfile.ZipFile(_feito(tmp_path)) as zf:
        datas = {i.date_time for i in zf.infolist()}
    assert datas == {ZIP_EPOCA}


# --- o que passar um ZipInfo poderia quebrar em silêncio ---------------------


def test_os_membros_continuam_deflate(tmp_path):
    """Passar `ZipInfo` desliga os defaults de `writestr`: sem repor
    `compress_type` os membros sairiam STORED, mudando o formato sem erro."""
    with zipfile.ZipFile(_feito(tmp_path)) as zf:
        tipos = {i.compress_type for i in zf.infolist()}
    assert tipos == {zipfile.ZIP_DEFLATED}


def test_os_membros_continuam_comprimidos(tmp_path):
    """Guarda contra `_compresslevel` não ser aplicado: se o nível se perdesse,
    o membro grande sairia do mesmo tamanho que a entrada."""
    with zipfile.ZipFile(_feito(tmp_path)) as zf:
        grande = next(i for i in zf.infolist() if i.filename == "_schema.desc")
    assert grande.compress_size < grande.file_size


def test_permissoes_do_membro_preservadas(tmp_path):
    with zipfile.ZipFile(_feito(tmp_path)) as zf:
        attrs = {i.external_attr for i in zf.infolist()}
    assert attrs == {0o600 << 16}


# --- o artefato continua legível --------------------------------------------


def test_o_zip_continua_valido_e_legivel_pelo_zipfile_padrao(tmp_path):
    with zipfile.ZipFile(_feito(tmp_path)) as zf:
        assert zf.testzip() is None
        nomes = set(zf.namelist())
        assert {"_schema.desc", "_schema.proto", "_meta.json"} <= nomes
        assert "00/000/001.pb" in nomes
        assert zf.read("00/000/001.pb")
        for kind in LOOKUP_KINDS:
            assert zf.read(f"_lookups/{kind}.pb")


@pytest.mark.parametrize("membro", ["00/000/001.pb", "00/000/002.pb"])
def test_conteudo_dos_membros_nao_muda_entre_execucoes(tmp_path, membro):
    _pack(tmp_path / "a.zip")
    _pack(tmp_path / "b.zip")
    with zipfile.ZipFile(tmp_path / "a.zip") as x, zipfile.ZipFile(tmp_path / "b.zip") as y:
        assert x.read(membro) == y.read(membro)


# --- o caminho real, não só o helper ----------------------------------------


def _fixture_parquets(tmp_path):
    """Parquets locais mínimos com o schema que `_COMPANIES_SQL` consome.

    Fixture local de propósito: a pergunta é se o caminho de produção usa o
    writer determinístico, não se o Internet Archive funciona.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("CREATE TABLE b AS SELECT * FROM (VALUES ('00000001'), ('00000002')) t(cnpj_base)")
    con.execute(f"""COPY (SELECT cnpj_base, 'RAZAO ' || cnpj_base AS razao_social,
        'RAZAO ' || cnpj_base AS razao_social_normalizada, '2062' AS natureza_juridica_codigo,
        '01' AS porte_empresa, 1000.0 AS capital_social, NULL AS ente_federativo_responsavel,
        1 AS qtd_estabelecimentos, 1 AS qtd_estabelecimentos_ativos FROM b)
        TO '{tmp_path}/raizes.parquet' (FORMAT PARQUET)""")
    cols = ", ".join(
        f"NULL AS {c}"
        for c in (
            "bairro cep cnae_principal_codigo cnpj_dv cnpj_ordem complemento correio_eletronico "
            "data_exclusao_mei data_exclusao_simples data_inicio_atividade data_opcao_mei "
            "data_opcao_simples data_situacao_cadastral data_situacao_especial ddd_1 ddd_2 "
            "ddd_fax fax identificador_matriz_filial logradouro "
            "motivo_situacao_cadastral_codigo municipio_codigo nome_cidade_exterior "
            "nome_fantasia numero opcao_mei opcao_simples pais_codigo situacao_cadastral "
            "situacao_especial telefone_1 telefone_2 tipo_logradouro uf"
        ).split()
    )
    con.execute(f"""COPY (SELECT cnpj_base, []::VARCHAR[] AS cnae_secundario_codigos, {cols}
        FROM b) TO '{tmp_path}/cnpjs.parquet' (FORMAT PARQUET)""")
    scols = ", ".join(
        f"NULL AS {c}"
        for c in (
            "cnpj_socio cpf_mascarado data_entrada_sociedade faixa_etaria "
            "nome_socio_razao_social pais_codigo qualificacao_codigo representante_legal_cpf "
            "representante_legal_nome representante_legal_qualificacao_codigo tipo"
        ).split()
    )
    con.execute(
        f"COPY (SELECT cnpj_base, {scols} FROM b) TO '{tmp_path}/socios.parquet' (FORMAT PARQUET)"
    )
    lk = tmp_path / "lookups"
    lk.mkdir()
    for kind in LOOKUP_KINDS:
        con.execute(
            f"COPY (SELECT '1' AS codigo, 'D' AS descricao) "
            f"TO '{lk}/{kind}.parquet' (FORMAT PARQUET)"
        )
    con.close()


def test_o_caminho_de_producao_tambem_e_byte_reproduzivel(tmp_path):
    """Prova de wiring: nenhum call site escapou do writer determinístico.

    Os testes acima chamam `pack_companies` diretamente, o que prova os call
    sites *dentro* dela. Este entra por `pack_from_parquets` — o caminho que a
    produção usa — e vai até o arquivo ZIP final.
    """
    fonte = tmp_path / "parquets"
    fonte.mkdir()
    _fixture_parquets(fonte)

    from ficha_etl.pack import pack_from_parquets

    a = tmp_path / "a.zip"
    b = tmp_path / "b.zip"
    pack_from_parquets("2026-05", a, parquets_base=str(fonte))
    time.sleep(1.1)  # relógios diferentes entre as duas execuções
    pack_from_parquets("2026-05", b, parquets_base=str(fonte))

    assert hashlib.sha256(a.read_bytes()).hexdigest() == hashlib.sha256(b.read_bytes()).hexdigest()
    with zipfile.ZipFile(a) as zf:
        assert zf.testzip() is None
        assert {i.date_time for i in zf.infolist()} == {ZIP_EPOCA}
        assert zf.read("00/000/001.pb")
