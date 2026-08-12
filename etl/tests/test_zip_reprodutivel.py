"""Reprodutibilidade byte a byte do ZIP produzido por `pack_companies` (#151).

Antes desta correção, `zipfile.writestr(str, ...)` construía cada `ZipInfo` a
partir de `time.localtime()`, então dois packs dos mesmos dados produziam
artefatos com `sha256` diferente — em todos os ~68 milhões de membros.

O escopo do que estes testes provam é estreito de propósito: **mesmo ambiente,
mesma stack de dependências**. Eles não afirmam nada sobre reprodutibilidade
entre versões de zlib ou de protobuf, que continuam sendo determinantes não
medidos dos bytes de saída.
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


def test_dois_packs_dos_mesmos_dados_sao_byte_identicos(tmp_path):
    a = _pack(tmp_path / "a.zip")
    time.sleep(1.1)  # garante que o relógio mudou entre as duas execuções
    b = _pack(tmp_path / "b.zip")
    assert a == b


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
