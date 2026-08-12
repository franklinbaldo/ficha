"""Lifecycle do artefato produzido por `pack_companies` e da conexão DuckDB.

O sharding (#147/#148) torna a retomada uma operação normal, e retomada precisa
poder decidir se um arquivo local é trabalho aproveitável. Enquanto um pack
interrompido deixasse um ZIP truncado no nome final, "o arquivo existe" e "o
artefato está completo" eram indistinguíveis — e o cenário não é hipotético:
disco cheio é justamente o que o sharding existe para atenuar.
"""

from __future__ import annotations

import zipfile

import pytest

from ficha_etl.pack import LOOKUP_KINDS, pack_companies


def _lookups():
    return {k: [{"codigo": "1", "descricao": "DESC"}] for k in LOOKUP_KINDS}


def _rows(n=3):
    for i in range(1, n + 1):
        yield {
            "cnpj_base": f"{i:08d}",
            "razao_social": f"EMPRESA {i}",
            "estabelecimentos": [],
            "socios": [],
        }


def _rows_que_explodem(n=2):
    yield from _rows(n)
    raise RuntimeError("falha simulada no meio do pack")


# --- o nome final só aparece quando o artefato está completo -----------------


def test_pack_bem_sucedido_deixa_so_o_arquivo_final(tmp_path):
    saida = tmp_path / "companies.zip"
    pack_companies(_rows(), _lookups(), saida, snapshot_month="2026-05")

    assert saida.exists()
    assert not (tmp_path / "companies.zip.part").exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["companies.zip"]


def test_falha_no_meio_nao_cria_o_arquivo_final(tmp_path):
    """A garantia central: existir no nome final significa estar completo."""
    saida = tmp_path / "companies.zip"

    with pytest.raises(RuntimeError, match="falha simulada"):
        pack_companies(_rows_que_explodem(), _lookups(), saida, snapshot_month="2026-05")

    assert not saida.exists(), "um pack interrompido não pode publicar o nome final"


def test_falha_deixa_o_parcial_identificavel(tmp_path):
    """O `.part` sobrevive de propósito — é diagnosticável e nenhuma retomada
    o confunde com trabalho durável, porque o nome não é o do artefato."""
    saida = tmp_path / "companies.zip"
    with pytest.raises(RuntimeError):
        pack_companies(_rows_que_explodem(), _lookups(), saida, snapshot_month="2026-05")

    assert (tmp_path / "companies.zip.part").exists()


def test_execucao_seguinte_descarta_o_parcial_anterior(tmp_path):
    """Retomar depois de uma falha não pode herdar bytes da tentativa morta."""
    saida = tmp_path / "companies.zip"
    with pytest.raises(RuntimeError):
        pack_companies(_rows_que_explodem(), _lookups(), saida, snapshot_month="2026-05")

    resultado = pack_companies(_rows(3), _lookups(), saida, snapshot_month="2026-05")

    assert resultado["count"] == 3
    assert not (tmp_path / "companies.zip.part").exists()
    with zipfile.ZipFile(saida) as zf:
        assert zf.testzip() is None
        pbs = [n for n in zf.namelist() if n.endswith(".pb") and not n.startswith("_")]
        assert len(pbs) == 3


def test_o_artefato_final_continua_valido_e_com_tamanho_correto(tmp_path):
    saida = tmp_path / "companies.zip"
    resultado = pack_companies(_rows(), _lookups(), saida, snapshot_month="2026-05")

    assert resultado["size_bytes"] == saida.stat().st_size
    with zipfile.ZipFile(saida) as zf:
        assert zf.testzip() is None
        assert zf.read("00/000/001.pb")


def test_sobrescrever_um_artefato_existente_funciona(tmp_path):
    saida = tmp_path / "companies.zip"
    pack_companies(_rows(3), _lookups(), saida, snapshot_month="2026-05")
    pack_companies(_rows(5), _lookups(), saida, snapshot_month="2026-05")

    with zipfile.ZipFile(saida) as zf:
        pbs = [n for n in zf.namelist() if n.endswith(".pb") and not n.startswith("_")]
    assert len(pbs) == 5


# --- a conexão DuckDB fecha nos dois caminhos --------------------------------


def _fixture_parquets(tmp_path):
    import duckdb

    con = duckdb.connect()
    con.execute("CREATE TABLE b AS SELECT '00000001' AS cnpj_base")
    con.execute(f"""COPY (SELECT cnpj_base, 'X' AS razao_social, 'X' AS razao_social_normalizada,
        '2062' AS natureza_juridica_codigo, '01' AS porte_empresa, 1.0 AS capital_social,
        NULL AS ente_federativo_responsavel, 1 AS qtd_estabelecimentos,
        1 AS qtd_estabelecimentos_ativos FROM b)
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
            f"COPY (SELECT '1' AS codigo, 'D' AS descricao) TO '{lk}/{kind}.parquet' (FORMAT PARQUET)"
        )
    con.close()


def test_conexao_duckdb_fecha_no_caminho_feliz(tmp_path, monkeypatch):
    import duckdb

    from ficha_etl import pack as pack_mod

    _fixture_parquets(tmp_path)
    abertas = []
    real = duckdb.connect

    def spy(*a, **kw):
        con = real(*a, **kw)
        abertas.append(con)
        return con

    monkeypatch.setattr(pack_mod.duckdb, "connect", spy)
    pack_mod.pack_from_parquets("2026-05", tmp_path / "out.zip", parquets_base=str(tmp_path))

    assert abertas, "o teste não observou nenhuma conexão"
    for con in abertas:
        with pytest.raises(Exception):
            con.execute("SELECT 1")


def test_conexao_duckdb_fecha_quando_o_pack_falha(tmp_path, monkeypatch):
    """Sem `finally`, uma falha no meio vazava a conexão — invisível no pack
    monolítico, que morria junto com o processo, e relevante no shardado."""
    import duckdb

    from ficha_etl import pack as pack_mod

    _fixture_parquets(tmp_path)
    abertas = []
    real = duckdb.connect

    def spy(*a, **kw):
        con = real(*a, **kw)
        abertas.append(con)
        return con

    monkeypatch.setattr(pack_mod.duckdb, "connect", spy)
    monkeypatch.setattr(
        pack_mod, "pack_companies", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(RuntimeError, match="boom"):
        pack_mod.pack_from_parquets("2026-05", tmp_path / "out.zip", parquets_base=str(tmp_path))

    assert abertas
    for con in abertas:
        with pytest.raises(Exception):
            con.execute("SELECT 1")
