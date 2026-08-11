"""Testes do pack shardado (#147).

O sharding existe porque o upload no Internet Archive degrada de forma
super-linear com o tamanho do objeto (medido em #133: 256 MiB a 13,2 MiB/s,
21,8 GiB a 1,64 MiB/s). Estes testes fixam as propriedades que tornam o shard
uma unidade de trabalho durável e independente.
"""

from __future__ import annotations

import json
import zipfile

import duckdb
import pytest

from ficha_etl.pack import (
    SHARD_PREFIX_LEN,
    pack_shards_from_parquets,
    shard_name,
    shard_of,
)


def _write_fixture(tmp_path, cnpj_bases: list[str]):
    """Três Parquets mínimos com o schema que `_COMPANIES_SQL` consome."""
    con = duckdb.connect()
    values = ", ".join(f"('{b}')" for b in cnpj_bases)
    con.execute(f"CREATE TABLE bases AS SELECT * FROM (VALUES {values}) t(cnpj_base)")

    con.execute(f"""
        COPY (
          SELECT cnpj_base, 'RAZAO ' || cnpj_base AS razao_social,
                 'RAZAO ' || cnpj_base AS razao_social_normalizada,
                 '2062' AS natureza_juridica_codigo, '01' AS porte_empresa,
                 1000.0 AS capital_social, NULL AS ente_federativo_responsavel,
                 1 AS qtd_estabelecimentos, 1 AS qtd_estabelecimentos_ativos
          FROM bases
        ) TO '{tmp_path}/raizes.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
          SELECT cnpj_base, '0001' AS cnpj_ordem_x,
                 []::VARCHAR[] AS cnae_secundario_codigos,
                 NULL AS bairro,
                 NULL AS cep,
                 NULL AS cnae_principal_codigo,
                 NULL AS cnpj_dv,
                 NULL AS cnpj_ordem,
                 NULL AS complemento,
                 NULL AS correio_eletronico,
                 NULL AS data_exclusao_mei,
                 NULL AS data_exclusao_simples,
                 NULL AS data_inicio_atividade,
                 NULL AS data_opcao_mei,
                 NULL AS data_opcao_simples,
                 NULL AS data_situacao_cadastral,
                 NULL AS data_situacao_especial,
                 NULL AS ddd_1,
                 NULL AS ddd_2,
                 NULL AS ddd_fax,
                 NULL AS fax,
                 NULL AS identificador_matriz_filial,
                 NULL AS logradouro,
                 NULL AS motivo_situacao_cadastral_codigo,
                 NULL AS municipio_codigo,
                 NULL AS nome_cidade_exterior,
                 NULL AS nome_fantasia,
                 NULL AS numero,
                 NULL AS opcao_mei,
                 NULL AS opcao_simples,
                 NULL AS pais_codigo,
                 NULL AS situacao_cadastral,
                 NULL AS situacao_especial,
                 NULL AS telefone_1,
                 NULL AS telefone_2,
                 NULL AS tipo_logradouro,
                 NULL AS uf
          FROM bases
        ) TO '{tmp_path}/cnpjs.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
          SELECT cnpj_base,
                 NULL AS cnpj_socio,
                 NULL AS cpf_mascarado,
                 NULL AS data_entrada_sociedade,
                 NULL AS faixa_etaria,
                 NULL AS nome_socio_razao_social,
                 NULL AS pais_codigo,
                 NULL AS qualificacao_codigo,
                 NULL AS representante_legal_cpf,
                 NULL AS representante_legal_nome,
                 NULL AS representante_legal_qualificacao_codigo,
                 NULL AS tipo
          FROM bases
        ) TO '{tmp_path}/socios.parquet' (FORMAT PARQUET)
    """)

    lookups = tmp_path / "lookups"
    lookups.mkdir()
    for kind in ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"):
        con.execute(f"""
            COPY (SELECT '1' AS codigo, 'DESC' AS descricao)
            TO '{lookups}/{kind}.parquet' (FORMAT PARQUET)
        """)
    con.close()


@pytest.fixture
def fixture_dir(tmp_path):
    # Três shards distintos, com contagens diferentes para detectar troca.
    _write_fixture(tmp_path, ["00000001", "00000002", "07000001", "42000001", "42000002"])
    return tmp_path


# --- nomenclatura e roteamento ----------------------------------------------


def test_shard_of_uses_the_first_digits_of_the_root():
    assert shard_of("07123456") == "07"
    assert shard_of(7123456) == "07", "int precisa ser zero-padded para 8"
    assert len(shard_of("99999999")) == SHARD_PREFIX_LEN


def test_shard_name_is_predictable_from_the_root():
    """A URL de acesso atômico continua O(1): o prefixo do shard é o primeiro
    nível do caminho interno, então `companies-07.zip/07/123/456.pb`."""
    assert shard_name(shard_of("07123456")) == "companies-07.zip"


# --- o shard é uma unidade independente -------------------------------------


def test_each_shard_contains_only_its_own_roots(fixture_dir, tmp_path):
    out = tmp_path / "out"
    results = pack_shards_from_parquets("2026-05", out, parquets_base=str(fixture_dir))

    por_shard = {r["shard"]: r for r in results if r["count"]}
    assert set(por_shard) == {"00", "07", "42"}
    assert por_shard["00"]["count"] == 2
    assert por_shard["07"]["count"] == 1
    assert por_shard["42"]["count"] == 2

    with zipfile.ZipFile(out / "companies-42.zip") as zf:
        pbs = [n for n in zf.namelist() if n.endswith(".pb") and not n.startswith("_")]
    assert pbs == ["42/000/001.pb", "42/000/002.pb"]


def test_each_shard_is_self_describing(fixture_dir, tmp_path):
    """Um shard tem que ser consultável sozinho — sem depender dos outros 99."""
    out = tmp_path / "out"
    pack_shards_from_parquets("2026-05", out, parquets_base=str(fixture_dir))

    with zipfile.ZipFile(out / "companies-07.zip") as zf:
        nomes = set(zf.namelist())
        assert "_schema.desc" in nomes
        assert "_schema.proto" in nomes
        assert "_meta.json" in nomes
        for kind in ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"):
            assert f"_lookups/{kind}.pb" in nomes
        meta = json.loads(zf.read("_meta.json"))
        assert meta["snapshot_month"] == "2026-05"
        assert meta["count"] == 1


def test_shards_are_readable_by_stdlib_zipfile(fixture_dir, tmp_path):
    out = tmp_path / "out"
    pack_shards_from_parquets("2026-05", out, parquets_base=str(fixture_dir))
    with zipfile.ZipFile(out / "companies-00.zip") as zf:
        assert zf.testzip() is None
        assert zf.read("00/000/001.pb")


# --- durabilidade incremental -----------------------------------------------


def test_on_shard_fires_before_the_next_shard_is_built(fixture_dir, tmp_path):
    """É o gancho que torna o trabalho durável: subir e apagar shard a shard.

    Se disparasse só no fim, uma interrupção continuaria custando a execução
    inteira — que é exatamente o problema que o sharding existe para resolver.
    """
    out = tmp_path / "out"
    observados: list[tuple[str, bool, list[str]]] = []

    def on_shard(shard, path, result):
        # No momento do callback, só os shards já processados existem em disco.
        existentes = sorted(p.name for p in out.glob("companies-*.zip"))
        observados.append((shard, path.exists(), existentes))
        path.unlink()  # simula "subiu e liberou o disco"

    pack_shards_from_parquets("2026-05", out, parquets_base=str(fixture_dir), on_shard=on_shard)

    com_dados = [o for o in observados if o[0] in {"00", "07", "42"}]
    assert [o[0] for o in com_dados] == ["00", "07", "42"]
    assert all(existia for _, existia, _ in com_dados)
    # Cada callback vê apenas o próprio shard em disco, porque o anterior foi
    # liberado — prova que o disco não acumula os 100 shards.
    assert all(len(existentes) <= 1 for _, _, existentes in com_dados)


def test_skip_shards_resumes_without_regenerating(fixture_dir, tmp_path):
    out = tmp_path / "out"
    results = pack_shards_from_parquets(
        "2026-05", out, parquets_base=str(fixture_dir), skip_shards={"00", "07"}
    )
    processados = {r["shard"] for r in results}
    assert "00" not in processados
    assert "07" not in processados
    assert "42" in processados
    assert not (out / "companies-00.zip").exists()
    assert (out / "companies-42.zip").exists()


def test_empty_shards_are_produced_but_carry_no_companies(fixture_dir, tmp_path):
    """Shard sem raízes ainda é um ZIP válido e auto-descritivo.

    Mantém o contrato uniforme: o consumidor resolve a URL pelo prefixo sem
    precisar saber quais shards têm dados.
    """
    out = tmp_path / "out"
    results = pack_shards_from_parquets("2026-05", out, parquets_base=str(fixture_dir))
    vazios = [r for r in results if r["count"] == 0]
    assert len(vazios) == 97
    with zipfile.ZipFile(out / "companies-01.zip") as zf:
        assert [n for n in zf.namelist() if n.endswith(".pb") and not n.startswith("_")] == []
        assert json.loads(zf.read("_meta.json"))["count"] == 0
