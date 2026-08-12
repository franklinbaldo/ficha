from __future__ import annotations

import json
import zipfile

import duckdb
import pytest

from ficha_etl.sharded_pack import ShardGeometry, ShardPackSession


LOOKUP_KINDS = ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")


def _write_fixture(root, bases: list[str]) -> None:
    con = duckdb.connect()
    values = ", ".join(f"('{base}')" for base in bases)
    con.execute(f"CREATE TABLE bases AS SELECT * FROM (VALUES {values}) t(cnpj_base)")

    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                'RAZAO ' || cnpj_base AS razao_social,
                'RAZAO ' || cnpj_base AS razao_social_normalizada,
                '2062' AS natureza_juridica_codigo,
                '01' AS porte_empresa,
                1000.0 AS capital_social,
                NULL::VARCHAR AS ente_federativo_responsavel,
                1 AS qtd_estabelecimentos,
                1 AS qtd_estabelecimentos_ativos
            FROM bases
        ) TO '{root}/raizes.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                NULL::VARCHAR AS cnpj_ordem,
                NULL::VARCHAR AS cnpj_dv,
                NULL::VARCHAR AS identificador_matriz_filial,
                NULL::VARCHAR AS nome_fantasia,
                NULL::VARCHAR AS situacao_cadastral,
                NULL::VARCHAR AS data_situacao_cadastral,
                NULL::VARCHAR AS motivo_situacao_cadastral_codigo,
                NULL::VARCHAR AS situacao_especial,
                NULL::VARCHAR AS data_situacao_especial,
                NULL::VARCHAR AS data_inicio_atividade,
                NULL::VARCHAR AS cnae_principal_codigo,
                []::VARCHAR[] AS cnae_secundario_codigos,
                NULL::VARCHAR AS tipo_logradouro,
                NULL::VARCHAR AS logradouro,
                NULL::VARCHAR AS numero,
                NULL::VARCHAR AS complemento,
                NULL::VARCHAR AS bairro,
                NULL::VARCHAR AS cep,
                NULL::VARCHAR AS uf,
                NULL::VARCHAR AS municipio_codigo,
                NULL::VARCHAR AS nome_cidade_exterior,
                NULL::VARCHAR AS pais_codigo,
                NULL::VARCHAR AS ddd_1,
                NULL::VARCHAR AS telefone_1,
                NULL::VARCHAR AS ddd_2,
                NULL::VARCHAR AS telefone_2,
                NULL::VARCHAR AS ddd_fax,
                NULL::VARCHAR AS fax,
                NULL::VARCHAR AS correio_eletronico,
                NULL::VARCHAR AS opcao_simples,
                NULL::VARCHAR AS data_opcao_simples,
                NULL::VARCHAR AS data_exclusao_simples,
                NULL::VARCHAR AS opcao_mei,
                NULL::VARCHAR AS data_opcao_mei,
                NULL::VARCHAR AS data_exclusao_mei
            FROM bases
        ) TO '{root}/cnpjs.parquet' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT
                cnpj_base,
                NULL::VARCHAR AS tipo,
                NULL::VARCHAR AS nome_socio_razao_social,
                NULL::VARCHAR AS cpf_mascarado,
                NULL::VARCHAR AS cnpj_socio,
                NULL::VARCHAR AS qualificacao_codigo,
                NULL::VARCHAR AS data_entrada_sociedade,
                NULL::VARCHAR AS pais_codigo,
                NULL::VARCHAR AS faixa_etaria,
                NULL::VARCHAR AS representante_legal_cpf,
                NULL::VARCHAR AS representante_legal_nome,
                NULL::VARCHAR AS representante_legal_qualificacao_codigo
            FROM bases
        ) TO '{root}/socios.parquet' (FORMAT PARQUET)
    """)

    lookups = root / "lookups"
    lookups.mkdir()
    for kind in LOOKUP_KINDS:
        con.execute(f"""
            COPY (SELECT '1' AS codigo, 'DESC' AS descricao)
            TO '{lookups}/{kind}.parquet' (FORMAT PARQUET)
        """)
    con.close()


@pytest.mark.parametrize(
    ("digits", "prefix", "cnpj", "bounds"),
    [
        (2, "07", "07123456", ("07000000", "08000000")),
        (3, "071", "07123456", ("07100000", "07200000")),
    ],
)
def test_geometry_is_explicit_and_validated(digits, prefix, cnpj, bounds):
    geometry = ShardGeometry(digits)
    assert geometry.shard_of(cnpj) == prefix
    assert geometry.shard_of(int(cnpj)) == prefix
    assert geometry.range_bounds(prefix) == bounds
    assert geometry.shard_name(prefix) == f"companies-{prefix}.zip"


@pytest.mark.parametrize("digits", [1, 4, 8])
def test_unmeasured_geometries_are_rejected(digits):
    with pytest.raises(ValueError, match="2 or 3"):
        ShardGeometry(digits)


def test_invalid_roots_and_prefixes_fail_high():
    geometry = ShardGeometry(2)
    for root in ("7123456", "abcdefgh", "123456789", -1, 100_000_000):
        with pytest.raises((TypeError, ValueError)):
            geometry.shard_of(root)
    for prefix in ("7", "007", "ab", " 7"):
        with pytest.raises((TypeError, ValueError)):
            geometry.validate_prefix(prefix)


@pytest.mark.parametrize(("digits", "prefix"), [(2, "07"), (3, "071")])
def test_one_shard_is_self_describing_and_materialized(tmp_path, digits, prefix):
    _write_fixture(tmp_path, ["07123456", "42000001"])
    geometry = ShardGeometry(digits)
    out = tmp_path / "out"

    session = ShardPackSession("2026-05", geometry, parquets_base=str(tmp_path))
    with session:
        spec = session.materialization_spec(
            prefix,
            input_sha1s={
                "cnpjs.parquet": "1" * 40,
                "raizes.parquet": "2" * 40,
                "socios.parquet": "3" * 40,
            },
        )
        artifact = session.pack(prefix, out, materialization=spec)

    assert artifact.count == 1
    assert artifact.path.name == f"companies-{prefix}.zip"
    assert artifact.size_bytes == artifact.path.stat().st_size
    assert artifact.materialization_id == spec.materialization_id()
    assert not artifact.path.with_name(artifact.path.name + ".part").exists()

    with zipfile.ZipFile(artifact.path) as zf:
        names = set(zf.namelist())
        assert "07/123/456.pb" in names
        assert "42/000/001.pb" not in names
        assert "_schema.desc" in names
        assert "_schema.proto" in names
        assert all(f"_lookups/{kind}.pb" in names for kind in LOOKUP_KINDS)
        meta = json.loads(zf.read("_meta.json"))

    assert meta["snapshot_month"] == "2026-05"
    assert meta["count"] == 1
    assert meta["materialization"]["id"] == spec.materialization_id()
    assert meta["materialization"]["spec"]["range"] == {
        "kind": "cnpj_base_prefix",
        "value": prefix,
    }
    assert meta["artifact"] is None

    with pytest.raises(RuntimeError, match="inside a with block"):
        session.pack(prefix, out, materialization=spec)


def test_materialization_must_match_the_exact_shard(tmp_path):
    _write_fixture(tmp_path, ["07123456"])
    geometry = ShardGeometry(2)
    with ShardPackSession("2026-05", geometry, parquets_base=str(tmp_path)) as session:
        wrong = session.materialization_spec("08", input_sha1s={"cnpjs.parquet": "a" * 40})
        with pytest.raises(ValueError, match="does not match shard"):
            session.pack("07", tmp_path / "out", materialization=wrong)
