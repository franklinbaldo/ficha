"""Regression cases from ficha-2026-08 and RFB's first alphanumeric CNPJ.

RFB source: https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2026/julho/receita-federal-gera-o-primeiro-cnpj-em-formato-alfanumerico
Conflict: raw/Empresas2.zip, K3241.K03200Y2.D60808.EMPRECSV,
lines 3572930-3572931; CSV SHA256 bbec30a33752606fe26db193e642d17a61615a8713cda035f6c26feec03695c9.
"""

from __future__ import annotations

import json
import zipfile

import duckdb

from ficha_etl.cnpj import shard_of
from ficha_etl.materialization import PACKER_FORMAT_VERSION
from ficha_etl.monthly_estabelecimento_gate import run_monthly_gate
from ficha_etl.pack import company_root, row_to_company
from ficha_etl.proto.ficha.v1.company_pb2 import Company
from ficha_etl.sharded_pack import ShardGeometry, ShardPackSession
from ficha_etl.transform import _dedupe_cnpj_basico_table
from tests.test_sharded_pack import _write_fixture


def test_real_rfb_identifier_survives_gate_and_published_shard(tmp_path):
    _write_fixture(tmp_path, ["00000000"])
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE updated AS SELECT * REPLACE ('E08G' AS cnpj_ordem, '12' AS cnpj_dv), "
        "'00000000E08G12' AS cnpj FROM read_parquet(?)",
        [str(tmp_path / "cnpjs.parquet")],
    )
    con.execute("COPY updated TO ? (FORMAT PARQUET)", [str(tmp_path / "new.parquet")])
    con.close()
    (tmp_path / "new.parquet").replace(tmp_path / "cnpjs.parquet")
    evidence = run_monthly_gate(
        "2026-08", tmp_path / "cnpjs.parquet", tmp_path / "gate.json", tmp_path / "audit"
    )
    assert evidence["status"] == "ok"
    with ShardPackSession("2026-08", ShardGeometry(2), parquets_base=str(tmp_path)) as session:
        spec = session.materialization_spec("00", input_sha1s={})
        assert spec.packer_format_version == PACKER_FORMAT_VERSION == 3
        result = session.pack("00", tmp_path / "out", materialization=spec)
    with zipfile.ZipFile(result.path) as z:
        c = Company.FromString(z.read("00/000/000.pb"))
        assert company_root(c) == "00000000"
        est = c.estabelecimentos[0]
        assert est.cnpj_ordem_text == "E08G"
        assert company_root(c) + est.cnpj_ordem_text + f"{est.cnpj_dv:02d}" == "00000000E08G12"
        assert json.loads(z.read("_meta.json"))["schema_version"] == "1.1.0"


def test_alpha_roots_are_assigned_exactly_once_without_changing_numeric_buckets(tmp_path):
    bases = ["00000000", "12345678", "12AB0001", "ABCD1234", "ZZZZZZZZ"]
    _write_fixture(tmp_path, bases)
    found = []
    geometry = ShardGeometry(2)
    assert shard_of("12345678") == "12"
    assert shard_of("12AB0001") == "12"
    assert shard_of("ABCD1234") == "71"
    with ShardPackSession("2026-08", geometry, parquets_base=str(tmp_path)) as session:
        for bucket in sorted({shard_of(b) for b in bases}):
            spec = session.materialization_spec(bucket, input_sha1s={})
            result = session.pack(bucket, tmp_path / "out", materialization=spec)
            with zipfile.ZipFile(result.path) as z:
                found.extend(
                    company_root(Company.FromString(z.read(n)))
                    for n in z.namelist()
                    if n.endswith(".pb") and not n.startswith("_")
                )
    assert sorted(found) == sorted(bases)


def test_text_partner_and_legacy_numeric_wire_both_roundtrip():
    c = row_to_company(
        {"cnpj_base": "ABCD1234", "socios": [{"tipo": "1", "cnpj_socio": "12ABC34501DE35"}]}
    )
    decoded = Company.FromString(c.SerializeToString())
    assert decoded.cnpj_base_text == "ABCD1234"
    assert decoded.socios[0].cnpj_socio_text == "12ABC34501DE35"
    # v1 wire for root 00000000, estabelecimento 0001, DV 91. No new fields.
    old = Company.FromString(bytes.fromhex("a201040801105b"))
    assert company_root(old) == "00000000"
    assert old.estabelecimentos[0].cnpj_ordem == 1
    assert old.estabelecimentos[0].cnpj_ordem_text == ""


def test_both_original_empresa_variants_are_logged_before_survivor_selection(caplog):
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE empresa (cnpj_basico VARCHAR, razao_social VARCHAR, "
        "natureza_juridica VARCHAR, qualificacao_responsavel VARCHAR, capital_social VARCHAR, "
        "porte_empresa VARCHAR, ente_federativo_responsavel VARCHAR)"
    )
    con.executemany(
        "INSERT INTO empresa VALUES (?,?,?,?,?,?,?)",
        [
            ("08314885", "FLAVIO PAVAO DE SOUZA", "4120", "59", "0,00", "05", None),
            ("08314885", None, "0000", "00", "0,00", None, None),
        ],
    )
    assert _dedupe_cnpj_basico_table(con, "empresa") == 1
    record = next(
        r.message.split("conflict evidence: ", 1)[1]
        for r in caplog.records
        if "conflict evidence: " in r.message
    )
    evidence = json.loads(record)
    assert evidence["conflicting_keys"] == 1
    assert {row["razao_social"] for row in evidence["rows"]} == {None, "FLAVIO PAVAO DE SOUZA"}
    assert con.execute("SELECT COUNT(*) FROM empresa").fetchone()[0] == 1
    con.close()
