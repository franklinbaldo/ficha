"""Contrato RFB de porte versus bytes produzidos pelo packer Protobuf.

A tabela normativa é da Receita Federal (00/01/03/05); os xfails *strict*
documentam divergências ainda não corrigidas. Não reinterpreta arquivos
publicados, não troca enum e não autoriza reempacotamento do snapshot.
Referência: https://github.com/franklinbaldo/ficha/issues/359
"""

import pytest

from ficha_etl.pack import row_to_company
from ficha_etl.proto.ficha.v1.company_pb2 import Company, Porte


@pytest.mark.parametrize(
    ("rfb_code", "expected_wire"),
    [
        pytest.param(
            "00",
            Porte.NAO_INFORMADO,
            marks=pytest.mark.xfail(
                strict=True, reason="#359: 00 RFB deveria significar não informado (1)"
            ),
        ),
        pytest.param(
            "01",
            Porte.MICRO_EMPRESA,
            marks=pytest.mark.xfail(
                strict=True, reason="#359: 01 RFB é microempresa (2), não NAO_INFORMADO"
            ),
        ),
        pytest.param(
            "02",
            Porte.PORTE_UNSPECIFIED,
            marks=pytest.mark.xfail(
                strict=True,
                reason="#359: 02 não pertence ao domínio público 00/01/03/05",
            ),
        ),
        ("03", Porte.PEQUENO_PORTE),
        ("05", Porte.DEMAIS),
        ("99", Porte.PORTE_UNSPECIFIED),
        (None, Porte.PORTE_UNSPECIFIED),
    ],
)
def test_porte_rfb_normativo_apos_serializacao(rfb_code, expected_wire):
    """Exigir o significado RFB após ida e volta pelo wire, não só _porte()."""
    source = {"cnpj_base": "12345678", "porte_empresa": rfb_code}
    encoded = row_to_company(source).SerializeToString()
    decoded = Company.FromString(encoded)
    assert decoded.porte_empresa == expected_wire


def test_wire_emitido_hoje_e_inadequado_para_rotulo_ingenuo():
    """Contraprova da tradução atual, não contrato desejado do futuro.

    O teste fixa o defeito *observável no produtor atual*, não afirma que este
    byte já foi recuperado do Internet Archive. Se a política de versão corrigir
    o packer, alterar este baseline junto com o teste normativo acima.
    """
    wire = row_to_company({"cnpj_base": "12345678", "porte_empresa": "01"}).SerializeToString()
    decoded = Company.FromString(wire)
    assert decoded.porte_empresa == 1
    assert Porte.Name(decoded.porte_empresa) == "NAO_INFORMADO"
    assert decoded.porte_empresa != Porte.MICRO_EMPRESA


def test_wire_publicado_202607_evidencia_numero_nao_origem_rfb():
    """Fixture de bytes reais publicados em schema 1.0.0, sem reinterpretar RFB.

    Fonte: https://archive.org/download/ficha-2026-07/companies-00.zip/00/000/006.pb
    Metadado: https://archive.org/download/ficha-2026-07/companies-00.zip/_meta.json
    Custódia: https://github.com/franklinbaldo/lontra/blob/main/workspace/ficha_pb_202607_00000006.b64

    O packer atual declara 1.1.0, mas o ZIP publicado declara 1.0.0.
    Isto prova a existência de wire=1 publicado, NÃO que a RFB originalmente
    atribuiu 01 a esta raiz. UI não deve traduzir o enum cru em fato.
    """
    import base64
    import hashlib

    published_b64 = (
        "CAYSJUFDQURFTUlBIERFIEJBTExFVCBQQVVMQSBGSVJFVFRJIExUREEaJUFDQURFTUlBIERFIEJBTExF"
        "VCBQQVVMQSBGSVJFVFRJIExUREFQjhBYAWEAAAAAAECPQHABeAGiAZ0BCAEQRRgBUAJYn+nHCXj/icEJ"
        "oAGFvIwEqgEIlPqPBMy2uATyAQNSVUH6AQ9MVUlTIERFIEFORFJBREWCAgI5OZICFFZJTEEgUEVSRUlS"
        "QSBCQVJSRVRPmALAnLIBogICU1CoAsM3wgICMTHKAgg5MzU4NTk5MPICGFBBVUxBRklSRVRUSUBIT1RN"
        "QUlMLkNPTZgDhdnPCaAD/6/QCaoBJwgCEhZQQVVMQSBDUklTVElOQSBGSVJFVFRJGIqwBigxMP+JwQlA"
        "BqAG764M"
    )
    payload = base64.b64decode(published_b64, validate=True)
    assert len(payload) == 306
    assert hashlib.sha256(payload).hexdigest() == (
        "515e989c74e5d44c4e7199f77d30b2ec578f85659ccfa5bf73f56bf9078aa203"
    )

    archived = Company.FromString(payload)
    assert archived.cnpj_base == 6
    assert archived.snapshot_yyyymm == 202607
    assert archived.porte_empresa == 1
    assert Porte.Name(archived.porte_empresa) == "NAO_INFORMADO"

    # Linha-ouro de OUTRO artefato publicado no mesmo item, consultado via
    # DuckDB HTTP Range pela Lontra R88, sem baixar o Parquet de ~3 GB:
    # https://archive.org/download/ficha-2026-07/raizes.parquet
    # https://github.com/franklinbaldo/lontra/blob/main/workspace/ficha_ia_parquet_pb_pair_r88.py
    # ETag/Last-Modified do servidor não equivalem a SHA integral do Parquet.
    derived_parquet_row = {
        "cnpj_base": "00000006",
        "porte_empresa": "01",
        "snapshot_yyyymm": 202607,  # item ficha-2026-07, não coluna física do Parquet
    }
    assert f"{archived.cnpj_base:08d}" == derived_parquet_row["cnpj_base"]
    assert archived.snapshot_yyyymm == derived_parquet_row["snapshot_yyyymm"]
    # Segundo o layout RFB, 01 = microempresa, mas o enum do PB diz
    # NAO_INFORMADO. É divergência ENTRE DERIVADOS PUBLICADOS, não
    # prova da entrada RFB nem identidade do produtor histórico v1.0.
    rfb_meanings = {
        "00": "NAO_INFORMADO",
        "01": "MICRO_EMPRESA",
        "03": "PEQUENO_PORTE",
        "05": "DEMAIS",
    }
    expected_semantics = rfb_meanings[derived_parquet_row["porte_empresa"]]
    assert expected_semantics == "MICRO_EMPRESA"
    assert Porte.Name(archived.porte_empresa) != expected_semantics


def test_meta_zip_publicado_202607_prefixo00_evidencia_versao_nao_semantica():
    """Pin exato do _meta.json do MESMO ZIP do PB real; nao prova produtor.

    Fonte: https://archive.org/download/ficha-2026-07/companies-00.zip/_meta.json
    Auditoria independente: https://github.com/franklinbaldo/ficha/issues/359
    O metadata e autoatestado pelo packer, nao assinatura do ZIP, do
    membro PB ou dos ZIPs da Receita. O manifesto nao atesta versao PB.
    Nao extrapolar 2026-07/00 para os 300 shards nem decodificar porte=1.
    """
    import base64
    import hashlib
    import json

    archived_meta = base64.b64decode(
        (
            "ewogICJhcnRpZmFjdCI6IG51bGwsCiAgImNvdW50IjogOTY0Nzk4LAogICJtYXRlcmlhbGl6YXRp"
            "b24iOiB7CiAgICAiaWQiOiAiNGQ0MzViMzJjMzVmOTcwZmMyNWFjODFjMWM3ZmY2Nzc5MGNlYjAw"
            "YzE4MTI3NTk3ODVmYWQwNTY1OGRmOWJlYiIsCiAgICAic3BlYyI6IHsKICAgICAgImlucHV0cyI6"
            "IHsKICAgICAgICAiY25wanMucGFycXVldCI6IHsKICAgICAgICAgICJzaGExIjogIjhhY2YzOTky"
            "ZDA4ZWM4OGYxMTM0MjdiNzk0Y2QwZTYxODdmMGQ4YmQiCiAgICAgICAgfSwKICAgICAgICAibG9v"
            "a3Vwcy9jbmFlcy5wYXJxdWV0IjogewogICAgICAgICAgInNoYTEiOiAiNzk2MGE1ZmY0Y2I4Mjg0"
            "OGEwMzM3YTk5ZjNjNWNlYzAxNDJkOGUzMCIKICAgICAgICB9LAogICAgICAgICJsb29rdXBzL21v"
            "dGl2b3MucGFycXVldCI6IHsKICAgICAgICAgICJzaGExIjogImM1ZGYxYzkyNTgxZDI1NmVhNDE3"
            "NjFmYTFkZTY4OTUxMzVkYzAwNTIiCiAgICAgICAgfSwKICAgICAgICAibG9va3Vwcy9tdW5pY2lw"
            "aW9zLnBhcnF1ZXQiOiB7CiAgICAgICAgICAic2hhMSI6ICIxMjA5ODA2Y2ViNWQwMWYwMzBjMGM1"
            "MzA2MGY0NmQ4ZTQzYWI5YWJhIgogICAgICAgIH0sCiAgICAgICAgImxvb2t1cHMvbmF0dXJlemFz"
            "LnBhcnF1ZXQiOiB7CiAgICAgICAgICAic2hhMSI6ICI1MTJkM2IxZWYyOWY1ZjYzOThlOTQyMmQz"
            "NDE5ZDU2YzE1NjRlYjZiIgogICAgICAgIH0sCiAgICAgICAgImxvb2t1cHMvcGFpc2VzLnBhcnF1"
            "ZXQiOiB7CiAgICAgICAgICAic2hhMSI6ICJlMTExYmI2MGRjOGU1ODIyMWUyN2Q1YWJlMWExYWU4"
            "MDJjNDFlM2Y2IgogICAgICAgIH0sCiAgICAgICAgImxvb2t1cHMvcXVhbGlmaWNhY29lcy5wYXJx"
            "dWV0IjogewogICAgICAgICAgInNoYTEiOiAiZmU4MzkxZjBiYThkMTRiYjgyNjYwMTFmOWQ2MDFm"
            "Y2Q2YmViZTM1YSIKICAgICAgICB9LAogICAgICAgICJyYWl6ZXMucGFycXVldCI6IHsKICAgICAg"
            "ICAgICJzaGExIjogIjFhMGYyNjIxZjQwMjZhNWYxY2RmMDE2NmEyMjQyOTM3N2E4Zjk1YjIiCiAg"
            "ICAgICAgfSwKICAgICAgICAic29jaW9zLnBhcnF1ZXQiOiB7CiAgICAgICAgICAic2hhMSI6ICI1"
            "MjZhMjcxZDZkMmRkZDdmNjkyNGMxOGM3MWViOTg3NmY0MTNlMTkxIgogICAgICAgIH0KICAgICAg"
            "fSwKICAgICAgIm1hdGVyaWFsaXphdGlvbl92ZXJzaW9uIjogMSwKICAgICAgInBhY2tlcl9mb3Jt"
            "YXRfdmVyc2lvbiI6IDEsCiAgICAgICJyYW5nZSI6IHsKICAgICAgICAia2luZCI6ICJjbnBqX2Jh"
            "c2VfcHJlZml4IiwKICAgICAgICAidmFsdWUiOiAiMDAiCiAgICAgIH0sCiAgICAgICJzY2hlbWEi"
            "OiB7CiAgICAgICAgImRlc2NyaXB0b3Jfc2hhMjU2IjogImY4NWY3OGJkNTkxZWJlOWJkM2QzODRk"
            "YmRhMjYxYzViNmFmZDNiMDZkZDdlZWFmNzAwNThmYzVmZWVlOTkzYmUiCiAgICAgIH0sCiAgICAg"
            "ICJzbmFwc2hvdCI6ICIyMDI2LTA3IgogICAgfQogIH0sCiAgInNjaGVtYV9zaGEyNTYiOiAiZjg1"
            "Zjc4YmQ1OTFlYmU5YmQzZDM4NGRiZGEyNjFjNWI2YWZkM2IwNmRkN2VlYWY3MDA1OGZjNWZlZWU5"
            "OTNiZSIsCiAgInNjaGVtYV92ZXJzaW9uIjogIjEuMC4wIiwKICAic25hcHNob3RfbW9udGgiOiAi"
            "MjAyNi0wNyIKfQ=="
        ),
        validate=True,
    )
    assert len(archived_meta) == 1606
    assert hashlib.sha256(archived_meta).hexdigest() == (
        "41fb2be20d89df053e5d5b4634f225019091582a62bb8cbe6e5875d811914e90"
    )
    metadata = json.loads(archived_meta)
    spec = metadata["materialization"]["spec"]
    assert metadata["schema_version"] == "1.0.0"
    assert metadata["snapshot_month"] == spec["snapshot"] == "2026-07"
    assert spec["range"] == {"kind": "cnpj_base_prefix", "value": "00"}
    assert metadata["schema_sha256"] == spec["schema"]["descriptor_sha256"]
    assert metadata["schema_sha256"] == (
        "f85f78bd591ebe9bd3d384dbda261c5b6afd3b06dd7eeaf70058fc5feee993be"
    )
    # SHA-1 dos derivados DECLARADOS no metadado; alinhados ao
    # manifesto na auditoria Lontra R90, nao verificados aqui por replay.
    assert {
        name: spec["inputs"][name]["sha1"]
        for name in ("cnpjs.parquet", "raizes.parquet", "socios.parquet")
    } == {
        "cnpjs.parquet": "8acf3992d08ec88f113427b794cd0e6187f0d8bd",
        "raizes.parquet": "1a0f2621f4026a5f1cdf0166a22429377a8f95b2",
        "socios.parquet": "526a271d6d2ddd7f6924c18c71eb9876f413e191",
    }
