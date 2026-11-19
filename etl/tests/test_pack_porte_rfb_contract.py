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
                strict=True, reason="#359: 02 não pertence ao domínio público 00/01/03/05"
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
