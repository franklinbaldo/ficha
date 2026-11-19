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
