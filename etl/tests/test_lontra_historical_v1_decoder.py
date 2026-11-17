"""Offline custody test against the actual published Protobuf v1 Python decoder.

The frozen generated source is a byte-for-byte copy from main@021ec8b5,
Git blob 68bd9bec5f4c663b689599cd0acb17ac8d32fa68.
Read its DESCRIPTOR literal in a SEPARATE protobuf pool so the old schema
cannot accidentally be replaced by the current ficha.v1 descriptor.

No remote ZIP, Internet Archive I/O, real ETL or publication is involved.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from google.protobuf import descriptor_pool, message_factory

from ficha_etl.proto.ficha.v1 import company_pb2 as current_pb

_OLD_BLOB = "68bd9bec5f4c663b689599cd0acb17ac8d32fa68"
_OLD_SOURCE = Path(__file__).parent / "fixtures" / "company_pb2_v1_at_021ec8b5.txt"


def _actual_v1_company_class():
    raw = _OLD_SOURCE.read_bytes()
    git_blob = b"blob " + str(len(raw)).encode("ascii") + bytes([0]) + raw
    assert hashlib.sha1(git_blob, usedforsecurity=False).hexdigest() == _OLD_BLOB

    tree = ast.parse(raw.decode("utf-8"))
    matches = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "DESCRIPTOR" for target in node.targets
        )
    ]
    assert len(matches) == 1
    call = matches[0].value
    assert isinstance(call, ast.Call) and len(call.args) == 1
    serialized_descriptor = ast.literal_eval(call.args[0])
    assert isinstance(serialized_descriptor, bytes)

    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(serialized_descriptor)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("ficha.v1.Company"))


def test_actual_historical_v1_decoder_still_reads_numeric_v11_wire() -> None:
    """Positive control: unchanged tags preserve numeric company identity."""
    legacy_company = _actual_v1_company_class()
    encoded = current_pb.Company(cnpj_base=12345678)
    establishment = encoded.estabelecimentos.add()
    establishment.cnpj_ordem = 1
    establishment.cnpj_dv = 90

    decoded = legacy_company.FromString(encoded.SerializeToString())
    assert decoded.cnpj_base == 12345678
    assert decoded.estabelecimentos[0].cnpj_ordem == 1
    assert decoded.estabelecimentos[0].cnpj_dv == 90
    assert (
        f"{decoded.cnpj_base:08d}"
        f"{decoded.estabelecimentos[0].cnpj_ordem:04d}"
        f"{decoded.estabelecimentos[0].cnpj_dv:02d}"
    ) == "12345678000190"


def test_actual_historical_v1_decoder_cannot_represent_alpha_branch_order() -> None:
    """Negative control: parse succeeds, but E08G is invisible to v1 consumers."""
    legacy_company = _actual_v1_company_class()
    company = current_pb.Company(cnpj_base=0)
    establishment = company.estabelecimentos.add()
    establishment.cnpj_ordem_text = "E08G"
    establishment.cnpj_dv = 12
    wire = company.SerializeToString()

    decoded_modern = current_pb.Company.FromString(wire)
    decoded_legacy = legacy_company.FromString(wire)
    assert decoded_modern.estabelecimentos[0].cnpj_ordem_text == "E08G"
    assert "cnpj_ordem_text" not in decoded_legacy.estabelecimentos[0].DESCRIPTOR.fields_by_name
    assert decoded_legacy.estabelecimentos[0].cnpj_ordem == 0
    legacy_cnpj = (
        f"{decoded_legacy.cnpj_base:08d}"
        f"{decoded_legacy.estabelecimentos[0].cnpj_ordem:04d}"
        f"{decoded_legacy.estabelecimentos[0].cnpj_dv:02d}"
    )
    assert legacy_cnpj == "00000000000012"
    assert legacy_cnpj != "00000000E08G12"


def test_actual_historical_v1_decoder_cannot_represent_alpha_root_or_partner() -> None:
    """Other new text tags are unknown to the pinned historical decoder."""
    legacy_company = _actual_v1_company_class()
    company = current_pb.Company(cnpj_base_text="0000000A")
    partner = company.socios.add()
    partner.cnpj_socio_text = "00000000E08G12"
    wire = company.SerializeToString()

    decoded_modern = current_pb.Company.FromString(wire)
    decoded_legacy = legacy_company.FromString(wire)
    assert decoded_modern.cnpj_base_text == "0000000A"
    assert decoded_modern.socios[0].cnpj_socio_text == "00000000E08G12"
    assert "cnpj_base_text" not in decoded_legacy.DESCRIPTOR.fields_by_name
    assert "cnpj_socio_text" not in decoded_legacy.socios[0].DESCRIPTOR.fields_by_name
    assert decoded_legacy.cnpj_base == 0
    assert decoded_legacy.socios[0].cnpj_socio == 0
