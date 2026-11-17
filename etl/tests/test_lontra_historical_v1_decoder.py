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

_OLD_BLOB = "68bd9bec5f4c663b689599cd0acb17ac8d32fa68"
_NEW_BLOB = "49195b4d26dcff7cc89feeb4bdc390fcdbfe5d3e"
_FIXTURES = {
    "v1": ("company_pb2_v1_at_021ec8b5.txt", _OLD_BLOB),
    "v11": ("company_pb2_v11_at_ef2a7a46.txt", _NEW_BLOB),
}


def _company_class(wire: str):
    name, expected_blob_sha = _FIXTURES[wire]
    raw = (Path(__file__).parent / "fixtures" / name).read_bytes()
    # Git for Windows may check out text fixtures as CRLF; compare canonical Git bytes.
    raw = raw.replace(b"\r\n", b"\n")
    git_blob = b"blob " + str(len(raw)).encode("ascii") + bytes([0]) + raw
    assert hashlib.sha1(git_blob, usedforsecurity=False).hexdigest() == expected_blob_sha

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
    legacy_company = _company_class("v1")
    encoded = _company_class("v11")(cnpj_base=12345678)
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
    legacy_company = _company_class("v1")
    company = _company_class("v11")(cnpj_base=0)
    establishment = company.estabelecimentos.add()
    establishment.cnpj_ordem_text = "E08G"
    establishment.cnpj_dv = 12
    wire = company.SerializeToString()

    decoded_modern = _company_class("v11").FromString(wire)
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
    legacy_company = _company_class("v1")
    company = _company_class("v11")(cnpj_base_text="0000000A")
    partner = company.socios.add()
    partner.cnpj_socio_text = "00000000E08G12"
    wire = company.SerializeToString()

    decoded_modern = _company_class("v11").FromString(wire)
    decoded_legacy = legacy_company.FromString(wire)
    assert decoded_modern.cnpj_base_text == "0000000A"
    assert decoded_modern.socios[0].cnpj_socio_text == "00000000E08G12"
    assert "cnpj_base_text" not in decoded_legacy.DESCRIPTOR.fields_by_name
    assert "cnpj_socio_text" not in decoded_legacy.socios[0].DESCRIPTOR.fields_by_name
    assert decoded_legacy.cnpj_base == 0
    assert decoded_legacy.socios[0].cnpj_socio == 0


def test_historical_v1_unknown_fields_survive_opaque_roundtrip_but_not_projection() -> None:
    """Distinguish opaque protobuf wire preservation from lossy v1 field mapping.

    A raw v1 parse/serialize roundtrip keeps unknown tags with this runtime.
    A consumer that rebuilds a v1 message from known numeric fields does not.
    Neither behavior makes a v1 UI capable of displaying the alpha identity.
    """
    legacy_company = _company_class("v1")
    modern_company = _company_class("v11")
    source = modern_company(cnpj_base_text="0000000A")
    establishment = source.estabelecimentos.add()
    establishment.cnpj_ordem_text = "E08G"
    establishment.cnpj_dv = 12
    source.socios.add().cnpj_socio_text = "00000000E08G12"

    original_wire = source.SerializeToString()
    old = legacy_company.FromString(original_wire)

    # Positive control: this historical protobuf runtime preserves opaque tags.
    opaque_roundtrip = modern_company.FromString(old.SerializeToString())
    assert opaque_roundtrip.cnpj_base_text == "0000000A"
    assert opaque_roundtrip.estabelecimentos[0].cnpj_ordem_text == "E08G"
    assert opaque_roundtrip.socios[0].cnpj_socio_text == "00000000E08G12"

    # Negative control: a v1 app projecting only familiar fields loses identity.
    projected = legacy_company(cnpj_base=old.cnpj_base)
    projected_establishment = projected.estabelecimentos.add()
    projected_establishment.cnpj_ordem = old.estabelecimentos[0].cnpj_ordem
    projected_establishment.cnpj_dv = old.estabelecimentos[0].cnpj_dv
    projected.socios.add().cnpj_socio = old.socios[0].cnpj_socio
    projected_wire = projected.SerializeToString()
    decoded_projection = modern_company.FromString(projected_wire)
    assert decoded_projection.cnpj_base_text == ""
    assert decoded_projection.estabelecimentos[0].cnpj_ordem_text == ""
    assert decoded_projection.socios[0].cnpj_socio_text == ""
    assert projected_wire != original_wire


def test_historical_v1_on_exact_castor_packer_fixture() -> None:
    """Cross-check the single packer-produced member, never a second generator.

    The fixture lives in Castor's branch and is regenerated by his test from
    CA/000/001.pb. The historical v1 source is pinned above by its Git blob.
    Parsing succeeds, but an old application cannot display alpha identity.
    """
    fixture = Path(__file__).parent / "fixtures" / "company-v11-ca000001.hex"
    wire = bytes.fromhex(fixture.read_text(encoding="ascii").strip())
    assert len(wire) == 45
    assert hashlib.sha256(wire).hexdigest() == (
        "041e2b5e6f039959b03fd185021493f422ce884ea155590c9e99916ca769ec48"
    )

    v11_class = _company_class("v11")
    v1_class = _company_class("v1")
    modern = v11_class.FromString(wire)
    historical = v1_class.FromString(wire)

    # Positive: these are the EXACT packer's 1.1 member bytes.
    assert modern.snapshot_yyyymm == 202608
    assert modern.cnpj_base_text == "CA000001"
    assert len(modern.estabelecimentos) == 1
    assert modern.estabelecimentos[0].cnpj_ordem_text == "E08G"
    assert modern.estabelecimentos[0].cnpj_dv == 12
    assert len(modern.socios) == 1
    assert modern.socios[0].cnpj_socio_text == "00000000E08G12"

    # Negative: historical v1 parses but exposes only numeric defaults.
    assert historical.cnpj_base == 0
    assert historical.estabelecimentos[0].cnpj_ordem == 0
    assert historical.estabelecimentos[0].cnpj_dv == 12
    assert historical.socios[0].cnpj_socio == 0
    historical_cnpj = (
        f"{historical.cnpj_base:08d}"
        f"{historical.estabelecimentos[0].cnpj_ordem:04d}"
        f"{historical.estabelecimentos[0].cnpj_dv:02d}"
    )
    assert historical_cnpj == "00000000000012"
    assert historical_cnpj != "CA000001E08G12"

    # Unknown tags SURVIVE an opaque roundtrip with this Python runtime.
    opaque = v11_class.FromString(historical.SerializeToString())
    assert opaque.cnpj_base_text == "CA000001"
    assert opaque.estabelecimentos[0].cnpj_ordem_text == "E08G"
    assert opaque.socios[0].cnpj_socio_text == "00000000E08G12"

    # But a v1 consumer reconstructing only known fields drops those tags.
    projection = v1_class(cnpj_base=historical.cnpj_base)
    projected_branch = projection.estabelecimentos.add()
    projected_branch.cnpj_ordem = historical.estabelecimentos[0].cnpj_ordem
    projected_branch.cnpj_dv = historical.estabelecimentos[0].cnpj_dv
    projection.socios.add().cnpj_socio = historical.socios[0].cnpj_socio
    lossy = v11_class.FromString(projection.SerializeToString())
    assert lossy.cnpj_base_text == ""
    assert lossy.estabelecimentos[0].cnpj_ordem_text == ""
    assert lossy.socios[0].cnpj_socio_text == ""
