"""Offline producer-to-planner test using an actual Protobuf 1.1 shard ZIP.

Only two synthetic rows are packed into tmp_path. No ETL, IA or network I/O.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from ficha_etl import pack, sharded_pack
from ficha_etl.cnpj import shard_of
from ficha_etl.proto.ficha.v1.company_pb2 import Company
from ficha_etl.shard_publish import ShardPublishError
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY
from ficha_etl.shard_transfer import PreparedShard, plan_opt_in_v11_shard


def _real_local_shard(tmp_path: Path):
    session = sharded_pack.ShardPackSession(
        "2026-08", PUBLIC_COMPANIES_GEOMETRY, parquets_base=str(tmp_path)
    )
    spec = session.materialization_spec("42", input_sha1s={})
    path = tmp_path / "companies-42.zip"
    rows = iter(
        [
            {"cnpj_base": "42000001"},
            {
                "cnpj_base": "CA000001",
                "estabelecimentos": [{"cnpj_ordem": "E08G", "cnpj_dv": "12"}],
                "socios": [{"cnpj_socio": "00000000E08G12"}],
            },
        ]
    )
    count, size = sharded_pack._pack_rows(
        rows,
        {kind: [] for kind in pack.LOOKUP_KINDS},
        path,
        snapshot_month="2026-08",
        materialization=spec,
        schema_desc=pack._schema_desc_bytes(),
        schema_sha256=session.schema_sha256,
    )
    assert count == 2
    assert size == path.stat().st_size
    prepared = PreparedShard(
        prefix="42",
        name=path.name,
        path=path,
        materialization_id=spec.materialization_id(),
        size=size,
        sha1=hashlib.sha1(path.read_bytes(), usedforsecurity=False).hexdigest(),
    )
    return session, spec, prepared


def test_genuine_zip_retains_alphanumeric_root_order_and_partner(tmp_path: Path) -> None:
    assert shard_of("42000001") == "42"  # numeric v1 bucket stays unchanged
    assert shard_of("CA000001") == "42"  # base36(CA) = 442, hence 42
    session, spec, prepared = _real_local_shard(tmp_path)

    with zipfile.ZipFile(prepared.path) as archive:
        assert archive.testzip() is None
        names = archive.namelist()
        assert len(names) == len(set(names))
        assert "42/000/001.pb" in names
        assert "CA/000/001.pb" in names
        assert "_schema.desc" in names
        assert "_schema.proto" in names
        assert all(f"_lookups/{kind}.pb" in names for kind in pack.LOOKUP_KINDS)

        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(archive.read("_schema.desc"))
        company_file = next(
            item for item in descriptor_set.file if item.name == "ficha/v1/company.proto"
        )
        fields = {
            msg.name: {field.name for field in msg.field} for msg in company_file.message_type
        }
        assert "cnpj_base_text" in fields["Company"]
        assert "cnpj_ordem_text" in fields["Estabelecimento"]
        assert "cnpj_socio_text" in fields["Socio"]

        company = Company.FromString(archive.read("CA/000/001.pb"))
        assert company.cnpj_base_text == "CA000001"
        assert company.estabelecimentos[0].cnpj_ordem_text == "E08G"
        assert company.socios[0].cnpj_socio_text == "00000000E08G12"
        assert company.snapshot_yyyymm == 202608
        numeric_company = Company.FromString(archive.read("42/000/001.pb"))
        assert numeric_company.cnpj_base == 42000001

        meta = json.loads(archive.read("_meta.json"))
        assert meta["schema_version"] == "1.1.0"
        assert meta["count"] == 2
        assert meta["materialization"]["id"] == spec.materialization_id()

    planned = plan_opt_in_v11_shard(session, prepared, pinned_inputs={})
    assert planned.name == "wire-1.1/companies-42.zip"
    assert planned.path == prepared.path
    assert (planned.materialization_id, planned.size, planned.sha1) == (
        prepared.materialization_id,
        prepared.size,
        prepared.sha1,
    )
    assert not (tmp_path / "wire-1.1").exists()


def test_genuine_zip_changed_after_receipt_fails_without_publish(tmp_path: Path) -> None:
    session, _spec, prepared = _real_local_shard(tmp_path)
    prepared.path.write_bytes(b"changed after local receipt")
    with pytest.raises(ShardPublishError, match="bytes locais mudaram"):
        plan_opt_in_v11_shard(session, prepared, pinned_inputs={})
    assert not (tmp_path / "wire-1.1").exists()
