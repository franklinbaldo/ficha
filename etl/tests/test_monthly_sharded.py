from __future__ import annotations

from pathlib import Path

import pytest

from ficha_etl.monthly_sharded import (
    _remote_descriptor_state,
    materialization_inputs_from_descriptor,
)
from ficha_etl.shard_remote import SHARD_INPUT_NAMES


def _identity(seed: str, size: int = 10) -> dict:
    return {
        "url": f"https://example.test/{seed}",
        "size": size,
        "sha1": seed * 40,
        "sha256": seed * 64,
    }


def _descriptor() -> dict:
    files = {
        "cnpjs": _identity("1", 11),
        "cnpj_contatos": _identity("2", 12),
        "cnpj_cnaes": _identity("3", 13),
        "raizes": _identity("4", 14),
        "socios": _identity("5", 15),
        "enderecos": _identity("6", 16),
        "pessoas": _identity("7", 17),
        "lookups": _identity("8", 18),
    }
    lookups = {
        "cnaes": _identity("a", 21),
        "motivos": _identity("b", 22),
        "municipios": _identity("c", 23),
        "naturezas": _identity("d", 24),
        "paises": _identity("e", 25),
        "qualificacoes": _identity("f", 26),
    }
    return {"date": "2026-06", "files": files, "lookups": lookups}


def _metadata_from_descriptor(descriptor: dict) -> dict:
    names = {
        "cnpjs": "cnpjs.parquet",
        "cnpj_contatos": "cnpj_contatos.parquet",
        "cnpj_cnaes": "cnpj_cnaes.parquet",
        "raizes": "raizes.parquet",
        "socios": "socios.parquet",
        "enderecos": "enderecos.parquet",
        "pessoas": "pessoas.parquet",
        "lookups": "lookups.json",
    }
    files = [
        {
            "name": remote_name,
            "size": str(descriptor["files"][key]["size"]),
            "sha1": descriptor["files"][key]["sha1"],
        }
        for key, remote_name in names.items()
    ]
    files += [
        {
            "name": f"lookups/{kind}.parquet",
            "size": str(entry["size"]),
            "sha1": entry["sha1"],
        }
        for kind, entry in descriptor["lookups"].items()
    ]
    return {"files": files}


def test_materialization_inputs_come_from_production_descriptor() -> None:
    descriptor = _descriptor()
    inputs = materialization_inputs_from_descriptor(descriptor)

    assert tuple(inputs) == SHARD_INPUT_NAMES
    assert inputs["cnpjs.parquet"] == descriptor["files"]["cnpjs"]["sha1"]
    assert inputs["raizes.parquet"] == descriptor["files"]["raizes"]["sha1"]
    assert inputs["socios.parquet"] == descriptor["files"]["socios"]["sha1"]
    assert inputs["lookups/cnaes.parquet"] == descriptor["lookups"]["cnaes"]["sha1"]


def test_remote_descriptor_state_accepts_exact_size_and_sha1() -> None:
    descriptor = _descriptor()
    pending, mismatches = _remote_descriptor_state(
        descriptor,
        _metadata_from_descriptor(descriptor),
    )
    assert pending == []
    assert mismatches == []


def test_remote_descriptor_state_never_adopts_remote_mismatch() -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    target["sha1"] = "0" * 40

    pending, mismatches = _remote_descriptor_state(descriptor, metadata)
    assert pending == []
    assert len(mismatches) == 1
    assert "cnpjs.parquet" in mismatches[0]



@pytest.mark.parametrize("opposite_first", [True, False])
@pytest.mark.parametrize("same_identity", [True, False])
def test_remote_descriptor_rejects_duplicate_target_regardless_of_order(
    opposite_first: bool, same_identity: bool
) -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    duplicate = target.copy()
    if not same_identity:
        duplicate["sha1"] = "0" * 40
    if opposite_first:
        metadata["files"].insert(0, duplicate)
    else:
        metadata["files"].append(duplicate)

    pending, mismatches = _remote_descriptor_state(descriptor, metadata)
    assert pending == ["cnpjs.parquet (duplicado)"]
    assert mismatches == []


@pytest.mark.parametrize(
    "bad_size",
    [11.9, True, "11.0", "+11", " 11", "١١", 0, -11, "9" * 5000],
)
def test_remote_descriptor_rejects_uncomparable_size(bad_size: object) -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    target["size"] = bad_size

    pending, mismatches = _remote_descriptor_state(descriptor, metadata)
    assert pending == ["cnpjs.parquet"]
    assert mismatches == []


def test_remote_descriptor_does_not_block_on_unrelated_duplicate() -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    metadata["files"].extend(
        [{"name": "unrelated.txt", "size": "1"}] * 2
    )
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])


def test_remote_descriptor_accepts_integer_and_zero_padded_size() -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    target["size"] = 11
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])
    target["size"] = "00011"
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])

def test_monthly_workflow_persists_receipts_before_remote_writes() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml").read_text(
        encoding="utf-8"
    )

    descriptor_artifact = workflow.index("Persist production descriptor before upload")
    outputs_upload = workflow.index("Upload derived outputs exactly as produced")
    receipt_artifact = workflow.index("Persist shard production receipt before PUT")
    shard_submit = workflow.index("Submit exactly the receipted bytes")

    assert descriptor_artifact < outputs_upload
    assert receipt_artifact < shard_submit
