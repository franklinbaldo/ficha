from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from ficha_etl.monthly_sharded import (
    _load_descriptor,
    _load_receipt,
    _remote_descriptor_state,
    command_finalize,
    materialization_inputs_from_descriptor,
    wait_for_descriptor_remote,
)
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, SHARD_INPUT_NAMES


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
    metadata["files"].extend([{"name": "unrelated.txt", "size": "1"}] * 2)
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])


def test_remote_descriptor_accepts_integer_and_zero_padded_size() -> None:
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    target = next(entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet")
    target["size"] = 11
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])
    target["size"] = "00011"
    assert _remote_descriptor_state(descriptor, metadata) == ([], [])


def test_wait_for_descriptor_remote_refuses_ambiguous_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The actual wait gate must not pass a last-wins catalog to finalize."""
    descriptor = _descriptor()
    metadata = _metadata_from_descriptor(descriptor)
    duplicate = next(
        entry for entry in metadata["files"] if entry["name"] == "cnpjs.parquet"
    ).copy()
    duplicate["sha1"] = "0" * 40
    metadata["files"].insert(0, duplicate)
    calls: list[str] = []

    def fake_fetch(month: str) -> dict:
        calls.append(month)
        return metadata

    monkeypatch.setattr("ficha_etl.monthly_sharded.fetch_item_metadata", fake_fetch)
    monkeypatch.setattr("ficha_etl.monthly_sharded.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match=r"cnpjs\.parquet \(duplicado\)"):
        wait_for_descriptor_remote("2026-06", descriptor, attempts=2, interval_s=0)
    assert calls == ["2026-06", "2026-06"]


def test_wait_for_descriptor_remote_retries_then_accepts_unambiguous_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ambiguous view can recover, without adopting either duplicate."""
    descriptor = _descriptor()
    valid = _metadata_from_descriptor(descriptor)
    ambiguous = {"files": [*valid["files"], valid["files"][0].copy()]}
    catalogs = iter((ambiguous, valid))
    calls: list[str] = []

    def fake_fetch(month: str) -> dict:
        calls.append(month)
        return next(catalogs)

    monkeypatch.setattr("ficha_etl.monthly_sharded.fetch_item_metadata", fake_fetch)
    monkeypatch.setattr("ficha_etl.monthly_sharded.time.sleep", lambda _: None)
    assert wait_for_descriptor_remote("2026-06", descriptor, attempts=2, interval_s=0) is valid
    assert calls == ["2026-06", "2026-06"]


@pytest.mark.parametrize(
    "bad_size",
    [11.9, True, "11.0", "+11", " 11", "١١", 0, -11, "9" * 5000],
)
def test_local_descriptor_rejects_nonintegral_size(tmp_path: Path, bad_size: object) -> None:
    """Invalid local size must fail before any remote identity comparison."""
    descriptor = _descriptor()
    descriptor["files"]["cnpjs"]["size"] = bad_size
    path = tmp_path / "descriptor.json"
    path.write_text(json.dumps(descriptor), encoding="utf-8")

    with pytest.raises(ValueError, match=r"files\.cnpjs: invalid size"):
        _load_descriptor(path, "2026-06")


@pytest.mark.parametrize("valid_size", [11, "11", "00011"])
def test_local_descriptor_preserves_valid_byte_sizes(tmp_path: Path, valid_size: object) -> None:
    descriptor = _descriptor()
    descriptor["files"]["cnpjs"]["size"] = valid_size
    path = tmp_path / "descriptor.json"
    path.write_text(json.dumps(descriptor), encoding="utf-8")

    assert _load_descriptor(path, "2026-06") == descriptor


def _valid_shard_receipt() -> dict:
    return {
        "month": "2026-06",
        "prefix": "00",
        "name": PUBLIC_COMPANIES_GEOMETRY.shard_name("00"),
        "size": 11,
        "sha1": "1" * 40,
        "materialization_id": "a" * 64,
    }


@pytest.mark.parametrize(
    "bad_size",
    [11.9, True, "11.0", "+11", " 11", "١١", 0, -11, "9" * 5000],
)
def test_shard_receipt_rejects_nonintegral_size(tmp_path: Path, bad_size: object) -> None:
    receipt = _valid_shard_receipt()
    receipt["size"] = bad_size
    path = tmp_path / "receipt-00.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(ValueError, match="invalid byte identity in receipt"):
        _load_receipt(path, "2026-06", "00")


@pytest.mark.parametrize("valid_size", [11, "11", "00011"])
def test_shard_receipt_accepts_integral_size(tmp_path: Path, valid_size: object) -> None:
    receipt = _valid_shard_receipt()
    receipt["size"] = valid_size
    path = tmp_path / "receipt-00.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    assert _load_receipt(path, "2026-06", "00") == receipt


def test_finalize_rejects_malformed_receipt_before_remote_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The finalizer must reject local receipt identity before contacting IA."""
    descriptor_path = tmp_path / "descriptor.json"
    descriptor_path.write_text(json.dumps(_descriptor()), encoding="utf-8")
    receipts_dir = tmp_path / "receipts"
    receipts_dir.mkdir()
    receipt = _valid_shard_receipt()
    receipt["size"] = 11.9
    (receipts_dir / "receipt-00.json").write_text(json.dumps(receipt), encoding="utf-8")
    calls: list[str] = []

    def must_not_fetch(*args: object, **kwargs: object) -> None:
        calls.append("remote")
        raise AssertionError("invalid local receipt must not touch remote metadata")

    monkeypatch.setattr("ficha_etl.monthly_sharded.wait_for_descriptor_remote", must_not_fetch)
    monkeypatch.setattr(
        "ficha_etl.monthly_sharded._require_source_attestation", lambda *a, **kw: None
    )
    # Preserve malformed-receipt coverage after the independent raw custody gate.
    monkeypatch.setattr(
        "ficha_etl.monthly_sharded._require_raw_custody_authorization",
        lambda *a, **kw: None,
    )
    args = argparse.Namespace(
        month="2026-06",
        descriptor=str(descriptor_path),
        source_receipt=str(tmp_path / "source-receipt.json"),
        receipts_dir=str(receipts_dir),
        snapshot_entry=str(tmp_path / "snapshot.json"),
        verification=str(tmp_path / "verification.json"),
        manifest=str(tmp_path / "manifest.json"),
        attempts=2,
        interval_seconds=0,
    )
    with pytest.raises(ValueError, match="invalid byte identity in receipt"):
        command_finalize(args)
    assert calls == []


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
