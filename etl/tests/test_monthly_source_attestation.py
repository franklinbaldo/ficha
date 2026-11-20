"""Offline contract for the first parsed-source authorization boundary.

This is NOT authentication of compressed ZIPs or immutable streaming.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path

import pytest

from ficha_etl import monthly_sharded as cli
from ficha_etl.monthly_source_bag_gate import EXPECTED_PARTS, _write_receipt


def _receipt(month: str = "2026-07") -> dict:
    return {
        "schema": "ficha-parsed-rfb-key-multiset-v1",
        "month": month,
        "identity_level": "parsed-duckdb-source-not-zip-custody",
        "status": "ok",
        "parts": [
            {"zip_name": name, "parsed_csv_size": 100 + i, "parsed_csv_rows": 1}
            for i, name in enumerate(EXPECTED_PARTS)
        ],
        "parsed_source_rows": 10,
        "produced_rows": 10,
        "source_only_key_exists": False,
        "output_only_key_exists": False,
    }


def _descriptor() -> dict:
    return {
        "date": "2026-07",
        "row_counts": {"cnpjs": 10},
        "files": {"cnpjs": {"sha256": "1" * 64}},
    }


def _prepare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", "test:run:7")
    out = tmp_path / "output"
    out.mkdir()
    path = cli._source_receipt_path(out)
    _write_receipt(path, _receipt())
    descriptor = _descriptor()
    cli._bind_source_receipt("2026-07", out, descriptor)
    # The helper prepares an eligible fixture. Production starts at pending.
    descriptor["source_attestation"]["raw_upload_status"] = "complete"
    return out, path, descriptor


def test_positive_bind_replay_same_attempt_and_raw_bytes(tmp_path: Path, monkeypatch):
    out, path, descriptor = _prepare(tmp_path, monkeypatch)
    raw = path.read_bytes()
    assert descriptor["source_attestation"]["receipt_sha256"] == hashlib.sha256(raw).hexdigest()
    assert descriptor["source_attestation"]["run_id"] == "test:run:7"
    cli._require_source_attestation("2026-07", out, descriptor)


@pytest.mark.parametrize(
    "mutation",
    [
        "absent",
        "failed",
        "month",
        "duplicate",
        "same_size",
        "same_total",
        "row_zero",
        "run",
        "different_receipt",
        "descriptor_sha",
        "descriptor_rows",
    ],
)
def test_invalid_source_receipts_refuse_before_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
):
    out, path, descriptor = _prepare(tmp_path, monkeypatch)
    if mutation == "absent":
        path.unlink()
    elif mutation == "failed":
        data = json.loads(path.read_text())
        data["status"] = "failed"
        _write_receipt(path, data)
    elif mutation == "month":
        data = json.loads(path.read_text())
        data["month"] = "2026-08"
        _write_receipt(path, data)
    elif mutation == "duplicate":
        data = json.loads(path.read_text())
        data["parts"][9]["zip_name"] = "Estabelecimentos0.zip"
        _write_receipt(path, data)
    elif mutation == "same_size":
        data = json.loads(path.read_text())
        data["parts"][0]["parsed_csv_size"] += 1
        _write_receipt(path, data)
    elif mutation == "same_total":
        data = json.loads(path.read_text())
        data["parts"][0]["parsed_csv_rows"] = 2
        data["parts"][1]["parsed_csv_rows"] = 0
        _write_receipt(path, data)
    elif mutation == "row_zero":
        data = json.loads(path.read_text())
        data["parts"][0]["parsed_csv_rows"] = 0
        _write_receipt(path, data)
    elif mutation == "run":
        monkeypatch.setenv("FICHA_SOURCE_RUN_ID", "another-run")
    elif mutation == "different_receipt":
        descriptor["source_attestation"]["receipt_sha256"] = "0" * 64
    elif mutation == "descriptor_sha":
        descriptor["files"]["cnpjs"]["sha256"] = "2" * 64
    elif mutation == "descriptor_rows":
        descriptor["row_counts"]["cnpjs"] = 9
    with pytest.raises((ValueError, FileNotFoundError)):
        cli._require_source_attestation("2026-07", out, descriptor)


def test_upload_outputs_checks_source_before_submit_and_secret(tmp_path: Path, monkeypatch):
    out, path, descriptor = _prepare(tmp_path, monkeypatch)
    calls = []

    monkeypatch.setattr(cli, "_require_safe_legacy_shard_upload", lambda: None)
    monkeypatch.setattr(cli, "_load_descriptor", lambda *args: descriptor)
    monkeypatch.setattr(
        cli,
        "submit_outputs_fail_closed",
        lambda *a, **k: calls.append("submit") or argparse.Namespace(upload=[], reuse=[]),
    )
    monkeypatch.setattr(cli, "_require_secret", lambda name: calls.append(name) or "offline")
    args = argparse.Namespace(
        month="2026-07", descriptor=tmp_path / "descriptor.json", output_dir=out
    )

    cli.command_upload_outputs(args)
    assert calls == ["IA_ACCESS_KEY", "IA_SECRET_KEY", "submit"]
    calls.clear()

    # Failed source even with a previously valid descriptor must not reach submit.
    data = json.loads(path.read_text())
    data["status"] = "failed"
    _write_receipt(path, data)
    with pytest.raises(ValueError):
        cli.command_upload_outputs(args)
    assert calls == []

    path.unlink()
    with pytest.raises(FileNotFoundError):
        cli.command_upload_outputs(args)
    assert calls == []


def test_legacy_descriptor_without_source_binding_is_rejected(tmp_path: Path, monkeypatch):
    out, _, descriptor = _prepare(tmp_path, monkeypatch)
    old = copy.deepcopy(descriptor)
    old.pop("source_attestation")
    with pytest.raises(ValueError, match="lacks matching"):
        cli._require_source_attestation("2026-07", out, old)


def test_initial_failed_receipt_can_be_overwritten_after_gate(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", "test:run:7")
    out = tmp_path / "output"
    out.mkdir()
    path = cli._source_receipt_path(out)
    _write_receipt(
        path,
        {
            "schema": "ficha-parsed-rfb-key-multiset-v1",
            "month": "2026-07",
            "status": "failed",
            "parts": [],
        },
    )
    descriptor = _descriptor()
    with pytest.raises(ValueError):
        cli._bind_source_receipt("2026-07", out, descriptor)
    _write_receipt(path, _receipt())
    cli._bind_source_receipt("2026-07", out, descriptor)
    assert descriptor["source_attestation"]["raw_upload_status"] == "pending"
    with pytest.raises(ValueError, match="raw source upload did not complete"):
        cli._require_source_attestation("2026-07", out, descriptor)
    descriptor["source_attestation"]["raw_upload_status"] = "complete"
    cli._require_source_attestation("2026-07", out, descriptor)


def test_produce_writes_attestation_before_first_raw_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", "test:run:7")
    monkeypatch.setattr(cli, "_require_safe_legacy_shard_upload", lambda: None)
    args = argparse.Namespace(
        month="2026-07",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        descriptor=tmp_path / "descriptor.json",
        skip_raw_upload=False,
        no_verify=True,
        verify_sample_size=10,
    )
    calls = []

    def parsed_source(*_args, **_kwargs):
        calls.append("transform")
        _write_receipt(cli._source_receipt_path(args.output_dir), _receipt())

    def descriptor_build(*_args):
        calls.append("build")
        return _descriptor()

    def descriptor_write(path, descriptor):
        calls.append("write")
        path.write_text(json.dumps(descriptor))

    def raw_upload(*_args, **_kwargs):
        calls.append("raw")
        descriptor = json.loads(args.descriptor.read_text())
        assert descriptor["source_attestation"]["raw_upload_status"] == "pending"
        with pytest.raises(ValueError, match="raw source upload did not complete"):
            cli._require_source_attestation(args.month, args.output_dir, descriptor)
        assert descriptor["source_attestation"]["run_id"] == "test:run:7"

    monkeypatch.setattr(cli.transform, "transform_snapshot", parsed_source)
    monkeypatch.setattr(cli, "run_monthly_gate", lambda *_args: calls.append("key-gate"))
    monkeypatch.setattr(cli, "build_production_descriptor", descriptor_build)
    monkeypatch.setattr(cli, "write_production_descriptor", descriptor_write)
    monkeypatch.setattr(cli, "_require_secret", lambda name: "offline-test-only")
    monkeypatch.setattr(cli.upload, "stream_raw_zips_to_ia", raw_upload)
    cli.command_produce(args)
    assert calls == ["transform", "key-gate", "build", "write", "raw", "write"]
    persisted = json.loads(args.descriptor.read_text())
    assert persisted["source_attestation"]["raw_upload_status"] == "complete"
    cli._require_source_attestation(args.month, args.output_dir, persisted)


def test_produce_invalidates_stale_success_before_extract_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(cli, "_require_safe_legacy_shard_upload", lambda: None)
    args = argparse.Namespace(
        month="2026-07",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        descriptor=tmp_path / "descriptor.json",
        skip_raw_upload=False,
        no_verify=True,
        verify_sample_size=10,
    )
    path = cli._source_receipt_path(args.output_dir)
    _write_receipt(path, {**_receipt(), "run_id": "stale"})
    monkeypatch.setattr(
        cli.transform,
        "transform_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("missing ZIP")),
    )
    monkeypatch.setattr(
        cli, "_require_secret", lambda name: pytest.fail("secrets after failed ZIP")
    )
    monkeypatch.setattr(
        cli.upload, "stream_raw_zips_to_ia", lambda *a, **k: pytest.fail("raw after failed ZIP")
    )
    with pytest.raises(RuntimeError, match="missing ZIP"):
        cli.command_produce(args)
    assert json.loads(path.read_text())["status"] == "failed"
    assert not args.descriptor.exists()
