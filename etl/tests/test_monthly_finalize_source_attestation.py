"""External parsed-source receipt must gate finalize before remote/manifest effects.

Only synthetic JSON, intercepted network boundaries and an offline 100-shard
positive. This is not RFB ZIP authenticity nor an actual IA promotion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ficha_etl import monthly_sharded as cli
from ficha_etl.monthly_source_bag_gate import EXPECTED_PARTS, _write_receipt
from ficha_etl.shard_transfer import ShardTransferAction

MONTH = "2026-07"
RUN = "test:finalize:7"


def _identity(digit: str) -> dict:
    return {"size": 10, "sha1": digit * 40, "sha256": digit * 64}


def _source() -> dict:
    return {
        "schema": "ficha-parsed-rfb-key-multiset-v1",
        "month": MONTH,
        "status": "ok",
        "run_id": RUN,
        "identity_level": "parsed-duckdb-source-not-zip-custody",
        "parts": [
            {"zip_name": name, "parsed_csv_size": 120 + i, "parsed_csv_rows": 1}
            for i, name in enumerate(EXPECTED_PARTS)
        ],
        "parsed_source_rows": 10,
        "produced_rows": 10,
        "source_only_key_exists": False,
        "output_only_key_exists": False,
    }


def _descriptor(receipt: Path) -> dict:
    files = {name: _identity(str(i)) for i, name in enumerate(cli._STANDARD_FILE_NAMES, start=1)}
    lookups = {
        name: _identity(letter)
        for letter, name in zip(
            "abcdef",
            ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"),
            strict=True,
        )
    }
    return {
        "date": MONTH,
        "files": files,
        "lookups": lookups,
        "row_counts": {"cnpjs": 10},
        "source_attestation": {
            "schema": "ficha-parsed-source-receipt-binding-v1",
            "month": MONTH,
            "run_id": RUN,
            "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
            "cnpjs_sha256": files["cnpjs"]["sha256"],
            "raw_upload_status": "complete",
        },
    }


def _prepare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", RUN)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ATTEMPT", raising=False)
    source = tmp_path / "source" / "monthly-estabelecimento-source-bag.json"
    source.parent.mkdir()
    _write_receipt(source, _source())
    descriptor = tmp_path / "descriptor.json"
    descriptor.write_text(json.dumps(_descriptor(source)), encoding="utf-8")
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    for number in range(100):
        prefix = f"{number:02d}"
        (receipts / f"receipt-{prefix}.json").write_text(
            json.dumps(
                {
                    "month": MONTH,
                    "prefix": prefix,
                    "name": f"companies-{prefix}.zip",
                    "size": 42,
                    "sha1": "b" * 40,
                    "materialization_id": "a" * 64,
                }
            ),
            encoding="utf-8",
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"current":"old"}\n')
    args = argparse.Namespace(
        month=MONTH,
        descriptor=descriptor,
        source_receipt=source,
        receipts_dir=receipts,
        manifest=manifest,
        snapshot_entry=tmp_path / "snapshot-entry.json",
        verification=tmp_path / "verification.json",
        attempts=1,
        interval_seconds=0,
    )
    return args


def _deny_remote(monkeypatch: pytest.MonkeyPatch, calls: list[str]):
    def forbidden(*args, **kwargs):
        calls.append("remote")
        raise AssertionError("preflight must precede remote effects")

    monkeypatch.setattr(cli, "wait_for_descriptor_remote", forbidden)
    monkeypatch.setattr(cli, "verify_all_shards", forbidden)
    monkeypatch.setattr(cli, "update_manifest", forbidden)


def _assert_untouched(args):
    assert args.manifest.read_bytes() == b'{"current":"old"}\n'
    assert not args.snapshot_entry.exists()
    assert not args.verification.exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "absent",
        "failed",
        "pending",
        "other_attempt",
        "other_month",
        "digest",
        "raw_pending",
        "missing_identity",
        "missing_source_flag",
    ],
)
def test_invalid_producer_artifact_never_reaches_remote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
):
    args = _prepare(tmp_path, monkeypatch)
    calls: list[str] = []
    _deny_remote(monkeypatch, calls)
    if mutation == "absent":
        args.source_receipt.unlink()
    elif mutation in ("failed", "pending", "other_attempt", "other_month"):
        data = json.loads(args.source_receipt.read_text())
        if mutation == "other_attempt":
            data["run_id"] = "test:finalize:6"
        elif mutation == "other_month":
            data["month"] = "2026-06"
        else:
            data["status"] = mutation
        _write_receipt(args.source_receipt, data)
    elif mutation == "digest":
        data = json.loads(args.descriptor.read_text())
        data["source_attestation"]["receipt_sha256"] = "f" * 64
        args.descriptor.write_text(json.dumps(data), encoding="utf-8")
    elif mutation == "raw_pending":
        data = json.loads(args.descriptor.read_text())
        data["source_attestation"]["raw_upload_status"] = "pending"
        args.descriptor.write_text(json.dumps(data), encoding="utf-8")
    elif mutation == "missing_identity":
        monkeypatch.delenv("FICHA_SOURCE_RUN_ID")
    elif mutation == "missing_source_flag":
        delattr(args, "source_receipt")
    with pytest.raises((ValueError, FileNotFoundError)):
        cli.command_finalize(args)
    assert calls == []
    _assert_untouched(args)


def test_github_attempt_cannot_be_masked_by_override(tmp_path: Path, monkeypatch):
    args = _prepare(tmp_path, monkeypatch)
    calls: list[str] = []
    _deny_remote(monkeypatch, calls)
    monkeypatch.setenv("GITHUB_RUN_ID", "1234")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    with pytest.raises(ValueError, match="cannot override GitHub"):
        cli.command_finalize(args)
    assert calls == []
    monkeypatch.delenv("FICHA_SOURCE_RUN_ID")
    with pytest.raises(ValueError, match="different execution attempt"):
        cli.command_finalize(args)
    assert calls == []
    _assert_untouched(args)


@pytest.mark.parametrize("alias", ["descriptor", "manifest", "shard"])
def test_source_receipt_cannot_alias_inputs_or_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, alias: str
):
    args = _prepare(tmp_path, monkeypatch)
    calls: list[str] = []
    _deny_remote(monkeypatch, calls)
    target = {
        "descriptor": args.descriptor,
        "manifest": args.manifest,
        "shard": args.receipts_dir / "receipt-00.json",
    }[alias]
    args.source_receipt.unlink()
    os.link(target, args.source_receipt)
    with pytest.raises(ValueError, match="aliases"):
        cli.command_finalize(args)
    assert calls == []
    _assert_untouched(args)


def test_positive_all_100_shards_and_failed_remote_still_protected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    args = _prepare(tmp_path, monkeypatch)
    events: list[str] = []
    monkeypatch.setattr(cli, "wait_for_descriptor_remote", lambda *a, **kw: events.append("wait"))
    monkeypatch.setattr(cli, "ShardPackSession", lambda *a: object())

    def verify(*a, **kw):
        events.append("verify")
        return [
            SimpleNamespace(
                prefix=f"{n:02d}",
                name=f"companies-{n:02d}.zip",
                size=42,
                sha1="b" * 40,
                materialization_id="a" * 64,
                action=ShardTransferAction.VERIFIED,
            )
            for n in range(100)
        ]

    monkeypatch.setattr(cli, "verify_all_shards", verify)

    def finalize_entry(descriptor, identities):
        assert len(identities) == 100
        assert len({entry.shard for entry in identities}) == 100
        events.append("entry")
        return {"date": MONTH, "files": {}}

    monkeypatch.setattr(cli, "finalize_sharded_snapshot_entry", finalize_entry)
    monkeypatch.setattr(cli, "update_manifest", lambda *a: events.append("manifest"))
    cli.command_finalize(args)
    assert events == ["wait", "verify", "entry", "manifest"]
    assert args.snapshot_entry.is_file() and args.verification.is_file()
    assert json.loads(args.verification.read_text())["company_shards_verified"] == 100

    events.clear()
    args.snapshot_entry.unlink()
    args.verification.unlink()
    monkeypatch.setattr(
        cli,
        "wait_for_descriptor_remote",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("HTTP failure")),
    )
    with pytest.raises(RuntimeError, match="HTTP failure"):
        cli.command_finalize(args)
    assert events == []
    _assert_untouched(args)


def test_receipt_change_during_remote_check_never_reaches_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    args = _prepare(tmp_path, monkeypatch)
    events: list[str] = []

    def mutate(*_args, **_kwargs):
        events.append("remote")
        source = json.loads(args.source_receipt.read_text())
        source["parts"][0]["parsed_csv_size"] += 1
        _write_receipt(args.source_receipt, source)

    monkeypatch.setattr(cli, "wait_for_descriptor_remote", mutate)
    monkeypatch.setattr(cli, "ShardPackSession", lambda *a: object())
    monkeypatch.setattr(
        cli,
        "verify_all_shards",
        lambda *a, **kw: [
            SimpleNamespace(
                prefix=f"{n:02d}",
                name=f"companies-{n:02d}.zip",
                size=42,
                sha1="b" * 40,
                materialization_id="a" * 64,
                action=ShardTransferAction.VERIFIED,
            )
            for n in range(100)
        ],
    )
    monkeypatch.setattr(cli, "update_manifest", lambda *a: events.append("manifest"))
    with pytest.raises(ValueError, match="receipt changed"):
        cli.command_finalize(args)
    assert events == ["remote"]
    _assert_untouched(args)


def test_real_cli_parser_uses_mandatory_external_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    args = _prepare(tmp_path, monkeypatch)
    events: list[str] = []
    _deny_remote(monkeypatch, events)
    argv = [
        "finalize",
        "--month",
        MONTH,
        "--descriptor",
        str(args.descriptor),
        "--receipts-dir",
        str(args.receipts_dir),
        "--manifest",
        str(args.manifest),
        "--snapshot-entry",
        str(args.snapshot_entry),
        "--verification",
        str(args.verification),
        "--attempts",
        "1",
        "--interval-seconds",
        "0",
    ]
    with pytest.raises(SystemExit) as missing_flag:
        cli.main(argv)
    assert missing_flag.value.code == 2
    args.source_receipt.unlink()
    with pytest.raises(FileNotFoundError):
        cli.main([*argv, "--source-receipt", str(args.source_receipt)])
    assert events == []
    _assert_untouched(args)


def test_workflow_routes_single_file_source_and_shell_errors():
    source = (Path(__file__).parents[2] / ".github/workflows/etl-monthly.yml").read_text()
    producer = source.split("- name: Persist mandatory source-bag attestation", 1)[1].split(
        "- name:", 1
    )[0]
    finalize = source.split("  finalize:", 1)[1]
    assert "if-no-files-found: error" in producer
    assert "monthly-estabelecimento-source-bag.json" in producer
    assert "monthly-estabelecimento-key-gate.json" not in producer
    assert "actions/download-artifact@v4" in finalize
    assert "estabelecimento-source-bag-${{ needs.resolve.outputs.month }}" in finalize
    command = finalize.split(
        "- name: Verify remote bytes against production receipts and build candidate", 1
    )[1].split("- name:", 1)[0]
    assert "shell: bash" in command and "set -euo pipefail" in command
    assert (
        '--source-receipt ".publication/$MONTH/source/monthly-estabelecimento-source-bag.json"'
        in command
    )
    assert "|| true" not in command and "continue-on-error" not in command
    candidate = finalize.split("- name: Persist exact promoted candidate and evidence", 1)[1].split(
        "- name:", 1
    )[0]
    assert "source/monthly-estabelecimento-source-bag.json" in candidate
    assert finalize.index("source/monthly-estabelecimento") < finalize.index(
        "Publish verified manifest"
    )
