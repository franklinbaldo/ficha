"""Raw monthly publication must HOLD without externally verified S/R/F (#433).

Real CLI authorization is exercised with synthetic artifacts. No RFB, IA or
manifest publication happens. A fake verifier tests API shape only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ficha_etl import monthly_sharded as cli
from ficha_etl import upload
from ficha_etl.monthly_raw_custody_authorization import (
    RAW_CONFIRMED,
    RAW_CUSTODY_SCHEMA,
    _require_raw_custody_authorization,
)
from ficha_etl.monthly_source_bag_gate import EXPECTED_PARTS, _write_receipt
from ficha_etl.sources import canonical_inventory

MONTH = "2026-07"
RUN = "castor:run-17"


def _parsed_source() -> dict:
    return {
        "schema": "ficha-parsed-rfb-key-multiset-v1",
        "month": MONTH,
        "status": "ok",
        "identity_level": "parsed-duckdb-source-not-zip-custody",
        "parts": [
            {"zip_name": name, "parsed_csv_size": 100, "parsed_csv_rows": 1}
            for name in EXPECTED_PARTS
        ],
        "parsed_source_rows": 10,
        "produced_rows": 10,
        "source_only_key_exists": False,
        "output_only_key_exists": False,
    }


def _identity() -> dict:
    return {"size": 10, "sha1": "1" * 40, "sha256": "1" * 64}


def _descriptor() -> dict:
    return {
        "date": MONTH,
        "files": {kind: _identity() for kind in cli._STANDARD_FILE_NAMES},
        "lookups": {
            kind: _identity()
            for kind in ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")
        },
        "row_counts": {"cnpjs": 10},
    }


def _claimed_raw(status: str = RAW_CONFIRMED) -> dict:
    return {
        "schema": RAW_CUSTODY_SCHEMA,
        "status": status,
        "month": MONTH,
        "run_id": RUN,
        "inventory_count": 37,
        "pin_set_sha256": "b" * 64,
        "cohort_witness_ref": "fake:S",
        "remote_bytes_witness_ref": "fake:R",
        "fence_witness_ref": "fake:F",
        "authority_claim_ref": "fake:authority",
    }


def _prepared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path, dict]:
    monkeypatch.setenv("FICHA_SOURCE_RUN_ID", RUN)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ATTEMPT", raising=False)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    source = cli._source_receipt_path(output_dir)
    source.parent.mkdir(parents=True, exist_ok=True)
    _write_receipt(source, _parsed_source())
    descriptor = _descriptor()
    cli._bind_source_receipt(MONTH, output_dir, descriptor)
    # Simulate a previously saved, allegedly successful v1 artifact.
    descriptor["source_attestation"]["raw_upload_status"] = "complete"
    path = tmp_path / "descriptor.json"
    path.write_text(json.dumps(descriptor), encoding="utf-8")
    return output_dir, source, path, descriptor


def _no_external_effects(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    def forbidden(*_args, **_kwargs):
        seen.append("external")
        raise AssertionError("unproven raw custody reached external effect")

    monkeypatch.setattr(cli, "_require_safe_legacy_shard_upload", lambda: None)
    for name in (
        "_require_secret",
        "submit_outputs_fail_closed",
        "wait_for_descriptor_remote",
        "verify_all_shards",
        "update_manifest",
    ):
        monkeypatch.setattr(cli, name, forbidden)
    return seen


@pytest.mark.parametrize("entrypoint", ["upload", "finalize"])
@pytest.mark.parametrize(
    "custody",
    [
        "legacy-v1-complete",
        "37-catalog-names-only",
        "SENT_UNKNOWN",
        "READBACK_MATCH_NOT_FINAL",
        "self-claimed-v2-confirmed",
    ],
)
def test_raw_unproven_descriptor_never_submits_or_promotes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entrypoint: str, custody: str
) -> None:
    output, source, path, descriptor = _prepared(tmp_path, monkeypatch)
    if custody == "37-catalog-names-only":
        descriptor["raw_custody"] = {"catalog_names": 37, "catalog_size_positive": True}
    elif custody != "legacy-v1-complete":
        descriptor["raw_custody"] = _claimed_raw(
            RAW_CONFIRMED if custody == "self-claimed-v2-confirmed" else custody
        )
    path.write_text(json.dumps(descriptor), encoding="utf-8")
    observed = _no_external_effects(monkeypatch)
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"current":"old"}\n')
    args = argparse.Namespace(
        month=MONTH,
        descriptor=path,
        output_dir=output,
        source_receipt=source,
        receipts_dir=tmp_path / "receipts",
        snapshot_entry=tmp_path / "entry.json",
        verification=tmp_path / "verification.json",
        manifest=manifest,
        attempts=1,
        interval_seconds=0,
    )
    with pytest.raises(ValueError, match="raw custody v2"):
        if entrypoint == "upload":
            cli.command_upload_outputs(args)
        else:
            cli.command_finalize(args)
    assert observed == []
    assert manifest.read_bytes() == b'{"current":"old"}\n'
    assert not args.snapshot_entry.exists()
    assert not args.verification.exists()


@pytest.mark.parametrize("state", ["pending", "unknown", "skipped_existing", "sent_unverified"])
def test_raw_transport_state_never_becomes_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    out, _source, path, descriptor = _prepared(tmp_path, monkeypatch)
    descriptor["source_attestation"]["raw_upload_status"] = state
    descriptor["raw_custody"] = _claimed_raw()
    path.write_text(json.dumps(descriptor), encoding="utf-8")
    observed = _no_external_effects(monkeypatch)
    with pytest.raises(ValueError, match="raw source upload did not complete"):
        cli.command_upload_outputs(
            argparse.Namespace(month=MONTH, descriptor=path, output_dir=out)
        )
    assert observed == []


def test_raw_catalog_complete_returns_unproven_not_verified(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    names = {f"raw/{spec.name}" for spec in canonical_inventory()}
    assert len(names) == 37
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda *_a: names)
    monkeypatch.setattr(
        upload.upstream, "discover_token", lambda: pytest.fail("upstream GET forbidden")
    )
    monkeypatch.setattr(upload, "_ia_s3_put", lambda *_a, **_kw: pytest.fail("PUT forbidden"))
    monkeypatch.setattr(upload.raw_preflight, "stage_all", lambda *_a: pytest.fail("spool forbidden"))
    assert (
        upload.stream_raw_zips_to_ia(MONTH, access_key="test", secret_key="test")
        is upload.RawStreamOutcome.ALL_NAMES_PRESENT_UNPROVEN
    )


def test_forged_v2_references_need_independent_monotonic_authority() -> None:
    descriptor = {
        "source_attestation": {"run_id": RUN, "raw_upload_status": "complete"},
        "raw_custody": _claimed_raw(),
    }
    with pytest.raises(ValueError, match="independent monotonic authority unavailable"):
        _require_raw_custody_authorization(MONTH, descriptor)

    class FakeAuthority:
        def confirm(self, *, month: str, run_id: str, claims: dict) -> bool:
            return month == MONTH and run_id == RUN and claims == descriptor["raw_custody"]

    # This proves only the dependency-injection interface; it is NOT a real S/R/F proof.
    _require_raw_custody_authorization(MONTH, descriptor, authority=FakeAuthority())

    class Denial:
        def confirm(self, **_kwargs) -> bool:
            return False

    with pytest.raises(ValueError, match="did not confirm"):
        _require_raw_custody_authorization(MONTH, descriptor, authority=Denial())


def test_raw_gate_cannot_be_optimized_away_with_python_o() -> None:
    snippet = (
        "from ficha_etl.monthly_raw_custody_authorization import "
        "_require_raw_custody_authorization as gate\n"
        "try:\n"
        "    gate('2026-07', {'source_attestation': {'run_id': 'x'}})\n"
        "except ValueError:\n"
        "    pass\n"
        "else:\n"
        "    raise SystemExit('FAIL: accepted unproven v1')\n"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", snippet], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
