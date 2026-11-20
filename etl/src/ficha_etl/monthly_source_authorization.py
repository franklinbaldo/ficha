"""Bind parsed-source receipts to the monthly upload entrypoints.

These receipts certify parsed DuckDB key equivalence only; they do not
authenticate RFB ZIP bytes, freeze mutable files, or pin raw upload retries.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

from .monthly_source_bag_gate import EXPECTED_PARTS, _write_receipt

_SOURCE_RECEIPT = "monthly-estabelecimento-source-bag.json"
_SOURCE_SCHEMA = "ficha-parsed-rfb-key-multiset-v1"


def _source_receipt_path(output_dir: Path) -> Path:
    return output_dir.parent / "evidence" / _SOURCE_RECEIPT


def _expected_source_run_id() -> str | None:
    """A GitHub retry is a different production attempt, never a silent replay."""
    if "FICHA_SOURCE_RUN_ID" in os.environ:
        value = os.environ["FICHA_SOURCE_RUN_ID"]
        if not value or len(value) > 128:
            raise ValueError("invalid FICHA_SOURCE_RUN_ID")
        return value
    job = os.environ.get("GITHUB_RUN_ID")
    if job:
        attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        if (
            not job.isascii()
            or not job.isdecimal()
            or not attempt.isascii()
            or not attempt.isdecimal()
        ):
            raise ValueError("invalid GITHUB_RUN_ID/GITHUB_RUN_ATTEMPT")
        return f"github:{job}:{attempt}"
    return None


def _source_receipt_bytes(path: Path, month: str, run_id: str) -> bytes:
    """Validate a parsed-source gate receipt, NOT RFB ZIP authentication."""
    raw = path.read_bytes()
    data = json.loads(raw)
    if not isinstance(data, dict) or (
        data.get("schema") != _SOURCE_SCHEMA
        or data.get("month") != month
        or data.get("status") != "ok"
        or data.get("run_id") != run_id
        or data.get("identity_level") != "parsed-duckdb-source-not-zip-custody"
    ):
        raise ValueError("monthly parsed-source receipt is absent, failed or from another run")
    parts = data.get("parts")
    if not isinstance(parts, list) or len(parts) != len(EXPECTED_PARTS):
        raise ValueError("monthly parsed-source receipt has incomplete inventory")
    names = [p.get("zip_name") for p in parts if isinstance(p, dict)]
    if len(names) != 10 or set(names) != set(EXPECTED_PARTS) or len(set(names)) != 10:
        raise ValueError("monthly parsed-source receipt has noncanonical parts")
    counts = [p.get("parsed_csv_rows") for p in parts]
    sizes = [p.get("parsed_csv_size") for p in parts]
    if any(type(n) is not int or n <= 0 for n in counts + sizes):
        raise ValueError("monthly parsed-source receipt has invalid sizes or rows")
    total = sum(counts)
    if (
        type(data.get("parsed_source_rows")) is not int
        or data["parsed_source_rows"] != total
        or data.get("produced_rows") != total
        or data.get("source_only_key_exists") is not False
        or data.get("output_only_key_exists") is not False
    ):
        raise ValueError("monthly parsed-source receipt rows disagree")
    return raw


def _bind_source_receipt(month: str, output_dir: Path, descriptor: dict) -> None:
    """Persist a run identifier and bind one exact source receipt to descriptor."""
    path = _source_receipt_path(output_dir)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("month") != month or data.get("status") != "ok":
        raise ValueError("no successful source receipt after transform")
    run_id = _expected_source_run_id() or f"local:{uuid.uuid4().hex}"
    data["run_id"] = run_id
    _write_receipt(path, data)
    raw = _source_receipt_bytes(path, month, run_id)
    if descriptor["row_counts"]["cnpjs"] != data["produced_rows"]:
        raise ValueError("source rows disagree with production descriptor")
    descriptor["source_attestation"] = {
        "schema": "ficha-parsed-source-receipt-binding-v1",
        "month": month,
        "run_id": run_id,
        "receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "cnpjs_sha256": descriptor["files"]["cnpjs"]["sha256"],
    }


def _require_source_attestation(month: str, output_dir: Path, descriptor: dict) -> None:
    """Refuse old/stale/failed source receipts BEFORE the upload entrypoint."""
    entry = descriptor.get("source_attestation")
    if not isinstance(entry, dict) or (
        entry.get("schema") != "ficha-parsed-source-receipt-binding-v1"
        or entry.get("month") != month
        or not isinstance(entry.get("run_id"), str)
        or not entry["run_id"]
    ):
        raise ValueError("production descriptor lacks matching source attestation")
    run_id = entry["run_id"]
    expected_run_id = _expected_source_run_id()
    if expected_run_id is not None and run_id != expected_run_id:
        raise ValueError("source attestation belongs to a different execution attempt")
    raw = _source_receipt_bytes(_source_receipt_path(output_dir), month, run_id)
    if entry.get("receipt_sha256") != hashlib.sha256(raw).hexdigest():
        raise ValueError("source receipt changed after descriptor production")
    if entry.get("cnpjs_sha256") != descriptor["files"]["cnpjs"]["sha256"]:
        raise ValueError("source attestation refers to different CNPJ output bytes")
    counts = descriptor.get("row_counts")
    if not isinstance(counts, dict) or counts.get("cnpjs") != json.loads(raw)["produced_rows"]:
        raise ValueError("source receipt rows disagree with descriptor")
