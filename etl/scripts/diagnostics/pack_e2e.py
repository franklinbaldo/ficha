"""End-to-end probe for the per-company ZIP layer (companies.zip).

For a target month (defaults to 2026-04), this script:

1. Runs `pack_from_parquets(month, /tmp/companies.zip)` — reads raizes/
   cnpjs/socios from the live IA item and builds the per-company ZIP.
2. Validates the resulting archive locally:
   - Required artifacts present (`_schema.desc`, `_schema.proto`,
     `_meta.json`, 6× `_lookups/{kind}.pb`).
   - Sample-decodes 100 random `.pb` members; each must have a non-zero
     `cnpj_base` and the path it lives at must match `cnpjpath(cnpj_base)`.
3. Optionally uploads to a separate POC item
   (`ficha-poc-companies-{month}`) when `SKIP_UPLOAD != "1"` — keeps the
   production `ficha-{month}` item untouched while we shake the layer out.
4. Writes `/tmp/pack_e2e.json` and prints the same payload. Exits non-zero
   on any validation failure.

Cheap to re-run: the heavy step is `pack_from_parquets`, which IA-side
DuckDB httpfs makes mostly bandwidth-bound (~17 GB of parquets).
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
import zipfile
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Add etl/src so we can import ficha_etl when invoked as
# `uv run python scripts/diagnostics/pack_e2e.py` from the etl/ working dir.
_ETL_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from ficha_etl.pack import (  # noqa: E402
    LOOKUP_KINDS,
    cnpjpath,
    pack_from_parquets,
)
from ficha_etl.proto.ficha.v1.company_pb2 import Company  # noqa: E402
from ficha_etl.upload import upload_companies_zip  # noqa: E402


def _required_members(meta_count: int) -> list[str]:
    members = ["_schema.desc", "_schema.proto", "_meta.json"]
    members.extend(f"_lookups/{k}.pb" for k in LOOKUP_KINDS)
    return members


def validate_zip(zip_path: Path, sample_size: int = 100) -> dict:
    """Open companies.zip and sanity-check its contents.

    Returns a report dict; raises AssertionError on any validation failure.
    """
    report: dict = {"path": str(zip_path), "size_bytes": zip_path.stat().st_size}

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())

        # 1. Required artifacts present
        for m in _required_members(0):
            if m not in names:
                raise AssertionError(f"missing required ZIP member: {m}")
        report["required_members_ok"] = True

        # 2. _meta.json round-trip
        meta = json.loads(zf.read("_meta.json"))
        report["meta"] = meta
        if meta.get("count", 0) <= 0:
            raise AssertionError(f"_meta.json count must be positive, got {meta.get('count')}")

        # 3. Sample-decode random .pb members
        pb_members = [n for n in names if n.endswith(".pb") and not n.startswith("_")]
        if len(pb_members) != meta["count"]:
            raise AssertionError(
                f"member count {len(pb_members)} != _meta.json count {meta['count']}"
            )

        rng = random.Random(42)  # deterministic sample for reproducible diagnostics
        sample = rng.sample(pb_members, min(sample_size, len(pb_members)))
        decoded = 0
        for path in sample:
            company = Company()
            company.ParseFromString(zf.read(path))
            if company.cnpj_base == 0:
                raise AssertionError(f"decoded company has cnpj_base=0 at {path}")
            # Path-derived cnpj_base must match the embedded field.
            expected_path = cnpjpath(company.cnpj_base)
            if expected_path != path:
                raise AssertionError(
                    f"path mismatch: file at {path} encodes cnpj_base={company.cnpj_base} "
                    f"which maps to {expected_path}"
                )
            decoded += 1
        report["sample_decoded"] = decoded
        report["sample_size"] = len(sample)

    return report


def main() -> int:
    month = os.environ.get("MONTH", "2026-04").strip() or "2026-04"
    skip_upload = os.environ.get("SKIP_UPLOAD", "1") != "0"
    sample_size = int(os.environ.get("SAMPLE_SIZE", "100"))
    zip_path = Path(os.environ.get("ZIP_PATH", "/tmp/companies.zip"))

    report: dict = {
        "month": month,
        "skip_upload": skip_upload,
        "zip_path": str(zip_path),
    }

    # ── Pack ─────────────────────────────────────────────────────────────
    log.info("packing companies.zip for %s → %s", month, zip_path)
    t0 = time.monotonic()
    try:
        pack_result = pack_from_parquets(month, zip_path)
    except Exception as exc:
        log.exception("pack_from_parquets failed")
        report["error"] = f"pack_from_parquets: {exc}"
        Path("/tmp/pack_e2e.json").write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps(report, indent=2, default=str))
        print(f"::error::pack_from_parquets failed: {exc}")
        return 1
    report["pack_seconds"] = round(time.monotonic() - t0, 1)
    report["pack"] = pack_result

    # ── Validate ─────────────────────────────────────────────────────────
    log.info("validating ZIP (sample_size=%d)", sample_size)
    try:
        report["validation"] = validate_zip(zip_path, sample_size=sample_size)
    except AssertionError as exc:
        log.error("validation failed: %s", exc)
        report["error"] = f"validation: {exc}"
        Path("/tmp/pack_e2e.json").write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps(report, indent=2, default=str))
        print(f"::error::ZIP validation failed: {exc}")
        return 1

    # ── Upload (optional) ────────────────────────────────────────────────
    if skip_upload:
        report["upload"] = None
        log.info("SKIP_UPLOAD=1 — skipping IA upload")
    else:
        access_key = os.environ.get("IA_ACCESS_KEY", "")
        secret_key = os.environ.get("IA_SECRET_KEY", "")
        if not access_key or not secret_key:
            report["error"] = "missing IA_ACCESS_KEY/IA_SECRET_KEY for upload"
            Path("/tmp/pack_e2e.json").write_text(json.dumps(report, indent=2, default=str))
            print(json.dumps(report, indent=2, default=str))
            print(f"::error::{report['error']}")
            return 1

        identifier = f"ficha-poc-companies-{month}"
        log.info("uploading to test item ia:%s", identifier)
        t0 = time.monotonic()
        try:
            upload_companies_zip(
                month,
                zip_path,
                access_key=access_key,
                secret_key=secret_key,
                identifier_override=identifier,
            )
        except Exception as exc:
            log.exception("upload failed")
            report["error"] = f"upload: {exc}"
            Path("/tmp/pack_e2e.json").write_text(json.dumps(report, indent=2, default=str))
            print(json.dumps(report, indent=2, default=str))
            print(f"::error::upload failed: {exc}")
            return 1
        report["upload"] = {
            "identifier": identifier,
            "url": f"https://archive.org/download/{identifier}/companies.zip",
            "seconds": round(time.monotonic() - t0, 1),
        }

    # ── Output ───────────────────────────────────────────────────────────
    # Persist the .pb path list so companies_zip_latency.py can sample it
    # without re-opening the ZIP. Capped at 1000 paths (~80 KB) to keep
    # this artifact small.
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            pb_paths = [n for n in zf.namelist() if n.endswith(".pb") and not n.startswith("_")]
        rng = random.Random(42)
        sampled_paths = rng.sample(pb_paths, min(1000, len(pb_paths)))
        Path("/tmp/pack_e2e_paths.json").write_text(json.dumps(sorted(sampled_paths)))
        report["paths_artifact"] = "/tmp/pack_e2e_paths.json"
        report["paths_count"] = len(sampled_paths)
    except Exception as exc:
        log.warning("failed to write paths artifact: %s", exc)

    Path("/tmp/pack_e2e.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    print("::notice::pack_e2e passed — companies.zip is well-formed and decodable")
    return 0


if __name__ == "__main__":
    sys.exit(main())
