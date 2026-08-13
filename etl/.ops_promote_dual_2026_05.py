"""Read-only retrofit/promotion candidate for Ficha 2026-05 (#110).

This is operational scaffolding and is never merged. It performs no remote
writes: all Internet Archive access is metadata/GET/HEAD only.

2026-05 predates the production-time dual-hash descriptor. Therefore this
one-off operation reconstructs the descriptor from the durable bytes exactly
once with current `build_snapshot_entry()`: standard files and lookup parquets
receive size+sha1+sha256; companies shards retain size+sha1 plus the separately
verified MaterializationSpec identity.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from ficha_etl.manifest import (
    CompanyShardIdentity,
    build_snapshot_entry,
    update_manifest,
    verify_snapshot_files,
)
from ficha_etl.remote_reuse import fetch_item_metadata
from ficha_etl.shard_publish import pin_materialization_inputs
from ficha_etl.shard_remote import PUBLIC_COMPANIES_GEOMETRY, fetch_remote_shard_meta
from ficha_etl.shard_transfer import ShardTransferAction, verify_all_shards
from ficha_etl.sharded_pack import ShardPackSession

MONTH = "2026-05"
OUTPUT_DIR = Path(".cache") / MONTH / "output"
MANIFEST_PATH = Path("..") / "web" / "public" / "manifest.json"
SHARDS_PATH = Path("company-shards-verified.json")
SUMMARY_PATH = Path("promotion-summary-dual.json")


def _metadata() -> dict | None:
    return fetch_item_metadata(MONTH)


def _fetch_meta_once(name: str) -> object | None:
    return fetch_remote_shard_meta(MONTH, name, attempts=1)


def verify_shards() -> None:
    metadata = _metadata()
    if metadata is None:
        raise RuntimeError("Internet Archive metadata unavailable")

    pinned = pin_materialization_inputs(lambda: metadata)
    session = ShardPackSession(MONTH, PUBLIC_COMPANIES_GEOMETRY)
    results = verify_all_shards(
        session,
        pinned_inputs=pinned,
        fetch_metadata=lambda: metadata,
        fetch_meta=_fetch_meta_once,
    )

    rows: list[dict] = []
    for result in results:
        if result.action is not ShardTransferAction.VERIFIED:
            raise RuntimeError(f"{result.name}: non-final action {result.action}")
        rows.append(
            {
                "shard": result.prefix,
                "name": result.name,
                "materialization_id": result.materialization_id,
                "size": result.size,
                "sha1": result.sha1,
            }
        )

    expected = list(PUBLIC_COMPANIES_GEOMETRY.prefixes())
    prefixes = [row["shard"] for row in rows]
    if prefixes != expected:
        raise RuntimeError("company shard evidence is not exactly ordered 00..99")

    SHARDS_PATH.write_text(
        json.dumps(
            {
                "month": MONTH,
                "pinned_inputs": dict(sorted(pinned.items())),
                "shards": rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print("100/100 company shards verified", flush=True)


def _load_shards() -> list[CompanyShardIdentity]:
    payload = json.loads(SHARDS_PATH.read_text(encoding="utf-8"))
    if payload.get("month") != MONTH:
        raise RuntimeError("wrong shard evidence month")
    rows = payload.get("shards")
    if not isinstance(rows, list) or len(rows) != PUBLIC_COMPANIES_GEOMETRY.count:
        raise RuntimeError("expected exactly 100 shard evidence rows")
    return [
        CompanyShardIdentity(
            shard=row["shard"],
            name=row["name"],
            size=int(row["size"]),
            sha1=row["sha1"],
        )
        for row in rows
    ]


def _valid_hex(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(ch in "0123456789abcdef" for ch in value)
    )


def _assert_dual_file(label: str, entry: object) -> None:
    if not isinstance(entry, dict):
        raise RuntimeError(f"{label}: not an object")
    if int(entry.get("size", 0)) <= 0:
        raise RuntimeError(f"{label}: invalid size")
    if not _valid_hex(entry.get("sha1"), 40):
        raise RuntimeError(f"{label}: invalid sha1")
    if not _valid_hex(entry.get("sha256"), 64):
        raise RuntimeError(f"{label}: invalid sha256")


def promote() -> None:
    base = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if "2026-04" not in {s["date"] for s in base.get("snapshots", [])}:
        raise RuntimeError("base manifest must preserve public 2026-04")

    entry = build_snapshot_entry(MONTH, OUTPUT_DIR, company_shards=_load_shards())

    for name, file_entry in entry["files"].items():
        if name == "companies":
            continue
        _assert_dual_file(f"files.{name}", file_entry)

    lookup_entries = entry.get("lookups")
    if not isinstance(lookup_entries, dict) or len(lookup_entries) != 6:
        raise RuntimeError("expected exactly six lookup parquet entries")
    for name, lookup_entry in lookup_entries.items():
        _assert_dual_file(f"lookups.{name}", lookup_entry)

    companies = entry["files"].get("companies")
    if not isinstance(companies, dict):
        raise RuntimeError("missing sharded companies entry")
    shards = companies.get("shards")
    if not isinstance(shards, list) or len(shards) != 100:
        raise RuntimeError("manifest candidate must contain exactly 100 company shards")
    if "companies_zip" in entry["files"]:
        raise RuntimeError("sharded candidate must not contain monolithic companies_zip")
    for index, shard in enumerate(shards):
        expected = f"{index:02d}"
        if shard.get("shard") != expected:
            raise RuntimeError(f"company shard order mismatch at {expected}")
        if int(shard.get("size", 0)) <= 0 or not _valid_hex(shard.get("sha1"), 40):
            raise RuntimeError(f"companies shard {expected}: invalid identity")

    broken = verify_snapshot_files(entry)
    if broken:
        raise RuntimeError("candidate URLs failed HEAD/size verification:\n" + "\n".join(broken))

    update_manifest(MANIFEST_PATH, entry)
    final = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    dates = [snapshot["date"] for snapshot in final.get("snapshots", [])]
    if final.get("current") != MONTH or "2026-04" not in dates or MONTH not in dates:
        raise RuntimeError(f"bad final manifest current/dates: {final.get('current')} {dates}")

    summary = {
        "month": MONTH,
        "current": final["current"],
        "snapshot_dates": dates,
        "company_shards": 100,
        "row_counts": entry["row_counts"],
        "cnpjs": entry["files"]["cnpjs"],
        "dual_hash_files": sorted(name for name in entry["files"] if name != "companies"),
        "dual_hash_lookups": sorted(lookup_entries),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--verify-shards", action="store_true")
    mode.add_argument("--promote", action="store_true")
    args = parser.parse_args()
    if args.verify_shards:
        verify_shards()
    else:
        promote()


if __name__ == "__main__":
    main()
