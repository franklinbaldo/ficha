"""Probe published FICHA Parquets as inputs for convenience views.

No output of this script is a canonical dataset. It inspects the public snapshot
contract and records enough evidence to decide which small DuckDB views are
worth naming and testing next.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("web/public/manifest.json"))
    parser.add_argument("--month")
    parser.add_argument("--output", type=Path, default=Path("experiments/derived-views/report.json"))
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text())
    month = args.month or manifest["current"]
    snapshot = next(s for s in manifest["snapshots"] if s["date"] == month)

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")

    report = {"month": month, "files": {}}
    for name, entry in snapshot["files"].items():
        if name in {"lookups", "companies"} or not isinstance(entry, dict) or "url" not in entry:
            continue
        url = entry["url"].replace("'", "''")
        started = time.monotonic()
        schema = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{url}')"
        ).fetchall()
        count = con.execute(
            f"SELECT count(*) FROM read_parquet('{url}')"
        ).fetchone()[0]
        report["files"][name] = {
            "url": entry["url"],
            "declared_rows": snapshot.get("row_counts", {}).get(name),
            "observed_rows": count,
            "schema": [
                {"name": row[0], "type": row[1], "null": row[2]}
                for row in schema
            ],
            "metadata_probe_seconds": round(time.monotonic() - started, 3),
        }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
