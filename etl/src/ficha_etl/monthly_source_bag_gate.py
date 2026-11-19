"""Exact multiset gate for ten parsed RFB estabelecimento parts.

The source witness is DuckDB's estabelecimento table reloaded from the ten
extracted CSVs, independent of the writer's cnpjs.parquet. This protects
against loss/replacement after parsing, NOT authenticated ZIP custody.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Mapping, Sequence

import duckdb

from .sources import is_valid_month

EXPECTED_PARTS = tuple(f"Estabelecimentos{i}.zip" for i in range(10))
_SOURCE_KEYS = "SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj FROM estabelecimento"


class MonthlySourceBagError(RuntimeError):
    """The parsed source multiset and the produced CNPJ multiset disagree."""


def _write_receipt(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    filename: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".source-bag-",
            suffix=".json",
            delete=False,
        ) as stream:
            filename = stream.name
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
        os.replace(filename, path)
    finally:
        if filename is not None:
            Path(filename).unlink(missing_ok=True)


def reconcile_loaded_source_keys(
    con: duckdb.DuckDBPyConnection,
    month: str,
    parts: Sequence[tuple[str, Path]],
    cnpjs_path: Path,
    receipt_path: Path,
    parsed_part_counts: Mapping[Path, int],
) -> dict:
    """Compare key multiplicities using the production CSV parser.

    The caller must invoke this before deleting extracted CSVs and before
    any remote publication. No independent Python CSV dialect is assumed.
    """
    if not is_valid_month(month):
        raise ValueError(f"invalid snapshot month: {month!r}")

    payload: dict = {
        "schema": "ficha-parsed-rfb-key-multiset-v1",
        "month": month,
        "identity_level": "parsed-duckdb-source-not-zip-custody",
        "status": "failed",
        "parts": [],
    }
    names = [name for name, _ in parts]
    paths = [Path(path) for _, path in parts]
    if (
        len(parts) != 10
        or set(names) != set(EXPECTED_PARTS)
        or len(set(names)) != 10
        or len({p.resolve() for p in paths}) != 10
        or any(
            p.parent.name != name or not p.is_file() or p.stat().st_size <= 0
            for (name, _), p in zip(parts, paths, strict=True)
        )
    ):
        payload["reason"] = "missing, duplicate, empty or noncanonical source part"
        _write_receipt(receipt_path, payload)
        raise MonthlySourceBagError(payload["reason"])

    # Source rows must be counted in the same DuckDB load that produced the
    # union table. Bytes > 0 are not evidence of a logical CSV record.
    # Refuse missing/aliased counters rather than silently skipping a part.
    expected_paths = {p.resolve() for p in paths}
    counts = {Path(path).resolve(): value for path, value in parsed_part_counts.items()}
    if set(counts) != expected_paths or any(
        type(value) is not int or value <= 0 for value in counts.values()
    ):
        payload["reason"] = "missing or zero logical rows in an RFB source part"
        _write_receipt(receipt_path, payload)
        raise MonthlySourceBagError(payload["reason"])

    payload["parts"] = [
        {
            "zip_name": name,
            "parsed_csv_size": p.stat().st_size,
            "parsed_csv_rows": counts[p.resolve()],
        }
        for (name, _), p in zip(parts, paths, strict=True)
    ]
    src_rows = con.execute(f"SELECT COUNT(*) FROM ({_SOURCE_KEYS})").fetchone()[0]
    if sum(counts.values()) != src_rows:
        payload["reason"] = "per-part logical row totals differ from loaded source table"
        payload["parsed_source_rows"] = int(src_rows)
        _write_receipt(receipt_path, payload)
        raise MonthlySourceBagError(payload["reason"])
    dst_rows = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(cnpjs_path)]).fetchone()[0]
    payload["parsed_source_rows"] = int(src_rows)
    payload["produced_rows"] = int(dst_rows)
    if src_rows == 0 or dst_rows != src_rows:
        payload["reason"] = "empty source or different source/output cardinality"
        _write_receipt(receipt_path, payload)
        raise MonthlySourceBagError(payload["reason"])

    # EXCEPT ALL preserves multiplicity, unlike distinct or aggregate count.
    # LIMIT 1 is enough to reject without exposing individual CNPJ values.
    missing = con.execute(
        f"SELECT cnpj FROM ({_SOURCE_KEYS} EXCEPT ALL SELECT cnpj FROM read_parquet(?)) LIMIT 1",
        [str(cnpjs_path)],
    ).fetchone()
    unexpected = con.execute(
        f"SELECT cnpj FROM (SELECT cnpj FROM read_parquet(?) EXCEPT ALL {_SOURCE_KEYS}) LIMIT 1",
        [str(cnpjs_path)],
    ).fetchone()
    payload["source_only_key_exists"] = missing is not None
    payload["output_only_key_exists"] = unexpected is not None
    if missing or unexpected:
        payload["reason"] = "CNPJ multiset mismatch, including equal-cardinality replacements"
        _write_receipt(receipt_path, payload)
        raise MonthlySourceBagError(payload["reason"])

    payload["status"] = "ok"
    _write_receipt(receipt_path, payload)
    return payload
