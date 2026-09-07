"""Fail-closed monthly uniqueness gate for ``estabelecimento`` (issue #242).

The monthly producer has already consumed the ten RFB ``EstabelecimentosN.zip``
parts when this gate runs.  Instead of downloading or parsing those sources a
second time, the gate reads the exact ``cnpjs.parquet`` produced by that run.
The published ``cnpj`` column is the lossless fixed-width concatenation
``cnpj_basico(8) + cnpj_ordem(4) + cnpj_dv(2)`` used by the production writer.

This module deliberately does *not* define another duplicate algorithm.  It
adapts the published 14-digit key back to the three columns expected by
``estabelecimento_key_audit.run_global_key_audit`` and delegates all blank-key,
duplicate-key and evidence-sample semantics to that existing audit engine.
Therefore a duplicate originating in two different source ZIP parts remains two
identical full keys in the produced parquet and fails here before any derived
output upload or manifest promotion.

Cost: one sequential scan of the already-produced ``cnpjs.parquet``, one
key-only ZSTD parquet intermediate, and the existing hash GROUP BY over that
projection.  The evidence receipt records the exact input checksum and
intermediate size.  No network I/O is performed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import duckdb

from . import canonical_history
from . import estabelecimento_key_audit as key_audit
from . import registry
from .sources import is_valid_month


class MonthlyEstabelecimentoKeyGateError(RuntimeError):
    """The produced monthly bytes do not satisfy estabelecimento key uniqueness."""


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _project_production_keys(
    con: duckdb.DuckDBPyConnection,
    cnpjs_path: Path,
    output_path: Path,
) -> tuple[int, int]:
    """Project the published CNPJ key into the audit engine's canonical columns.

    Returns ``(source_rows, invalid_cnpj_rows)``.  The component names come
    from the existing audit module, so this adapter cannot silently drift to a
    second key vocabulary.
    """
    if not cnpjs_path.is_file():
        raise FileNotFoundError(cnpjs_path)

    source = _literal(str(cnpjs_path))
    source_rows, invalid_rows = con.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT,
            COUNT(*) FILTER (
                WHERE cnpj IS NULL
                   OR length(cnpj) <> 14
                   OR NOT regexp_full_match(cnpj, '[0-9]{{14}}')
            )::BIGINT
        FROM read_parquet({source})
        """
    ).fetchone()

    # These names are intentionally imported from the established audit
    # mechanism rather than repeated here as a second key definition.
    basico, ordem, dv = key_audit._KEY_COLUMNS  # noqa: SLF001
    q_basico = registry.quote_identifier(basico)
    q_ordem = registry.quote_identifier(ordem)
    q_dv = registry.quote_identifier(dv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output = _literal(str(output_path))
    source_label = _literal(cnpjs_path.name)
    con.execute(
        f"""
        COPY (
            SELECT
                substr(cnpj, 1, 8) AS {q_basico},
                substr(cnpj, 9, 4) AS {q_ordem},
                substr(cnpj, 13, 2) AS {q_dv},
                {source_label} AS "_source_file"
            FROM read_parquet({source})
        ) TO {output} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return int(source_rows), int(invalid_rows)


def run_monthly_gate(
    month: str,
    cnpjs_path: Path,
    evidence_path: Path,
    work_dir: Path,
) -> dict[str, Any]:
    """Audit produced monthly bytes and raise before publication on any violation."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    cnpjs_path = cnpjs_path.resolve()
    evidence_path = evidence_path.resolve()
    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    key_projection = work_dir / "estabelecimento-production-keys.parquet"
    database = work_dir / "monthly-estabelecimento-key-gate.duckdb"
    temp = work_dir / "duckdb_tmp"

    con = key_audit._connection(database, temp)  # noqa: SLF001
    try:
        source_rows, invalid_cnpj_rows = _project_production_keys(con, cnpjs_path, key_projection)
        audit = key_audit.run_global_key_audit(con, [key_projection])
    finally:
        con.close()
        database.unlink(missing_ok=True)
        database.with_suffix(".duckdb.wal").unlink(missing_ok=True)
        shutil.rmtree(temp, ignore_errors=True)

    input_identity = canonical_history._checked_file(cnpjs_path)  # noqa: SLF001
    projection_identity = canonical_history._checked_file(key_projection)  # noqa: SLF001
    blanks = sum(audit.blank_or_null_counts_by_component.values())
    violations: list[str] = []
    if invalid_cnpj_rows:
        violations.append(f"{invalid_cnpj_rows} malformed published CNPJ row(s)")
    if audit.total_rows_scanned != source_rows:
        violations.append(
            "key projection row count diverged from produced cnpjs.parquet: "
            f"{audit.total_rows_scanned} != {source_rows}"
        )
    if blanks:
        violations.append(f"{blanks} blank/null canonical key component occurrence(s)")
    if audit.duplicate_key_count:
        violations.append(
            f"{audit.duplicate_key_count} duplicate full estabelecimento key(s), "
            f"{audit.excess_duplicate_row_count} excess row(s)"
        )

    payload: dict[str, Any] = {
        "format_version": 1,
        "status": "failed" if violations else "ok",
        "snapshot_month": month,
        "input": {
            "kind": "produced-cnpjs-parquet",
            **input_identity,
        },
        "adapter": {
            "published_key": "cnpj",
            "fixed_width_components": [8, 4, 2],
            "canonical_columns_source": "estabelecimento_key_audit._KEY_COLUMNS",
            "audit_engine": "estabelecimento_key_audit.run_global_key_audit",
            "network_reads": 0,
        },
        "intermediate": {
            "kind": "key-only-zstd-parquet",
            **projection_identity,
            "lifecycle": "job-local evidence input; upload is not required for promotion",
        },
        "source_rows": source_rows,
        "invalid_cnpj_rows": invalid_cnpj_rows,
        "audit": asdict(audit),
        "violations": violations,
    }
    canonical_history._write_json_atomic(evidence_path, payload)  # noqa: SLF001

    if violations:
        raise MonthlyEstabelecimentoKeyGateError("; ".join(violations))
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--cnpjs", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        payload = run_monthly_gate(args.month, args.cnpjs, args.evidence, args.work_dir)
    except (FileNotFoundError, OSError, ValueError, duckdb.Error, MonthlyEstabelecimentoKeyGateError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    audit = payload["audit"]
    print(
        "monthly estabelecimento key gate OK — "
        f"{audit['distinct_valid_full_keys']:,} unique full keys over "
        f"{payload['source_rows']:,} produced rows"
    )
    print(f"evidence: {args.evidence}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "MonthlyEstabelecimentoKeyGateError",
    "main",
    "run_monthly_gate",
]
