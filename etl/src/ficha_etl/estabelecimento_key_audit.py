"""Cross-part uniqueness audit for estabelecimento's full CNPJ key (issue #100).

PR #101 found that ``ESTABELECIMENTO_CANONICAL.source_cardinality="unique"``
(the full key ``cnpj_basico + cnpj_ordem + cnpj_dv``) was never actually
verified against a real snapshot -- nothing in the pipeline can detect a key
duplicated ACROSS ``estabelecimento``'s ten ``EstabelecimentosN.zip`` parts:

- ``canonical_shadow.py``'s fail-closed duplicate gate only ever sees one
  part at a time (``_create_table_from_csvs`` is called with a single CSV).
- The legacy loader combines all ten parts into one table but never queries
  it for duplicates (``_dedupe_cnpj_basico_table`` only runs for
  empresa/simples).
- ``assert_roundtrip``'s row-count comparison wouldn't catch it either: a
  key duplicated across two parts inflates both sides equally, since each
  part is written to ``cnpjs.parquet`` independently.

This module is evidence-only. It measures; it does not decide a
duplicate-resolution policy, does not change ``ESTABELECIMENTO_CANONICAL``,
``canonical_shadow.py``, or the monthly pipeline.

Design, per issue #100:

- one ZIP processed at a time, extracted CSV deleted before the next --
  never all ten CSVs live simultaneously (~65GB across ten parts);
- the real registry-backed reader (``transform._create_table_from_csvs``
  with ``registry.main_table("estabelecimento").source``), not a
  simplified parser -- same semantics the production loader and the
  canonical shadow writer both already rely on;
- projects only the three key columns + ``_source_file`` lineage into a
  small key-only Parquet checkpoint per part (no stable row ordinal: the
  production connection profile sets ``preserve_insertion_order=false``,
  so a ``row_number()`` assigned after load would not reliably reflect
  source file order -- not worth the risk for an optional field);
- reuses ``canonical_history``'s download/extract/checksum/atomic-write
  helpers (same checkpoint discipline as PRs #85/#88/#90) instead of
  duplicating them -- this is a sibling orchestration, not a shadow-writer
  variant, so it does NOT reuse ``canonical_shadow.run_shadow_part`` itself
  (that writes the full canonical projection and fails closed on
  within-part duplicates, which is the wrong shape for a tool whose whole
  point is to measure duplicates without refusing to record them);
- the global cross-part aggregation reads only the ten small key-only
  Parquets together (never the ~65GB of raw CSVs at once) via a hash
  GROUP BY, not a full sort;
- restartable per part (source ZIP checksum + code fingerprint + output
  checksum must all match to reuse a checkpoint); the global aggregation
  itself is NOT separately checkpointed -- it is cheap enough (a GROUP BY
  over ten small Parquets, not the raw data) to always recompute once all
  ten part checkpoints are confirmed fresh, which keeps this module
  simpler than duplicating canonical_history's full manifest-matching
  logic for a second, differently-shaped report.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import uuid
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import httpx

from . import canonical_history, metrics, registry, transform
from .sources import is_valid_month

log = logging.getLogger(__name__)

_RAW_TABLE = "_raw_estabelecimento_key_audit"
_KEY_COLUMNS = ("cnpj_basico", "cnpj_ordem", "cnpj_dv")
_FORMAT_VERSION = 1
_TOOL_VERSION = "2026-07-v1"
_PARTS: tuple[int, ...] = tuple(range(10))
_CODEC = "ZSTD"
_EVIDENCE_SAMPLE_LIMIT = 20


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quoted_keys() -> list[str]:
    return [registry.quote_identifier(name) for name in _KEY_COLUMNS]


# -----------------------------------------------------------------------------
# Per-part diagnostic: load one CSV with the production reader, project the
# key columns, record blank-key and within-part duplicate counts.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class PartKeyAuditReport:
    status: str  # "ok" | "failed"
    part: int
    source_file: str
    source_csv: str
    rows_raw: int
    blank_key_counts: dict[str, int]
    within_part_duplicate_keys: int
    within_part_excess_rows: int
    output_path: str
    error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json_dict(), ensure_ascii=False, indent=2) + "\n")


def _blank_key_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        name: int(
            con.execute(
                f"SELECT COUNT(*) FROM {_RAW_TABLE} WHERE "
                f"{registry.quote_identifier(name)} IS NULL OR "
                f"TRIM({registry.quote_identifier(name)}) = ''"
            ).fetchone()[0]
        )
        for name in _KEY_COLUMNS
    }


def _within_part_duplicates(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """(distinct duplicate key count, excess row count) among valid (non-blank) keys.

    Same query shape as ``canonical_shadow._key_diagnostics``, but returns
    the distinct duplicate-key count too -- that diagnostic only needed the
    excess-row count (to decide fail-closed or not); this one needs both
    numbers reported separately, per issue #100's evidence requirements.
    """
    keys = _quoted_keys()
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    row = con.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT AS duplicate_keys,
            COALESCE(SUM(n - 1), 0)::BIGINT AS excess_rows
        FROM (
            SELECT COUNT(*)::BIGINT AS n
            FROM {_RAW_TABLE}
            WHERE {valid}
            GROUP BY {", ".join(keys)}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0]), int(row[1])


def run_part_key_audit(
    con: duckdb.DuckDBPyConnection,
    csv: Path,
    output: Path,
    *,
    part: int,
    source_file: str,
) -> PartKeyAuditReport:
    """Read one CSV with the production reader; write a key-only Parquet checkpoint."""
    if not csv.exists():
        raise FileNotFoundError(csv)
    if not source_file:
        raise ValueError("source_file cannot be empty")

    table = registry.main_table("estabelecimento")
    transform._create_table_from_csvs(con, _RAW_TABLE, [csv], table.source)  # noqa: SLF001
    rows_raw = int(con.execute(f"SELECT COUNT(*) FROM {_RAW_TABLE}").fetchone()[0])
    blanks = _blank_key_counts(con)
    dup_keys, excess_rows = _within_part_duplicates(con)

    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    partial.unlink(missing_ok=True)
    keys_sql = ", ".join(_quoted_keys())
    try:
        con.execute(
            f'COPY (SELECT {keys_sql}, {_literal(source_file)} AS "_source_file" '
            f"FROM {_RAW_TABLE}) TO {_literal(str(partial))} "
            f"(FORMAT PARQUET, COMPRESSION {_CODEC})"
        )
        partial.replace(output)
    except Exception:
        partial.unlink(missing_ok=True)
        raise

    return PartKeyAuditReport(
        status="ok",
        part=part,
        source_file=source_file,
        source_csv=str(csv),
        rows_raw=rows_raw,
        blank_key_counts=blanks,
        within_part_duplicate_keys=dup_keys,
        within_part_excess_rows=excess_rows,
        output_path=str(output),
    )


def _connection(database: Path, temp: Path) -> duckdb.DuckDBPyConnection:
    """Same production profile as canonical_shadow._connection: file-backed,
    production memory/thread pragmas, dedicated temp directory."""
    database.parent.mkdir(parents=True, exist_ok=True)
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database))
    con.execute(f"PRAGMA memory_limit='{transform.pick_memory_limit_gb()}GB'")
    con.execute(f"PRAGMA temp_directory={_literal(str(temp))}")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={transform.pick_threads()}")
    return con


def run_part_key_audit_with_metrics(
    csv: Path,
    output: Path,
    *,
    part: int,
    source_file: str,
    snapshot: str,
    work_dir: Path,
    metrics_path: Path,
    keep_workdir: bool = False,
) -> PartKeyAuditReport:
    """Production DuckDB profile + persisted resource evidence for one part."""
    work_dir.mkdir(parents=True, exist_ok=True)
    database = work_dir / "estabelecimento-key-audit.duckdb"
    temp = work_dir / "duckdb_tmp"
    recorder = metrics.MetricsRecorder(
        month=snapshot, schema_version="key-audit-1", filesystem_path=work_dir
    )
    con = _connection(database, temp)
    recorder.capture_pragmas(con)
    report: PartKeyAuditReport | None = None
    try:
        with recorder.stage(
            f"estabelecimento_key_audit_part_{part}", duckdb_tmp_dir=temp, workdir=work_dir
        ) as handle:
            report = run_part_key_audit(con, csv, output, part=part, source_file=source_file)
            handle.rows_read = report.rows_raw
            handle.rows_written = report.rows_raw
            handle.files_read = 1
            handle.duplicate_rows = report.within_part_excess_rows
            handle.extra.update(
                blank_key_counts=report.blank_key_counts,
                within_part_duplicate_keys=report.within_part_duplicate_keys,
            )
    finally:
        con.close()
        recorder.write_json(metrics_path)
        if not keep_workdir:
            database.unlink(missing_ok=True)
            database.with_suffix(".duckdb.wal").unlink(missing_ok=True)
            shutil.rmtree(temp, ignore_errors=True)
    if report is None:  # pragma: no cover
        raise RuntimeError("key audit finished without a report")
    return report


# -----------------------------------------------------------------------------
# Per-part checkpoint/resume orchestration -- mirrors canonical_history's
# discipline (source checksum + code fingerprint + output checksum must all
# match to reuse), reusing its download/extract/checksum/atomic-write
# primitives directly rather than re-implementing them.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class PartCheckpointResult:
    root: Path
    output_path: Path
    report_path: Path
    manifest_path: Path
    reused: bool
    manifest: dict[str, Any]


def _paths(root: Path, part: int) -> dict[str, Path]:
    return {
        "raw_dir": root / "raw",
        "extract_dir": root / "extracted",
        "work_dir": root / "work" / f"part-{part}",
        "output": root / "keys" / f"part-{part}.parquet",
        "report": root / "evidence" / f"part-{part}.key-audit.json",
        "metrics": root / "evidence" / f"part-{part}.key-audit.metrics.json",
        "manifest": root / "evidence" / f"part-{part}.key-audit.manifest.json",
        "failure": root / "evidence" / f"part-{part}.key-audit.failure.json",
    }


def _code_fingerprints() -> dict[str, str]:
    modules = {
        "estabelecimento_key_audit": Path(__file__).resolve(),
        "registry": Path(registry.__file__).resolve(),
    }
    return {name: canonical_history._sha256(path) for name, path in modules.items()}  # noqa: SLF001


def _reusable_part_manifest(
    paths: dict[str, Path],
    *,
    month: str,
    part: int,
    remote: Any,
    code: dict[str, str],
) -> dict[str, Any] | None:
    required = (paths["raw_dir"] / remote.name, paths["output"], paths["report"], paths["manifest"])
    if not all(path.is_file() for path in required):
        return None
    try:
        payload = canonical_history._load_json(paths["manifest"])  # noqa: SLF001
        expected_zip = canonical_history._sha256(paths["raw_dir"] / remote.name)  # noqa: SLF001
        expected_output = canonical_history._sha256(paths["output"])  # noqa: SLF001
        matches = (
            payload.get("format_version") == _FORMAT_VERSION
            and payload.get("tool_version") == _TOOL_VERSION
            and payload.get("status") == "ok"
            and payload.get("month") == month
            and payload.get("part") == part
            and payload.get("code") == code
            and payload["source"]["name"] == remote.name
            and payload["source"]["url"] == remote.url
            and payload["source"]["zip"]["sha256"] == expected_zip
            and payload["output"]["sha256"] == expected_output
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    return payload if matches else None


def run_part_checkpoint(
    month: str,
    part: int,
    root: Path,
    *,
    force: bool = False,
    zip_override: Path | None = None,
    keep_extracted: bool = False,
    client: httpx.Client | None = None,
) -> PartCheckpointResult:
    """Build or reuse one checksummed key-only checkpoint for one estabelecimento part."""
    remote = canonical_history.estabelecimento_remote(month, part)
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    paths = _paths(root, part)
    code = _code_fingerprints()

    if not force:
        reusable = _reusable_part_manifest(paths, month=month, part=part, remote=remote, code=code)
        if reusable is not None:
            return PartCheckpointResult(
                root, paths["output"], paths["report"], paths["manifest"], True, reusable
            )

    zip_path: Path | None = None
    csv_path: Path | None = None
    acquisition = "unknown"
    paths["failure"].unlink(missing_ok=True)
    try:
        zip_path, acquisition = canonical_history._ensure_zip(  # noqa: SLF001
            remote, paths["raw_dir"], zip_override=zip_override, client=client
        )
        csv_path = canonical_history._extract_one(zip_path, paths["extract_dir"])  # noqa: SLF001
        report = run_part_key_audit_with_metrics(
            csv_path,
            paths["output"],
            part=part,
            source_file=remote.name,
            snapshot=month,
            work_dir=paths["work_dir"],
            metrics_path=paths["metrics"],
        )
        report.write_json(paths["report"])

        manifest: dict[str, Any] = {
            "format_version": _FORMAT_VERSION,
            "tool_version": _TOOL_VERSION,
            "status": "ok",
            "month": month,
            "part": part,
            "code": code,
            "source": {
                "name": remote.name,
                "url": remote.url,
                "acquisition": acquisition,
                "zip": canonical_history._checked_file(zip_path),  # noqa: SLF001
                "csv": canonical_history._checked_file(csv_path),  # noqa: SLF001
            },
            "output": canonical_history._checked_file(paths["output"]),  # noqa: SLF001
            "report": canonical_history._checked_file(paths["report"]),  # noqa: SLF001
        }
        canonical_history._write_json_atomic(paths["manifest"], manifest)  # noqa: SLF001
        return PartCheckpointResult(
            root, paths["output"], paths["report"], paths["manifest"], False, manifest
        )
    except Exception as exc:
        failure: dict[str, Any] = {
            "format_version": _FORMAT_VERSION,
            "tool_version": _TOOL_VERSION,
            "status": "failed",
            "month": month,
            "part": part,
            "code": code,
            "source": {"name": remote.name, "url": remote.url, "acquisition": acquisition},
            "error": str(exc),
        }
        if zip_path is not None and zip_path.is_file():
            failure["source"]["zip"] = canonical_history._checked_file(zip_path)  # noqa: SLF001
        if csv_path is not None and csv_path.is_file():
            failure["source"]["csv"] = canonical_history._checked_file(csv_path)  # noqa: SLF001
        canonical_history._write_json_atomic(paths["failure"], failure)  # noqa: SLF001
        raise
    finally:
        if not keep_extracted:
            shutil.rmtree(paths["extract_dir"], ignore_errors=True)


# -----------------------------------------------------------------------------
# Global cross-part aggregation -- reads only the ten small key-only
# Parquets together, never the raw CSVs.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class GlobalKeyAuditReport:
    total_rows_scanned: int
    distinct_valid_full_keys: int
    duplicate_key_count: int
    excess_duplicate_row_count: int
    cross_part_duplicate_key_count: int
    blank_or_null_counts_by_component: dict[str, int]
    evidence_sample: list[dict[str, Any]]

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_global_key_audit(
    con: duckdb.DuckDBPyConnection,
    key_parquets: list[Path],
    *,
    evidence_sample_limit: int = _EVIDENCE_SAMPLE_LIMIT,
) -> GlobalKeyAuditReport:
    paths_sql = registry.paths_literal(key_parquets)
    keys_sql = ", ".join(_quoted_keys())
    quoted_keys = _quoted_keys()
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in quoted_keys)

    total_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({paths_sql})").fetchone()[0])

    blank_counts = {
        name: int(
            con.execute(
                f"SELECT COUNT(*) FROM read_parquet({paths_sql}) WHERE "
                f"{registry.quote_identifier(name)} IS NULL OR "
                f"TRIM({registry.quote_identifier(name)}) = ''"
            ).fetchone()[0]
        )
        for name in _KEY_COLUMNS
    }

    # Materialized ONCE, reused for both the headline counts and the sample
    # below -- at real-snapshot scale (~70M distinct keys) this GROUP BY is
    # already the expensive part. Computing a `list(DISTINCT "_source_file")`
    # aggregate state for every one of those ~70M groups (only to discard
    # non-duplicates via HAVING afterwards) is what actually OOM'd the first
    # real run: `COUNT`/`COUNT(DISTINCT)` are cheap running counters per
    # group, but a LIST aggregate accumulates real memory per group. Only the
    # tiny number of ACTUAL duplicates (n > 1) ever need their source files
    # listed, so that's deferred to a second, targeted query below.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _grouped_keys AS
        SELECT {keys_sql},
               COUNT(*)::BIGINT AS n,
               COUNT(DISTINCT "_source_file")::BIGINT AS n_parts
        FROM read_parquet({paths_sql})
        WHERE {valid}
        GROUP BY {keys_sql}
        """
    )
    agg = con.execute(
        """
        SELECT
            COUNT(*)::BIGINT AS distinct_valid_keys,
            COALESCE(SUM(CASE WHEN n > 1 THEN 1 ELSE 0 END), 0)::BIGINT AS duplicate_key_count,
            COALESCE(SUM(CASE WHEN n > 1 THEN n - 1 ELSE 0 END), 0)::BIGINT AS excess_rows,
            COALESCE(SUM(CASE WHEN n_parts > 1 THEN 1 ELSE 0 END), 0)::BIGINT AS cross_part_keys
        FROM _grouped_keys
        """
    ).fetchone()

    # Only the (at most `evidence_sample_limit`) duplicate keys selected here
    # ever get their source-file list computed -- the join probes the full
    # key parquets, but the list aggregate itself only accumulates state for
    # this small, already-bounded set of groups.
    top_keys_sql = ", ".join(f"top.{key}" for key in quoted_keys)
    join_on = " AND ".join(f"top.{key} = full_scan.{key}" for key in quoted_keys)
    sample_rows = con.execute(
        f"""
        WITH top AS (
            SELECT {keys_sql}, n
            FROM _grouped_keys
            WHERE n > 1
            ORDER BY n DESC, {keys_sql}
            LIMIT {int(evidence_sample_limit)}
        )
        SELECT {top_keys_sql}, top.n,
               list(DISTINCT full_scan."_source_file") AS source_files
        FROM top
        JOIN (
            SELECT {keys_sql}, "_source_file"
            FROM read_parquet({paths_sql})
            WHERE {valid}
        ) AS full_scan
        ON {join_on}
        GROUP BY {top_keys_sql}, top.n
        ORDER BY top.n DESC, {top_keys_sql}
        """
    ).fetchall()
    con.execute("DROP TABLE _grouped_keys")

    evidence_sample = [
        {
            "cnpj_basico": row[0],
            "cnpj_ordem": row[1],
            "cnpj_dv": row[2],
            "count": int(row[3]),
            "source_files": sorted(row[4]),
        }
        for row in sample_rows
    ]

    return GlobalKeyAuditReport(
        total_rows_scanned=total_rows,
        distinct_valid_full_keys=int(agg[0]),
        duplicate_key_count=int(agg[1]),
        excess_duplicate_row_count=int(agg[2]),
        cross_part_duplicate_key_count=int(agg[3]),
        blank_or_null_counts_by_component=blank_counts,
        evidence_sample=evidence_sample,
    )


# -----------------------------------------------------------------------------
# Top-level orchestration: all ten part checkpoints, then the global report.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class KeyAuditResult:
    root: Path
    report_path: Path
    report: dict[str, Any]
    part_results: tuple[PartCheckpointResult, ...]


def run_key_audit(
    month: str,
    root: Path,
    *,
    force: bool = False,
    zip_overrides: dict[int, Path] | None = None,
    client: httpx.Client | None = None,
) -> KeyAuditResult:
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    root = root.resolve()
    overrides = zip_overrides or {}

    part_results: list[PartCheckpointResult] = []
    for part in _PARTS:
        result = run_part_checkpoint(
            month,
            part,
            root,
            force=force,
            zip_override=overrides.get(part),
            client=client,
        )
        part_results.append(result)

    key_parquets = [result.output_path for result in part_results]
    global_work = root / "work" / "global"
    database = global_work / "estabelecimento-key-audit-global.duckdb"
    temp = global_work / "duckdb_tmp"
    con = _connection(database, temp)
    try:
        global_report = run_global_key_audit(con, key_parquets)
        duckdb_version = duckdb.__version__
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        memory_limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        con.close()
        database.unlink(missing_ok=True)
        database.with_suffix(".duckdb.wal").unlink(missing_ok=True)
        shutil.rmtree(temp, ignore_errors=True)

    payload: dict[str, Any] = {
        "format_version": _FORMAT_VERSION,
        "tool_version": _TOOL_VERSION,
        "snapshot_month": month,
        "source_commit": metrics._git_sha(),  # noqa: SLF001
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "duckdb_version": duckdb_version,
        "execution_profile": {
            "threads": str(threads),
            "memory_limit": str(memory_limit),
        },
        "parts": [
            {
                "part": result.manifest.get("part"),
                "source_file": result.manifest.get("source", {}).get("name"),
                "source_zip_sha256": result.manifest.get("source", {}).get("zip", {}).get("sha256"),
                "rows_raw": None,
                "reused_checkpoint": result.reused,
            }
            for result in part_results
        ],
        **global_report.to_json_dict(),
        "checkpoint_checksums": {
            f"part-{result.manifest.get('part')}": result.manifest.get("output", {}).get("sha256")
            for result in part_results
        },
    }
    # rows_raw per part comes from the part-level report, not the manifest.
    for entry, result in zip(payload["parts"], part_results, strict=True):
        try:
            part_report = canonical_history._load_json(result.report_path)  # noqa: SLF001
            entry["rows_raw"] = part_report.get("rows_raw")
            entry["blank_key_counts"] = part_report.get("blank_key_counts")
            entry["within_part_duplicate_keys"] = part_report.get("within_part_duplicate_keys")
            entry["within_part_excess_rows"] = part_report.get("within_part_excess_rows")
        except (OSError, ValueError, json.JSONDecodeError):  # pragma: no cover
            pass

    report_path = root / "evidence" / "global.key-audit.json"
    canonical_history._write_json_atomic(report_path, payload)  # noqa: SLF001
    return KeyAuditResult(root, report_path, payload, tuple(part_results))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--zip",
        action="append",
        default=[],
        metavar="PART=PATH",
        help="Local ZIP override for one part, e.g. --zip 0=/path/to/Estabelecimentos0.zip "
        "(repeatable; smoke/offline runs only)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    zip_overrides: dict[int, Path] = {}
    for entry in args.zip:
        part_str, _, path_str = entry.partition("=")
        if not part_str.isdigit() or not path_str:
            print(f"error: --zip must be PART=PATH, got {entry!r}", file=sys.stderr)
            return 2
        zip_overrides[int(part_str)] = Path(path_str)

    try:
        result = run_key_audit(args.month, args.root, force=args.force, zip_overrides=zip_overrides)
    except (
        FileNotFoundError,
        OSError,
        RuntimeError,
        ValueError,
        httpx.HTTPError,
        zipfile.BadZipFile,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    report = result.report
    print(
        f"key audit done — {report['distinct_valid_full_keys']:,} distinct valid keys, "
        f"{report['duplicate_key_count']:,} duplicate key(s), "
        f"{report['cross_part_duplicate_key_count']:,} cross-part"
    )
    print(f"report: {result.report_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "GlobalKeyAuditReport",
    "KeyAuditResult",
    "PartCheckpointResult",
    "PartKeyAuditReport",
    "main",
    "run_global_key_audit",
    "run_key_audit",
    "run_part_checkpoint",
    "run_part_key_audit",
    "run_part_key_audit_with_metrics",
]
