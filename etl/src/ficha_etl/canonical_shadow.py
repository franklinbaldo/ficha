"""Write one shadow canonical ``estabelecimento`` Parquet part (RFC 0001 Phase 2).

The command consumes an already-extracted RFB CSV. It deliberately does not
fetch, publish, or feed the monthly product pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import duckdb

from . import metrics, registry, sources, transform

_RAW_TABLE = "_raw_estabelecimento_shadow"
_SAMPLE_SEED = 42
# Experimental physical profile for the shadow slice; ParquetSpec keeps these
# choices open until real-run evidence exists.
_CODEC = "ZSTD"
_ROW_GROUP_SIZE = 200_000


@dataclass(frozen=True)
class CanonicalPartReport:
    status: Literal["ok", "failed"]
    schema_version: int
    source_csv: str
    source_file: str
    source_snapshot: str
    output_path: str
    rows_raw: int
    rows_canonical: int | None
    bytes_read: int
    bytes_written: int | None
    required_key_failures: dict[str, int]
    duplicate_key_rows: int
    invalid_casts_by_column: dict[str, int]
    sample_seed: int
    sample_size: int
    sample_fingerprint: str
    sample_mismatches: int | None
    schema_matches: bool | None
    codec: str = _CODEC
    row_group_size: int = _ROW_GROUP_SIZE
    error: str | None = None

    @property
    def invalid_casts_total(self) -> int:
        return sum(self.invalid_casts_by_column.values())

    def to_json_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["invalid_casts_total"] = self.invalid_casts_total
        return payload

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json_dict(), ensure_ascii=False, indent=2))


class CanonicalValidationError(RuntimeError):
    def __init__(self, message: str, report: CanonicalPartReport) -> None:
        super().__init__(message)
        self.report = report


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _spec() -> tuple[registry.TableSpec, registry.ParquetSpec]:
    table = registry.main_table("estabelecimento")
    if table.canonical is None:  # pragma: no cover
        raise RuntimeError("estabelecimento has no canonical contract")
    return table, table.canonical


def _key_diagnostics(
    con: duckdb.DuckDBPyConnection, spec: registry.ParquetSpec
) -> tuple[dict[str, int], int]:
    failures = {
        name: int(
            con.execute(
                f"SELECT COUNT(*) FROM {_RAW_TABLE} WHERE "
                f"{registry.quote_identifier(name)} IS NULL OR "
                f"TRIM({registry.quote_identifier(name)}) = ''"
            ).fetchone()[0]
        )
        for name in spec.primary_key
    }
    keys = [registry.quote_identifier(name) for name in spec.primary_key]
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    duplicate_rows = int(
        con.execute(
            f"""
            SELECT COALESCE(SUM(n - 1), 0)::BIGINT
            FROM (
                SELECT COUNT(*)::BIGINT AS n
                FROM {_RAW_TABLE}
                WHERE {valid}
                GROUP BY {", ".join(keys)}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    return failures, duplicate_rows


def _invalid_casts(con: duckdb.DuckDBPyConnection, spec: registry.ParquetSpec) -> dict[str, int]:
    result: dict[str, int] = {}
    for column in spec.columns:
        if column.duckdb_type != "DATE" or column.invalid_policy != "null-and-count":
            continue
        raw = f"src.{registry.quote_identifier(column.source)}"
        cast = registry.canonical_expression_sql(column, source_alias="src")
        result[column.name] = int(
            con.execute(
                f"SELECT COUNT(*) FROM {_RAW_TABLE} AS src "
                f"WHERE {raw} IS NOT NULL AND TRIM({raw}) <> '' "
                f"AND ({cast}) IS NULL"
            ).fetchone()[0]
        )
    return result


def _sample(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    requested: int,
    rows_raw: int,
) -> tuple[int, str]:
    size = min(max(0, requested), rows_raw)
    if not size:
        return 0, hashlib.sha256(b"[]").hexdigest()
    keys = ", ".join(registry.quote_identifier(name) for name in spec.primary_key)
    rows = con.execute(
        f"SELECT {keys} FROM {_RAW_TABLE} "
        f"USING SAMPLE reservoir({size} ROWS) REPEATABLE({_SAMPLE_SEED}) "
        f"ORDER BY {keys}"
    ).fetchall()
    encoded = json.dumps(rows, default=str, separators=(",", ":")).encode()
    return len(rows), hashlib.sha256(encoded).hexdigest()


def _select_sql(spec: registry.ParquetSpec, source_file: str, snapshot: str) -> str:
    projection = registry.canonical_projection_sql(spec, source_alias="src")
    return (
        f"SELECT\n{projection},\n"
        f'    {_literal(source_file)} AS "_source_file",\n'
        f'    {_literal(snapshot)} AS "_source_snapshot"\n'
        f"FROM {_RAW_TABLE} AS src"
    )


def _expected_schema(spec: registry.ParquetSpec) -> list[tuple[str, str]]:
    return [
        *((column.name, column.duckdb_type) for column in spec.columns),
        *((column.name, column.duckdb_type) for column in spec.lineage),
    ]


def _sample_mismatches(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    parquet: Path,
    size: int,
    source_file: str,
    snapshot: str,
) -> int:
    if not size:
        return 0
    keys = [registry.quote_identifier(name) for name in spec.primary_key]
    join = " AND ".join(f"can.{key} = src.{key}" for key in keys)
    checks = ['can."_source_file" IS NULL']
    for column in spec.columns:
        raw = f"src.{registry.quote_identifier(column.source)}"
        canonical = f"can.{registry.quote_identifier(column.name)}"
        if column.duckdb_type == "DATE":
            parsed = f"TRY_STRPTIME(TRIM({raw}), '%Y%m%d')::DATE"
            checks.append(
                "(CASE "
                f"WHEN {raw} IS NULL OR TRIM({raw}) IN ('', '0') THEN {canonical} IS NOT NULL "
                f"WHEN {parsed} IS NULL THEN {canonical} IS NOT NULL "
                f"ELSE STRFTIME({canonical}, '%Y%m%d') IS DISTINCT FROM TRIM({raw}) END)"
            )
        else:
            checks.append(f"{raw} IS DISTINCT FROM {canonical}")
    checks += [
        f'can."_source_file" IS DISTINCT FROM {_literal(source_file)}',
        f'can."_source_snapshot" IS DISTINCT FROM {_literal(snapshot)}',
    ]
    return int(
        con.execute(
            f"""
            WITH sampled AS (
                SELECT * FROM {_RAW_TABLE}
                USING SAMPLE reservoir({size} ROWS) REPEATABLE({_SAMPLE_SEED})
            )
            SELECT COUNT(*)
            FROM sampled AS src
            LEFT JOIN read_parquet({_literal(str(parquet))}) AS can ON {join}
            WHERE {" OR ".join(checks)}
            """
        ).fetchone()[0]
    )


def _make_report(
    *,
    status: Literal["ok", "failed"],
    spec: registry.ParquetSpec,
    csv: Path,
    output: Path,
    source_file: str,
    snapshot: str,
    rows_raw: int,
    rows_canonical: int | None,
    key_failures: dict[str, int],
    duplicate_rows: int,
    invalid_casts: dict[str, int],
    sample_size: int,
    fingerprint: str,
    mismatches: int | None,
    schema_matches: bool | None,
    error: str | None = None,
) -> CanonicalPartReport:
    return CanonicalPartReport(
        status=status,
        schema_version=spec.schema_version,
        source_csv=str(csv),
        source_file=source_file,
        source_snapshot=snapshot,
        output_path=str(output),
        rows_raw=rows_raw,
        rows_canonical=rows_canonical,
        bytes_read=csv.stat().st_size,
        bytes_written=output.stat().st_size if status == "ok" else None,
        required_key_failures=key_failures,
        duplicate_key_rows=duplicate_rows,
        invalid_casts_by_column=invalid_casts,
        sample_seed=_SAMPLE_SEED,
        sample_size=sample_size,
        sample_fingerprint=fingerprint,
        sample_mismatches=mismatches,
        schema_matches=schema_matches,
        error=error,
    )


def write_estabelecimento_canonical_part(
    con: duckdb.DuckDBPyConnection,
    csv: Path,
    output: Path,
    *,
    source_file: str,
    source_snapshot: str,
    sample_size: int = 1_000,
) -> CanonicalPartReport:
    """Read one CSV with the production reader and atomically write one part."""
    if not csv.exists():
        raise FileNotFoundError(csv)
    if not source_file:
        raise ValueError("source_file cannot be empty")
    if not sources.is_valid_month(source_snapshot):
        raise ValueError(f"source_snapshot must be YYYY-MM, got {source_snapshot!r}")
    if sample_size < 0:
        raise ValueError("sample_size cannot be negative")

    table, spec = _spec()
    transform._create_table_from_csvs(con, _RAW_TABLE, [csv], table.source)  # noqa: SLF001
    rows_raw = int(con.execute(f"SELECT COUNT(*) FROM {_RAW_TABLE}").fetchone()[0])
    key_failures, duplicate_rows = _key_diagnostics(con, spec)
    invalid_casts = _invalid_casts(con, spec)
    actual_sample, fingerprint = _sample(con, spec, sample_size, rows_raw)

    errors = []
    if any(key_failures.values()):
        errors.append(f"required key failures: {key_failures}")
    if duplicate_rows:
        errors.append(f"duplicate full-CNPJ excess rows: {duplicate_rows}")
    if errors:
        message = "; ".join(errors)
        report = _make_report(
            status="failed",
            spec=spec,
            csv=csv,
            output=output,
            source_file=source_file,
            snapshot=source_snapshot,
            rows_raw=rows_raw,
            rows_canonical=None,
            key_failures=key_failures,
            duplicate_rows=duplicate_rows,
            invalid_casts=invalid_casts,
            sample_size=actual_sample,
            fingerprint=fingerprint,
            mismatches=None,
            schema_matches=None,
            error=message,
        )
        raise CanonicalValidationError(message, report)

    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    partial.unlink(missing_ok=True)
    try:
        con.execute(
            f"COPY ({_select_sql(spec, source_file, source_snapshot)}) "
            f"TO {_literal(str(partial))} (FORMAT PARQUET, COMPRESSION {_CODEC}, "
            f"ROW_GROUP_SIZE {_ROW_GROUP_SIZE})"
        )
        rows_canonical = int(
            con.execute(f"SELECT COUNT(*) FROM read_parquet({_literal(str(partial))})").fetchone()[
                0
            ]
        )
        actual_schema = [
            (str(row[0]), str(row[1]))
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({_literal(str(partial))})"
            ).fetchall()
        ]
        schema_matches = actual_schema == _expected_schema(spec)
        mismatches = _sample_mismatches(
            con,
            spec,
            partial,
            actual_sample,
            source_file,
            source_snapshot,
        )
        errors = []
        if rows_canonical != rows_raw:
            errors.append(f"row-count mismatch: raw={rows_raw}, canonical={rows_canonical}")
        if not schema_matches:
            errors.append(
                f"schema mismatch: expected={_expected_schema(spec)!r}, actual={actual_schema!r}"
            )
        if mismatches:
            errors.append(f"deterministic sample mismatches: {mismatches}")
        if errors:
            message = "; ".join(errors)
            report = _make_report(
                status="failed",
                spec=spec,
                csv=csv,
                output=output,
                source_file=source_file,
                snapshot=source_snapshot,
                rows_raw=rows_raw,
                rows_canonical=rows_canonical,
                key_failures=key_failures,
                duplicate_rows=duplicate_rows,
                invalid_casts=invalid_casts,
                sample_size=actual_sample,
                fingerprint=fingerprint,
                mismatches=mismatches,
                schema_matches=schema_matches,
                error=message,
            )
            raise CanonicalValidationError(message, report)
        partial.replace(output)
    except Exception:
        partial.unlink(missing_ok=True)
        raise

    return _make_report(
        status="ok",
        spec=spec,
        csv=csv,
        output=output,
        source_file=source_file,
        snapshot=source_snapshot,
        rows_raw=rows_raw,
        rows_canonical=rows_canonical,
        key_failures=key_failures,
        duplicate_rows=duplicate_rows,
        invalid_casts=invalid_casts,
        sample_size=actual_sample,
        fingerprint=fingerprint,
        mismatches=mismatches,
        schema_matches=schema_matches,
    )


def _connection(database: Path, temp: Path) -> duckdb.DuckDBPyConnection:
    database.parent.mkdir(parents=True, exist_ok=True)
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database))
    con.execute(f"PRAGMA memory_limit='{transform.pick_memory_limit_gb()}GB'")
    con.execute(f"PRAGMA temp_directory={_literal(str(temp))}")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={transform.pick_threads()}")
    return con


def _record(handle: metrics.StageHandle, report: CanonicalPartReport) -> None:
    handle.rows_read = report.rows_raw
    handle.rows_written = report.rows_canonical
    handle.bytes_read = report.bytes_read
    handle.bytes_written = report.bytes_written
    handle.files_read = 1
    handle.casts_invalid = report.invalid_casts_total
    handle.duplicate_rows = report.duplicate_key_rows
    handle.extra.update(
        status=report.status,
        canonical_schema_version=report.schema_version,
        sample_seed=report.sample_seed,
        sample_size=report.sample_size,
        sample_mismatches=report.sample_mismatches or 0,
        codec=report.codec,
        row_group_size=report.row_group_size,
    )


def run_shadow_part(
    csv: Path,
    output: Path,
    *,
    source_file: str,
    source_snapshot: str,
    work_dir: Path,
    report_path: Path,
    metrics_path: Path,
    sample_size: int = 1_000,
    keep_workdir: bool = False,
) -> CanonicalPartReport:
    """Use the production DuckDB profile and persist quality/resource evidence."""
    work_dir.mkdir(parents=True, exist_ok=True)
    database = work_dir / "canonical-estabelecimento.duckdb"
    temp = work_dir / "duckdb_tmp"
    recorder = metrics.MetricsRecorder(
        month=source_snapshot,
        schema_version=str(registry.ESTABELECIMENTO_CANONICAL.schema_version),
        filesystem_path=work_dir,
    )
    con = _connection(database, temp)
    recorder.capture_pragmas(con)
    report: CanonicalPartReport | None = None
    try:
        with recorder.stage(
            "canonical_estabelecimento_part", duckdb_tmp_dir=temp, workdir=work_dir
        ) as handle:
            try:
                report = write_estabelecimento_canonical_part(
                    con,
                    csv,
                    output,
                    source_file=source_file,
                    source_snapshot=source_snapshot,
                    sample_size=sample_size,
                )
            except CanonicalValidationError as exc:
                report = exc.report
                _record(handle, report)
                raise
            _record(handle, report)
    finally:
        con.close()
        if report is not None:
            report.write_json(report_path)
        recorder.write_json(metrics_path)
        if not keep_workdir:
            database.unlink(missing_ok=True)
            database.with_suffix(".duckdb.wal").unlink(missing_ok=True)
            shutil.rmtree(temp, ignore_errors=True)
            try:
                work_dir.rmdir()
            except OSError:
                pass
    if report is None:  # pragma: no cover
        raise RuntimeError("shadow writer finished without a report")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--metrics", type=Path)
    parser.add_argument("--sample-size", type=int, default=1_000)
    parser.add_argument("--keep-workdir", action="store_true")
    args = parser.parse_args(argv)
    if not sources.is_valid_month(args.snapshot):
        print(f"error: snapshot must be YYYY-MM, got {args.snapshot!r}", file=sys.stderr)
        return 2

    work = args.work_dir or args.output.parent / ".canonical-shadow" / args.output.stem
    quality = args.report or args.output.with_suffix(".quality.json")
    resource_metrics = args.metrics or args.output.with_suffix(".metrics.json")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        report = run_shadow_part(
            args.csv,
            args.output,
            source_file=args.source_file,
            source_snapshot=args.snapshot,
            work_dir=work,
            report_path=quality,
            metrics_path=resource_metrics,
            sample_size=args.sample_size,
            keep_workdir=args.keep_workdir,
        )
    except (
        CanonicalValidationError,
        FileNotFoundError,
        OSError,
        ValueError,
        duckdb.Error,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"canonical shadow OK — {report.rows_canonical:,} rows, "
        f"{report.invalid_casts_total} invalid cast(s), output={args.output}"
    )
    print(f"quality report: {quality}")
    print(f"metrics: {resource_metrics}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
