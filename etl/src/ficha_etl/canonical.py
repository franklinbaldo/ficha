"""Shadow writer for canonical RFC 0001 datasets.

This module implements the first Phase 2 vertical slice: one already-extracted
``estabelecimento`` CSV becomes one typed canonical Parquet part plus a
checksummed manifest. It does not participate in ``transform_snapshot`` and
does not feed public products.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from . import metrics, registry, transform
from .sources import is_valid_month

_MANIFEST_FORMAT_VERSION = 1
_SAMPLE_SEED = 42
_ALLOWED_CODECS = frozenset({"ZSTD", "LZ4", "SNAPPY", "UNCOMPRESSED"})


class CanonicalValidationError(RuntimeError):
    """Raised when a shadow canonical part fails a load-bearing gate."""

    def __init__(self, message: str, evidence: dict[str, Any]) -> None:
        super().__init__(message)
        self.evidence = evidence


@dataclass(frozen=True)
class CanonicalPartResult:
    """Paths and persisted manifest for a canonical part."""

    output_path: Path
    manifest_path: Path
    reused: bool
    manifest: dict[str, Any]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _manifest_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + ".manifest.json")


def _failure_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + ".failure.json")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _configure_connection(
    con: duckdb.DuckDBPyConnection,
    *,
    temp_directory: Path,
    recorder: metrics.MetricsRecorder,
) -> None:
    temp_directory.mkdir(parents=True, exist_ok=True)
    con.execute(f"PRAGMA memory_limit='{transform.pick_memory_limit_gb()}GB'")
    con.execute(f"PRAGMA temp_directory={_sql_literal(temp_directory)}")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={transform.pick_threads()}")
    recorder.capture_pragmas(con)


def _typed_invalid_counts(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    source_table: str,
) -> dict[str, int]:
    result: dict[str, int] = {}
    for column in spec.columns:
        if column.cast_sql is None or column.invalid_policy != "null-and-count":
            continue
        source = f"raw.{registry.quote_identifier(column.source)}"
        cast = registry.canonical_expression_sql(column, source_alias="raw")
        result[column.name] = int(
            con.execute(
                f"SELECT COUNT(*) FROM {registry.quote_identifier(source_table)} AS raw "
                f"WHERE {source} IS NOT NULL "
                f"AND TRIM({source}) NOT IN ('', '0') "
                f"AND ({cast}) IS NULL"
            ).fetchone()[0]
        )
    return result


def _parquet_relation(path: Path) -> str:
    return f"read_parquet({_sql_literal(path)})"


def _expected_schema(spec: registry.ParquetSpec) -> list[tuple[str, str]]:
    return [
        *((column.name, column.duckdb_type) for column in spec.columns),
        *((column.name, column.duckdb_type) for column in spec.lineage),
    ]


def _actual_schema(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_parquet_relation(path)}").fetchall()
    return [(str(name), str(duckdb_type)) for name, duckdb_type, *_ in rows]


def _sample_keys(
    con: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    primary_key: tuple[str, ...],
    source_rows: int,
    requested: int,
) -> tuple[int, str]:
    size = min(requested, source_rows)
    if size == 0:
        return 0, hashlib.sha256(b"[]").hexdigest()
    keys = ", ".join(registry.quote_identifier(key) for key in primary_key)
    rows = con.execute(
        f"SELECT {keys} FROM {registry.quote_identifier(source_table)} "
        f"USING SAMPLE reservoir({size} ROWS) REPEATABLE({_SAMPLE_SEED}) "
        f"ORDER BY {keys}"
    ).fetchall()
    payload = json.dumps(
        rows,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    ).encode()
    return len(rows), hashlib.sha256(payload).hexdigest()


def _reversible_sample_mismatches(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    source_table: str,
    output_path: Path,
    sample_size: int,
    source_file: str,
    source_snapshot: str,
) -> int:
    if sample_size == 0:
        return 0
    key_names = [registry.quote_identifier(key) for key in spec.primary_key]
    join = " AND ".join(f"can.{key} = raw.{key}" for key in key_names)
    checks = [f"can.{key_names[0]} IS NULL"]
    for column in spec.columns:
        raw = f"raw.{registry.quote_identifier(column.source)}"
        canonical = f"can.{registry.quote_identifier(column.name)}"
        if column.duckdb_type == "DATE":
            parsed = f"TRY_STRPTIME(TRIM({raw}), '%Y%m%d')::DATE"
            checks.append(
                "(CASE "
                f"WHEN {raw} IS NULL OR TRIM({raw}) IN ('', '0') "
                f"THEN {canonical} IS NOT NULL "
                f"WHEN {parsed} IS NULL THEN {canonical} IS NOT NULL "
                f"ELSE STRFTIME({canonical}, '%Y%m%d') "
                f"IS DISTINCT FROM TRIM({raw}) END)"
            )
        else:
            checks.append(f"{raw} IS DISTINCT FROM {canonical}")
    checks.extend(
        (
            f'can."_source_file" IS DISTINCT FROM {_sql_literal(source_file)}',
            f'can."_source_snapshot" IS DISTINCT FROM {_sql_literal(source_snapshot)}',
        )
    )
    return int(
        con.execute(
            f"""
            WITH sampled AS (
                SELECT *
                FROM {registry.quote_identifier(source_table)}
                USING SAMPLE reservoir({sample_size} ROWS)
                REPEATABLE({_SAMPLE_SEED})
            )
            SELECT COUNT(*)
            FROM sampled AS raw
            LEFT JOIN {_parquet_relation(output_path)} AS can ON {join}
            WHERE {" OR ".join(checks)}
            """
        ).fetchone()[0]
    )


def _duplicate_diagnostics(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    output_path: Path,
) -> tuple[int, int, int]:
    relation = _parquet_relation(output_path)
    keys = [registry.quote_identifier(key) for key in spec.primary_key]
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    total = int(
        con.execute(
            "SELECT COALESCE(SUM(group_size - 1), 0) FROM ("
            f"SELECT COUNT(*) AS group_size FROM {relation} "
            f"WHERE {valid} GROUP BY {', '.join(keys)} HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    )
    all_columns = [
        *(registry.quote_identifier(column.name) for column in spec.columns),
        *(registry.quote_identifier(column.name) for column in spec.lineage),
    ]
    identical = int(
        con.execute(
            "SELECT COALESCE(SUM(group_size - 1), 0) FROM ("
            f"SELECT COUNT(*) AS group_size FROM {relation} "
            f"WHERE {valid} GROUP BY {', '.join(all_columns)} HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    )
    return total, identical, max(0, total - identical)


def _validate_output(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    source_table: str,
    source_rows: int,
    output_path: Path,
    source_file: str,
    source_snapshot: str,
    sample_size: int,
) => dict[str, Any]:
    relation = _parquet_relation(output_path)
    output_rows = int(con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])
    schema = _actual_schema(con, output_path)
    expected_schema = _expected_schema(spec)

    missing_terms: list[str] = []
    for key in spec.primary_key:
        identifier = registry.quote_identifier(key)
        missing_terms.extend((f"{identifier} IS NULL", f"TRIM({identifier}) = ''"))
    missing_key_rows = int(
        con.execute(f"SELECT COUNT(*) FROM {relation} WHERE" + " OR ".join(missing_terms)).fetchone()[0]
    )
    duplicate_rows, identical_duplicates, conflicting_duplicates = _duplicate_diagnostics(
        con, spec, output_path=output_path
    )

    critical_terms: list[str] = []
    for column in (*spec.columns, *spec.lineage):
        if not column.publication_critical:
            continue
        identifier = registry.quote_identifier(column.name)
        critical_terms.append(f"{identifier} IS NULL")
        if column.duckdb_type == "VARCHAR":
            critical_terms.append(f"TRIM({identifier}) = ''")
    critical_predicate = " OR ".join(critical_terms) or "FALSE"
    critical_missing_rows = int(
        con.execute(f"SELECT COUNT(*) FROM {relation} WHERE {critical_predicate}").fetchone()[0]
    )

    actual_sample_size, sample_fingerprint = _sample_keys(
        con,
        source_table=source_table,
        primary_key=spec.primary_key,
        source_rows=source_rows,
        requested=sample_size,
    )
    sample_mismatches = _reversible_sample_mismatches(
        con,
        spec,
        source_table=source_table,
        output_path=output_path,
        sample_size=actual_sample_size,
        source_file=source_file,
        source_snapshot=source_snapshot,
    )

    failures: list[str] = []
    if output_rows != source_rows:
        failures.append(f"row count {source_rows} -> {output_rows}")
    if schema != expected_schema:
        failures.append(f"schema {schema!r} != {expected_schema!r}")
    if missing_key_rows:
        failures.append(f"{missing_key_rows} rows with missing primary key")
    if duplicate_rows:
        failures.append(
            f"{duplicate_rows} duplicate primary-key rows "
            f"({identical_duplicates} identical, {conflicting_duplicates} conflicting)"
        )
    if critical_missing_rows:
        failures.append(f"{critical_missing_rows} rows with missing critical values")
    if sample_mismatches:
        failures.append(
            f"{sample_mismatches} deterministic raw/canonical sample mismatches"
        )

    return {
        "status": "failed" if failures else "passed",
        "failures": failures,
        "row_count_match": output_rows == source_rows,
        "source_rows": source_rows,
        "output_rows": output_rows,
        "schema_match": schema == expected_schema,
        "schema": [{"name": name, "type": kind} for name, kind in schem],
        "primary_key_missing_rows": missing_key_rows,
        "duplicate_primary_key_rows": duplicate_rows,
        "identical_duplicate_rows": identical_duplicates,
        "conflicting_duplicate_rows": conflicting_duplicates,
        "critical_missing_rows": critical_missing_rows,
        "sample_seed": _SAMPLE_SEED,
        "sample_size": actual_sample_size,
        "sample_fingerprint": sample_fingerprint,
        "sample_mismatches": sample_mismatches,
    }


def _reusable_manifest(
    output_path: Path,
    manifest_path: Path,
    *,
    source_sha256: str,
    source_file: str,
    source_snapshot: str,
    schema_version: int,
    codec: str,
    row_group_size: int,
    sample_size: int,
) -> dict[str, Any] | None:
    if not output_path.exists() or not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        validation = payload["validation"]
        matches = (
            payload["format_version"] == _MANIFEST_FORMAT_VERSION
            and payload["table"] == "estabelecimento"
            and payload["source"]["sha256"] == source_sha256
            and payload["source"]["file"] == source_file
            and payload["source"]["snapshot"] == source_snapshot
            and payload["output"]["schema_version"] == schema_version
            and payload["output"]["codec"] == codec
            and payload["output"]["row_group_size"] == row_group_size
            and payload["output"]["path"] == output_path.name
            and payload["output"]["bytes"] == output_path.stat().st_size
            and payload["output"]["sha256"] == _sha256(output_path)
            and validation["status"] == "passed"
            and validation["sample_size"] == min(sample_size, validation["source_rows"])
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    return payload if matches else None


def _publish_pair(
    temporary_output: Path,
    output_path: Path,
    temporary_manifest: Path,
    manifest_path: Path,
) -> None:
    token = uuid.uuid4().hex
    output_backup = output_path.with_name(f".{output_path.name}.{token}.backup")
    manifest_backup = manifest_path.with_name(f".{manifest_path.name}.{token}.backup")
    output_had_previous = output_path.exists()
    manifest_had_previous = manifest_path.exists()
    try:
        if output_had_previous:
            os.replace(output_path, output_backup)
        if manifest_had_previous:
            os.replace(manifest_path, manifest_backup)
        os.replace(temporary_output, output_path)
        os.replace(temporary_manifest, manifest_path)
    except Exception:
        output_path.unlink(missing_ok=True)
        manifest_path.unlink( missing_ok=True)
        if output_had_previous and output_backup.exists():
            os.replace(output_backup, output_path)
        if manifest_had_previous and manifest_backup.exists():
            os.replace(manifest_backup, manifest_path)
        raise
    finally:
        output_backup.unlink(missing_ok=True)
        manifest_backup.unlink(missing_ok=True)


def write_estabelecimento_part(
    csv_path: Path,
    output_path: Path,
    *,
    source_file: str,
    source_snapshot: str,
    work_dir: Path | None = None,
    codec: str = "ZSTD",
    row_group_size: int = 200_000,
    sample_size: int = 1_000,
    resume: bool = True,
) => CanonicalPartResult:
    """Write and validate one canonical `estabelecimento` Parquet part."""
    csv_path = csv_path.resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    if not source_file.strip():
        raise ValueError("source_file must not be blank")
    if not is_valid_month(source_snapshot):
        raise ValueError(f"source_snapshot must be YYYY-MM, got {source_snapshot!r}")
    codec = codec.upper()
    if codec not in _ALLOWED_CODECS:
        raise ValueError(f"unsupported Parquet codec: {codec!r}")
    if row_group_size < 1:
        raise ValueError("row_group_size must be positive")
    if sample_size < 0:
        raise ValueError("sample_size must not be negative")

    table = registry.main_table("estabelecimento")
    spec = table.canonical
    if spec is None:  # pragma: no cover
        raise RuntimeError("estabelecimento canonical contract is missing")

    output_path = output_path.resolve()
    if output_path == csv_path:
        raise ValueError("output_path must differ from csv_path")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = _manifest_path(output_path)
    failure_path = _failure_path(output_path)
    source_sha256 = _sha256(csv_path)
    if resume:
        reusable = _reusable_manifest(
            output_path,
            manifest_path,
            source_sha256=source_sha256,
            source_file=source_file,
            source_snapshot=source_snapshot,
            schema_version=spec.schema_version,
            codec=codec,
            row_group_size=row_group_size,
            sample_size=sample_size,
        )
        if reusable is not None:
            return CanonicalPartResult(output_path, manifest_path, True, reusable)

    work_root = (work_dir or output_path.parent / ".canonical-work").resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    if work_root.stat().st_dev != output_path.parent.stat().st_dev:
        raise ValueError(
            "work_dir and output_path must be on the same filesystem so peak-disk "
            "measurement and atomic rename describe one checkpoint boundary"
        )
    token = uuid.uuid4().hex
    run_dir = work_root / f"estabelecimento-{token}"
    run_dir.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_name(f".{output_path.name}.{token}.tmp.parquet")
    temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.{token}.tmp")
    db_path = run_dir / "state.duckdb"
    duckdb_tmp = run_dir / "duckdb_tmp"

    recorder = metrics.MetricsRecorder(
        month=source_snapshot,
        schema_version=str(spec.schema_version),
        filesystem_path=output_path.parent,
    )
    con: duckdb.DuckDBPyConnection | None = None
    invalid_counts: dict[str, int] = {}
    validation: dict[str, Any] | None = None
    source_rows = 0
    try:
        con = duckdb.connect(str(db_path))
        _configure_connection(con, temp_directory=duckdb_tmp, recorder=recorder)
        with recorder.stage(
            "canonical_estabelecimento_part",
            duckdb_tmp_dir=duckdb_tmp,
            workdir=run_dir,
            sample_interval=0.1,
        ) as handle:
            transform._create_table_from_csvs(  # noqa: SLF001
                con,
                "raw_estabelecimento",
                [csv_path],
                table.source,
            )
            source_rows = int(con.execute("SELECT COUNT(*) FROM raw_estabelecimento").fetchone()[0])
            invalid_counts = _typed_invalid_counts(
                con,
                spec,
                source_table="raw_estabelecimento",
            )
            projection = registry.canonical_projection_sql(spec, source_alias="raw")
            con.execute(
                f"""
                COPY (
                    SELECT
{projection},
                        {_sql_literal(source_file)} AS "_source_file",
                        {_sql_literal(source_snapshot)} AS "_source_snapshot"
                    FROM raw_estabelecimento AS raw
                ) TO {_sql_literal(temporary_output)}
                (FORMAT PARQUET, COMPRESSION {codec}, ROW_GROUP_SIZE {row_group_size})
                """
            )
            validation = _validate_output(
                con,
                spec,
                source_table="raw_estabelecimento",
                source_rows=source_rows,
                output_path=temporary_output,
                source_file=source_file,
                source_snapshot=source_snapshot,
                sample_size=sample_size,
            )
            handle.rows_read = source_rows
            handle.rows_written = int(validation["output_rows"])
            handle.bytes_read = csv_path.stat().st_size
            handle.bytes_written = temporary_output.stat().st_size
            handle.files_read = 1
            handle.casts_invalid = sum(invalid_counts.values())
            handle.duplicate_rows = int(validation["duplicate_primary_key_rows"])
            handle.quarantine_rows = 0
            handle.extra.update(
                {
                    "status": validation["status"],
                    "codec": codec,
                    "row_group_size": row_group_size,
                    "source_file": source_file,
                    "input_shape": "one-extracted-csv",
                    "sample_seed": validation["sample_seed"],
                    "sample_size": validation["sample_size"],
                    "sample_mismatches": validation["sample_mismatches"],
                }
            )
        con.close()
        con = None

        evidence: dict[str, Any] = {
            "format_version": _MANIEST_FORMAT_VERSION, 
            "table": "estabelecimento",
            "source": {
                "file": source_file,
                "snapshot": source_snapshot,
                "csv_path": csv_path.name,
                "sha256": source_sha256,
                "bytes": csv_path.stat().st_size,
            },
            "output": {
                "path": output_path.name,
                "sha256": _sha256(temporary_output),
                "bytes": temporary_output.stat().st_size,
                "rows": int(validation["output_rows"]),
                "schema_version": spec.schema_version,
                "codec": codec,
                "row_group_size": row_group_size,
            },
            "casts_invalid": invalid_counts,
            "validation": validation,
            "metrics": recorder.to_envelope(),
        }
        if validation["status"] != "passed":
            failure = {**evidence, "output": {**evidence["output"], "published": False}}
            _write_json_atomic(failure_path, failure)
            raise CanonicalValidationError("; ".join(validation["failures"]), failure)

        temporary_manifest.write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        _publish_pair(
            temporary_output,
            output_path,
            temporary_manifest,
            manifest_path,
        )
        failure_path.unlink(missing_ok=True)
        return CanonicalPartResult(output_path, manifest_path, False, evidence)
    except CanonicalValidationError:
        raise
    except Exception as exc:
        if validation is None:
            failure = {
                "format_version": _MANIEST_FORMAT_VERSION, 
                "table": "estabelecimento",
                "source": {
                    "file": source_file,
                    "snapshot": source_snapshot,
                    "csv_path": csv_path.name,
                    "sha256": source_sha256,
                    "bytes": csv_path.stat().st_size,
                },
                "output": {"path": output_path.name, "published": False},
                "casts_invalid": invalid_counts,
                "validation": {
                    "status": "failed",
                    "failures": [str(exc)],
                    "source_rows": source_rows,
                },
                "metrics": recorder.to_envelope(),
            }
            _write_json_atomic(failure_path, failure)
        raise
    finally:
        if con is not None:
            con.close()
        temporary_output.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        try:
            work_root.rmdir()
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--codec", default="ZSTD")
    parser.add_argument("--row-group-size", type=int, default=200_000)
    parser.add_argument("--sample-size", type=int, default=1_000)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = write_estabelecimento_part(
            args.csv,
            args.output,
            source_file=args.source_file,
            source_snapshot=args.snapshot,
            work_dir=args.work_dir,
            codec=args.codec,
            row_group_size=args.row_group_size,
            sample_size=args.sample_size,
            resume=not args.no_resume,
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
    action = "reused" if result.reused else "written"
    print(f"canonical estabelecimento part {action}: {result.output_path}")
    print(f"manifest: {result.manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "CanonicalPartResult",
    "CanonicalValidationError",
    "main",
    "write_estabelecimento_part",
]
