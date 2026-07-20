"""Shadow writer for canonical RFC 0001 datasets.

This module implements the first Phase 2 vertical slice: one extracted
``estabelecimento`` CSV becomes one typed canonical Parquet part plus an
immutable manifest. It does not participate in ``transform_snapshot`` and does
not feed public products.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from . import metrics, registry, transform
from .sources import is_valid_month

_MANIFEST_FORMAT_VERSION = 1
_ALLOWED_CODECS = frozenset({"ZSTD", "LZ4", "SNAPPY", "UNCOMPRESSED"})


class CanonicalValidationError(RuntimeError):
    """Raised when a shadow canonical part fails a load-bearing gate."""


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


def _typed_invalid_counts(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    source_table: str,
) -> dict[str, int]:
    typed = [column for column in spec.columns if column.cast_sql is not None]
    if not typed:
        return {}
    metrics_sql: list[str] = []
    for column in typed:
        source = (
            f"{registry.quote_identifier('raw_est')}."
            f"{registry.quote_identifier(column.source)}"
        )
        cast = registry.canonical_expression_sql(column, source_alias="raw_est")
        alias = registry.quote_identifier(column.name)
        metrics_sql.append(
            "COUNT(*) FILTER (WHERE "
            f"{source} IS NOT NULL AND {source} <> '' AND {cast} IS NULL) AS {alias}"
        )
    cursor = con.execute(
        "SELECT "
        + ", ".join(metrics_sql)
        + f" FROM {registry.quote_identifier(source_table)} AS raw_est"
    )
    row = cursor.fetchone()
    return {
        description[0]: int(value)
        for description, value in zip(cursor.description, row, strict=True)
    }


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


def _validate_output(
    con: duckdb.DuckDBPyConnection,
    spec: registry.ParquetSpec,
    *,
    source_rows: int,
    output_path: Path,
) -> dict[str, Any]:
    relation = _parquet_relation(output_path)
    output_rows = int(con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])
    schema = _actual_schema(con, output_path)
    expected_schema = _expected_schema(spec)

    key_columns = [
        next(column for column in spec.columns if column.name == key)
        for key in spec.primary_key
    ]
    missing_terms: list[str] = []
    valid_terms: list[str] = []
    for column in key_columns:
        identifier = registry.quote_identifier(column.name)
        missing_terms.append(f"{identifier} IS NULL")
        valid_terms.append(f"{identifier} IS NOT NULL")
        if column.duckdb_type == "VARCHAR":
            missing_terms.append(f"{identifier} = ''")
            valid_terms.append(f"{identifier} <> ''")

    missing_key_rows = int(
        con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE " + " OR ".join(missing_terms)
        ).fetchone()[0]
    )
    key_sql = ", ".join(registry.quote_identifier(key) for key in spec.primary_key)
    duplicate_rows = int(
        con.execute(
            "SELECT COALESCE(SUM(group_size - 1), 0) FROM ("
            f"SELECT COUNT(*) AS group_size FROM {relation} "
            f"WHERE {' AND '.join(valid_terms)} GROUP BY {key_sql} HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]
    )

    critical_terms: list[str] = []
    for column in spec.columns:
        if not column.publication_critical:
            continue
        identifier = registry.quote_identifier(column.name)
        critical_terms.append(f"{identifier} IS NULL")
        if column.duckdb_type == "VARCHAR":
            critical_terms.append(f"{identifier} = ''")
    for column in spec.lineage:
        identifier = registry.quote_identifier(column.name)
        critical_terms.append(f"{identifier} IS NULL")
        if column.duckdb_type == "VARCHAR":
            critical_terms.append(f"{identifier} = ''")
    critical_predicate = " OR ".join(critical_terms) or "FALSE"
    critical_missing_rows = int(
        con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE {critical_predicate}"
        ).fetchone()[0]
    )

    gates = {
        "row_count_match": output_rows == source_rows,
        "schema_match": schema == expected_schema,
        "primary_key_missing_rows": missing_key_rows,
        "duplicate_primary_key_rows": duplicate_rows,
        "critical_missing_rows": critical_missing_rows,
    }
    failures: list[str] = []
    if not gates["row_count_match"]:
        failures.append(f"row count {source_rows} -> {output_rows}")
    if not gates["schema_match"]:
        failures.append(f"schema {schema!r} != {expected_schema!r}")
    if missing_key_rows:
        failures.append(f"{missing_key_rows} rows with missing primary key")
    if duplicate_rows:
        failures.append(f"{duplicate_rows} duplicate primary-key rows")
    if critical_missing_rows:
        failures.append(f"{critical_missing_rows} rows with missing critical values")
    if failures:
        raise CanonicalValidationError("; ".join(failures))

    return {
        **gates,
        "status": "passed",
        "source_rows": source_rows,
        "output_rows": output_rows,
        "schema": [{"name": name, "type": kind} for name, kind in schema],
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
) -> dict[str, Any] | None:
    if not output_path.exists() or not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
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
            and payload["validation"]["status"] == "passed"
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    return payload if matches else None


def _configure_connection(
    con: duckdb.DuckDBPyConnection,
    *,
    temp_directory: Path,
    recorder: metrics.MetricsRecorder,
) -> None:
    memory_gb = transform.pick_memory_limit_gb()
    threads = transform.pick_threads()
    temp_directory.mkdir(parents=True, exist_ok=True)
    con.execute(f"PRAGMA memory_limit='{memory_gb}GB'")
    con.execute(f"PRAGMA temp_directory={_sql_literal(temp_directory)}")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={threads}")
    recorder.capture_pragmas(con)


def write_estabelecimento_part(
    csv_path: Path,
    output_path: Path,
    *,
    source_file: str,
    source_snapshot: str,
    work_dir: Path | None = None,
    codec: str = "ZSTD",
    row_group_size: int = 200_000,
    resume: bool = True,
) -> CanonicalPartResult:
    """Write and validate one canonical ``estabelecimento`` Parquet part.

    The source CSV is read through the exact registry-backed legacy reader.
    The final Parquet and manifest only appear after all gates pass. Existing
    output is reused only when its manifest and checksums match the full input
    and writer configuration.
    """
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

    table = registry.main_table("estabelecimento")
    spec = table.canonical
    if spec is None:  # pragma: no cover - registry construction guarantees it
        raise RuntimeError("estabelecimento canonical contract is missing")

    output_path = output_path.resolve()
    if output_path == csv_path:
        raise ValueError("output_path must differ from csv_path")
    manifest_path = _manifest_path(output_path)
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
        )
        if reusable is not None:
            return CanonicalPartResult(output_path, manifest_path, True, reusable)

    output_path.parent.mkdir(parents=True, exist_ok=True)
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
    # The final rename is atomic only when the temporary Parquet lives on the
    # destination filesystem. It deliberately does not live under run_dir.
    temp_output = output_path.with_name(f".{output_path.name}.{token}.tmp.parquet")
    temp_manifest = manifest_path.with_name(f".{manifest_path.name}.{token}.tmp")
    db_path = run_dir / "state.duckdb"
    duckdb_tmp = run_dir / "duckdb_tmp"

    recorder = metrics.MetricsRecorder(
        month=source_snapshot,
        schema_version=str(spec.schema_version),
        filesystem_path=work_root,
    )
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = duckdb.connect(str(db_path))
        _configure_connection(con, temp_directory=duckdb_tmp, recorder=recorder)
        with recorder.stage(
            "canonical_estabelecimento_part",
            duckdb_tmp_dir=duckdb_tmp,
            workdir=run_dir,
            sample_interval=0.1,
        ) as handle:
            transform._create_table_from_csvs(  # noqa: SLF001 - exact reader boundary
                con,
                "raw_estabelecimento",
                [csv_path],
                table.source,
            )
            source_rows = int(
                con.execute("SELECT COUNT(*) FROM raw_estabelecimento").fetchone()[0]
            )
            invalid_counts = _typed_invalid_counts(
                con,
                spec,
                source_table="raw_estabelecimento",
            )
            projection = registry.canonical_projection_sql(spec, source_alias="raw_est")
            lineage = ",\n".join(
                (
                    f"    {_sql_literal(source_file)} AS {registry.quote_identifier('_source_file')}",
                    f"    {_sql_literal(source_snapshot)} AS "
                    f"{registry.quote_identifier('_source_snapshot')}",
                )
            )
            con.execute(
                f"""
                COPY (
                    SELECT
                {projection},
                {lineage}
                    FROM raw_estabelecimento AS raw_est
                ) TO {_sql_literal(temp_output)}
                (FORMAT PARQUET, COMPRESSION {codec}, ROW_GROUP_SIZE {row_group_size})
                """
            )
            validation = _validate_output(
                con,
                spec,
                source_rows=source_rows,
                output_path=temp_output,
            )
            handle.rows_read = source_rows
            handle.rows_written = int(validation["output_rows"])
            handle.bytes_read = csv_path.stat().st_size
            handle.bytes_written = temp_output.stat().st_size
            handle.files_read = 1
            handle.casts_invalid = sum(invalid_counts.values())
            handle.duplicate_rows = int(validation["duplicate_primary_key_rows"])
            handle.quarantine_rows = 0
            handle.extra.update(
                {
                    "codec": codec,
                    "row_group_size": row_group_size,
                    "source_file": source_file,
                    "input_shape": "one-extracted-csv",
                }
            )
        con.close()
        con = None

        payload: dict[str, Any] = {
            "format_version": _MANIFEST_FORMAT_VERSION,
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
                "sha256": _sha256(temp_output),
                "bytes": temp_output.stat().st_size,
                "rows": int(validation["output_rows"]),
                "schema_version": spec.schema_version,
                "codec": codec,
                "row_group_size": row_group_size,
            },
            "casts_invalid": invalid_counts,
            "validation": validation,
            "metrics": recorder.to_envelope(),
        }
        temp_manifest.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temp_output, output_path)
        try:
            os.replace(temp_manifest, manifest_path)
        except OSError:
            output_path.unlink(missing_ok=True)
            raise
        return CanonicalPartResult(output_path, manifest_path, False, payload)
    finally:
        if con is not None:
            con.close()
        temp_output.unlink(missing_ok=True)
        temp_manifest.unlink(missing_ok=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        try:
            work_root.rmdir()
        except OSError:
            pass


__all__ = [
    "CanonicalPartResult",
    "CanonicalValidationError",
    "write_estabelecimento_part",
]
