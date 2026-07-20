"""Write one shadow canonical Parquet part for a registry-declared main table
(RFC 0001 Phase 2/3).

Table-driven: any ``TableSpec`` with a canonical ``ParquetSpec`` contract can
be written through the same path. Entity-specific behavior comes only from
the registry contract (``duplicate_policy``, column casts, primary key) --
there is no parallel per-entity orchestration function.

Two entry points, chosen by physical layout, not by entity name:

- :func:`write_canonical_part` -- one physical source CSV in, one canonical
  Parquet part out. Correct whenever duplicate validation is legitimately
  part-local: ``estabelecimento`` (``duplicate_policy="fail"`` -- any
  duplicate full key fails the part closed, so there is nothing to combine
  across parts) and, in general, any table whose ``sources.py`` inventory
  has exactly one physical file for its kind.
- :func:`write_canonical_dataset` -- ALL physical parts of a multi-file
  table in, one canonical Parquet out. Required for a table with more than
  one physical source file AND ``duplicate_policy="deterministic-collapse"``
  (``empresa`` today; ``socio`` once a later slice extends canonical
  coverage to it): a primary key repeated across two different physical
  ZIPs (e.g. ``Empresas0.zip`` and ``Empresas7.zip``) can only be collapsed
  to one surviving row if both parts are in the SAME deduplication scope.
  Feeding one such part at a time into :func:`write_canonical_part` would
  let that key survive once in EACH part's output, silently violating the
  contract's own primary-key uniqueness guarantee -- the same class of
  cross-part blind spot issue #100 found for estabelecimento's read-only
  uniqueness audit, except here it would corrupt the WRITTEN canonical
  data rather than just an evidence report. ``write_canonical_part`` refuses
  to run against such a table (see the guard near its top) rather than
  silently producing an apparently-valid-but-incomplete part.

:func:`write_estabelecimento_canonical_part` is a thin backward-compatible
wrapper over ``write_canonical_part`` kept for existing callers
(``canonical_history.py``, tests).

The command consumes an already-extracted RFB CSV. It deliberately does not
fetch, publish, or feed the monthly product pipeline. The real historical
empresa run (all ten parts against a live snapshot) is a separate slice --
this module only has to be correct against synthetic/offline fixtures here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import sys
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

import duckdb

from . import metrics, registry, sources, transform

_SAMPLE_SEED = 42
# Experimental physical profile for the shadow slice; ParquetSpec keeps these
# choices open until real-run evidence exists.
_CODEC = "ZSTD"
_ROW_GROUP_SIZE = 200_000
_DEFAULT_TABLE = "estabelecimento"
_CONFLICT_SAMPLE_LIMIT = 5
_KNOWN_DUPLICATE_POLICIES = ("fail", "deterministic-collapse")
_SOURCE_FILE_TAG = "_source_file_tag"


def _raw_table_name(table_name: str) -> str:
    return f"_raw_{table_name}_shadow"


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
    # Distinct duplicate-KEY count, separate from duplicate_key_rows (excess
    # ROWS beyond 1 per key) -- added for #97 slice 2 (empresa). Always 0 on
    # an "ok" report for duplicate_policy="fail" tables (any duplicate there
    # fails the part before an "ok" report is ever built); meaningful for
    # duplicate_policy="deterministic-collapse" tables whether the collapse
    # found 0 or more duplicate keys.
    duplicate_key_count: int = 0
    # Among duplicate keys, how many have genuinely CONFLICTING payloads
    # (different field values for the same key, not just a repeated row) --
    # same distinction transform._dedupe_cnpj_basico_table makes for the
    # production empresa/simples path (see issue #76: the deterministic
    # collapse applied to these is transitional, not a verified semantic
    # truth). Only ever nonzero for duplicate_policy="deterministic-collapse"
    # tables.
    conflicting_key_count: int = 0
    conflicting_sample: list[dict[str, object]] = field(default_factory=list)

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


def _spec(table_name: str) -> tuple[registry.TableSpec, registry.ParquetSpec]:
    table = registry.main_table(table_name)
    if table.canonical is None:
        raise RuntimeError(f"{table_name}: no canonical contract in the registry")
    return table, table.canonical


def _validate_policy(table_name: str, spec: registry.ParquetSpec) -> None:
    """Fail closed on an unsupported/inconsistent ``duplicate_policy``.

    Called unconditionally, before any data is even loaded -- a bad policy
    must fail even when the input happens to contain zero duplicate keys.
    ``ParquetSpec.__post_init__`` already restricts the value through normal
    construction, so this is unreachable via the public registry API; it
    only matters against a spec that bypassed that validation (as the tests
    do deliberately, via ``object.__setattr__``, to prove this defense
    actually fires).
    """
    if spec.duplicate_policy not in _KNOWN_DUPLICATE_POLICIES:
        raise RuntimeError(f"{table_name}: unsupported duplicate_policy {spec.duplicate_policy!r}")


def _expected_source_files(table: registry.TableSpec) -> tuple[str, ...]:
    """The complete set of physical ZIP names ``sources.canonical_inventory()``
    declares for this table's kind (e.g. ``Empresas0.zip``..``Empresas9.zip``
    for empresa; a single ``Simples.zip`` for simples)."""
    return tuple(
        sorted(spec.name for spec in sources.canonical_inventory() if spec.kind == table.kind)
    )


def _key_diagnostics(
    con: duckdb.DuckDBPyConnection, raw_table: str, spec: registry.ParquetSpec
) -> tuple[dict[str, int], int, int]:
    """(required-key failures per component, distinct duplicate-key count,
    excess-row count).

    Required-key failures are counted over EVERY row regardless of key
    validity -- a blank/null key component is itself the failure. Duplicate/
    excess counts are computed only over rows where every key component is
    present: a blank/null key is a key-integrity failure, never an ordinary
    duplicate -- it never matches another row via SQL `=`, and grouping every
    NULL together would misreport genuinely distinct null-keyed rows as
    "duplicates" of each other.
    """
    failures = {
        name: int(
            con.execute(
                f"SELECT COUNT(*) FROM {raw_table} WHERE "
                f"{registry.quote_identifier(name)} IS NULL OR "
                f"TRIM({registry.quote_identifier(name)}) = ''"
            ).fetchone()[0]
        )
        for name in spec.primary_key
    }
    keys = [registry.quote_identifier(name) for name in spec.primary_key]
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    row = con.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT AS duplicate_keys,
            COALESCE(SUM(n - 1), 0)::BIGINT AS excess_rows
        FROM (
            SELECT COUNT(*)::BIGINT AS n
            FROM {raw_table}
            WHERE {valid}
            GROUP BY {", ".join(keys)}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return failures, int(row[0]), int(row[1])


def _conflict_diagnostics(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    spec: registry.ParquetSpec,
    source_columns: tuple[str, ...],
    *,
    sample_limit: int = _CONFLICT_SAMPLE_LIMIT,
    source_file_column: str | None = None,
) -> tuple[int, list[dict[str, object]]]:
    """(conflicting-key count, bounded sample) among duplicate keys.

    Same algorithm as ``transform._dedupe_cnpj_basico_table``'s struct-based
    distinctness check, generalized to any registry ``primary_key``: a
    duplicate key whose rows all pack into the SAME struct-of-every-source-
    column is an exact (parsed-value-identical) duplicate; a key with more
    than one distinct struct has rows that genuinely disagree on some field.
    Only called for tables whose declared ``duplicate_policy`` is
    ``"deterministic-collapse"`` -- ``"fail"`` tables never reach this (any
    duplicate there fails the part before collapse is even considered).

    ``source_file_column``, when given (dataset-level, multi-part calls
    only), attaches a ``"source_files"`` entry to each sampled conflict
    listing which physical parts contributed to it -- evidence must
    identify not just the key but where the conflicting rows came from.
    This runs as a second, targeted query scoped to only the (small,
    bounded-by-``sample_limit``) sampled keys, not to every conflicting key
    -- computing it for the full set up front is the same class of
    unbounded-aggregate mistake that caused the OOM fixed in
    ``estabelecimento_key_audit.py`` (issue #100).
    """
    keys = [registry.quote_identifier(name) for name in spec.primary_key]
    key_list = ", ".join(keys)
    t_keys = ", ".join(f"t.{key}" for key in keys)
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    join_on = " AND ".join(f"t.{key} = dk.{key}" for key in keys)
    cols = [registry.quote_identifier(name) for name in source_columns]
    struct_expr = "{" + ", ".join(f"'{c}': t.{c}" for c in cols) + "}"
    key_struct = (
        "{"
        + ", ".join(f"'{name}': c.{registry.quote_identifier(name)}" for name in spec.primary_key)
        + "}"
    )

    row = con.execute(
        f"""
        WITH dupe_keys AS (
            SELECT {key_list} FROM {raw_table}
            WHERE {valid}
            GROUP BY {key_list} HAVING COUNT(*) > 1
        ),
        conflicting_keys AS (
            SELECT {t_keys}
            FROM {raw_table} t
            JOIN dupe_keys dk ON {join_on}
            GROUP BY {t_keys}
            HAVING COUNT(DISTINCT {struct_expr}) > 1
        )
        SELECT COUNT(*), list({key_struct} ORDER BY {key_list})[1:{int(sample_limit)}]
        FROM conflicting_keys c
        """
    ).fetchone()
    total = int(row[0])
    sample: list[dict[str, object]] = [dict(entry) for entry in (row[1] or [])]

    if source_file_column and sample:
        for entry in sample:
            key_match = " AND ".join(
                f"{registry.quote_identifier(name)} = {_literal(str(entry[name]))}"
                for name in spec.primary_key
            )
            files = con.execute(
                f"SELECT list(DISTINCT {source_file_column} ORDER BY {source_file_column}) "
                f"FROM {raw_table} WHERE {key_match}"
            ).fetchone()[0]
            entry["source_files"] = list(files or [])

    return total, sample


def _collapse_deterministic(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    spec: registry.ParquetSpec,
    source_columns: tuple[str, ...],
    *,
    extra_tiebreak_columns: tuple[str, ...] = (),
) -> None:
    """Collapse ``raw_table`` to 1 row per valid (non-blank) primary-key value.

    Same algorithm as ``transform._dedupe_cnpj_basico_table``: deterministic
    full-row tiebreak (``ORDER BY`` every source column, not just the key --
    a key-only order wouldn't break ties between duplicate rows at all), so
    the survivor depends only on row VALUES, never on input file/row order
    (a rerun with the same rows in a different file order collapses to the
    identical output). Rows with any blank/null key component are left
    untouched -- see ``_key_diagnostics``'s docstring for why grouping every
    NULL together would be unsafe.

    ``extra_tiebreak_columns`` is appended AFTER every payload column --
    dataset-level (multi-part) calls pass the per-row source-file tag column
    here so that two rows with byte-identical payloads but different origin
    files (e.g. the exact same empresa row republished in two different
    ``EmpresasN.zip`` parts) still resolve to a deterministic survivor and a
    deterministic surviving ``_source_file`` lineage, instead of leaving
    that choice to an unspecified tie in ``row_number()``.
    """
    keys = [registry.quote_identifier(name) for name in spec.primary_key]
    key_list = ", ".join(keys)
    valid = " AND ".join(f"{key} IS NOT NULL AND TRIM({key}) <> ''" for key in keys)
    invalid = " OR ".join(f"{key} IS NULL OR TRIM({key}) = ''" for key in keys)
    tiebreak_cols = ", ".join(
        [registry.quote_identifier(name) for name in source_columns] + list(extra_tiebreak_columns)
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {raw_table} AS
        SELECT * FROM {raw_table} WHERE {invalid}
        UNION ALL
        SELECT * FROM {raw_table} WHERE {valid}
        QUALIFY row_number() OVER (PARTITION BY {key_list} ORDER BY {tiebreak_cols}) = 1
        """
    )


def _invalid_casts(
    con: duckdb.DuckDBPyConnection, raw_table: str, spec: registry.ParquetSpec
) -> dict[str, int]:
    """Count, per column, nonblank raw values whose cast produced NULL.

    Any typed column declared ``invalid_policy="null-and-count"`` -- not
    just ``DATE`` columns. The original estabelecimento-only slice only ever
    had DATE columns using this policy; empresa's ``capital_social``
    (DECIMAL) is the first non-DATE user, so the DATE-only filter that used
    to gate this loop would have silently reported an empty dict for it.
    """
    result: dict[str, int] = {}
    for column in spec.columns:
        if column.invalid_policy != "null-and-count":
            continue
        raw = f"src.{registry.quote_identifier(column.source)}"
        cast = registry.canonical_expression_sql(column, source_alias="src")
        result[column.name] = int(
            con.execute(
                f"SELECT COUNT(*) FROM {raw_table} AS src "
                f"WHERE {raw} IS NOT NULL AND TRIM({raw}) <> '' "
                f"AND ({cast}) IS NULL"
            ).fetchone()[0]
        )
    return result


def _sample(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    spec: registry.ParquetSpec,
    requested: int,
    rows_available: int,
) -> tuple[int, str]:
    size = min(max(0, requested), rows_available)
    if not size:
        return 0, hashlib.sha256(b"[]").hexdigest()
    keys = ", ".join(registry.quote_identifier(name) for name in spec.primary_key)
    rows = con.execute(
        f"SELECT {keys} FROM {raw_table} "
        f"USING SAMPLE reservoir({size} ROWS) REPEATABLE({_SAMPLE_SEED}) "
        f"ORDER BY {keys}"
    ).fetchall()
    encoded = json.dumps(rows, default=str, separators=(",", ":")).encode()
    return len(rows), hashlib.sha256(encoded).hexdigest()


def _select_sql(
    raw_table: str, spec: registry.ParquetSpec, source_file_sql: str, snapshot: str
) -> str:
    """``source_file_sql`` is a SQL expression (over the ``src`` alias),
    evaluated per row -- a quoted literal for a single-part write, or the
    per-row source-file tag column for a dataset-level (multi-part) write.
    """
    projection = registry.canonical_projection_sql(spec, source_alias="src")
    return (
        f"SELECT\n{projection},\n"
        f'    {source_file_sql} AS "_source_file",\n'
        f'    {_literal(snapshot)} AS "_source_snapshot"\n'
        f"FROM {raw_table} AS src"
    )


def _expected_schema(spec: registry.ParquetSpec) -> list[tuple[str, str]]:
    return [
        *((column.name, column.duckdb_type) for column in spec.columns),
        *((column.name, column.duckdb_type) for column in spec.lineage),
    ]


def _sample_mismatches(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    spec: registry.ParquetSpec,
    parquet: Path,
    size: int,
    source_file_sql: str,
    snapshot: str,
) -> int:
    """``source_file_sql`` mirrors ``_select_sql``'s parameter -- a quoted
    literal for single-part writes, or ``src.<tag column>`` for dataset
    writes, so the written ``_source_file`` lineage is checked against
    whichever expression actually produced it."""
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
        elif column.cast_sql is not None:
            # Generic typed column (e.g. empresa's DECIMAL capital_social):
            # recompute the SAME cast the canonical projection used, rather
            # than comparing the raw string against the typed value directly
            # (which would almost always read as "distinct" -- "150000,00"
            # is never equal to the DECIMAL 150000.00 by naive comparison).
            expected = registry.canonical_expression_sql(column, source_alias="src")
            checks.append(f"({expected}) IS DISTINCT FROM {canonical}")
        else:
            checks.append(f"{raw} IS DISTINCT FROM {canonical}")
    checks += [
        f'can."_source_file" IS DISTINCT FROM {source_file_sql}',
        f'can."_source_snapshot" IS DISTINCT FROM {_literal(snapshot)}',
    ]
    return int(
        con.execute(
            f"""
            WITH sampled AS (
                SELECT * FROM {raw_table}
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
    source_csv: str,
    bytes_read: int,
    output: Path,
    source_file: str,
    snapshot: str,
    rows_raw: int,
    rows_canonical: int | None,
    key_failures: dict[str, int],
    duplicate_rows: int,
    duplicate_key_count: int,
    conflicting_key_count: int,
    conflicting_sample: list[dict[str, object]],
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
        source_csv=source_csv,
        source_file=source_file,
        source_snapshot=snapshot,
        output_path=str(output),
        rows_raw=rows_raw,
        rows_canonical=rows_canonical,
        bytes_read=bytes_read,
        bytes_written=output.stat().st_size if status == "ok" else None,
        required_key_failures=key_failures,
        duplicate_key_rows=duplicate_rows,
        duplicate_key_count=duplicate_key_count,
        conflicting_key_count=conflicting_key_count,
        conflicting_sample=conflicting_sample,
        invalid_casts_by_column=invalid_casts,
        sample_seed=_SAMPLE_SEED,
        sample_size=sample_size,
        sample_fingerprint=fingerprint,
        sample_mismatches=mismatches,
        schema_matches=schema_matches,
        error=error,
    )


def write_canonical_part(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv: Path,
    output: Path,
    *,
    source_file: str,
    source_snapshot: str,
    sample_size: int = 1_000,
) -> CanonicalPartReport:
    """Read one CSV with the production reader and atomically write one
    canonical Parquet part, for any main table where single-part duplicate
    validation is legitimate.

    Duplicate handling branches ONLY on the table's declared
    ``duplicate_policy`` -- there is no per-entity orchestration function:

    - ``"fail"`` (estabelecimento): any duplicate full-key fails the part
      closed, exactly as before this table-driven generalization.
    - ``"deterministic-collapse"`` on a table with exactly one physical
      source file: duplicates -- exact or genuinely conflicting alike --
      are collapsed to one row per key via the same deterministic full-row
      tiebreak ``transform._dedupe_cnpj_basico_table`` uses in production,
      and conflicting keys are recorded as bounded evidence rather than
      failing the part. This is the current, transitional production
      policy (see issue #76) -- this writer does not add fail-on-conflict
      or quarantine behavior beyond what the registry already declares.

    A table with MORE than one physical source file AND
    ``duplicate_policy="deterministic-collapse"`` (empresa today) is
    refused here -- a key duplicated *across* two different physical ZIPs
    would never be visible to two separate single-part calls, so each
    would emit an apparently-valid part with that key surviving in both,
    silently violating the canonical primary-key contract. Use
    :func:`write_canonical_dataset` with the complete physical part set
    instead.

    A registry ``duplicate_policy`` value this function doesn't recognize
    fails closed with a ``RuntimeError`` rather than silently defaulting to
    either branch -- ``ParquetSpec.__post_init__`` already restricts the
    value to these two, so this should be unreachable in practice, but a
    silent fallback would be worse than a loud one if that ever changes.
    """
    if not csv.exists():
        raise FileNotFoundError(csv)
    if not source_file:
        raise ValueError("source_file cannot be empty")
    if not sources.is_valid_month(source_snapshot):
        raise ValueError(f"source_snapshot must be YYYY-MM, got {source_snapshot!r}")
    if sample_size < 0:
        raise ValueError("sample_size cannot be negative")

    table, spec = _spec(table_name)
    _validate_policy(table_name, spec)
    expected_parts = _expected_source_files(table)
    if len(expected_parts) > 1 and spec.duplicate_policy == "deterministic-collapse":
        raise ValueError(
            f"{table_name}: has {len(expected_parts)} physical source parts "
            f"({expected_parts[0]}..{expected_parts[-1]}) and "
            "duplicate_policy='deterministic-collapse' -- a key duplicated across "
            "parts would not be detected by single-part processing; call "
            "write_canonical_dataset() with the complete part set instead"
        )

    raw_table = _raw_table_name(table_name)
    transform._create_table_from_csvs(con, raw_table, [csv], table.source)  # noqa: SLF001
    rows_raw = int(con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0])
    bytes_read = csv.stat().st_size
    key_failures, duplicate_keys, excess_rows = _key_diagnostics(con, raw_table, spec)

    conflicting_count = 0
    conflicting_sample: list[dict[str, object]] = []
    will_collapse = duplicate_keys and spec.duplicate_policy == "deterministic-collapse"
    if will_collapse:
        conflicting_count, conflicting_sample = _conflict_diagnostics(
            con, raw_table, spec, table.source.columns
        )

    errors: list[str] = []
    if any(key_failures.values()):
        errors.append(f"required key failures: {key_failures}")
    if duplicate_keys and spec.duplicate_policy == "fail":
        errors.append(f"duplicate full-CNPJ excess rows: {excess_rows}")
    if errors:
        message = "; ".join(errors)
        report = _make_report(
            status="failed",
            spec=spec,
            source_csv=str(csv),
            bytes_read=bytes_read,
            output=output,
            source_file=source_file,
            snapshot=source_snapshot,
            rows_raw=rows_raw,
            rows_canonical=None,
            key_failures=key_failures,
            duplicate_rows=excess_rows,
            duplicate_key_count=duplicate_keys,
            conflicting_key_count=conflicting_count,
            conflicting_sample=conflicting_sample,
            invalid_casts={},
            sample_size=0,
            fingerprint=hashlib.sha256(b"[]").hexdigest(),
            mismatches=None,
            schema_matches=None,
            error=message,
        )
        raise CanonicalValidationError(message, report)

    # Raw invalid-cast evidence must be counted BEFORE any collapse mutates
    # raw_table -- a discarded duplicate row's malformed value would
    # otherwise vanish from the count along with the row itself. estabele-
    # cimento never collapses, so this reorder is a no-op for it.
    invalid_casts = _invalid_casts(con, raw_table, spec)

    rows_expected = rows_raw
    if will_collapse:
        _collapse_deterministic(con, raw_table, spec, table.source.columns)
        rows_expected = rows_raw - excess_rows

    actual_sample, fingerprint = _sample(con, raw_table, spec, sample_size, rows_expected)

    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    partial.unlink(missing_ok=True)
    try:
        con.execute(
            f"COPY ({_select_sql(raw_table, spec, _literal(source_file), source_snapshot)}) "
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
            raw_table,
            spec,
            partial,
            actual_sample,
            _literal(source_file),
            source_snapshot,
        )
        errors = []
        if rows_canonical != rows_expected:
            errors.append(
                f"row-count mismatch: expected={rows_expected}, canonical={rows_canonical}"
            )
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
                source_csv=str(csv),
                bytes_read=bytes_read,
                output=output,
                source_file=source_file,
                snapshot=source_snapshot,
                rows_raw=rows_raw,
                rows_canonical=rows_canonical,
                key_failures=key_failures,
                duplicate_rows=excess_rows,
                duplicate_key_count=duplicate_keys,
                conflicting_key_count=conflicting_count,
                conflicting_sample=conflicting_sample,
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
        source_csv=str(csv),
        bytes_read=bytes_read,
        output=output,
        source_file=source_file,
        snapshot=source_snapshot,
        rows_raw=rows_raw,
        rows_canonical=rows_canonical,
        key_failures=key_failures,
        duplicate_rows=excess_rows,
        duplicate_key_count=duplicate_keys,
        conflicting_key_count=conflicting_count,
        conflicting_sample=conflicting_sample,
        invalid_casts=invalid_casts,
        sample_size=actual_sample,
        fingerprint=fingerprint,
        mismatches=mismatches,
        schema_matches=schema_matches,
    )


def _load_dataset_parts(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    table: registry.TableSpec,
    parts: Sequence[tuple[Path, str]],
) -> None:
    """Load every physical CSV part into one combined ``raw_table``, tagging
    each row with its own part's source filename (``_SOURCE_FILE_TAG``).

    Each part is read separately through
    ``transform._create_table_from_csvs`` -- the exact same production
    reader call a single-part write already makes, just once per physical
    file -- so per-file reader semantics (encoding fallback, empty-file
    handling) are unchanged. The parts are then combined with UNION ALL so
    every downstream diagnostic (key/conflict/collapse) sees the full
    logical dataset, not one physical file at a time.
    """
    cols = ", ".join(registry.quote_identifier(name) for name in table.source.columns)
    selects: list[str] = []
    staging_tables: list[str] = []
    for index, (csv, source_file) in enumerate(parts):
        staging = f"{raw_table}_p{index}"
        staging_tables.append(staging)
        transform._create_table_from_csvs(con, staging, [csv], table.source)  # noqa: SLF001
        selects.append(
            f"SELECT {cols}, {_literal(source_file)} AS {_SOURCE_FILE_TAG} FROM {staging}"
        )
    con.execute(f"CREATE OR REPLACE TABLE {raw_table} AS\n" + "\nUNION ALL\n".join(selects))
    for staging in staging_tables:
        con.execute(f"DROP TABLE IF EXISTS {staging}")


def write_canonical_dataset(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    parts: Sequence[tuple[Path, str]],
    output: Path,
    *,
    source_snapshot: str,
    sample_size: int = 1_000,
) -> CanonicalPartReport:
    """Read ALL physical parts of a multi-file main table and atomically
    write one canonical Parquet part covering the complete logical dataset.

    Required for any table with more than one physical source file (per
    ``sources.canonical_inventory()``) and ``duplicate_policy=
    "deterministic-collapse"`` -- empresa today. Key diagnostics, conflict
    classification and deterministic collapse all run AFTER every part has
    been combined into one raw table, so a primary key duplicated across
    two different physical ZIPs is deduplicated exactly like a duplicate
    found within a single part -- unlike :func:`write_canonical_part`,
    which this function's caller must not use for such a table (see the
    guard there).

    ``parts`` must be exactly the complete expected physical set for this
    table's kind (``Empresas0.zip``..``Empresas9.zip`` for empresa) --
    fewer, extra, or substituted filenames fail closed rather than silently
    producing an apparently-valid-but-incomplete canonical part.

    For rows with byte-identical payloads duplicated across different
    parts, the deterministic collapse tiebreak also orders on each row's
    origin filename as a FINAL tiebreak (after every payload column), so
    both the surviving row and its surviving ``_source_file`` lineage are
    reproducible regardless of input part/row order -- see
    ``_collapse_deterministic``.
    """
    if not parts:
        raise ValueError("parts cannot be empty")
    for csv, source_file in parts:
        if not csv.exists():
            raise FileNotFoundError(csv)
        if not source_file:
            raise ValueError("source_file cannot be empty")
    if not sources.is_valid_month(source_snapshot):
        raise ValueError(f"source_snapshot must be YYYY-MM, got {source_snapshot!r}")
    if sample_size < 0:
        raise ValueError("sample_size cannot be negative")

    table, spec = _spec(table_name)
    _validate_policy(table_name, spec)

    expected = _expected_source_files(table)
    provided = tuple(sorted(source_file for _, source_file in parts))
    if provided != expected:
        missing = sorted(set(expected) - set(provided))
        unexpected = sorted(set(provided) - set(expected))
        raise ValueError(
            f"{table_name}: incomplete physical part set -- missing={missing!r}, "
            f"unexpected={unexpected!r} (expected exactly {list(expected)!r})"
        )

    raw_table = _raw_table_name(table_name)
    _load_dataset_parts(con, raw_table, table, parts)
    rows_raw = int(con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0])
    bytes_read = sum(csv.stat().st_size for csv, _ in parts)
    source_csv = ",".join(str(csv) for csv, _ in sorted(parts, key=lambda item: item[1]))
    source_file = ",".join(expected)
    key_failures, duplicate_keys, excess_rows = _key_diagnostics(con, raw_table, spec)

    conflicting_count = 0
    conflicting_sample: list[dict[str, object]] = []
    will_collapse = duplicate_keys and spec.duplicate_policy == "deterministic-collapse"
    if will_collapse:
        conflicting_count, conflicting_sample = _conflict_diagnostics(
            con,
            raw_table,
            spec,
            table.source.columns,
            source_file_column=_SOURCE_FILE_TAG,
        )

    errors: list[str] = []
    if any(key_failures.values()):
        errors.append(f"required key failures: {key_failures}")
    if duplicate_keys and spec.duplicate_policy == "fail":
        errors.append(f"duplicate full-CNPJ excess rows: {excess_rows}")
    if errors:
        message = "; ".join(errors)
        report = _make_report(
            status="failed",
            spec=spec,
            source_csv=source_csv,
            bytes_read=bytes_read,
            output=output,
            source_file=source_file,
            snapshot=source_snapshot,
            rows_raw=rows_raw,
            rows_canonical=None,
            key_failures=key_failures,
            duplicate_rows=excess_rows,
            duplicate_key_count=duplicate_keys,
            conflicting_key_count=conflicting_count,
            conflicting_sample=conflicting_sample,
            invalid_casts={},
            sample_size=0,
            fingerprint=hashlib.sha256(b"[]").hexdigest(),
            mismatches=None,
            schema_matches=None,
            error=message,
        )
        raise CanonicalValidationError(message, report)

    # Same ordering fix as write_canonical_part: count raw invalid casts
    # before the collapse can discard the row that held them.
    invalid_casts = _invalid_casts(con, raw_table, spec)

    rows_expected = rows_raw
    if will_collapse:
        _collapse_deterministic(
            con,
            raw_table,
            spec,
            table.source.columns,
            extra_tiebreak_columns=(_SOURCE_FILE_TAG,),
        )
        rows_expected = rows_raw - excess_rows

    actual_sample, fingerprint = _sample(con, raw_table, spec, sample_size, rows_expected)

    source_file_sql = f"src.{_SOURCE_FILE_TAG}"
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    partial.unlink(missing_ok=True)
    try:
        con.execute(
            f"COPY ({_select_sql(raw_table, spec, source_file_sql, source_snapshot)}) "
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
            raw_table,
            spec,
            partial,
            actual_sample,
            source_file_sql,
            source_snapshot,
        )
        errors = []
        if rows_canonical != rows_expected:
            errors.append(
                f"row-count mismatch: expected={rows_expected}, canonical={rows_canonical}"
            )
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
                source_csv=source_csv,
                bytes_read=bytes_read,
                output=output,
                source_file=source_file,
                snapshot=source_snapshot,
                rows_raw=rows_raw,
                rows_canonical=rows_canonical,
                key_failures=key_failures,
                duplicate_rows=excess_rows,
                duplicate_key_count=duplicate_keys,
                conflicting_key_count=conflicting_count,
                conflicting_sample=conflicting_sample,
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
        source_csv=source_csv,
        bytes_read=bytes_read,
        output=output,
        source_file=source_file,
        snapshot=source_snapshot,
        rows_raw=rows_raw,
        rows_canonical=rows_canonical,
        key_failures=key_failures,
        duplicate_rows=excess_rows,
        duplicate_key_count=duplicate_keys,
        conflicting_key_count=conflicting_count,
        conflicting_sample=conflicting_sample,
        invalid_casts=invalid_casts,
        sample_size=actual_sample,
        fingerprint=fingerprint,
        mismatches=mismatches,
        schema_matches=schema_matches,
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
    """Backward-compatible wrapper -- estabelecimento only. New code should
    call :func:`write_canonical_part` directly with an explicit table name.
    """
    return write_canonical_part(
        con,
        _DEFAULT_TABLE,
        csv,
        output,
        source_file=source_file,
        source_snapshot=source_snapshot,
        sample_size=sample_size,
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
        duplicate_key_count=report.duplicate_key_count,
        conflicting_key_count=report.conflicting_key_count,
    )


def run_canonical_shadow_part(
    table_name: str,
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
    """Use the production DuckDB profile and persist quality/resource
    evidence, for any main table with a registry canonical contract."""
    _, spec = _spec(table_name)
    work_dir.mkdir(parents=True, exist_ok=True)
    database = work_dir / f"canonical-{table_name}.duckdb"
    temp = work_dir / "duckdb_tmp"
    recorder = metrics.MetricsRecorder(
        month=source_snapshot,
        schema_version=str(spec.schema_version),
        filesystem_path=work_dir,
    )
    con = _connection(database, temp)
    recorder.capture_pragmas(con)
    report: CanonicalPartReport | None = None
    try:
        with recorder.stage(
            f"canonical_{table_name}_part", duckdb_tmp_dir=temp, workdir=work_dir
        ) as handle:
            try:
                report = write_canonical_part(
                    con,
                    table_name,
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
    """Backward-compatible wrapper -- estabelecimento only. New code should
    call :func:`run_canonical_shadow_part` directly with an explicit table
    name. ``canonical_history.py`` still calls this exact entry point.
    """
    return run_canonical_shadow_part(
        _DEFAULT_TABLE,
        csv,
        output,
        source_file=source_file,
        source_snapshot=source_snapshot,
        work_dir=work_dir,
        report_path=report_path,
        metrics_path=metrics_path,
        sample_size=sample_size,
        keep_workdir=keep_workdir,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        default=_DEFAULT_TABLE,
        help=f"main table to write a canonical part for (default: {_DEFAULT_TABLE})",
    )
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
        report = run_canonical_shadow_part(
            args.table,
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
        RuntimeError,
        ValueError,
        duckdb.Error,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"canonical shadow OK ({args.table}) — {report.rows_canonical:,} rows, "
        f"{report.invalid_casts_total} invalid cast(s), output={args.output}"
    )
    print(f"quality report: {quality}")
    print(f"metrics: {resource_metrics}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
