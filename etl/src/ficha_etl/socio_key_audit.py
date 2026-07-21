"""Row-identity and cardinality investigation for `socio` (issue #97 slice 5).

Unlike `empresa`/`simples` (`cnpj_basico` alone) or `estabelecimento`
(`cnpj_basico`+`cnpj_ordem`+`cnpj_dv`), `socio`'s raw layout
(`registry.SOCIO_COLUMNS`) has no obvious single- or few-column identity: it
records one company-partner relationship per row, and the RFB layout does
not publish a row id. This module is evidence-only. It measures cardinality
and conflict rates for several CANDIDATE composite keys against the
complete real snapshot; it does not declare `SOCIO_CANONICAL`, does not
write a canonical socio Parquet, and does not decide a key. The
recommendation belongs in `docs/socio-key-investigation.md`, written from
this tool's measured output, not hard-coded here.

`identificador_socio` splits every row into exactly three categories with
structurally different partner-identity content (confirmed against the
complete real 2026-04 snapshot -- see the doc for the numbers), so the
candidate keys below are CATEGORY-SPECIFIC, not one flat composite tested
uniformly across all rows:

- `"1"` Pessoa Juridica -- `cnpj_cpf_socio` is always a complete, unmasked
  14-digit CNPJ. Treated as the primary partner identity on its own;
  normalized partner name is measured only as a consistency diagnostic
  (does the same CNPJ ever show a different name), not folded into the key.
- `"2"` Pessoa Fisica -- `cnpj_cpf_socio` is always masked down to its
  middle six digits (e.g. `"***816343**"`) and is NOT a reliable individual
  identifier alone: two genuinely different people can share the same
  masked value. The identity tested is masked CPF + normalized partner
  name. `faixa_etaria` (age bracket) is measured at both the identity level
  (`pf:cpf_nome_faixa`) and the relationship level
  (`pf:relationship_with_faixa`) for comparison, but is deliberately NOT
  part of the recommended identity: it is a temporally unstable attribute
  (a real person's age bracket changes as they age across snapshots) and
  is not the kind of fact `qualificacao_socio`/`data_entrada_sociedade`
  are -- those are fixed at the moment a partner relationship began.
- `"3"` Socio Estrangeiro -- `cnpj_cpf_socio` is always blank; this is not
  a data-quality gap, it is the entire foreign-partner category, which
  structurally has no CPF/CNPJ field. The only identity signal available is
  normalized partner name + `pais` (country code) -- documented as a weak
  technical identity, not a strong one.

For each category, two layers are measured:

- identity-level candidates -- "who is this partner," independent of which
  company (`<prefix>:cnpj`/`<prefix>:cnpj_nome` for PJ,
  `<prefix>:cpf`/`<prefix>:cpf_nome`/`<prefix>:cpf_nome_faixa` for PF,
  `<prefix>:nome`/`<prefix>:nome_pais` for foreign partners);
- relationship-level candidates -- `cnpj_basico` (company) + that
  category's recommended partner identity, narrowed in stages by
  `qualificacao_socio` then `data_entrada_sociedade`
  (`<prefix>:company_partner` -> `<prefix>:company_partner_qualificacao` ->
  `<prefix>:relationship`), mirroring the same "does the next column
  absorb a real conflict" technique the original flat design used.

`representante_legal`/`nome_representante_legal`/
`qualificacao_representante_legal` are measured for independent variation
within a duplicate relationship group (`_representante_independence`) but
are NOT included in any candidate key unless that measurement shows they
define a genuinely separate relationship.

Normalized partner name (`_nome_socio_norm`, built once in `_socio_base`)
strips accents (`strip_accents()`), uppercases, collapses internal
whitespace, and trims -- RFB free text in this field is not consistently
accented or cased across records, and a literal byte comparison would
wrongly treat two spellings of the same name as different people.

For each candidate key, this measures: blank/null key-component rows (a
key-integrity failure, never an ordinary duplicate -- same distinction
`canonical_shadow.py`'s writers make), distinct valid key count, duplicate
key count, excess row count, cross-part duplicate key count, and --
for RELATIONSHIP-level (company-scoped) candidates only -- how many of
those duplicate keys are "exact" (every OTHER raw column, including both
free-text name fields, also matches via real per-column comparison, never
a hash -- a hash match is evidence, not proof, of row equality) versus
"conflicting" (something else genuinely differs). IDENTITY-level
(company-unscoped) candidates skip this check: duplication there is
usually just the same partner appearing in different companies, not a
conflict to resolve, and for a category like PF the duplicate-key set can
cover nearly the entire source, making a full-row comparison both
expensive and not a meaningful measurement (see `_audit_one_candidate`'s
`compute_conflicting` parameter). A high conflict rate at a relationship
candidate means that candidate does not actually identify a real-world
fact; near-zero duplicates AND near-zero conflicts is a real, if
unproven, contract. Where residual conflicts remain (e.g. foreign
partners), they are reported and preserved, not resolved by folding more
columns like `representante_legal` into the identity without evidence
that they define a genuinely separate relationship.

Design, same discipline as `estabelecimento_key_audit.py` (issue #100):

- one ZIP processed at a time, extracted CSV deleted before the next;
- the real registry-backed reader (`transform._create_table_from_csvs`),
  not a simplified parser;
- projects ALL eleven raw socio columns (not just a fixed key) into a
  per-part Parquet checkpoint, since which columns matter is exactly the
  open question -- narrowing early would foreclose candidates before
  measuring them;
- reuses `canonical_history`'s download/extract/checksum/atomic-write
  helpers, restartable per part (source ZIP checksum + code fingerprint +
  output checksum must all match to reuse);
- the global cross-part aggregation reads only the ten per-part Parquets
  together (never the raw CSVs at once), and -- learning from the exact
  OOM bug fixed in `estabelecimento_key_audit.py` -- never computes a
  `list(...)`-shaped aggregate over every group; only the small, bounded
  evidence sample of actual duplicates gets that treatment.
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

from . import canonical_history, metrics, mirror, registry, transform
from .sources import RemoteFile, is_valid_month

log = logging.getLogger(__name__)

_RAW_TABLE = "_raw_socio_key_audit"
_FORMAT_VERSION = 1
_TOOL_VERSION = "2026-07-v1"
_PARTS: tuple[int, ...] = tuple(range(10))
_CODEC = "ZSTD"
_EVIDENCE_SAMPLE_LIMIT = 20
_PREFLIGHT_TIMEOUT = httpx.Timeout(30.0)

_NAME_COLUMNS = ("nome_socio_razao_social", "nome_representante_legal")
# The only two columns whose blankness is unambiguously a key-integrity
# failure (a socio row that doesn't say which company or which partner is
# broken data, full stop). Every other raw column -- including ones a
# WIDER candidate key adds -- can be legitimately blank for real rows.
# `cnpj_cpf_socio` only gates candidates that actually include it in their
# own column list, so this is a no-op for the foreign-partner category
# (identificador_socio="3"), where it is ALWAYS blank by construction --
# that category's candidates never reference it in the first place.
_KEY_INTEGRITY_COLUMNS = ("cnpj_basico", "cnpj_cpf_socio")
_ALL_COLUMNS: tuple[str, ...] = registry.SOCIO_COLUMNS

_CATEGORY_PJ = "1"
_CATEGORY_PF = "2"
_CATEGORY_FOREIGN = "3"
_CATEGORIES: tuple[str, ...] = (_CATEGORY_PJ, _CATEGORY_PF, _CATEGORY_FOREIGN)
_CATEGORY_LABELS: dict[str, str] = {
    _CATEGORY_PJ: "pessoa_juridica",
    _CATEGORY_PF: "pessoa_fisica",
    _CATEGORY_FOREIGN: "socio_estrangeiro",
}
_CATEGORY_PREFIX: dict[str, str] = {
    _CATEGORY_PJ: "pj",
    _CATEGORY_PF: "pf",
    _CATEGORY_FOREIGN: "foreign",
}

# Derived, normalized-name columns built once in `_socio_base` -- not part
# of registry.SOCIO_COLUMNS since they don't exist in the RFB layout, but
# every category's candidates/diagnostics below reuse the same computation.
_NOME_SOCIO_NORM = "_nome_socio_norm"
_NOME_REPRESENTANTE_NORM = "_nome_representante_norm"

# Recommended partner-identity columns per category (see module docstring):
# PJ's CNPJ is already unmasked and complete, so name is a diagnostic only
# and is deliberately NOT part of this dict's PJ entry. PF's CPF is masked,
# so name is required for the identity to mean anything. Foreign partners
# have no CPF/CNPJ at all, so name+country is the only signal available.
_CATEGORY_PARTNER_IDENTITY: dict[str, tuple[str, ...]] = {
    _CATEGORY_PJ: ("cnpj_cpf_socio",),
    _CATEGORY_PF: ("cnpj_cpf_socio", _NOME_SOCIO_NORM),
    _CATEGORY_FOREIGN: (_NOME_SOCIO_NORM, "pais"),
}

# Identity-level candidates tested per category -- the recommended identity
# above plus the specific diagnostic variants the investigation asked for
# (does name resolve a collision CNPJ alone doesn't; how much do name and
# faixa_etaria each narrow PF's masked-CPF collisions; how much does
# country narrow a foreign partner's name-only collisions).
_CATEGORY_IDENTITY_CANDIDATES: dict[str, dict[str, tuple[str, ...]]] = {
    _CATEGORY_PJ: {
        "cnpj": ("cnpj_cpf_socio",),
        "cnpj_nome": ("cnpj_cpf_socio", _NOME_SOCIO_NORM),
    },
    _CATEGORY_PF: {
        "cpf": ("cnpj_cpf_socio",),
        "cpf_nome": ("cnpj_cpf_socio", _NOME_SOCIO_NORM),
        "cpf_nome_faixa": ("cnpj_cpf_socio", _NOME_SOCIO_NORM, "faixa_etaria"),
    },
    _CATEGORY_FOREIGN: {
        "nome": (_NOME_SOCIO_NORM,),
        "nome_pais": (_NOME_SOCIO_NORM, "pais"),
    },
}


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quoted(names: tuple[str, ...]) -> list[str]:
    return [registry.quote_identifier(name) for name in names]


def socio_remote(month: str, part: int) -> RemoteFile:
    """Return the IA mirror source for one socio partition."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    if not 0 <= part <= 9:
        raise ValueError(f"part must be between 0 and 9, got {part}")
    name = f"Socios{part}.zip"
    return RemoteFile(name=name, url=mirror.raw_file_url(month, name), kind="socios")


def preflight_parts(
    month: str, parts: tuple[int, ...] = _PARTS, *, client: httpx.Client | None = None
) -> list[str]:
    """HEAD the given socio parts (all ten by default). Returns names NOT
    confirmed downloadable -- empty means the checked parts are available.
    Does not download anything."""
    remotes = [socio_remote(month, part) for part in parts]
    own_client = client is None
    client = client or httpx.Client(timeout=_PREFLIGHT_TIMEOUT, follow_redirects=True)
    missing: list[str] = []
    try:
        for remote in remotes:
            try:
                response = client.head(remote.url)
            except httpx.HTTPError as exc:
                log.warning("preflight: HEAD failed for %s: %s", remote.url, exc)
                missing.append(remote.name)
                continue
            if response.status_code != 200:
                log.warning("preflight: %s -> HTTP %d", remote.url, response.status_code)
                missing.append(remote.name)
    finally:
        if own_client:
            client.close()
    return missing


# -----------------------------------------------------------------------------
# Per-part diagnostic: load one CSV with the production reader, project
# EVERY raw column (not a fixed key -- see module docstring).
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class PartKeyAuditReport:
    status: str  # "ok" | "failed"
    part: int
    source_file: str
    source_csv: str
    rows_raw: int
    output_path: str
    error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json_dict(), ensure_ascii=False, indent=2) + "\n")


def run_part_key_audit(
    con: duckdb.DuckDBPyConnection,
    csv: Path,
    output: Path,
    *,
    part: int,
    source_file: str,
) -> PartKeyAuditReport:
    """Read one CSV with the production reader; write an all-columns
    Parquet checkpoint (every candidate key is a projection of this)."""
    if not csv.exists():
        raise FileNotFoundError(csv)
    if not source_file:
        raise ValueError("source_file cannot be empty")

    table = registry.main_table("socio")
    transform._create_table_from_csvs(con, _RAW_TABLE, [csv], table.source)  # noqa: SLF001
    rows_raw = int(con.execute(f"SELECT COUNT(*) FROM {_RAW_TABLE}").fetchone()[0])

    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.{uuid.uuid4().hex}.partial")
    partial.unlink(missing_ok=True)
    cols_sql = ", ".join(_quoted(_ALL_COLUMNS))
    try:
        con.execute(
            f'COPY (SELECT {cols_sql}, {_literal(source_file)} AS "_source_file" '
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
        output_path=str(output),
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
    database = work_dir / "socio-key-audit.duckdb"
    temp = work_dir / "duckdb_tmp"
    recorder = metrics.MetricsRecorder(
        month=snapshot, schema_version="socio-key-audit-1", filesystem_path=work_dir
    )
    con = _connection(database, temp)
    recorder.capture_pragmas(con)
    report: PartKeyAuditReport | None = None
    try:
        with recorder.stage(
            f"socio_key_audit_part_{part}", duckdb_tmp_dir=temp, workdir=work_dir
        ) as handle:
            report = run_part_key_audit(con, csv, output, part=part, source_file=source_file)
            handle.rows_read = report.rows_raw
            handle.rows_written = report.rows_raw
            handle.files_read = 1
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
# Per-part checkpoint/resume orchestration -- same discipline as
# canonical_history.py / estabelecimento_key_audit.py.
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
        "output": root / "columns" / f"part-{part}.parquet",
        "report": root / "evidence" / f"part-{part}.key-audit.json",
        "metrics": root / "evidence" / f"part-{part}.key-audit.metrics.json",
        "manifest": root / "evidence" / f"part-{part}.key-audit.manifest.json",
        "failure": root / "evidence" / f"part-{part}.key-audit.failure.json",
    }


def _code_fingerprints() -> dict[str, str]:
    modules = {
        "socio_key_audit": Path(__file__).resolve(),
        "canonical_history": Path(canonical_history.__file__).resolve(),
        "transform": Path(transform.__file__).resolve(),
        "registry": Path(registry.__file__).resolve(),
    }
    return {name: canonical_history._sha256(path) for name, path in modules.items()}  # noqa: SLF001


def _reusable_part_manifest(
    paths: dict[str, Path],
    *,
    month: str,
    part: int,
    remote: RemoteFile,
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
    """Build or reuse one checksummed all-columns checkpoint for one socio part."""
    remote = socio_remote(month, part)
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
        # Unlike canonical_history_empresa.py/canonical_history_simples.py
        # (which never checkpoint and so delete the ZIP immediately), this
        # module DOES implement per-part checkpoint reuse -- the ZIP must
        # stay on disk so a later invocation can re-verify its checksum
        # against `_reusable_part_manifest`. Only the much larger extracted
        # CSV is deleted (see the outer `finally` below), same trade-off
        # estabelecimento_key_audit.py already makes.
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
# Global cross-part aggregation, per candidate key -- reads only the ten
# per-part Parquets together, never the raw CSVs.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateKeyReport:
    name: str
    columns: tuple[str, ...]
    blank_or_null_counts_by_component: dict[str, int]
    distinct_valid_key_count: int
    duplicate_key_count: int
    excess_duplicate_row_count: int
    cross_part_duplicate_key_count: int
    conflicting_key_count: int | None
    evidence_sample: list[dict[str, Any]]

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def _audit_one_candidate(
    con: duckdb.DuckDBPyConnection,
    source: str,
    name: str,
    columns: tuple[str, ...],
    *,
    collect_sample: bool,
    compute_conflicting: bool = True,
    sample_limit: int = _EVIDENCE_SAMPLE_LIMIT,
) -> CandidateKeyReport:
    """``source`` is a FROM-clause-ready SQL fragment (a temp table or view
    name, e.g. one of the category views built by `_category_view`) rather
    than always the raw per-part Parquets, so this can be reused against a
    category-filtered, name-normalized source.

    ``valid`` (which rows are eligible for the cardinality/duplicate
    analysis at all) only requires `_KEY_INTEGRITY_COLUMNS` -- `cnpj_basico`
    and `cnpj_cpf_socio`, whichever the candidate includes -- to be
    non-blank. Every OTHER column a wider candidate adds (`pais`,
    `representante_legal`, etc.) is legitimately blank for most real rows
    (e.g. `pais` is only populated for foreign partners) -- treating a
    blank there as a key-integrity failure the way a blank `cnpj_basico`
    is would incorrectly disqualify the majority of rows from a wide
    candidate's analysis instead of measuring it. Blank counts are still
    reported per component as a diagnostic either way.

    Joins use ``IS NOT DISTINCT FROM`` rather than ``=`` because
    `CsvSpec.null_padding=True` loads blank RFB fields as SQL NULL, and a
    candidate key can legitimately contain a column that is NULL for some
    duplicate rows (e.g. `pais` for domestic partners) -- plain `=` would
    silently drop those groups from the conflict count and evidence sample
    even though the GROUP BY above (which treats NULLs as equal) counted
    them correctly.

    ``compute_conflicting`` gates the "conflicting vs exact" full-row
    comparison (``conflicting_key_count`` is ``None`` when skipped). This is
    real per-column struct equality -- NOT a hash -- because a hash match is
    not proof of row equality, only evidence of it; a collision would
    silently misreport a genuine conflict as an exact duplicate. Real struct
    comparison is only run for candidates where the duplicate-key set is
    small (company-scoped relationship candidates in practice): for a
    company-UNSCOPED identity candidate where duplication is the very thing
    being measured (e.g. masked CPF alone, ~1e6 possible values colliding
    across 26.8M real PF rows), the "duplicate keys" JOIN covers nearly the
    entire source and a full 11-column struct comparison over that many
    rows is not just slow, it is not a meaningful measurement either --
    apparent "conflicts" there are mostly just different real-world
    relationships in different companies. Callers should pass
    ``compute_conflicting=False`` for identity-level candidates and rely on
    the category-specific diagnostics (`_pj_diagnostics`/`_pf_diagnostics`)
    for what genuinely varies at that level.
    """
    quoted = _quoted(columns)
    key_list = ", ".join(quoted)
    integrity_cols = [registry.quote_identifier(c) for c in columns if c in _KEY_INTEGRITY_COLUMNS]
    valid = " AND ".join(f"{c} IS NOT NULL AND TRIM({c}) <> ''" for c in integrity_cols) or "TRUE"

    blanks = {
        col: int(
            con.execute(
                f"SELECT COUNT(*) FROM {source} WHERE "
                f"{registry.quote_identifier(col)} IS NULL OR "
                f"TRIM({registry.quote_identifier(col)}) = ''"
            ).fetchone()[0]
        )
        for col in columns
    }

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _grouped AS
        SELECT {key_list},
               COUNT(*)::BIGINT AS n,
               COUNT(DISTINCT "_source_file")::BIGINT AS n_parts
        FROM {source}
        WHERE {valid}
        GROUP BY {key_list}
        """
    )
    agg = con.execute(
        """
        SELECT
            COUNT(*)::BIGINT AS distinct_keys,
            COALESCE(SUM(CASE WHEN n > 1 THEN 1 ELSE 0 END), 0)::BIGINT AS duplicate_keys,
            COALESCE(SUM(CASE WHEN n > 1 THEN n - 1 ELSE 0 END), 0)::BIGINT AS excess_rows,
            COALESCE(SUM(CASE WHEN n_parts > 1 THEN 1 ELSE 0 END), 0)::BIGINT AS cross_part_keys
        FROM _grouped
        """
    ).fetchone()

    conflicting: int | None = None
    if compute_conflicting:
        # Conflicting-vs-exact: among duplicate keys, how many have more
        # than one distinct FULL ROW (every raw column, including both
        # free-text name fields -- the most conservative definition, never
        # UNDER-counts a conflict). Real per-column struct equality, so
        # NULL fields compare IS NOT DISTINCT FROM (same semantics as the
        # GROUP BY above), not a hash -- see the docstring above for why.
        all_cols = [f"t.{c}" for c in _quoted(_ALL_COLUMNS)]
        full_row_struct = (
            "{"
            + ", ".join(f"'{c}': {expr}" for c, expr in zip(_ALL_COLUMNS, all_cols, strict=True))
            + "}"
        )
        t_keys = ", ".join(f"t.{c}" for c in quoted)
        join_on = " AND ".join(f"t.{c} IS NOT DISTINCT FROM dk.{c}" for c in quoted)
        conflicting = int(
            con.execute(
                f"""
                WITH dupe_keys AS (SELECT {key_list} FROM _grouped WHERE n > 1),
                conflicting_keys AS (
                    SELECT {t_keys}
                    FROM {source} t
                    JOIN dupe_keys dk ON {join_on}
                    GROUP BY {t_keys}
                    HAVING COUNT(DISTINCT {full_row_struct}) > 1
                )
                SELECT COUNT(*) FROM conflicting_keys
                """
            ).fetchone()[0]
        )

    evidence_sample: list[dict[str, Any]] = []
    if collect_sample:
        top_keys_sql = ", ".join(f"top.{c}" for c in quoted)
        join_on2 = " AND ".join(f"top.{c} IS NOT DISTINCT FROM full_scan.{c}" for c in quoted)
        sample_rows = con.execute(
            f"""
            WITH top AS (
                SELECT {key_list}, n
                FROM _grouped
                WHERE n > 1
                ORDER BY n DESC, {key_list}
                LIMIT {int(sample_limit)}
            )
            SELECT {top_keys_sql}, top.n,
                   list(DISTINCT full_scan."_source_file") AS source_files
            FROM top
            JOIN (
                SELECT {key_list}, "_source_file" FROM {source} WHERE {valid}
            ) AS full_scan
            ON {join_on2}
            GROUP BY {top_keys_sql}, top.n
            ORDER BY top.n DESC, {top_keys_sql}
            """
        ).fetchall()
        evidence_sample = [
            {
                **{col: row[i] for i, col in enumerate(columns)},
                "count": int(row[len(columns)]),
                "source_files": sorted(row[len(columns) + 1]),
            }
            for row in sample_rows
        ]

    con.execute("DROP TABLE _grouped")

    return CandidateKeyReport(
        name=name,
        columns=columns,
        blank_or_null_counts_by_component=blanks,
        distinct_valid_key_count=int(agg[0]),
        duplicate_key_count=int(agg[1]),
        excess_duplicate_row_count=int(agg[2]),
        cross_part_duplicate_key_count=int(agg[3]),
        conflicting_key_count=conflicting,
        evidence_sample=evidence_sample,
    )


def _normalized_name_expr(column: str) -> str:
    """Strip accents (`strip_accents()`, a DuckDB core string function, no
    extension needed), uppercase, collapse internal whitespace, trim. RFB
    free text is not consistently accented across records -- the same real
    name can appear with and without diacritics in different snapshots or
    even different rows of the same snapshot -- so without accent removal,
    two spellings of the same name would wrongly compare as different
    people. This matches how Portuguese free text should be compared for
    identity purposes: case- and diacritic-insensitive, not a literal byte
    comparison.
    """
    quoted = registry.quote_identifier(column)
    return rf"TRIM(REGEXP_REPLACE(UPPER(strip_accents({quoted})), '\s+', ' ', 'g'))"


def _build_socio_base(con: duckdb.DuckDBPyConnection, paths_sql: str) -> str:
    """`_socio_base` -- every raw `SOCIO_COLUMNS` plus `_source_file`, plus
    the normalized-name diagnostic columns every category's candidates and
    diagnostics below reuse."""
    cols_sql = ", ".join(_quoted(_ALL_COLUMNS))
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _socio_base AS
        SELECT {cols_sql}, "_source_file",
               {_normalized_name_expr("nome_socio_razao_social")}
                   AS {registry.quote_identifier(_NOME_SOCIO_NORM)},
               {_normalized_name_expr("nome_representante_legal")}
                   AS {registry.quote_identifier(_NOME_REPRESENTANTE_NORM)}
        FROM read_parquet({paths_sql})
        """
    )
    return "_socio_base"


def _category_view(con: duckdb.DuckDBPyConnection, base: str, category: str) -> str:
    view = f"_socio_{_CATEGORY_PREFIX[category]}"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {view} AS
        SELECT * FROM {base}
        WHERE {registry.quote_identifier("identificador_socio")} = {_literal(category)}
        """
    )
    return view


_REPRESENTANTE_COLUMNS = (
    "representante_legal",
    "nome_representante_legal",
    "qualificacao_representante_legal",
)


def _null_aware_varies_expr(column_ref: str) -> str:
    """True if `column_ref` takes more than one distinct value within the
    enclosing GROUP BY, where NULL counts as a value in its own right.
    Plain ``COUNT(DISTINCT column_ref) > 1`` silently ignores NULL rows
    entirely, so a group with one NULL row and one non-NULL row -- e.g. one
    occurrence has a legal representative on file and the other doesn't --
    would be reported as "consistent" when it is not."""
    return (
        f"(COUNT(DISTINCT {column_ref}) > 1 OR "
        f"(COUNT(*) FILTER (WHERE {column_ref} IS NULL) > 0 "
        f"AND COUNT(*) FILTER (WHERE {column_ref} IS NOT NULL) > 0))"
    )


def _representante_independence(
    con: duckdb.DuckDBPyConnection, source: str, relationship_columns: tuple[str, ...]
) -> dict[str, Any]:
    """Among relationship groups that already share the same
    company+partner-identity+qualificacao+data_entrada key (i.e. groups
    `_audit_one_candidate` would call duplicates), does the legal
    representative vary WITHIN the group? If so, the representative fields
    might define a genuinely separate relationship; if not, there is no
    evidence they belong in the identity.

    Two passes, same discipline as `_audit_one_candidate`: a cheap single-
    aggregate GROUP BY over the full (potentially tens of millions of rows)
    source finds which keys are duplicates, then the NULL-aware variation
    check -- expensive per group -- only runs on a JOIN restricted to that
    small duplicate subset. Doing this in one GROUP BY over the full source
    measurably OOM'd on the real PF category (26.8M rows).
    """
    quoted = _quoted(relationship_columns)
    key_list = ", ".join(quoted)
    t_key_list = ", ".join(f"t.{c}" for c in quoted)
    join_on = " AND ".join(f"t.{c} IS NOT DISTINCT FROM dk.{c}" for c in quoted)
    varies_expr = " OR ".join(
        _null_aware_varies_expr(f"t.{registry.quote_identifier(c)}") for c in _REPRESENTANTE_COLUMNS
    )
    row = con.execute(
        f"""
        WITH dupe_keys AS (
            SELECT {key_list} FROM {source} GROUP BY {key_list} HAVING COUNT(*) > 1
        ),
        variation AS (
            SELECT ({varies_expr}) AS varies
            FROM {source} t
            JOIN dupe_keys dk ON {join_on}
            GROUP BY {t_key_list}
        )
        SELECT
            COUNT(*)::BIGINT AS duplicate_relationship_groups,
            COALESCE(SUM(CASE WHEN varies THEN 1 ELSE 0 END), 0)::BIGINT
                AS groups_with_representante_variation
        FROM variation
        """
    ).fetchone()
    return {
        "duplicate_relationship_groups": int(row[0]),
        "groups_with_representante_variation": int(row[1]),
    }


def _pj_diagnostics(
    con: duckdb.DuckDBPyConnection, source: str, identity_candidates: dict[str, Any]
) -> dict[str, Any]:
    cnpj = registry.quote_identifier("cnpj_cpf_socio")
    nome_norm = registry.quote_identifier(_NOME_SOCIO_NORM)
    fmt = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (
                WHERE {cnpj} IS NOT NULL AND LENGTH(TRIM({cnpj})) = 14
                      AND TRIM({cnpj}) NOT SIMILAR TO '%[^0-9]%'
            ) AS valid,
            COUNT(*) FILTER (
                WHERE {cnpj} IS NULL OR LENGTH(TRIM({cnpj})) <> 14
                      OR TRIM({cnpj}) SIMILAR TO '%[^0-9]%'
            ) AS invalid
        FROM {source}
        """
    ).fetchone()
    same_cnpj_different_name = int(
        con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {cnpj}
                FROM {source}
                WHERE {cnpj} IS NOT NULL AND TRIM({cnpj}) <> ''
                GROUP BY {cnpj}
                HAVING COUNT(DISTINCT {nome_norm}) > 1
            )
            """
        ).fetchone()[0]
    )
    cnpj_only = identity_candidates["pj:cnpj"]
    cnpj_and_name = identity_candidates["pj:cnpj_nome"]
    return {
        "cnpj_format_valid_count": int(fmt[0]),
        "cnpj_format_invalid_count": int(fmt[1]),
        "same_cnpj_different_normalized_name_count": same_cnpj_different_name,
        "name_resolves_collision_beyond_valid_cnpj": (
            cnpj_and_name["distinct_valid_key_count"] > cnpj_only["distinct_valid_key_count"]
        ),
    }


def _pf_diagnostics(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, Any]:
    cpf = registry.quote_identifier("cnpj_cpf_socio")
    nome_norm = registry.quote_identifier(_NOME_SOCIO_NORM)
    faixa = registry.quote_identifier("faixa_etaria")
    same_cpf_different_name = int(
        con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {cpf}
                FROM {source}
                WHERE {cpf} IS NOT NULL AND TRIM({cpf}) <> ''
                GROUP BY {cpf}
                HAVING COUNT(DISTINCT {nome_norm}) > 1
            )
            """
        ).fetchone()[0]
    )
    same_cpf_nome_different_faixa = int(
        con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {cpf}, {nome_norm}
                FROM {source}
                WHERE {cpf} IS NOT NULL AND TRIM({cpf}) <> ''
                GROUP BY {cpf}, {nome_norm}
                HAVING COUNT(DISTINCT {faixa}) > 1
            )
            """
        ).fetchone()[0]
    )
    return {
        "same_masked_cpf_different_normalized_name_count": same_cpf_different_name,
        "same_masked_cpf_and_name_different_faixa_etaria_count": same_cpf_nome_different_faixa,
    }


def _audit_category(con: duckdb.DuckDBPyConnection, source: str, category: str) -> dict[str, Any]:
    prefix = _CATEGORY_PREFIX[category]
    row_count = int(con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0])
    recommended_identity = _CATEGORY_PARTNER_IDENTITY[category]

    # compute_conflicting=False for identity candidates: they are
    # company-UNSCOPED by design (that's what "identity" means here), so
    # for a category like PF the "duplicate keys" set can cover nearly the
    # entire source (see _audit_one_candidate's docstring) -- a real
    # full-row comparison there is both too expensive and not a meaningful
    # measurement (apparent overlaps are mostly different real-world
    # relationships in different companies, not conflicts to resolve).
    # Category-specific diagnostics below measure what genuinely varies at
    # the identity level instead.
    identity_candidates = {
        f"{prefix}:{suffix}": _audit_one_candidate(
            con,
            source,
            f"{prefix}:{suffix}",
            columns,
            collect_sample=True,
            compute_conflicting=False,
        ).to_json_dict()
        for suffix, columns in _CATEGORY_IDENTITY_CANDIDATES[category].items()
    }

    relationship_defs: dict[str, tuple[str, ...]] = {
        f"{prefix}:company_partner": ("cnpj_basico", *recommended_identity),
        f"{prefix}:company_partner_qualificacao": (
            "cnpj_basico",
            *recommended_identity,
            "qualificacao_socio",
        ),
        f"{prefix}:relationship": (
            "cnpj_basico",
            *recommended_identity,
            "qualificacao_socio",
            "data_entrada_sociedade",
        ),
    }
    if category == _CATEGORY_PF:
        # Measured for comparison, NOT recommended -- see module docstring:
        # faixa_etaria (age bracket) is temporally unstable, unlike
        # qualificacao_socio/data_entrada_sociedade which are facts fixed
        # at the moment a partner entered. Excluding it is a semantic
        # judgment, not just "it doesn't move the numbers much".
        relationship_defs[f"{prefix}:relationship_with_faixa"] = (
            "cnpj_basico",
            *recommended_identity,
            "qualificacao_socio",
            "data_entrada_sociedade",
            "faixa_etaria",
        )
    relationship_candidates = {
        cname: _audit_one_candidate(
            con, source, cname, cols, collect_sample=True, compute_conflicting=True
        ).to_json_dict()
        for cname, cols in relationship_defs.items()
    }

    diagnostics: dict[str, Any] = {
        "representante_independence": _representante_independence(
            con, source, relationship_defs[f"{prefix}:relationship"]
        ),
    }
    if category == _CATEGORY_PJ:
        diagnostics.update(_pj_diagnostics(con, source, identity_candidates))
    elif category == _CATEGORY_PF:
        diagnostics.update(_pf_diagnostics(con, source))

    return {
        "identificador_socio": category,
        "row_count": row_count,
        "recommended_partner_identity": list(recommended_identity),
        "identity_candidates": identity_candidates,
        "relationship_candidates": relationship_candidates,
        "diagnostics": diagnostics,
    }


def run_global_key_audit(
    con: duckdb.DuckDBPyConnection, part_parquets: list[Path]
) -> dict[str, Any]:
    paths_sql = registry.paths_literal(part_parquets)
    base = _build_socio_base(con, paths_sql)
    total_rows = int(con.execute(f"SELECT COUNT(*) FROM {base}").fetchone()[0])

    categories: dict[str, Any] = {}
    for category in _CATEGORIES:
        view = _category_view(con, base, category)
        categories[_CATEGORY_LABELS[category]] = _audit_category(con, view, category)
        con.execute(f"DROP VIEW {view}")
    con.execute(f"DROP TABLE {base}")

    return {"total_rows_scanned": total_rows, "categories": categories}


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

    to_check = tuple(part for part in _PARTS if part not in overrides)
    if to_check:
        missing_names = preflight_parts(month, to_check, client=client)
        if missing_names:
            raise RuntimeError(
                f"preflight failed: {len(missing_names)}/{len(to_check)} socio part(s) not "
                f"available in the mirror for {month}: {sorted(missing_names)!r} -- refusing "
                "to start a partial-source-set run"
            )

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

    part_parquets = [result.output_path for result in part_results]
    global_work = root / "work" / "global"
    database = global_work / "socio-key-audit-global.duckdb"
    temp = global_work / "duckdb_tmp"
    con = _connection(database, temp)
    try:
        global_report = run_global_key_audit(con, part_parquets)
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
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "duckdb_version": duckdb_version,
        "execution_profile": {"threads": str(threads), "memory_limit": str(memory_limit)},
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
        **global_report,
        "checkpoint_checksums": {
            f"part-{result.manifest.get('part')}": result.manifest.get("output", {}).get("sha256")
            for result in part_results
        },
    }
    for entry, result in zip(payload["parts"], part_results, strict=True):
        try:
            part_report = canonical_history._load_json(result.report_path)  # noqa: SLF001
            entry["rows_raw"] = part_report.get("rows_raw")
        except (OSError, ValueError, json.JSONDecodeError):  # pragma: no cover
            pass

    report_path = root / "evidence" / "global.socio-key-audit.json"
    canonical_history._write_json_atomic(report_path, payload)  # noqa: SLF001
    return KeyAuditResult(root, report_path, payload, tuple(part_results))


@dataclass(frozen=True)
class AggregationOnlyResult:
    root: Path
    report_path: Path
    report: dict[str, Any]
    verified_checksums: dict[str, str]


def run_aggregation_only(root: Path, month: str) -> AggregationOnlyResult:
    """Run ONLY the global cross-part aggregation (current code) against
    per-part checkpoint Parquets that are ALREADY PRESENT under
    `root/columns/` -- e.g. restored from a prior run's GH Actions
    artifact. No ZIP download, no CSV extraction, no raw-source network
    access of any kind: this exists specifically so the category-aware
    analysis can be re-run against already-verified real data whenever the
    code changes, without re-downloading the ten real `SociosN.zip` files
    from the Internet Archive mirror every time.

    Requires all ten `columns/part-N.parquet` files to already exist under
    `root`. If the matching `evidence/part-N.key-audit.manifest.json`
    files are also present (restored from the same artifact), each part's
    checksum is independently RE-COMPUTED and compared against that
    manifest before aggregation runs -- a corrupted or tampered restored
    checkpoint fails loudly here instead of silently producing a report
    from wrong data.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    root = root.resolve()

    part_paths: list[Path] = []
    verified_checksums: dict[str, str] = {}
    for part in _PARTS:
        paths = _paths(root, part)
        output = paths["output"]
        if not output.is_file():
            raise FileNotFoundError(
                f"missing checkpoint for part {part}: {output} -- aggregation-only mode "
                "requires all ten columns/part-N.parquet files to already be present "
                "(restore them from a prior run's artifact first)"
            )
        actual = canonical_history._sha256(output)  # noqa: SLF001
        manifest_path = paths["manifest"]
        if manifest_path.is_file():
            manifest = canonical_history._load_json(manifest_path)  # noqa: SLF001
            expected = manifest.get("output", {}).get("sha256")
            if expected and expected != actual:
                raise RuntimeError(
                    f"checksum mismatch for part {part}: manifest recorded {expected}, "
                    f"restored file hashes to {actual} -- refusing to aggregate over a "
                    "checkpoint that does not match its own recorded checksum"
                )
        verified_checksums[f"part-{part}"] = actual
        part_paths.append(output)

    global_work = root / "work" / "global"
    database = global_work / "socio-key-audit-global.duckdb"
    temp = global_work / "duckdb_tmp"
    con = _connection(database, temp)
    try:
        global_report = run_global_key_audit(con, part_paths)
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
        "mode": "aggregation-only",
        "source_commit": metrics._git_sha(),  # noqa: SLF001
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "duckdb_version": duckdb_version,
        "execution_profile": {"threads": str(threads), "memory_limit": str(memory_limit)},
        "verified_checkpoint_checksums": verified_checksums,
        **global_report,
    }
    report_path = root / "evidence" / "global.socio-key-audit.json"
    canonical_history._write_json_atomic(report_path, payload)  # noqa: SLF001
    return AggregationOnlyResult(root, report_path, payload, verified_checksums)


def _print_report(report: dict[str, Any], report_path: Path) -> None:
    print(f"socio key audit done — {report['total_rows_scanned']:,} rows scanned")
    for label, category in report["categories"].items():
        print(
            f"  {label} (identificador_socio={category['identificador_socio']!r}): "
            f"{category['row_count']:,} rows"
        )
        for group in ("identity_candidates", "relationship_candidates"):
            for name, candidate in category[group].items():
                conflicting = candidate["conflicting_key_count"]
                conflicting_str = (
                    f"{conflicting:,} conflicting"
                    if conflicting is not None
                    else "conflicting not computed (identity-level)"
                )
                print(
                    f"    {name}: {candidate['distinct_valid_key_count']:,} distinct, "
                    f"{candidate['duplicate_key_count']:,} duplicate key(s), "
                    f"{conflicting_str}"
                )
    print(f"report: {report_path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--aggregate-only",
        action="store_true",
        help="Skip download/extract entirely and run only the global cross-part "
        "aggregation against columns/part-N.parquet files that already exist under "
        "--root (e.g. restored from a prior run's artifact). No network access.",
    )
    parser.add_argument(
        "--zip",
        action="append",
        default=[],
        metavar="PART=PATH",
        help="Local ZIP override for one part, e.g. --zip 0=/path/to/Socios0.zip "
        "(repeatable; smoke/offline runs only)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.aggregate_only:
        if args.zip or args.force:
            print(
                "error: --aggregate-only cannot be combined with --zip or --force", file=sys.stderr
            )
            return 2
        try:
            agg_result = run_aggregation_only(args.root, args.month)
        except (FileNotFoundError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        _print_report(agg_result.report, agg_result.report_path)
        return 0

    zip_overrides: dict[int, Path] = {}
    for entry in args.zip:
        part_str, _, path_str = entry.partition("=")
        if not part_str.isdigit() or not path_str:
            print(f"error: --zip must be PART=PATH, got {entry!r}", file=sys.stderr)
            return 2
        part = int(part_str)
        if not 0 <= part <= 9:
            print(f"error: --zip part must be 0..9, got {part}", file=sys.stderr)
            return 2
        if part in zip_overrides:
            print(f"error: --zip given more than once for part {part}", file=sys.stderr)
            return 2
        path = Path(path_str)
        if not path.is_file():
            print(f"error: --zip {part}: file not found: {path}", file=sys.stderr)
            return 2
        if not zipfile.is_zipfile(path):
            print(f"error: --zip {part}: not a valid ZIP: {path}", file=sys.stderr)
            return 2
        zip_overrides[part] = path

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

    _print_report(result.report, result.report_path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "AggregationOnlyResult",
    "CandidateKeyReport",
    "KeyAuditResult",
    "PartCheckpointResult",
    "PartKeyAuditReport",
    "main",
    "preflight_parts",
    "run_aggregation_only",
    "run_global_key_audit",
    "run_key_audit",
    "run_part_checkpoint",
    "run_part_key_audit",
    "run_part_key_audit_with_metrics",
    "socio_remote",
]
