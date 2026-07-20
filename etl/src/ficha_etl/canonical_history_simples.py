"""Historical shadow orchestration for the canonical simples dataset
(RFC 0001 Phase 3, #97 slice 4).

Unlike empresa (``canonical_history_empresa.py``, ten physical
``EmpresasN.zip`` parts), simples is published as ONE physical file
(``Simples.zip`` -- ``sources.canonical_inventory()`` has exactly one entry
of kind ``"simples"``), so this module goes through the SAME single-part
writer path estabelecimento uses --
:func:`canonical_shadow.run_canonical_shadow_part` -- never
``write_canonical_dataset`` and never a second deduplication
implementation. The only writer-level difference from estabelecimento is
that simples declares ``duplicate_policy="deterministic-collapse"`` (the
production loader already runs ``_dedupe_cnpj_basico_table`` for
``simples``, the same call as for ``empresa`` -- see
``registry.SIMPLES_CANONICAL``'s comment), which
``write_canonical_part`` already handles: it collapses duplicates AND
switches on explicit primary-key output ordering for that policy,
regardless of entity name.

This module deliberately does NOT copy ``canonical_history.py``'s
estabelecimento-specific per-part checkpoint-reuse machinery
(``_reusable_manifest``, the ``force`` flag) -- see *Not restartable*
below. What it DOES reuse, directly (imported, not copied), same
discipline as ``canonical_history_empresa.py`` and
``estabelecimento_key_audit.py``:

- ``canonical_history.single_file_remote()`` -- shared, table-driven
  single-physical-file remote resolution (added in this slice specifically
  so this module and any FUTURE single-file table's historical runner
  don't each reimplement it);
- ``canonical_history``'s low-level download/extract/checksum/atomic-write
  primitives (``_ensure_zip``, ``_extract_one``, ``_checked_file``,
  ``_sha256``, ``_write_json_atomic``, ``_load_json``).

Not restartable/checkpointed. ``canonical_history.py``'s per-part
checkpoint reuse (source ZIP + code fingerprint + output checksum
matching) is a real mechanism with known gaps tracked in issue #103
(acquisition mode and retained evidence checksums do not fully participate
in checkpoint identity yet). Reproducing that design here without first
closing #103 would just add a second copy of the same trap -- a fresh run
is preferable. Every dispatch of this workflow performs a full fresh run.

Disk lifecycle: the one ZIP is downloaded (or read from a local override),
checksummed, extracted to its one CSV, and deleted immediately -- the
downloaded ZIP is never kept around once its CSV is safely extracted and
checksummed, on either the success or the failure path (e.g. a ZIP with
more than one member). The extracted CSV and the DuckDB work directory are
removed after the writer returns, success or failure, unless
``keep_extracted``/``keep_raw`` is set for debugging.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from . import canonical_history, canonical_shadow, metrics, registry, sources, transform
from .sources import RemoteFile

log = logging.getLogger(__name__)

_MANIFEST_FORMAT_VERSION = 1
_ORCHESTRATOR_VERSION = "2026-07-v1"
_TABLE_NAME = "simples"
_PREFLIGHT_TIMEOUT = httpx.Timeout(30.0)


@dataclass(frozen=True)
class HistoricalSimplesResult:
    """Paths and manifest for one historical canonical simples run."""

    root: Path
    output_path: Path
    quality_path: Path
    metrics_path: Path
    manifest_path: Path
    manifest: dict[str, Any]


def simples_remote(month: str) -> RemoteFile:
    """The one physical simples source for `month` -- ``Simples.zip``."""
    return canonical_history.single_file_remote(_TABLE_NAME, month)


def _expected_name() -> str:
    """The one expected physical filename -- month-independent (physical
    layout, not a URL), so an override's name can be validated without
    needing a month at all."""
    return [
        spec.name
        for spec in sources.canonical_inventory()
        if spec.kind == registry.main_table(_TABLE_NAME).kind
    ][0]


def _validate_override(name: str | None, path: Path | None) -> None:
    """Reject an override name that isn't the one expected physical file,
    and validate the override path exists and is a real ZIP -- all of this
    BEFORE any remote download/preflight, so a typo or a bad local file
    fails fast.
    """
    if path is None:
        return
    expected = _expected_name()
    if name is not None and name != expected:
        raise ValueError(
            f"--zip override name must be {expected!r} (simples has exactly one physical "
            f"file), got {name!r}"
        )
    if not path.is_file():
        raise FileNotFoundError(f"{expected}: local ZIP override not found: {path}")
    if not zipfile.is_zipfile(path):
        raise RuntimeError(f"{expected}: local ZIP override is not a valid ZIP: {path}")


def _code_fingerprints() -> dict[str, str]:
    modules = {
        "canonical_history_simples": Path(__file__).resolve(),
        "canonical_history": Path(canonical_history.__file__).resolve(),
        "canonical_shadow": Path(canonical_shadow.__file__).resolve(),
        "transform": Path(transform.__file__).resolve(),
        "registry": Path(registry.__file__).resolve(),
        "sources": Path(sources.__file__).resolve(),
    }
    return {name: canonical_history._sha256(path) for name, path in modules.items()}  # noqa: SLF001


def _paths(root: Path) -> dict[str, Path]:
    return {
        "raw_dir": root / "raw",
        "extract_dir": root / "extracted",
        "work_dir": root / "work",
        "output": root / "canonical" / "simples.parquet",
        "quality": root / "evidence" / "simples.quality.json",
        "metrics": root / "evidence" / "simples.metrics.json",
        "manifest": root / "evidence" / "simples.history.json",
        "failure": root / "evidence" / "simples.history.failure.json",
    }


def run_historical_simples(
    month: str,
    root: Path,
    *,
    sample_size: int = 1_000,
    zip_override: Path | None = None,
    keep_extracted: bool = False,
    keep_raw: bool = False,
    client: httpx.Client | None = None,
) -> HistoricalSimplesResult:
    """Download, extract and canonicalize the one-file simples dataset for
    one historical snapshot, and persist a durable manifest.

    ``zip_override``, if given, is validated (exists, is a real ZIP) before
    any preflight/download -- smoke/offline runs only. Otherwise the
    remote is preflight-checked (HEAD) before downloading.
    """
    if sample_size < 0:
        raise ValueError("sample_size must not be negative")
    _validate_override(None, zip_override)
    remote = simples_remote(month)

    if zip_override is None:
        own_client = client is None
        preflight_client = client or httpx.Client(timeout=_PREFLIGHT_TIMEOUT, follow_redirects=True)
        try:
            preflight_error = _preflight_one(remote, client=preflight_client)
        finally:
            if own_client:
                preflight_client.close()
        if preflight_error is not None:
            raise RuntimeError(
                f"preflight failed: {remote.name} not available in the mirror for {month} "
                f"({preflight_error}) -- refusing to start"
            )

    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    paths = _paths(root)
    code = _code_fingerprints()
    paths["failure"].unlink(missing_ok=True)

    # Tracked individually, not as one dict built only on full success, so
    # the failure handler below can report whatever evidence WAS gathered
    # before things went wrong (e.g. the zip checksum even if extraction
    # itself failed) -- same discipline as canonical_history.py's own
    # estabelecimento failure handler.
    acquisition = "unknown"
    zip_entry: dict[str, Any] | None = None
    csv_entry: dict[str, Any] | None = None
    try:
        zip_path, acquisition = canonical_history._ensure_zip(  # noqa: SLF001
            remote, paths["raw_dir"], zip_override=zip_override, client=client
        )
        zip_entry = canonical_history._checked_file(zip_path)  # noqa: SLF001
        try:
            csv_path = canonical_history._extract_one(zip_path, paths["extract_dir"])  # noqa: SLF001
        finally:
            if not keep_raw:
                zip_path.unlink(missing_ok=True)
        csv_entry = canonical_history._checked_file(csv_path)  # noqa: SLF001

        report = canonical_shadow.run_canonical_shadow_part(
            _TABLE_NAME,
            csv_path,
            paths["output"],
            source_file=remote.name,
            source_snapshot=month,
            work_dir=paths["work_dir"],
            report_path=paths["quality"],
            metrics_path=paths["metrics"],
            sample_size=sample_size,
        )
        if report.status != "ok":  # pragma: no cover - writer raises on failed gates
            raise RuntimeError(f"canonical writer returned status={report.status!r}")

        quality = canonical_history._load_json(paths["quality"])  # noqa: SLF001
        metrics_envelope = canonical_history._load_json(paths["metrics"])  # noqa: SLF001
        stage = (metrics_envelope.get("stages") or [{}])[0]

        manifest: dict[str, Any] = {
            "format_version": _MANIFEST_FORMAT_VERSION,
            "orchestrator_version": _ORCHESTRATOR_VERSION,
            "status": "ok",
            "table": _TABLE_NAME,
            "month": month,
            "sample_size": sample_size,
            "code": code,
            "source_commit": metrics_envelope.get("code_version"),
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "duckdb_version": metrics_envelope.get("duckdb_version"),
            "execution_profile": metrics_envelope.get("pragmas"),
            "source": {
                "name": remote.name,
                "url": remote.url,
                "acquisition": acquisition,
                "zip": zip_entry,
                "csv": csv_entry,
            },
            "output": canonical_history._checked_file(paths["output"]),  # noqa: SLF001
            "quality": canonical_history._checked_file(paths["quality"]),  # noqa: SLF001
            "metrics": canonical_history._checked_file(paths["metrics"]),  # noqa: SLF001
            "quality_summary": {
                "rows_raw": quality.get("rows_raw"),
                "rows_canonical": quality.get("rows_canonical"),
                "required_key_failures": quality.get("required_key_failures"),
                "duplicate_key_count": quality.get("duplicate_key_count"),
                "duplicate_key_rows": quality.get("duplicate_key_rows"),
                "conflicting_key_count": quality.get("conflicting_key_count"),
                "conflicting_sample": quality.get("conflicting_sample"),
                "invalid_casts_by_column": quality.get("invalid_casts_by_column"),
                "sample_size": quality.get("sample_size"),
                "sample_fingerprint": quality.get("sample_fingerprint"),
                "sample_mismatches": quality.get("sample_mismatches"),
                "schema_matches": quality.get("schema_matches"),
            },
            # See canonical_history_empresa.py's identical field for the
            # full rationale: "scope" makes explicit that every field here
            # is the ONE metrics stage this module instruments (the
            # canonical writer's own canonical_simples_part stage), NOT
            # this orchestrator's own preflight/download/extract time.
            # Value names are the stable metrics.StageMetrics.to_json_dict()
            # keys (wall_seconds/rss_peak_delta_mib), unrenamed.
            "resource_summary": {
                "scope": "canonical-writer-stage",
                "wall_seconds": stage.get("wall_seconds"),
                "rss_peak_mib": stage.get("rss_peak_mib"),
                "rss_peak_delta_mib": stage.get("rss_peak_delta_mib"),
                "duckdb_tmp_peak_mib": stage.get("duckdb_tmp_peak_mib"),
                "workdir_peak_mib": stage.get("workdir_peak_mib"),
                "filesystem_used_peak_mib": stage.get("filesystem_used_peak_mib"),
                "filesystem_used_peak_percent": stage.get("filesystem_used_peak_percent"),
                "files_read": stage.get("files_read"),
            },
        }
        canonical_history._write_json_atomic(paths["manifest"], manifest)  # noqa: SLF001
        return HistoricalSimplesResult(
            root, paths["output"], paths["quality"], paths["metrics"], paths["manifest"], manifest
        )
    except Exception as exc:
        failure: dict[str, Any] = {
            "format_version": _MANIFEST_FORMAT_VERSION,
            "orchestrator_version": _ORCHESTRATOR_VERSION,
            "status": "failed",
            "table": _TABLE_NAME,
            "month": month,
            "sample_size": sample_size,
            "code": code,
            "source_commit": metrics._git_sha(),  # noqa: SLF001
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "source": {"name": remote.name, "url": remote.url, "acquisition": acquisition},
            "error": str(exc),
        }
        if zip_entry is not None:
            failure["source"]["zip"] = zip_entry
        if csv_entry is not None:
            failure["source"]["csv"] = csv_entry
        for name in ("output", "quality", "metrics"):
            if paths[name].is_file():
                failure[name] = canonical_history._checked_file(paths[name])  # noqa: SLF001
        canonical_history._write_json_atomic(paths["failure"], failure)  # noqa: SLF001
        raise
    finally:
        if not keep_extracted:
            shutil.rmtree(paths["extract_dir"], ignore_errors=True)


def _preflight_one(remote: RemoteFile, *, client: httpx.Client) -> str | None:
    """HEAD the one simples remote. Returns an explanatory string if it is
    NOT confirmed downloadable, else None. Does not download anything."""
    try:
        response = client.head(remote.url)
    except httpx.HTTPError as exc:
        log.warning("preflight: HEAD failed for %s: %s", remote.url, exc)
        return str(exc)
    if response.status_code != 200:
        log.warning("preflight: %s -> HTTP %d", remote.url, response.status_code)
        return f"HTTP {response.status_code}"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=1_000)
    parser.add_argument(
        "--zip",
        metavar="NAME=PATH",
        help="Local ZIP override, e.g. --zip Simples.zip=/path/to/Simples.zip "
        "(smoke/offline runs only)",
    )
    parser.add_argument("--keep-extracted", action="store_true")
    parser.add_argument("--keep-raw", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    zip_override: Path | None = None
    override_name: str | None = None
    if args.zip:
        override_name, _, path_str = args.zip.partition("=")
        if not override_name or not path_str:
            print(f"error: --zip must be NAME=PATH, got {args.zip!r}", file=sys.stderr)
            return 2
        zip_override = Path(path_str)

    try:
        _validate_override(override_name, zip_override)
        result = run_historical_simples(
            args.month,
            args.root,
            sample_size=args.sample_size,
            zip_override=zip_override,
            keep_extracted=args.keep_extracted,
            keep_raw=args.keep_raw,
        )
    except (
        canonical_shadow.CanonicalValidationError,
        FileNotFoundError,
        OSError,
        RuntimeError,
        ValueError,
        httpx.HTTPError,
        zipfile.BadZipFile,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    summary = result.manifest["quality_summary"]
    print(
        f"historical canonical simples dataset written: {result.output_path} — "
        f"{summary['rows_canonical']:,} rows, "
        f"{summary['duplicate_key_count']:,} duplicate key(s), "
        f"{summary['conflicting_key_count']:,} conflicting"
    )
    print(f"manifest: {result.manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "HistoricalSimplesResult",
    "main",
    "run_historical_simples",
    "simples_remote",
]
