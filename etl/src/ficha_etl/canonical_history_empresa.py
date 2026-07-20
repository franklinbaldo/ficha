"""Historical shadow orchestration for the complete canonical empresa dataset
(RFC 0001 Phase 3, #97 slice 3).

Unlike ``canonical_history.py`` (one estabelecimento physical part per
invocation), empresa declares ``duplicate_policy="deterministic-collapse"``:
a ``cnpj_basico`` duplicated across two different physical ``EmpresasN.zip``
parts can only be collapsed correctly if both parts are in the same
deduplication scope (see ``canonical_shadow.py``'s module docstring and
``write_canonical_dataset``). This orchestrator therefore always resolves
the COMPLETE ten-part physical set for one snapshot before writing
anything -- not optional the way it is for estabelecimento.

To be precise about why estabelecimento's per-part runner is NOT a
counterexample: its ``duplicate_policy="fail"`` only guarantees no
duplicate key WITHIN the one physical part it processes -- it does not, by
itself, prove the same key is absent from the OTHER nine
``EstabelecimentosN.zip`` parts of the same snapshot (each single-part run
only ever sees its own CSV). That per-part runner is legitimate for
estabelecimento only because complete-snapshot key uniqueness was
separately verified once, as an external, one-time gate
(``estabelecimento_key_audit.py``, issue #100 / PR #102) -- not because a
"fail" policy makes single-part processing globally sufficient on its own.
Empresa has no such external gate (nor could it: "deterministic-collapse"
requires actually merging cross-part duplicates, not just detecting they
don't exist), which is exactly why it needs the dataset-level entry point
this module drives.

Division of responsibility, so this module never re-implements writer logic:

- this module resolves remote files, downloads/extracts/checksums each ZIP,
  and writes the durable orchestration manifest;
- ``canonical_shadow.run_canonical_shadow_dataset`` (the merged, table-driven
  writer) owns deduplication, deterministic ordering, sampling and casting;
- ``canonical_history.py``'s download/extract/checksum/atomic-write helpers
  are reused directly (imported, not copied), same discipline as
  ``estabelecimento_key_audit.py``.

Not restartable/checkpointed. ``canonical_history.py``'s per-part checkpoint
reuse (source ZIP + code fingerprint + output checksum matching) is a real
mechanism with known gaps tracked in issue #103 (acquisition mode and
retained evidence checksums do not fully participate in checkpoint
identity yet). Reproducing that same design here -- now across ten parts
instead of one -- without first closing #103 would just add a second copy
of the same trap. This module always performs a full fresh run instead;
fixing #103 globally is explicitly out of scope for this slice.

Disk lifecycle is deliberate: ZIPs are downloaded/checksummed and extracted
one at a time, and each ZIP is deleted immediately after its single CSV is
extracted and checksummed -- all ten several-hundred-MB ZIPs are never on
disk at once. The ten *extracted CSVs* do need to coexist for the duration
of the single ``write_canonical_dataset`` call (that is the entire point of
dataset-level deduplication -- there is no way to avoid it while still
seeing every part in one scope); they and the DuckDB work directory are
removed after the writer returns, success or failure, unless
``keep_extracted``/``keep_workdir`` is set for debugging.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from . import canonical_history, canonical_shadow, metrics, mirror, registry, sources, transform
from .sources import RemoteFile, is_valid_month

log = logging.getLogger(__name__)

_MANIFEST_FORMAT_VERSION = 1
_ORCHESTRATOR_VERSION = "2026-07-v1"
_TABLE_NAME = "empresa"
_PART_COUNT = 10
_PREFLIGHT_TIMEOUT = httpx.Timeout(30.0)


@dataclass(frozen=True)
class HistoricalDatasetResult:
    """Paths and manifest for one historical canonical empresa dataset run."""

    root: Path
    output_path: Path
    quality_path: Path
    metrics_path: Path
    manifest_path: Path
    manifest: dict[str, Any]


def _expected_part_names() -> list[str]:
    """The complete expected set of physical empresa part filenames --
    month-independent (physical layout, not URLs), so this can validate a
    ``--zip`` override's name before any month or network involvement."""
    names = sorted(spec.name for spec in sources.canonical_inventory() if spec.kind == "empresas")
    if len(names) != _PART_COUNT:  # pragma: no cover - sources.py invariant
        raise RuntimeError(f"expected {_PART_COUNT} empresa parts, sources.py declares {names!r}")
    return names


def empresa_remotes(month: str) -> list[RemoteFile]:
    """The complete expected set of ten physical empresa parts for `month`,
    same source-of-truth as ``sources.canonical_inventory()``."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return [
        RemoteFile(name=name, url=mirror.raw_file_url(month, name), kind="empresas")
        for name in _expected_part_names()
    ]


def _validate_overrides(overrides: dict[str, Path]) -> None:
    """Reject an override name outside the expected physical part set, and
    validate every override path exists and is a real ZIP -- all of this
    BEFORE any remote download starts, so a typo or a bad local file fails
    fast instead of after several parts have already been downloaded.

    Duplicate override names cannot actually reach this function (a
    ``dict`` can only hold one value per key), so the CLI layer is where a
    repeated ``--zip NAME=...`` is caught and rejected instead of silently
    overwriting the earlier one -- see ``main()``.
    """
    expected = set(_expected_part_names())
    unexpected = sorted(set(overrides) - expected)
    if unexpected:
        raise ValueError(
            f"--zip override name(s) not in the expected empresa part set: {unexpected!r} "
            f"(expected one of {sorted(expected)!r})"
        )
    for name, path in overrides.items():
        if not path.is_file():
            raise FileNotFoundError(f"{name}: local ZIP override not found: {path}")
        if not zipfile.is_zipfile(path):
            raise RuntimeError(f"{name}: local ZIP override is not a valid ZIP: {path}")


def preflight_remote_availability(
    remotes: Sequence[RemoteFile], *, client: httpx.Client | None = None
) -> list[str]:
    """HEAD every remote URL. Returns the names that are NOT confirmed
    downloadable (non-200 response or request error) -- an empty list means
    the complete part set is available. Does not download or extract
    anything, so it is safe to call before committing to expensive
    processing.
    """
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


def _code_fingerprints() -> dict[str, str]:
    modules = {
        "canonical_history_empresa": Path(__file__).resolve(),
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
        "output": root / "canonical" / "empresa.parquet",
        "quality": root / "evidence" / "empresa.quality.json",
        "metrics": root / "evidence" / "empresa.metrics.json",
        "manifest": root / "evidence" / "empresa.history.json",
        "failure": root / "evidence" / "empresa.history.failure.json",
    }


def _ensure_and_extract_one(
    remote: RemoteFile,
    paths: dict[str, Path],
    zip_override: Path | None,
    client: httpx.Client | None,
    *,
    keep_zip: bool = False,
) -> tuple[dict[str, Any], Path]:
    """Download (or use a local override), checksum, extract exactly one
    CSV, then delete the ZIP -- see module docstring for why ZIPs are not
    retained once extracted. The ZIP is removed whether extraction succeeds
    or fails (e.g. a ZIP with more than one member) unless ``keep_zip`` is
    set for debugging -- a downloaded ZIP must not linger on disk just
    because ITS OWN extraction failed. Returns (source manifest entry,
    extracted CSV path).
    """
    zip_path, acquisition = canonical_history._ensure_zip(  # noqa: SLF001
        remote, paths["raw_dir"], zip_override=zip_override, client=client
    )
    zip_entry = canonical_history._checked_file(zip_path)  # noqa: SLF001
    extract_dir = paths["extract_dir"] / remote.name
    try:
        csv_path = canonical_history._extract_one(zip_path, extract_dir)  # noqa: SLF001
    finally:
        if not keep_zip:
            zip_path.unlink(missing_ok=True)
    csv_entry = canonical_history._checked_file(csv_path)  # noqa: SLF001

    entry = {
        "name": remote.name,
        "url": remote.url,
        "acquisition": acquisition,
        "zip": zip_entry,
        "csv": csv_entry,
    }
    return entry, csv_path


def run_historical_empresa_dataset(
    month: str,
    root: Path,
    *,
    sample_size: int = 1_000,
    zip_overrides: dict[str, Path] | None = None,
    keep_extracted: bool = False,
    keep_raw: bool = False,
    client: httpx.Client | None = None,
) -> HistoricalDatasetResult:
    """Download, extract and canonicalize the complete ten-part empresa
    dataset for one historical snapshot, and persist a durable manifest.

    ``zip_overrides`` maps a part's filename (e.g. ``"Empresas0.zip"``) to a
    local ZIP path -- smoke/offline runs only. Every override is validated
    (name is one of the ten expected parts, path exists, path is a real
    ZIP) before anything else -- including before any remote download for
    the OTHER, non-overridden parts starts, so a typo or bad local file
    fails fast instead of after several real parts have already been
    downloaded. Any remote NOT covered by an override is then
    preflight-checked (HEAD) before any download starts; a partial real
    source set fails closed rather than silently proceeding with fewer
    than ten parts.
    """
    if sample_size < 0:
        raise ValueError("sample_size must not be negative")
    remotes = empresa_remotes(month)
    overrides = zip_overrides or {}
    _validate_overrides(overrides)

    to_check = [remote for remote in remotes if remote.name not in overrides]
    if to_check:
        own_client = client is None
        preflight_client = client or httpx.Client(timeout=_PREFLIGHT_TIMEOUT, follow_redirects=True)
        try:
            missing = preflight_remote_availability(to_check, client=preflight_client)
        finally:
            if own_client:
                preflight_client.close()
        if missing:
            raise RuntimeError(
                f"preflight failed: {len(missing)}/{len(to_check)} empresa part(s) not "
                f"available in the mirror for {month}: {sorted(missing)!r} -- refusing to "
                "start a partial-source-set run"
            )

    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    paths = _paths(root)
    code = _code_fingerprints()
    paths["failure"].unlink(missing_ok=True)

    sources_manifest: list[dict[str, Any]] = []
    csv_parts: list[tuple[Path, str]] = []
    try:
        for remote in remotes:
            entry, csv_path = _ensure_and_extract_one(
                remote, paths, overrides.get(remote.name), client, keep_zip=keep_raw
            )
            sources_manifest.append(entry)
            csv_parts.append((csv_path, remote.name))

        report = canonical_shadow.run_canonical_shadow_dataset(
            _TABLE_NAME,
            csv_parts,
            paths["output"],
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
            "sources": sources_manifest,
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
            # Every field here comes from the ONE metrics stage this module
            # instruments -- the canonical writer's own
            # canonical_empresa_dataset stage. It does NOT cover this
            # orchestrator's download/extract/preflight time, which happens
            # entirely outside that stage -- "scope" says so explicitly so a
            # reader cannot mistake wall_seconds for the total end-to-end run
            # duration (that belongs in the historical documentation as a
            # separately measured GitHub Actions job duration, not in this
            # manifest). Value names are the STABLE machine-readable keys
            # from metrics.StageMetrics.to_json_dict()'s actual contract,
            # unchanged and not prefixed -- renaming them here would break
            # the manifest contract real evidence (e.g. workflow run
            # 29777100790) was already produced under, for a label-only
            # clarification that belongs in comments/docs instead. Correct
            # key names are wall_seconds/rss_peak_delta_mib (NOT
            # wall_time_seconds/rss_delta_mib) -- copying the wrong names
            # here would silently produce an all-null resource_summary.
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
        return HistoricalDatasetResult(
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
            "sources": sources_manifest,
            "error": str(exc),
        }
        for name in ("output", "quality", "metrics"):
            if paths[name].is_file():
                failure[name] = canonical_history._checked_file(paths[name])  # noqa: SLF001
        canonical_history._write_json_atomic(paths["failure"], failure)  # noqa: SLF001
        raise
    finally:
        if not keep_extracted:
            shutil.rmtree(paths["extract_dir"], ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=1_000)
    parser.add_argument(
        "--zip",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Local ZIP override for one part, e.g. --zip Empresas0.zip=/path/to/Empresas0.zip "
        "(repeatable; smoke/offline runs only)",
    )
    parser.add_argument("--keep-extracted", action="store_true")
    parser.add_argument("--keep-raw", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    zip_overrides: dict[str, Path] = {}
    for entry in args.zip:
        name, _, path_str = entry.partition("=")
        if not name or not path_str:
            print(f"error: --zip must be NAME=PATH, got {entry!r}", file=sys.stderr)
            return 2
        if name in zip_overrides:
            print(f"error: --zip given more than once for {name!r}", file=sys.stderr)
            return 2
        zip_overrides[name] = Path(path_str)

    try:
        result = run_historical_empresa_dataset(
            args.month,
            args.root,
            sample_size=args.sample_size,
            zip_overrides=zip_overrides,
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
        f"historical canonical empresa dataset written: {result.output_path} — "
        f"{summary['rows_canonical']:,} rows, "
        f"{summary['duplicate_key_count']:,} duplicate key(s), "
        f"{summary['conflicting_key_count']:,} conflicting"
    )
    print(f"manifest: {result.manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "HistoricalDatasetResult",
    "empresa_remotes",
    "main",
    "preflight_remote_availability",
    "run_historical_empresa_dataset",
]
