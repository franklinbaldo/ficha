"""Historical shadow orchestration for one canonical establishment part.

This module is the transport/checkpoint slice after ``canonical_shadow``: it
resolves one historical Internet Archive ZIP, retains a checksummed local cache,
extracts exactly one CSV, runs the canonical writer, and persists a manifest
that makes retries verifiable. It never uploads to IA or feeds public products.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from . import canonical_shadow, download, mirror, registry, sources, transform
from .sources import RemoteFile, is_valid_month

_MANIFEST_FORMAT_VERSION = 1
_ORCHESTRATOR_VERSION = "2026-07-v1"


@dataclass(frozen=True)
class HistoricalShadowResult:
    """Paths and manifest for one historical shadow part."""

    root: Path
    output_path: Path
    quality_path: Path
    metrics_path: Path
    manifest_path: Path
    reused: bool
    manifest: dict[str, Any]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _code_fingerprints() -> dict[str, str]:
    modules = {
        "canonical_history": Path(__file__).resolve(),
        "canonical_shadow": Path(canonical_shadow.__file__).resolve(),
        "registry": Path(registry.__file__).resolve(),
    }
    return {name: _sha256(path) for name, path in modules.items()}


def estabelecimento_remote(month: str, part: int) -> RemoteFile:
    """Return the IA mirror source for one establishment partition."""
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    if not 0 <= part <= 9:
        raise ValueError(f"part must be between 0 and 9, got {part}")
    name = f"Estabelecimentos{part}.zip"
    return RemoteFile(
        name=name,
        url=mirror.raw_file_url(month, name),
        kind="estabelecimentos",
    )


def single_file_remote(table_name: str, month: str) -> RemoteFile:
    """Return the IA mirror source for a table with EXACTLY ONE physical
    file per ``sources.canonical_inventory()`` (e.g. ``simples`` --
    ``estabelecimento``/``empresa``/``socio`` are NOT single-file, so they
    don't use this: estabelecimento keeps its own per-part
    ``estabelecimento_remote`` above, and empresa's ten-part resolution
    lives in ``canonical_history_empresa.py``). Shared, reusable single-file
    remote resolution, so a new single-file table's historical runner
    doesn't need to reimplement this lookup.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    table = registry.main_table(table_name)
    matches = [spec for spec in sources.canonical_inventory() if spec.kind == table.kind]
    if len(matches) != 1:
        raise ValueError(
            f"{table_name}: single_file_remote requires exactly one physical file, "
            f"sources.canonical_inventory() declares {len(matches)}: "
            f"{[spec.name for spec in matches]!r}"
        )
    name = matches[0].name
    return RemoteFile(name=name, url=mirror.raw_file_url(month, name), kind=table.kind)


def _paths(root: Path, part: int) -> dict[str, Path]:
    return {
        "raw_dir": root / "raw",
        "extract_dir": root / "extracted",
        "work_dir": root / "work",
        "output": root / "canonical" / f"part-{part}.parquet",
        "quality": root / "evidence" / f"part-{part}.quality.json",
        "metrics": root / "evidence" / f"part-{part}.metrics.json",
        "manifest": root / "evidence" / f"part-{part}.history.json",
        "failure": root / "evidence" / f"part-{part}.history.failure.json",
    }


def _copy_override(source: Path, target: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() != target.resolve():
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copy2(source, temporary)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def _ensure_zip(
    remote: RemoteFile,
    raw_dir: Path,
    *,
    zip_override: Path | None,
    client: httpx.Client | None,
) -> tuple[Path, str]:
    target = raw_dir / remote.name
    if zip_override is not None:
        _copy_override(zip_override, target)
        if not zipfile.is_zipfile(target):
            raise RuntimeError(f"local override is not a valid ZIP: {zip_override}")
        return target, "local-override"
    if target.is_file() and zipfile.is_zipfile(target):
        return target, "local-cache"
    result = download.download_one(remote, raw_dir, client=client)
    if not zipfile.is_zipfile(result.path):
        raise RuntimeError(f"downloaded file is not a valid ZIP: {result.path}")
    return result.path, "range-resume" if result.resumed else "downloaded"


def _extract_one(zip_path: Path, extract_dir: Path) -> Path:
    shutil.rmtree(extract_dir, ignore_errors=True)
    files = [path for path in transform.extract_zip(zip_path, extract_dir) if path.is_file()]
    if len(files) != 1:
        raise RuntimeError(
            f"{zip_path.name}: expected exactly one extracted CSV, got {len(files)}: "
            f"{[path.name for path in files]}"
        )
    return files[0]


def _checked_file(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _reusable_manifest(
    paths: dict[str, Path],
    *,
    month: str,
    part: int,
    remote: RemoteFile,
    sample_size: int,
    code: dict[str, str],
) -> dict[str, Any] | None:
    required = (
        paths["raw_dir"] / remote.name,
        paths["output"],
        paths["quality"],
        paths["metrics"],
        paths["manifest"],
    )
    if not all(path.is_file() for path in required):
        return None
    try:
        payload = _load_json(paths["manifest"])
        quality = _load_json(paths["quality"])
        expected = {
            "source_zip": _sha256(paths["raw_dir"] / remote.name),
            "output": _sha256(paths["output"]),
            "quality": _sha256(paths["quality"]),
            "metrics": _sha256(paths["metrics"]),
        }
        matches = (
            payload["format_version"] == _MANIFEST_FORMAT_VERSION
            and payload["orchestrator_version"] == _ORCHESTRATOR_VERSION
            and payload["status"] == "ok"
            and payload["month"] == month
            and payload["part"] == part
            and payload["sample_size"] == sample_size
            and payload["source"]["name"] == remote.name
            and payload["source"]["url"] == remote.url
            and payload["code"] == code
            and payload["source"]["zip"]["sha256"] == expected["source_zip"]
            and payload["output"]["sha256"] == expected["output"]
            and payload["quality"]["sha256"] == expected["quality"]
            and payload["metrics"]["sha256"] == expected["metrics"]
            and quality["status"] == "ok"
            and quality["sample_size"] == min(sample_size, quality["rows_raw"])
            and quality["sample_mismatches"] == 0
            and quality["schema_matches"] is True
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    return payload if matches else None


def run_historical_shadow(
    month: str,
    part: int,
    root: Path,
    *,
    sample_size: int = 1_000,
    force: bool = False,
    zip_override: Path | None = None,
    keep_extracted: bool = False,
    client: httpx.Client | None = None,
) -> HistoricalShadowResult:
    """Build or reuse one checksummed historical canonical shadow part."""
    remote = estabelecimento_remote(month, part)
    if sample_size < 0:
        raise ValueError("sample_size must not be negative")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    paths = _paths(root, part)
    code = _code_fingerprints()

    if not force:
        reusable = _reusable_manifest(
            paths,
            month=month,
            part=part,
            remote=remote,
            sample_size=sample_size,
            code=code,
        )
        if reusable is not None:
            return HistoricalShadowResult(
                root,
                paths["output"],
                paths["quality"],
                paths["metrics"],
                paths["manifest"],
                True,
                reusable,
            )

    zip_path: Path | None = None
    csv_path: Path | None = None
    acquisition = "unknown"
    paths["failure"].unlink(missing_ok=True)
    try:
        zip_path, acquisition = _ensure_zip(
            remote,
            paths["raw_dir"],
            zip_override=zip_override,
            client=client,
        )
        csv_path = _extract_one(zip_path, paths["extract_dir"])
        report = canonical_shadow.run_shadow_part(
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

        manifest: dict[str, Any] = {
            "format_version": _MANIFEST_FORMAT_VERSION,
            "orchestrator_version": _ORCHESTRATOR_VERSION,
            "status": "ok",
            "month": month,
            "part": part,
            "sample_size": sample_size,
            "code": code,
            "source": {
                "name": remote.name,
                "url": remote.url,
                "acquisition": acquisition,
                "zip": _checked_file(zip_path),
                "csv": _checked_file(csv_path),
            },
            "output": _checked_file(paths["output"]),
            "quality": _checked_file(paths["quality"]),
            "metrics": _checked_file(paths["metrics"]),
        }
        _write_json_atomic(paths["manifest"], manifest)
        return HistoricalShadowResult(
            root,
            paths["output"],
            paths["quality"],
            paths["metrics"],
            paths["manifest"],
            False,
            manifest,
        )
    except Exception as exc:
        failure: dict[str, Any] = {
            "format_version": _MANIFEST_FORMAT_VERSION,
            "orchestrator_version": _ORCHESTRATOR_VERSION,
            "status": "failed",
            "month": month,
            "part": part,
            "sample_size": sample_size,
            "code": code,
            "source": {
                "name": remote.name,
                "url": remote.url,
                "acquisition": acquisition,
            },
            "error": str(exc),
        }
        if zip_path is not None and zip_path.is_file():
            failure["source"]["zip"] = _checked_file(zip_path)
        if csv_path is not None and csv_path.is_file():
            failure["source"]["csv"] = _checked_file(csv_path)
        for name in ("output", "quality", "metrics"):
            if paths[name].is_file():
                failure[name] = _checked_file(paths[name])
        _write_json_atomic(paths["failure"], failure)
        raise
    finally:
        if not keep_extracted:
            shutil.rmtree(paths["extract_dir"], ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--part", type=int, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=1_000)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--zip", type=Path, help="Local ZIP override for smoke/offline runs")
    parser.add_argument("--keep-extracted", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = run_historical_shadow(
            args.month,
            args.part,
            args.root,
            sample_size=args.sample_size,
            force=args.force,
            zip_override=args.zip,
            keep_extracted=args.keep_extracted,
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
    action = "reused" if result.reused else "written"
    print(f"historical canonical shadow {action}: {result.output_path}")
    print(f"manifest: {result.manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "HistoricalShadowResult",
    "estabelecimento_remote",
    "main",
    "run_historical_shadow",
]
