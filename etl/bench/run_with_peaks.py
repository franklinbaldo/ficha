"""Run a command while sampling file/directory and process resource peaks.

This is intentionally generic evidence plumbing.  It does not interpret the
benchmark result or declare a winner; it adds an independently sampled resource
envelope around commands whose own JSON focuses on query timings.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def _size_bytes(path: Path) -> int:
    try:
        if path.is_file():
            return path.stat().st_size
        if not path.exists():
            return 0
    except OSError:
        return 0

    total = 0
    for root, _dirs, files in os.walk(path):
        for filename in files:
            try:
                total += (Path(root) / filename).stat().st_size
            except OSError:
                continue
    return total


def _disk_usage(path: Path) -> shutil._ntuple_diskusage | None:  # noqa: SLF001
    probe = path
    while not probe.exists():
        if probe.parent == probe:
            return None
        probe = probe.parent
    try:
        return shutil.disk_usage(probe)
    except OSError:
        return None


def _parse_watch(value: str) -> tuple[str, Path]:
    key, separator, raw_path = value.partition("=")
    if not separator or not key or not raw_path:
        raise argparse.ArgumentTypeError("--watch must be KEY=PATH")
    return key, Path(raw_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--watch", action="append", type=_parse_watch, default=[])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise SystemExit("a command is required after --")
    if command[0] in {"python", "python3"}:
        command[0] = sys.executable

    watches = dict(args.watch)
    peaks = dict.fromkeys(watches, 0)
    final_sizes = dict.fromkeys(watches, 0)
    filesystem_peak = 0
    filesystem_total = 0
    filesystem_path = next(iter(watches.values()), Path.cwd())

    def sample() -> None:
        nonlocal filesystem_peak, filesystem_total
        for key, path in watches.items():
            size = _size_bytes(path)
            final_sizes[key] = size
            peaks[key] = max(peaks[key], size)
        usage = _disk_usage(filesystem_path)
        if usage is not None:
            filesystem_peak = max(filesystem_peak, usage.used)
            filesystem_total = usage.total

    # resource (RUSAGE_CHILDREN) is POSIX-only -- this wrapper otherwise only
    # touches subprocess/shutil/os.walk, all cross-platform, so the import is
    # guarded here rather than at module level, and the CPU/RSS fields are
    # None on Windows (documented as unavailable) instead of blocking the
    # whole script from loading. Production/CI are Linux; this only matters
    # for a dev running the wrapper locally on Windows.
    has_rusage = sys.platform != "win32"
    if has_rusage:
        import resource

        child_usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    process = subprocess.Popen(command)
    sample()
    while process.poll() is None:
        time.sleep(args.interval)
        sample()
    sample()
    wall_seconds = time.monotonic() - started

    result = {
        "command": command,
        "exit_code": process.returncode,
        "wall_seconds": wall_seconds,
        "watched_peak_bytes": peaks,
        "watched_final_bytes": final_sizes,
        "filesystem_used_peak_bytes": filesystem_peak,
        "filesystem_total_bytes": filesystem_total,
    }
    if has_rusage:
        child_usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        result.update(
            {
                "child_user_cpu_seconds": child_usage_after.ru_utime - child_usage_before.ru_utime,
                "child_system_cpu_seconds": child_usage_after.ru_stime
                - child_usage_before.ru_stime,
                # Linux Actions runner: ru_maxrss is KiB. This wrapper runs one
                # command, so RUSAGE_CHILDREN is a useful process-tree peak envelope.
                "child_rss_peak_mib": child_usage_after.ru_maxrss / 1024,
            }
        )
    else:
        result.update(
            {
                "child_user_cpu_seconds": None,
                "child_system_cpu_seconds": None,
                "child_rss_peak_mib": None,
            }
        )
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(result, indent=2))
    raise SystemExit(process.returncode)


if __name__ == "__main__":
    main()
