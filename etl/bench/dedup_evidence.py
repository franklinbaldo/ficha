"""Measure the real main-table loader under no, exact, and conflicting duplicates.

Fixture preparation is untimed.  Each mode/repetition runs in a fresh child
process, file-backed DuckDB database, and spill directory so RSS and cache state
cannot leak between measurements.  The timed region is exactly
`load_main_tables_into_duckdb`; lookups are loaded beforehand to match the
production phase boundary. Disk sampling deliberately continues through
`CHECKPOINT` and connection close so database/WAL/temp coexistence is captured.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import resource
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from _profile import capture_environment, open_production_connection
from ficha_etl import metrics, transform
from ficha_etl.transform import ExtractedFile

DATA = Path("bench/.work/data")
FIXTURES = Path("bench/.work/dedup-fixtures")
RUNS = Path("bench/.work/dedup-runs")
RESULT_ROOT = Path("bench/.evidence")
DUP_EVERY_N = 500
MODES = ("none", "exact", "conflicting")


def _read_unique(path: Path) -> list[list[str]]:
    unique: dict[str, list[str]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle, delimiter=";", quotechar='"'):
            if row:
                unique.setdefault(row[0], row)
    return list(unique.values())


def _selected(rows: list[list[str]]) -> list[list[str]]:
    return [row for row in rows if int(row[0]) % DUP_EVERY_N == 0]


def _write_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter=";", quotechar='"', lineterminator="\n")
        writer.writerows(rows)


def _prepare_fixtures() -> dict[str, Any]:
    shutil.rmtree(FIXTURES, ignore_errors=True)
    empresa_unique = _read_unique(DATA / "empresa.csv")
    simples_unique = _read_unique(DATA / "simples.csv")
    selected_empresa = _selected(empresa_unique)
    selected_simples = _selected(simples_unique)

    manifest: dict[str, Any] = {
        "unique_rows": {"empresa": len(empresa_unique), "simples": len(simples_unique)},
        "modes": {},
    }
    for mode in MODES:
        empresa_rows = [row.copy() for row in empresa_unique]
        simples_rows = [row.copy() for row in simples_unique]
        expected_duplicates = 0
        if mode == "exact":
            empresa_rows.extend(row.copy() for row in selected_empresa)
            simples_rows.extend(row.copy() for row in selected_simples)
            expected_duplicates = len(selected_empresa) + len(selected_simples)
        elif mode == "conflicting":
            for row in selected_empresa:
                duplicate = row.copy()
                duplicate[1] = f"{duplicate[1]} CONFLICT"
                empresa_rows.append(duplicate)
            for row in selected_simples:
                duplicate = row.copy()
                duplicate[1] = "N" if duplicate[1] == "S" else "S"
                simples_rows.append(duplicate)
            expected_duplicates = len(selected_empresa) + len(selected_simples)

        mode_dir = FIXTURES / mode
        _write_rows(mode_dir / "empresa.csv", empresa_rows)
        _write_rows(mode_dir / "simples.csv", simples_rows)
        manifest["modes"][mode] = {
            "expected_duplicate_rows": expected_duplicates,
            "empresa_input_rows": len(empresa_rows),
            "simples_input_rows": len(simples_rows),
        }

    (FIXTURES / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _rss_peak_mib() -> float:
    # Workers run on Linux in Actions. ru_maxrss is KiB there and is cumulative
    # for the lifetime of this worker process; callers must compare against a
    # baseline captured immediately before the measured loader region.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _worker(mode: str, repetition: int, output_json: Path) -> None:
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    expected_duplicates = manifest["modes"][mode]["expected_duplicate_rows"]
    state_dir = RUNS / f"{mode}-rep{repetition}"
    shutil.rmtree(state_dir, ignore_errors=True)
    db_path = state_dir / "bench.duckdb"
    con = open_production_connection(db_path)
    sampler: metrics._DiskPeakSampler | None = None  # noqa: SLF001
    try:
        for kind in (
            "cnaes",
            "municipios",
            "naturezas",
            "qualificacoes",
            "paises",
            "motivos",
        ):
            transform.load_lookup_into_duckdb(con, kind, DATA / f"lookup_{kind}.csv")

        est_paths = sorted(DATA.glob("estabelecimento-*.csv"))
        files = [
            ExtractedFile(
                kind="empresas",
                zip_name=f"empresa-{mode}.zip",
                csv_path=FIXTURES / mode / "empresa.csv",
            ),
            ExtractedFile(
                kind="simples",
                zip_name=f"simples-{mode}.zip",
                csv_path=FIXTURES / mode / "simples.csv",
            ),
            ExtractedFile(kind="socios", zip_name="socio.zip", csv_path=DATA / "socio.csv"),
            *(
                ExtractedFile(kind="estabelecimentos", zip_name=path.name, csv_path=path)
                for path in est_paths
            ),
        ]
        environment = capture_environment(con, db_path)
        rss_baseline_mib = _rss_peak_mib()
        sampler = metrics._DiskPeakSampler(  # noqa: SLF001 -- benchmark reuses production sampler
            {"duckdb_tmp": state_dir / "duckdb_tmp", "state": state_dir},
            interval=0.1,
            filesystem_path=state_dir,
        )
        sampler.start()

        wall_start = time.monotonic()
        cpu_start = time.process_time()
        duplicate_rows = transform.load_main_tables_into_duckdb(con, files)
        wall_seconds = time.monotonic() - wall_start
        cpu_seconds = time.process_time() - cpu_start
        rss_loader_end_peak_mib = _rss_peak_mib()

        if duplicate_rows != expected_duplicates:
            raise AssertionError(
                f"{mode}: loader returned {duplicate_rows} duplicate rows; "
                f"fixture expected {expected_duplicates}"
            )
        empresa_rows = con.execute("SELECT COUNT(*) FROM empresa").fetchone()[0]
        simples_rows = con.execute("SELECT COUNT(*) FROM simples").fetchone()[0]
        if empresa_rows != manifest["unique_rows"]["empresa"]:
            raise AssertionError(f"{mode}: empresa dedup result has {empresa_rows} rows")
        if simples_rows != manifest["unique_rows"]["simples"]:
            raise AssertionError(f"{mode}: simples dedup result has {simples_rows} rows")

        # Keep disk sampling alive beyond the loader timer: checkpoint/flush and
        # close can briefly coexist with WAL, the consolidated DB, and temp files.
        con.execute("CHECKPOINT")
        database_bytes = db_path.stat().st_size
        con.close()
        con = None
        peaks = sampler.stop()
        sampler = None
        rss_post_close_peak_mib = _rss_peak_mib()

        result = {
            "mode": mode,
            "repetition": repetition,
            "environment": environment,
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "rss_baseline_mib": rss_baseline_mib,
            "rss_loader_end_peak_mib": rss_loader_end_peak_mib,
            "rss_loader_delta_mib": max(0.0, rss_loader_end_peak_mib - rss_baseline_mib),
            "rss_post_close_peak_mib": rss_post_close_peak_mib,
            "rss_post_close_delta_mib": max(0.0, rss_post_close_peak_mib - rss_baseline_mib),
            "rss_measurement_note": (
                "Linux ru_maxrss is process-lifetime cumulative. Baseline is captured after "
                "lookup setup and immediately before the timed loader; deltas show only new "
                "high-water marks after that baseline."
            ),
            "disk_sampling_window": "loader start through CHECKPOINT and connection close",
            "duplicate_rows": duplicate_rows,
            "result_rows": {"empresa": empresa_rows, "simples": simples_rows},
            "database_bytes": database_bytes,
            "duckdb_tmp_peak_bytes": peaks.get("duckdb_tmp", 0),
            "state_peak_bytes": peaks.get("state", 0),
            "filesystem_used_peak_bytes": peaks.get("filesystem", 0),
            "filesystem_total_bytes": peaks.get("filesystem_total", 0),
        }
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(result, indent=2))
        print(json.dumps(result, sort_keys=True))
    finally:
        if sampler is not None:
            sampler.stop()
        if con is not None:
            con.close()
        shutil.rmtree(state_dir, ignore_errors=True)


def _aggregate(runs: list[dict[str, Any]]) -> dict[str, Any]:
    wall = [float(run["wall_seconds"]) for run in runs]
    cpu = [float(run["cpu_seconds"]) for run in runs]
    return {
        "runs": runs,
        "wall_median_seconds": statistics.median(wall),
        "wall_spread_seconds": max(wall) - min(wall) if len(wall) > 1 else 0.0,
        "cpu_median_seconds": statistics.median(cpu),
        "cpu_spread_seconds": max(cpu) - min(cpu) if len(cpu) > 1 else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--json", type=Path, default=RESULT_ROOT / "dedup.json")
    parser.add_argument("--seed", type=int, default=20260719)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=MODES, help=argparse.SUPPRESS)
    parser.add_argument("--repetition", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--worker-json", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        if args.mode is None or args.worker_json is None:
            raise SystemExit("worker mode requires --mode and --worker-json")
        _worker(args.mode, args.repetition, args.worker_json)
        return

    if not sorted(DATA.glob("estabelecimento-*.csv")):
        raise SystemExit("generate bench/.work/data before running dedup evidence")
    manifest = _prepare_fixtures()
    shutil.rmtree(RUNS, ignore_errors=True)
    raw_results = Path("bench/.work/dedup-worker-results")
    shutil.rmtree(raw_results, ignore_errors=True)

    by_mode: dict[str, list[dict[str, Any]]] = {mode: [] for mode in MODES}
    execution_order: list[list[str]] = []
    start = random.Random(args.seed).randrange(len(MODES))
    for repetition in range(args.repeats):
        order = [MODES[(start + repetition + offset) % len(MODES)] for offset in range(len(MODES))]
        execution_order.append(order)
        for mode in order:
            worker_json = raw_results / f"{mode}-rep{repetition}.json"
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    "--mode",
                    mode,
                    "--repetition",
                    str(repetition),
                    "--worker-json",
                    str(worker_json),
                ],
                check=True,
            )
            by_mode[mode].append(json.loads(worker_json.read_text()))

    result = {
        "fixture_manifest": manifest,
        "repeats": args.repeats,
        "seed": args.seed,
        "execution_order": execution_order,
        "results": {mode: _aggregate(runs) for mode, runs in by_mode.items()},
    }
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(result, indent=2))
    print(f"wrote {args.json}")
    shutil.rmtree(FIXTURES, ignore_errors=True)
    shutil.rmtree(RUNS, ignore_errors=True)
    shutil.rmtree(raw_results, ignore_errors=True)


if __name__ == "__main__":
    main()
