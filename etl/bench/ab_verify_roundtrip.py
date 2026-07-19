"""A/B the historical multi-query and current single-query roundtrip verifiers.

The benchmark normalizes both variants to the exact same deterministic reservoir
sample (REPEATABLE(42)). This intentionally removes the historical
``ORDER BY random()`` global sort from the legacy side, making the comparison
conservative for the current verifier: the measured delta isolates N Parquet
point lookups versus one sampled join instead of crediting the current side for
also replacing the old sampling algorithm.

A production-shaped base state and valid/corrupted Parquets are prepared once.
Every timed variant/repetition copies that closed DuckDB file into an isolated
state directory and runs in a fresh child process. The valid file must pass and
a same-row-count file with every ``razao_social`` corrupted must fail for both
variants. The sample fingerprint is recorded and asserted identical across all
runs.
"""

from __future__ import annotations

import argparse
import hashlib
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
from ficha_etl import metrics, registry, transform
from ficha_etl.transform import ExtractedFile

DATA = Path("bench/.work/data")
ROOT = Path("bench/.work/verify-roundtrip")
BASE_DB = ROOT / "base.duckdb"
VALID_PARQUET = ROOT / "cnpjs-valid.parquet"
CORRUPT_PARQUET = ROOT / "cnpjs-corrupt.parquet"
RUNS = ROOT / "runs"
RAW_RESULTS = ROOT / "worker-results"
RESULT_ROOT = Path("bench/.evidence")
VARIANTS = ("legacy", "current")
SAMPLE_REPEATABLE_SEED = 42
DEFAULT_SAMPLE_SIZE = 1000


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _main_files() -> tuple[list[ExtractedFile], list[Path]]:
    est_paths = sorted(DATA.glob("estabelecimento-*.csv"))
    files = [
        ExtractedFile(
            kind="empresas", zip_name="empresa.zip", csv_path=DATA / "empresa.csv"
        ),
        ExtractedFile(
            kind="simples", zip_name="simples.zip", csv_path=DATA / "simples.csv"
        ),
        ExtractedFile(kind="socios", zip_name="socio.zip", csv_path=DATA / "socio.csv"),
        *(
            ExtractedFile(kind="estabelecimentos", zip_name=path.name, csv_path=path)
            for path in est_paths
        ),
    ]
    return files, est_paths


def _sample_rows(con, sample_size: int) -> list[tuple[Any, ...]]:
    expected_n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    if expected_n == 0:
        return []
    n = min(sample_size, expected_n)
    fields = ", ".join(
        f"{expr} AS {alias}"
        for alias, expr in transform._ROUNDTRIP_FIELDS  # noqa: SLF001
    )
    return con.execute(
        f"""
        WITH sampled_est AS (
            SELECT * FROM estabelecimento
            USING SAMPLE reservoir({n} ROWS) REPEATABLE({SAMPLE_REPEATABLE_SEED})
        )
        SELECT est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
               {fields}
        FROM sampled_est est
        LEFT JOIN empresa emp ON emp.cnpj_basico = est.cnpj_basico
        """
    ).fetchall()


def _sample_fingerprint(con, sample_size: int) -> str:
    payload = json.dumps(
        _sample_rows(con, sample_size), ensure_ascii=False, default=str
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _legacy_assert_roundtrip(con, cnpjs_parquet: Path, *, sample_size: int) -> None:
    """Historical N-point-query verifier with sampling normalized to current.

    The original implementation used ``ORDER BY random() LIMIT N``. We use the
    current deterministic reservoir sample so both A/B sides validate the same
    rows and seed. Everything after sampling preserves the historical shape:
    one Parquet point query per sampled CNPJ.
    """
    expected_n = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
    actual_n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_quoted(cnpjs_parquet)}')"
    ).fetchone()[0]
    if expected_n != actual_n:
        raise transform.RoundtripError(
            f"row count mismatch: estabelecimento has {expected_n}, "
            f"cnpjs.parquet has {actual_n}"
        )
    if expected_n == 0:
        return

    sampled = _sample_rows(con, sample_size)
    aliases = [alias for alias, _ in transform._ROUNDTRIP_FIELDS]  # noqa: SLF001
    parquet_select = ", ".join(["cnpj", *aliases])
    divergences: list[str] = []
    for row in sampled:
        cnpj = row[0]
        actual = con.execute(
            f"SELECT {parquet_select} "
            f"FROM read_parquet('{_quoted(cnpjs_parquet)}') WHERE cnpj = ?",
            [cnpj],
        ).fetchone()
        if actual is None:
            divergences.append(f"{cnpj}: missing from parquet")
            continue
        for index, alias in enumerate(aliases, start=1):
            if row[index] != actual[index]:
                divergences.append(
                    f"{cnpj}.{alias}: source={row[index]!r} parquet={actual[index]!r}"
                )

    if divergences:
        head = divergences[:10]
        more = len(divergences) - len(head)
        message = "\n  ".join(head)
        if more:
            message += f"\n  ... and {more} more"
        raise transform.RoundtripError(
            f"roundtrip mismatch over {len(sampled)} sampled CNPJs:\n  {message}"
        )


def _verify(variant: str, con, parquet: Path, sample_size: int) -> None:
    if variant == "legacy":
        _legacy_assert_roundtrip(con, parquet, sample_size=sample_size)
    elif variant == "current":
        transform.assert_roundtrip(con, parquet, sample_size=sample_size)
    else:  # pragma: no cover - argparse constrains this
        raise ValueError(f"unknown variant: {variant}")


def _rss_peak_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _prepare_fixture() -> dict[str, Any]:
    shutil.rmtree(ROOT, ignore_errors=True)
    ROOT.mkdir(parents=True, exist_ok=True)
    files, est_paths = _main_files()
    if not est_paths:
        raise SystemExit("generate bench/.work/data before running roundtrip evidence")

    con = open_production_connection(BASE_DB)
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
        duplicate_rows = transform.load_main_tables_into_duckdb(con, files)

        transform.write_cnpjs_parquet_chunked(con, est_paths, VALID_PARQUET)
        transform._create_table_from_csvs(  # noqa: SLF001
            con,
            "estabelecimento",
            est_paths,
            registry.main_table("estabelecimento").source,
        )
        transform.assert_roundtrip(con, VALID_PARQUET, sample_size=DEFAULT_SAMPLE_SIZE)

        con.execute(
            f"""
            COPY (
                SELECT * REPLACE ('__ROUNDTRIP_CORRUPT__' AS razao_social)
                FROM read_parquet('{_quoted(VALID_PARQUET)}')
            ) TO '{_quoted(CORRUPT_PARQUET)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
            """
        )
        try:
            transform.assert_roundtrip(
                con, CORRUPT_PARQUET, sample_size=DEFAULT_SAMPLE_SIZE
            )
        except transform.RoundtripError:
            pass
        else:
            raise AssertionError(
                "current verifier accepted deliberately corrupted Parquet"
            )

        row_count = con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
        sample_fingerprint = _sample_fingerprint(con, DEFAULT_SAMPLE_SIZE)
        environment = capture_environment(con, BASE_DB)
        con.execute("CHECKPOINT")
    finally:
        con.close()

    return {
        "source_rows": row_count,
        "estabelecimento_chunks": len(est_paths),
        "duplicate_rows_collapsed": duplicate_rows,
        "sample_size": min(DEFAULT_SAMPLE_SIZE, row_count),
        "sample_repeatable_seed": SAMPLE_REPEATABLE_SEED,
        "sample_fingerprint": sample_fingerprint,
        "base_database_bytes": BASE_DB.stat().st_size,
        "valid_parquet_bytes": VALID_PARQUET.stat().st_size,
        "corrupt_parquet_bytes": CORRUPT_PARQUET.stat().st_size,
        "environment": environment,
    }


def _worker(variant: str, repetition: int, sample_size: int, output_json: Path) -> None:
    state_dir = RUNS / f"{variant}-rep{repetition}"
    shutil.rmtree(state_dir, ignore_errors=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "bench.duckdb"
    shutil.copy2(BASE_DB, db_path)

    con = open_production_connection(db_path)
    sampler: metrics._DiskPeakSampler | None = None  # noqa: SLF001
    try:
        environment = capture_environment(con, db_path)
        rss_baseline_mib = _rss_peak_mib()
        database_baseline_bytes = db_path.stat().st_size
        sampler = metrics._DiskPeakSampler(  # noqa: SLF001
            {"duckdb_tmp": state_dir / "duckdb_tmp", "state": state_dir},
            interval=0.1,
            filesystem_path=state_dir,
        )
        sampler.start()
        wall_start = time.monotonic()
        cpu_start = time.process_time()
        _verify(variant, con, VALID_PARQUET, sample_size)
        wall_seconds = time.monotonic() - wall_start
        cpu_seconds = time.process_time() - cpu_start
        rss_end_peak_mib = _rss_peak_mib()
        peaks = sampler.stop()
        sampler = None

        corruption_rejected = False
        corruption_error = ""
        try:
            _verify(variant, con, CORRUPT_PARQUET, sample_size)
        except transform.RoundtripError as exc:
            corruption_rejected = True
            corruption_error = str(exc).splitlines()[0]
        if not corruption_rejected:
            raise AssertionError(f"{variant} accepted deliberately corrupted Parquet")

        result = {
            "variant": variant,
            "repetition": repetition,
            "environment": environment,
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "rss_baseline_mib": rss_baseline_mib,
            "rss_end_peak_mib": rss_end_peak_mib,
            "rss_delta_mib": max(0.0, rss_end_peak_mib - rss_baseline_mib),
            "rss_measurement_note": (
                "Linux ru_maxrss is process-lifetime cumulative; delta records only a new "
                "high-water mark after the copied database was opened."
            ),
            "database_baseline_bytes": database_baseline_bytes,
            "database_bytes": db_path.stat().st_size,
            "duckdb_tmp_peak_bytes": peaks.get("duckdb_tmp", 0),
            "state_peak_bytes": peaks.get("state", 0),
            "filesystem_used_peak_bytes": peaks.get("filesystem", 0),
            "filesystem_total_bytes": peaks.get("filesystem_total", 0),
            "valid_parquet_passed": True,
            "corrupt_parquet_rejected": corruption_rejected,
            "corruption_error_head": corruption_error,
            "sample_size": min(
                sample_size,
                con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0],
            ),
            "sample_repeatable_seed": SAMPLE_REPEATABLE_SEED,
            "sample_fingerprint": _sample_fingerprint(con, sample_size),
        }
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(result, indent=2))
        print(json.dumps(result, sort_keys=True))
    finally:
        if sampler is not None:
            sampler.stop()
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
        "rss_delta_peak_mib": max(float(run["rss_delta_mib"]) for run in runs),
        "duckdb_tmp_peak_bytes": max(int(run["duckdb_tmp_peak_bytes"]) for run in runs),
        "state_peak_bytes": max(int(run["state_peak_bytes"]) for run in runs),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20260719)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument(
        "--json", type=Path, default=RESULT_ROOT / "verify-roundtrip.json"
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--variant", choices=VARIANTS, help=argparse.SUPPRESS)
    parser.add_argument("--repetition", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--worker-json", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        if args.variant is None or args.worker_json is None:
            raise SystemExit("worker mode requires --variant and --worker-json")
        _worker(args.variant, args.repetition, args.sample_size, args.worker_json)
        return
    if args.repeats < 1 or args.sample_size < 1:
        raise SystemExit("repeats and sample-size must be positive")

    fixture = _prepare_fixture()
    RAW_RESULTS.mkdir(parents=True, exist_ok=True)
    by_variant: dict[str, list[dict[str, Any]]] = {variant: [] for variant in VARIANTS}
    execution_order: list[str] = []
    start_with_legacy = random.Random(args.seed).choice([True, False])
    for repetition in range(args.repeats):
        legacy_first = (
            start_with_legacy if repetition % 2 == 0 else not start_with_legacy
        )
        order = VARIANTS if legacy_first else tuple(reversed(VARIANTS))
        execution_order.append("legacy-current" if legacy_first else "current-legacy")
        for variant in order:
            worker_json = RAW_RESULTS / f"{variant}-rep{repetition}.json"
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    "--variant",
                    variant,
                    "--repetition",
                    str(repetition),
                    "--sample-size",
                    str(args.sample_size),
                    "--worker-json",
                    str(worker_json),
                ],
                check=True,
            )
            by_variant[variant].append(json.loads(worker_json.read_text()))

    fingerprints = {
        run["sample_fingerprint"] for runs in by_variant.values() for run in runs
    }
    if fingerprints != {fixture["sample_fingerprint"]}:
        raise AssertionError(
            "variants/repetitions did not use the same deterministic sample: "
            f"fixture={fixture['sample_fingerprint']} observed={sorted(fingerprints)}"
        )
    for runs in by_variant.values():
        if not all(run["valid_parquet_passed"] for run in runs):
            raise AssertionError("a verifier rejected the valid Parquet")
        if not all(run["corrupt_parquet_rejected"] for run in runs):
            raise AssertionError("a verifier accepted the corrupted Parquet")

    result = {
        "fixture": fixture,
        "repeats": args.repeats,
        "alternation_seed": args.seed,
        "execution_order": execution_order,
        "methodology": {
            "legacy_shape": "same reservoir sample plus one Parquet point query per CNPJ",
            "current_shape": "production assert_roundtrip reservoir sample plus one join",
            "fairness": (
                "Both use reservoir REPEATABLE(42) and identical copied DuckDB states. "
                "Removing historical ORDER BY random() makes this conservative for current."
            ),
            "timed_region": (
                "valid-Parquet verification only; fixture/setup/corruption check untimed"
            ),
        },
        "results": {variant: _aggregate(runs) for variant, runs in by_variant.items()},
    }
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(result, indent=2))
    print(f"wrote {args.json}")
    for variant, aggregate in result["results"].items():
        print(
            f"  {variant:<8} wall median={aggregate['wall_median_seconds']:.4f}s "
            f"spread={aggregate['wall_spread_seconds']:.4f}s; "
            f"cpu median={aggregate['cpu_median_seconds']:.4f}s"
        )
    shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    main()
