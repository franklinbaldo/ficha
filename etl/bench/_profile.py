"""Shared production-profile helpers for the ETL benchmark harness.

RFC 0001 §7.10: "Resultado de laptop com paralelismo diferente do runner é
exploração, não decisão." A benchmark that opens `duckdb.connect()` (in-memory,
default threads) is measuring a different execution regime than the one that
actually decides production behavior -- `transform_snapshot` opens a
file-backed connection with `memory_limit`, `temp_directory`,
`preserve_insertion_order=false`, and (deliberately, after OOM/spill
incidents) `threads=1`.

This module exists so every bench script under `bench/` measures under the
SAME configuration, and records what configuration + machine actually ran --
not just the timing numbers, which are meaningless without that context.
"""

from __future__ import annotations

import os
import platform
import random
import statistics
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import duckdb

from ficha_etl.transform import pick_memory_limit_gb, pick_threads

# Bump when the *methodology* in this module changes (connection setup,
# alternation scheme, what capture_environment records) -- independent of
# git_sha, which tracks the exact commit but doesn't say at a glance whether
# two JSON results are comparable under the same rules.
HARNESS_VERSION = "2026-07-profile-v3"


def open_production_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """File-backed DuckDB connection with the exact PRAGMAs
    `transform_snapshot` uses in production (see transform.py's PHASE 2/4
    connection setup) -- `memory_limit`/`threads` via the same
    `pick_memory_limit_gb`/`pick_threads` production uses (so
    `FICHA_MEMORY_LIMIT_GB`/`FICHA_THREADS` env overrides apply identically
    here), `temp_directory` next to the db file, `preserve_insertion_order`
    off.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    mem_gb = pick_memory_limit_gb()
    con.execute(f"PRAGMA memory_limit='{mem_gb}GB'")
    tmp_dir = db_path.parent / "duckdb_tmp"
    con.execute(f"PRAGMA temp_directory='{tmp_dir}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    threads = pick_threads()
    con.execute(f"PRAGMA threads={threads}")
    return con


def capture_environment(con: duckdb.DuckDBPyConnection, db_path: Path) -> dict[str, Any]:
    """Effective (not requested) connection config + machine metadata.

    Reads back via `current_setting` rather than trusting the value we asked
    for -- same discipline as `MetricsRecorder.capture_pragmas` (RFC 0001
    §16): a benchmark result without this is a number with no way to tell,
    six months later, whether it ran under production conditions or a
    laptop default.
    """
    memory_limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    preserve_order = con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    temp_directory = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    return {
        "harness_version": HARNESS_VERSION,
        "git_sha": _git_sha(),
        "duckdb_version": duckdb.__version__,
        "threads": str(threads),
        "memory_limit": str(memory_limit),
        "preserve_insertion_order": str(preserve_order),
        "temp_directory": str(temp_directory),
        "connection_type": "file-backed",
        "connection_path": str(db_path),
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": _cpu_count(),
        "python_version": platform.python_version(),
    }


def _cpu_count() -> int | None:
    return os.cpu_count()


def _git_sha() -> str:
    """Best-effort exact commit a result was produced against -- a JSON
    result without this can't be tied back to the code that ran it once the
    branch moves on. Falls back to "unknown" rather than raising: a missing
    git binary or a non-repo checkout shouldn't crash the benchmark itself.
    """
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


@dataclass
class ABResult:
    """Timings from an alternating A/B comparison. Never collapses to a
    single "winner" number -- median + spread are the primary numbers;
    callers decide what counts as a meaningful difference given the spread,
    not this module.
    """

    label_a: str
    label_b: str
    times_a: list[float] = field(default_factory=list)
    times_b: list[float] = field(default_factory=list)
    order: list[str] = field(default_factory=list)  # "AB" or "BA" per iteration
    seed: int = 0

    @property
    def median_a(self) -> float:
        return statistics.median(self.times_a)

    @property
    def median_b(self) -> float:
        return statistics.median(self.times_b)

    @property
    def spread_a(self) -> float:
        return (max(self.times_a) - min(self.times_a)) if len(self.times_a) > 1 else 0.0

    @property
    def spread_b(self) -> float:
        return (max(self.times_b) - min(self.times_b)) if len(self.times_b) > 1 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "label_a": self.label_a,
            "label_b": self.label_b,
            "seed": self.seed,
            "order": self.order,
            "times_a": [round(t, 4) for t in self.times_a],
            "times_b": [round(t, 4) for t in self.times_b],
            "median_a": round(self.median_a, 4),
            "median_b": round(self.median_b, 4),
            "spread_a": round(self.spread_a, 4),
            "spread_b": round(self.spread_b, 4),
        }

    def print_summary(self) -> None:
        print(
            f"  {self.label_a:<10} median={self.median_a:.3f}s "
            f"spread={self.spread_a:.3f}s  n={len(self.times_a)}"
        )
        print(
            f"  {self.label_b:<10} median={self.median_b:.3f}s "
            f"spread={self.spread_b:.3f}s  n={len(self.times_b)}"
        )
        ratio = self.median_b / self.median_a if self.median_a else float("nan")
        # A spread at least as wide as the median delta means "noise-dominated".
        # No automatic "X faster" verdict: with n=1 both spreads are zero and
        # a single run must not be declared a meaningful winner.
        delta = abs(self.median_a - self.median_b)
        noisy = delta <= max(self.spread_a, self.spread_b)
        note = " (WITHIN NOISE: spread >= delta)" if noisy else ""
        print(f"  ratio {self.label_b}/{self.label_a} = {ratio:.2f}{note}")


def run_ab(
    n: int,
    seed: int,
    fn_a: Callable[[], float],
    fn_b: Callable[[], float],
    label_a: str = "A",
    label_b: str = "B",
) -> ABResult:
    """Run `fn_a`/`fn_b` (each returning an elapsed-seconds float) `n` times
    each, STRICTLY alternating which one runs first -- never "always A before
    B" (which lets warm-cache/CPU-throttle drift always favor the same side).
    The seed only picks which side starts; every iteration after that flips
    deterministically. A per-iteration coin flip (the previous approach) is
    reproducible but not balanced -- at low `n` it can land on the same side
    several times in a row, which is exactly the drift this is meant to
    cancel out.
    """
    start_with_a = random.Random(seed).choice([True, False])
    result = ABResult(label_a=label_a, label_b=label_b, seed=seed)
    for i in range(n):
        a_first = start_with_a if i % 2 == 0 else not start_with_a
        if a_first:
            result.times_a.append(fn_a())
            result.times_b.append(fn_b())
            result.order.append("AB")
        else:
            result.times_b.append(fn_b())
            result.times_a.append(fn_a())
            result.order.append("BA")
    return result


def assert_parquet_equivalent(path_a: Path, path_b: Path, label_a: str, label_b: str) -> None:
    """Fail loudly if `path_a`/`path_b` don't hold the same rows (regardless
    of order) and the same schema.

    An A/B that only measures wall-clock and never checks this would happily
    report a "faster" variant that's actually wrong -- a rewritten query that
    silently drops rows, mishandles NULLs, or changes a column's type reads
    as a win on this harness unless something checks the output, not just
    the time it took to produce it. Meant to run ONCE, untimed, before the
    timed A/B loop -- not on every iteration.
    """
    con = duckdb.connect()
    try:
        schema_a = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_a}')").fetchall()
        schema_b = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_b}')").fetchall()
        if schema_a != schema_b:
            raise AssertionError(
                f"{label_a} vs {label_b}: schema mismatch\n  {label_a}: {schema_a}\n"
                f"  {label_b}: {schema_b}"
            )
        diff = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                (SELECT * FROM read_parquet('{path_a}')
                 EXCEPT ALL
                 SELECT * FROM read_parquet('{path_b}'))
                UNION ALL
                (SELECT * FROM read_parquet('{path_b}')
                 EXCEPT ALL
                 SELECT * FROM read_parquet('{path_a}'))
            )
            """
        ).fetchone()[0]
        if diff != 0:
            raise AssertionError(
                f"{label_a} vs {label_b}: {diff} row(s) differ -- not equivalent, "
                "timed comparison would be measuring a correctness change, not a speed change"
            )
    finally:
        con.close()
