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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import duckdb

from ficha_etl.transform import pick_memory_limit_gb, pick_threads


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
    return {
        "duckdb_version": duckdb.__version__,
        "threads": str(threads),
        "memory_limit": str(memory_limit),
        "preserve_insertion_order": str(preserve_order),
        "connection_type": "file-backed",
        "connection_path": str(db_path),
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": _cpu_count(),
        "python_version": platform.python_version(),
    }


def _cpu_count() -> int | None:
    return os.cpu_count()


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
        # A spread wider than the median delta means "noise-dominated" --
        # flagged explicitly rather than silently picking a winner anyway.
        delta = abs(self.median_a - self.median_b)
        noisy = delta < max(self.spread_a, self.spread_b)
        verdict = (
            "WITHIN NOISE (spread >= delta)"
            if noisy
            else (
                f"{self.label_b} faster"
                if self.median_b < self.median_a
                else f"{self.label_a} faster"
            )
        )
        print(f"  ratio {self.label_b}/{self.label_a} = {ratio:.2f}  -> {verdict}")


def run_ab(
    n: int,
    seed: int,
    fn_a: Callable[[], float],
    fn_b: Callable[[], float],
    label_a: str = "A",
    label_b: str = "B",
) -> ABResult:
    """Run `fn_a`/`fn_b` (each returning an elapsed-seconds float) `n` times
    each, alternating which one runs FIRST each iteration using a seeded RNG
    -- never "always A before B" (which lets warm-cache/CPU-throttle drift
    always favor the same side). Same seed -> same alternation sequence,
    every run -- reproducible, not just "randomized".
    """
    rng = random.Random(seed)
    result = ABResult(label_a=label_a, label_b=label_b, seed=seed)
    for _ in range(n):
        if rng.random() < 0.5:
            result.times_a.append(fn_a())
            result.times_b.append(fn_b())
            result.order.append("AB")
        else:
            result.times_b.append(fn_b())
            result.times_a.append(fn_a())
            result.order.append("BA")
    return result
