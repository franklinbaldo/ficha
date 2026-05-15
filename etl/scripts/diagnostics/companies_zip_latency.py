"""Measure IA transparent-unzip latency for per-company `.pb` requests.

Depends on a prior `pack_e2e` run that uploaded `companies.zip` to a test
item and wrote `/tmp/pack_e2e_paths.json` (a sample of `XX/XXX/XXX.pb`
member paths). Three modes:

1. **Cold**: GET each path once. The IA edge cache may be empty.
2. **Warm**: GET each path twice; report the second timing only.
3. **Parallel**: GET 50 paths concurrently via `httpx.AsyncClient`.

Reports p50/p95/p99 + bytes + throughput; writes
`/tmp/companies_zip_latency.json`.

Env vars:
  MONTH           target month (default 2026-04)
  ITEM            IA item identifier (default ficha-poc-companies-{month})
  NUM_PATHS       how many paths to sample (default 100)
  PARALLELISM     concurrent requests in parallel mode (default 50)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import statistics
import sys
import time
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _percentiles(samples: list[float]) -> dict:
    if not samples:
        return {"p50": None, "p95": None, "p99": None, "mean": None}
    s = sorted(samples)
    return {
        "p50": round(statistics.median(s), 3),
        "p95": round(s[int(0.95 * (len(s) - 1))], 3),
        "p99": round(s[int(0.99 * (len(s) - 1))], 3),
        "mean": round(statistics.fmean(s), 3),
    }


def _fetch(client: httpx.Client, url: str) -> tuple[float, int]:
    """Return (seconds, bytes) for a single GET."""
    t0 = time.monotonic()
    r = client.get(url, timeout=30.0)
    r.raise_for_status()
    return (time.monotonic() - t0, len(r.content))


def probe_cold(client: httpx.Client, urls: list[str]) -> dict:
    log.info("cold: %d sequential GETs", len(urls))
    samples: list[float] = []
    total_bytes = 0
    for u in urls:
        sec, nbytes = _fetch(client, u)
        samples.append(sec)
        total_bytes += nbytes
    return {"count": len(samples), "bytes": total_bytes, **_percentiles(samples)}


def probe_warm(client: httpx.Client, urls: list[str]) -> dict:
    """Fetch each URL twice; report the second timing."""
    log.info("warm: %d×2 sequential GETs", len(urls))
    samples: list[float] = []
    total_bytes = 0
    for u in urls:
        _fetch(client, u)  # prime cache
        sec, nbytes = _fetch(client, u)
        samples.append(sec)
        total_bytes += nbytes
    return {"count": len(samples), "bytes": total_bytes, **_percentiles(samples)}


async def _afetch(client: httpx.AsyncClient, url: str) -> tuple[float, int]:
    t0 = time.monotonic()
    r = await client.get(url, timeout=30.0)
    r.raise_for_status()
    return (time.monotonic() - t0, len(r.content))


async def _probe_parallel(urls: list[str], parallelism: int) -> dict:
    sem = asyncio.Semaphore(parallelism)
    samples: list[float] = []
    byte_counts: list[int] = []

    async with httpx.AsyncClient(http2=False) as client:

        async def one(u: str) -> None:
            async with sem:
                sec, nbytes = await _afetch(client, u)
                samples.append(sec)
                byte_counts.append(nbytes)

        t0 = time.monotonic()
        await asyncio.gather(*(one(u) for u in urls))
        wall = time.monotonic() - t0

    total_bytes = sum(byte_counts)

    return {
        "count": len(samples),
        "bytes": total_bytes,
        "parallelism": parallelism,
        "wall_seconds": round(wall, 3),
        "throughput_mb_s": round(total_bytes / 1e6 / wall, 2) if wall > 0 else None,
        **_percentiles(samples),
    }


def probe_parallel(urls: list[str], parallelism: int) -> dict:
    log.info("parallel: %d GETs at concurrency %d", len(urls), parallelism)
    return asyncio.run(_probe_parallel(urls, parallelism))


def main() -> int:
    month = os.environ.get("MONTH", "2026-04").strip() or "2026-04"
    item = os.environ.get("ITEM", f"ficha-poc-companies-{month}")
    num_paths = int(os.environ.get("NUM_PATHS", "100"))
    parallelism = int(os.environ.get("PARALLELISM", "50"))

    paths_file = Path("/tmp/pack_e2e_paths.json")
    if not paths_file.exists():
        print(f"::error::{paths_file} not found — run pack_e2e first")
        return 1

    all_paths = json.loads(paths_file.read_text())
    paths = all_paths[:num_paths]
    base_url = f"https://archive.org/download/{item}/companies.zip"
    urls = [f"{base_url}/{p}" for p in paths]

    report: dict = {"month": month, "item": item, "num_paths": len(urls)}

    # Sanity check: _meta.json must be reachable before the probes.
    meta_url = f"{base_url}/_meta.json"
    log.info("HEAD %s", meta_url)
    try:
        with httpx.Client() as client:
            r = client.head(meta_url, timeout=30.0, follow_redirects=True)
            r.raise_for_status()
            report["meta_head_status"] = r.status_code
    except Exception as exc:
        print(f"::error::failed to HEAD {meta_url}: {exc}")
        return 1

    with httpx.Client() as client:
        try:
            report["cold"] = probe_cold(client, urls)
            report["warm"] = probe_warm(client, urls)
        except Exception as exc:
            log.exception("sequential probe failed")
            print(f"::error::sequential probe: {exc}")
            return 1

    try:
        report["parallel"] = probe_parallel(urls, parallelism)
    except Exception as exc:
        log.exception("parallel probe failed")
        print(f"::error::parallel probe: {exc}")
        return 1

    Path("/tmp/companies_zip_latency.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print("::notice::companies_zip_latency probe finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
