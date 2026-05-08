"""Phase 1 diagnostics — Claude's harness, runs in GitHub Actions.

Goals:
1. Inventory existing `ficha-*` items on Internet Archive (so Phase 2/3
   ETL doesn't re-download what's already mirrored).
2. Phase 1 step 1.2: simples cardinality query. Loads JUST the
   `simples` ZIP (smallest of the four large tables) from IA mirror or
   RFB upstream, and runs the GROUP BY 1 HAVING COUNT(*) > 1 probe.
   Result decides whether `transform.py:478`'s LEFT JOIN is silently
   inflating cnpjs.parquet rows.
3. Phase 1 step 1.4: confirm FIFO probe in CI environment too.

Exits non-zero on diagnostic failure (workflow then surfaces via PR
comment + alert via subscribe_pr_activity).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def ia_inventory() -> list[dict]:
    """Query IA advanced search for all ficha-* items."""
    section("IA inventory: ficha-* items")
    url = (
        "https://archive.org/advancedsearch.php?"
        "q=identifier:ficha-*"
        "&fl[]=identifier&fl[]=item_size&fl[]=publicdate&fl[]=files_count"
        "&output=json&rows=100&sort[]=identifier+desc"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "ficha-claude-diag/1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)
    docs = data.get("response", {}).get("docs", [])
    print(f"found {len(docs)} ficha-* items on IA")
    for d in docs:
        size_gb = (d.get("item_size") or 0) / (1024**3)
        print(
            f"  {d.get('identifier'):30s}  "
            f"{size_gb:6.2f} GB  "
            f"{d.get('files_count', '?'):>5} files  "
            f"{d.get('publicdate', '')}"
        )
    return docs


def pick_month(docs: list[dict]) -> str | None:
    """Pick the most recent month with raw files for the cardinality probe."""
    env_month = os.environ.get("MONTH", "").strip()
    if env_month:
        log.info("using MONTH from env: %s", env_month)
        return env_month
    candidates = sorted(
        d.get("identifier", "") for d in docs if d.get("identifier", "").startswith("ficha-")
    )
    if not candidates:
        return None
    # identifier like ficha-2026-04
    latest = candidates[-1]
    return latest.removeprefix("ficha-") if latest.startswith("ficha-") else None


def find_simples_zip(month: str) -> str | None:
    """Look for a Simples ZIP within the IA item; return its download URL."""
    section(f"Locating simples ZIP for {month}")
    meta_url = f"https://archive.org/metadata/ficha-{month}"
    req = urllib.request.Request(meta_url, headers={"User-Agent": "ficha-claude-diag/1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        meta = json.loads(resp.read())
    files = meta.get("files", [])
    simples_files = [
        f for f in files if "simples" in f.get("name", "").lower() and f.get("name", "").endswith(".zip")
    ]
    print(f"  {len(simples_files)} simples ZIP(s) in ficha-{month}")
    for f in simples_files:
        size_mb = int(f.get("size") or 0) / (1024**2)
        print(f"    {f['name']:40s}  {size_mb:6.1f} MB")
    if not simples_files:
        return None
    name = simples_files[0]["name"]
    return f"https://archive.org/download/ficha-{month}/{name}"


def download(url: str, dest: Path) -> int:
    """Stream-download a URL to dest. Returns size in bytes."""
    req = urllib.request.Request(url, headers={"User-Agent": "ficha-claude-diag/1"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(dest, "wb") as f:
        total = 0
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
    return total


def simples_cardinality(month: str, simples_url: str) -> dict:
    """Run the cardinality query on the simples table."""
    section(f"Phase 1 step 1.2 — simples cardinality (month={month})")
    import duckdb
    import zipfile

    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        zip_path = tdp / "simples.zip"
        log.info("downloading %s", simples_url)
        t0 = time.monotonic()
        size = download(simples_url, zip_path)
        log.info("downloaded %.1f MB in %.1fs", size / (1024**2), time.monotonic() - t0)

        # Extract — Simples ZIPs from RFB historically contain exactly one CSV.
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.infolist() if not m.is_dir()]
            if not members:
                raise RuntimeError("simples ZIP is empty")
            zf.extract(members[0], tdp)
            csv_path = tdp / members[0].filename
        log.info("extracted CSV: %.1f MB", csv_path.stat().st_size / (1024**2))

        con = duckdb.connect()
        con.execute("PRAGMA memory_limit='2GB'")

        # Same encoding fallback chain as production.
        for enc, ie in [("latin-1", False), ("utf-8", True)]:
            try:
                con.execute(
                    f"""
                    CREATE OR REPLACE TABLE simples AS
                    SELECT * FROM read_csv(
                        '{csv_path}',
                        delim=';', header=false, quote='"',
                        encoding='{enc}',
                        columns={{
                            'cnpj_basico':'VARCHAR','opcao_simples':'VARCHAR',
                            'data_opcao_simples':'VARCHAR','data_exclusao_simples':'VARCHAR',
                            'opcao_mei':'VARCHAR','data_opcao_mei':'VARCHAR',
                            'data_exclusao_mei':'VARCHAR'
                        }},
                        max_line_size=16777216,
                        ignore_errors={'true' if ie else 'false'}
                    )
                    """
                )
                break
            except Exception as exc:
                log.warning("encoding=%s ie=%s failed: %s", enc, ie, exc)
        else:
            raise RuntimeError("could not load simples CSV with any encoding")

        n_total = con.execute("SELECT COUNT(*) FROM simples").fetchone()[0]
        n_distinct = con.execute("SELECT COUNT(DISTINCT cnpj_basico) FROM simples").fetchone()[0]
        dup_rows = con.execute(
            "SELECT cnpj_basico, COUNT(*) AS n FROM simples "
            "GROUP BY 1 HAVING n > 1 ORDER BY n DESC LIMIT 10"
        ).fetchall()
        n_dup_groups = con.execute(
            "SELECT COUNT(*) FROM (SELECT cnpj_basico FROM simples GROUP BY 1 HAVING COUNT(*) > 1)"
        ).fetchone()[0]

        result = {
            "month": month,
            "total_rows": n_total,
            "distinct_cnpj_basico": n_distinct,
            "groups_with_dupes": n_dup_groups,
            "is_one_to_one": n_total == n_distinct,
            "top_duplicates": [(c, n) for c, n in dup_rows],
        }

        print(f"  total rows               : {n_total:>15,}")
        print(f"  distinct cnpj_basico     : {n_distinct:>15,}")
        print(f"  cnpj_basico w/ dup rows  : {n_dup_groups:>15,}")
        print(f"  1:1 with cnpj_basico?    : {result['is_one_to_one']}")
        if dup_rows:
            print("  top duplicate counts:")
            for cnpj, n in dup_rows:
                print(f"    {cnpj}  →  {n} rows")
        return result


def fifo_probe() -> bool:
    """Phase 1 step 1.4 — confirm FIFO probe works in CI runner too."""
    section("Phase 1 step 1.4 — FIFO probe (CI confirmation)")
    import duckdb
    import threading

    with tempfile.TemporaryDirectory() as td:
        fifo = Path(td) / "p"
        os.mkfifo(fifo)
        n_rows = 100_000

        def writer():
            with open(fifo, "w") as f:
                for i in range(n_rows):
                    f.write(f"{i};x_{i}\n")

        t = threading.Thread(target=writer)
        t.start()
        con = duckdb.connect()
        n = con.execute(
            f"SELECT count(*) FROM read_csv('{fifo}', delim=';', header=false, "
            "columns={'a':'INTEGER','b':'VARCHAR'})"
        ).fetchone()[0]
        t.join()
        ok = n == n_rows
        print(f"  rows piped through FIFO: {n:,} (expected {n_rows:,}) — {'OK' if ok else 'FAIL'}")
        return ok


def main() -> int:
    failures: list[str] = []

    try:
        docs = ia_inventory()
    except Exception as exc:
        log.exception("ia_inventory failed")
        print(f"::error::ia_inventory failed: {exc}")
        failures.append("ia_inventory")
        docs = []

    try:
        if not fifo_probe():
            failures.append("fifo_probe")
    except Exception as exc:
        log.exception("fifo_probe failed")
        print(f"::error::fifo_probe failed: {exc}")
        failures.append("fifo_probe")

    try:
        month = pick_month(docs)
        if not month:
            print("::warning::no MONTH env and no IA items found; skipping simples cardinality")
        else:
            simples_url = find_simples_zip(month)
            if not simples_url:
                print(f"::warning::no simples ZIP found in ficha-{month}; skipping cardinality")
            else:
                result = simples_cardinality(month, simples_url)
                section("Result summary")
                print(json.dumps(result, indent=2, default=str))
                # Surface the verdict prominently
                if result["is_one_to_one"]:
                    print("::notice::simples is 1:1 with cnpj_basico — no LEFT JOIN bug")
                else:
                    print(
                        "::error::simples is NOT 1:1 — write_cnpjs_parquet "
                        "LEFT JOIN at transform.py:478 silently multiplies rows"
                    )
                    failures.append("simples_left_join_bug")
    except Exception as exc:
        log.exception("simples_cardinality failed")
        print(f"::error::simples_cardinality failed: {exc}")
        failures.append("simples_cardinality")

    section("Summary")
    if failures:
        print(f"FAILURES: {failures}")
        return 1
    print("All diagnostics passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
