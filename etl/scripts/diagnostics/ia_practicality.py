"""IA practicality probe — exercise the published parquets end-to-end.

For the most recent `ficha-YYYY-MM` item on Internet Archive (or
`MONTH` env override), verify the analytical layer is actually usable
from the canonical access paths:

1. HEAD each canonical artifact (cnpjs/raizes/socios/lookups + per-kind
   lookups) — record size, Accept-Ranges, Content-Type.
2. DuckDB httpfs queries against each parquet — schema, COUNT(*),
   sample, a representative filter — measure wall-clock per query.
3. Schema sanity check: compare top-level Parquet columns against the
   field names declared in `web/src/schemas/v1/`.

Outputs a JSON report to `/tmp/ia_practicality.json` and prints it.
Exits non-zero on hard failures (e.g. a parquet returns no rows or
COUNT(*) errors); soft warnings (missing optional artifact, schema
drift) surface as `::warning::` annotations.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

UA = {"User-Agent": "ficha-claude-diag/ia-practicality"}
_FICHA_ID_RE = re.compile(r"^ficha-\d{4}-(0[1-9]|1[0-2])$")

CANONICAL_ARTIFACTS = ["cnpjs.parquet", "raizes.parquet", "socios.parquet", "lookups.json"]

# Mirrors etl/src/ficha_etl/upload.py:_LOOKUP_KINDS; kept inline to
# keep this script free of the project package import.
LOOKUP_KINDS = ["cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]

# Field-name expectations from web/src/schemas/v1/ — top-level columns
# only. We don't try to validate nested array element types here.
EXPECTED_COLUMNS = {
    "cnpjs.parquet": {
        "cnpj", "cnpj_base", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "razao_social", "natureza_juridica_codigo", "capital_social", "porte_empresa",
        "situacao_cadastral", "data_inicio_atividade",
        "cnae_principal_codigo", "cnae_secundario_codigos",
        "uf", "municipio_codigo",
        "opcao_simples", "opcao_mei",
    },
    "raizes.parquet": {"cnpj_base", "razao_social"},
    "socios.parquet": {"cnpj_base"},
}


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def ia_inventory() -> list[dict]:
    section("IA inventory: FICHA project items")
    url = (
        "https://archive.org/advancedsearch.php?"
        "q=creator%3Afranklinbaldo+AND+identifier%3Aficha-*"
        "&fl[]=identifier&fl[]=item_size&fl[]=publicdate&fl[]=files_count"
        "&output=json&rows=200&sort[]=identifier+desc"
    )
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    raw = data.get("response", {}).get("docs", [])
    docs = [d for d in raw if _FICHA_ID_RE.match(d.get("identifier", ""))]
    print(f"creator=franklinbaldo: {len(raw)} items; ficha-YYYY-MM: {len(docs)}")
    for d in sorted(docs, key=lambda d: d.get("identifier", "")):
        size_gb = (d.get("item_size") or 0) / (1024**3)
        print(f"  {d.get('identifier'):20s}  {size_gb:7.2f} GB  {d.get('files_count','?'):>5} files")
    return docs


def pick_month(docs: list[dict]) -> str | None:
    env_month = os.environ.get("MONTH", "").strip()
    if env_month:
        log.info("using MONTH from env: %s", env_month)
        return env_month
    ids = sorted(d.get("identifier", "") for d in docs)
    return ids[-1].removeprefix("ficha-") if ids else None


def item_files(month: str) -> dict[str, dict]:
    """Map filename → {size, format} from IA metadata."""
    url = f"https://archive.org/metadata/ficha-{month}"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as resp:
        meta = json.loads(resp.read())
    return {f["name"]: f for f in meta.get("files", [])}


def head(url: str) -> dict:
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            elapsed = time.monotonic() - t0
            return {
                "status": resp.status,
                "size": int(resp.headers.get("Content-Length", 0)),
                "accept_ranges": resp.headers.get("Accept-Ranges"),
                "content_type": resp.headers.get("Content-Type"),
                "elapsed_s": round(elapsed, 3),
            }
    except Exception as exc:
        return {"status": 0, "error": str(exc), "elapsed_s": round(time.monotonic() - t0, 3)}


def probe_artifacts(month: str, files: dict[str, dict]) -> dict:
    section(f"HEAD canonical artifacts (month={month})")
    base = f"https://archive.org/download/ficha-{month}"
    targets = list(CANONICAL_ARTIFACTS) + [f"lookups/{k}.parquet" for k in LOOKUP_KINDS]
    report: dict[str, dict] = {}
    for name in targets:
        if name not in files:
            print(f"  {name:36s}  MISSING in item metadata")
            report[name] = {"present": False}
            continue
        url = f"{base}/{name}"
        h = head(url)
        present = h.get("status") == 200
        size_mb = h.get("size", 0) / (1024**2)
        ar = h.get("accept_ranges", "?")
        print(f"  {name:36s}  status={h.get('status')}  {size_mb:8.2f} MB  ranges={ar}")
        report[name] = {"present": present, "url": url, **h}
    return report


def duckdb_probes(month: str, report: dict) -> dict:
    section(f"DuckDB httpfs probes (month={month})")
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_progress_bar=false;")

    out: dict[str, dict] = {}
    for name in ["cnpjs.parquet", "raizes.parquet", "socios.parquet"]:
        if not report.get(name, {}).get("present"):
            out[name] = {"skipped": "not present"}
            continue
        url = report[name]["url"]
        entry: dict = {"url": url}

        # Footer-only read — schema.
        t0 = time.monotonic()
        try:
            cols = con.execute(
                f"SELECT name FROM parquet_schema('{url}') WHERE NOT path_in_schema LIKE '%.list.%'"
            ).fetchall()
            entry["schema_elapsed_s"] = round(time.monotonic() - t0, 3)
            entry["columns"] = sorted({c[0] for c in cols})
        except Exception as exc:
            entry["schema_error"] = str(exc)
            print(f"  {name}: parquet_schema FAILED — {exc}")
            out[name] = entry
            continue

        # COUNT(*) — touches metadata + row group stats only.
        t0 = time.monotonic()
        try:
            n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{url}')").fetchone()[0]
            entry["count_elapsed_s"] = round(time.monotonic() - t0, 3)
            entry["row_count"] = n
        except Exception as exc:
            entry["count_error"] = str(exc)

        # LIMIT 5 — exercises a real data fetch.
        t0 = time.monotonic()
        try:
            con.execute(f"SELECT * FROM read_parquet('{url}') LIMIT 5").fetchall()
            entry["sample_elapsed_s"] = round(time.monotonic() - t0, 3)
        except Exception as exc:
            entry["sample_error"] = str(exc)

        # Representative filter — only for cnpjs (the big one frontend will query).
        if name == "cnpjs.parquet":
            t0 = time.monotonic()
            try:
                rows = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{url}') WHERE uf = 'AC'"
                ).fetchone()[0]
                entry["filter_uf_ac_elapsed_s"] = round(time.monotonic() - t0, 3)
                entry["filter_uf_ac_rows"] = rows
            except Exception as exc:
                entry["filter_error"] = str(exc)

        # Schema sanity vs web/src/schemas/v1.
        expected = EXPECTED_COLUMNS.get(name, set())
        missing = sorted(expected - set(entry.get("columns") or []))
        entry["schema_missing_expected"] = missing
        if missing:
            print(f"::warning::{name}: missing expected columns: {missing}")

        rc = entry.get("row_count")
        rc_s = f"{rc:,}" if isinstance(rc, int) else "?"
        print(
            f"  {name:18s}  rows={rc_s:>15s}  "
            f"schema={entry.get('schema_elapsed_s','?')}s  "
            f"count={entry.get('count_elapsed_s','?')}s  "
            f"sample={entry.get('sample_elapsed_s','?')}s"
        )
        out[name] = entry

    return out


def main() -> int:
    failures: list[str] = []
    report: dict = {"month": None}

    try:
        docs = ia_inventory()
    except Exception as exc:
        log.exception("ia_inventory failed")
        print(f"::error::ia_inventory failed: {exc}")
        return 1

    month = pick_month(docs)
    if not month:
        print("::warning::no MONTH env and no IA items found; nothing to probe")
        return 0
    report["month"] = month

    try:
        files = item_files(month)
    except Exception as exc:
        print(f"::error::item_files({month}) failed: {exc}")
        return 1

    report["artifacts"] = probe_artifacts(month, files)

    for name in CANONICAL_ARTIFACTS:
        if not report["artifacts"].get(name, {}).get("present"):
            print(f"::error::canonical artifact missing or unreachable: {name}")
            failures.append(f"missing:{name}")

    try:
        report["duckdb"] = duckdb_probes(month, report["artifacts"])
    except Exception as exc:
        log.exception("duckdb_probes failed")
        print(f"::error::duckdb_probes failed: {exc}")
        failures.append("duckdb_probes")
        report["duckdb"] = {"error": str(exc)}

    for name, entry in report.get("duckdb", {}).items():
        if not isinstance(entry, dict):
            continue
        for err_key in ("schema_error", "count_error", "sample_error", "filter_error"):
            if err_key in entry:
                failures.append(f"duckdb:{name}:{err_key}")
        if entry.get("row_count") == 0:
            failures.append(f"empty:{name}")

    out_path = Path("/tmp/ia_practicality.json")
    out_path.write_text(json.dumps(report, indent=2, default=str))
    section("Result summary")
    print(json.dumps(report, indent=2, default=str))

    section("Verdict")
    if failures:
        print(f"::error::failures: {failures}")
        return 1
    print("::notice::IA practicality probe passed — analytical layer is queryable end-to-end")
    return 0


if __name__ == "__main__":
    sys.exit(main())
