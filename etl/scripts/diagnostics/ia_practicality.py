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

# Minimal required column names that downstream code (web/src/schemas/v1)
# touches. Intentionally a sanity check, not a full schema validation —
# we only care that the parquet has the columns we read. Types are not
# checked here; that needs Zod-side validation against real samples
# (see SocioSchema, RaizSchema, EstabelecimentoSchema). Renaming
# this would be a good follow-up if we ever invest in full type drift
# detection.
MINIMAL_REQUIRED_COLUMNS = {
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


def _ia_session():
    """Authenticated `internetarchive` session if IA_ACCESS_KEY/SECRET
    are set, otherwise None. Authenticated scrapes/metadata see items
    immediately without waiting for the public search index."""
    import internetarchive as ia

    access = os.environ.get("IA_ACCESS_KEY", "").strip()
    secret = os.environ.get("IA_SECRET_KEY", "").strip()
    if access and secret:
        log.info("using authenticated IA session")
        return ia.get_session(config={"s3": {"access": access, "secret": secret}})
    log.info("no IA_ACCESS_KEY/IA_SECRET_KEY in env — anonymous session")
    return ia.get_session()


def ia_inventory() -> list[dict]:
    """Authenticated scrape for FICHA items. Falls back to per-month
    direct metadata probes for the last 12 months so we still find
    items that the public search index hasn't surfaced yet."""
    section("IA inventory: FICHA project items (authenticated scrape)")
    sess = _ia_session()
    docs: list[dict] = []
    try:
        for entry in sess.search_items(
            "creator:franklinbaldo AND identifier:ficha-*",
            fields=["identifier", "item_size", "publicdate", "files_count"],
        ):
            if _FICHA_ID_RE.match(entry.get("identifier", "")):
                docs.append(dict(entry))
    except Exception as exc:
        log.warning("scrape search failed: %s — falling back to direct metadata probes", exc)

    print(f"scrape returned {len(docs)} ficha-YYYY-MM items")

    # Direct metadata probe — bypasses *all* indexing.
    section("Direct metadata probe (last 12 months)")
    from datetime import date

    today = date.today()
    seen = {d.get("identifier") for d in docs}
    for offset in range(0, 13):
        # walk backwards from current month
        y = today.year
        m = today.month - offset
        while m <= 0:
            m += 12
            y -= 1
        ident = f"ficha-{y:04d}-{m:02d}"
        if ident in seen:
            continue
        url = f"https://archive.org/metadata/{ident}"
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=15) as resp:
                meta = json.loads(resp.read())
        except Exception as exc:
            print(f"  {ident:20s}  metadata fetch failed: {exc}")
            continue
        files = meta.get("files") or []
        if not files and not meta.get("metadata"):
            print(f"  {ident:20s}  (no such item)")
            continue
        item_meta = meta.get("metadata") or {}
        size = sum(int(f.get("size") or 0) for f in files)
        docs.append({
            "identifier": ident,
            "item_size": size,
            "files_count": len(files),
            "publicdate": item_meta.get("publicdate", "?"),
        })
        seen.add(ident)
        print(f"  {ident:20s}  FOUND via metadata  ({len(files)} files, {size/(1024**3):.2f} GB)")

    for d in sorted(docs, key=lambda d: d.get("identifier", "")):
        size_gb = (d.get("item_size") or 0) / (1024**3)
        print(f"  {d.get('identifier'):20s}  {size_gb:7.2f} GB  {d.get('files_count','?'):>5} files")
    return docs


_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_month(s: str) -> str:
    """Allow only `YYYY-MM`. Refuse anything else loudly — `month` flows
    into f-string SQL (DuckDB httpfs URLs) and into a URL path; both
    must be tightly constrained even though the workflow input is
    operator-controlled."""
    if not _MONTH_RE.match(s):
        raise ValueError(f"invalid month {s!r} — must match YYYY-MM with month in 01..12")
    return s


def pick_month(docs: list[dict]) -> str | None:
    env_month = os.environ.get("MONTH", "").strip()
    if env_month:
        log.info("using MONTH from env: %s", env_month)
        return _validate_month(env_month)
    ids = sorted(d.get("identifier", "") for d in docs)
    if not ids:
        return None
    candidate = ids[-1].removeprefix("ficha-")
    return _validate_month(candidate)


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


def probe_raw_zips(month: str, files: dict[str, dict]) -> dict:
    """Inventory the raw RFB ZIPs mirrored at `ficha-{month}/raw/*.zip`,
    plus HEAD the first one to confirm Accept-Ranges / Content-Length."""
    section(f"Raw RFB ZIPs at ficha-{month}/raw/")
    base = f"https://archive.org/download/ficha-{month}"
    raw_files = {name: meta for name, meta in files.items() if name.startswith("raw/") and name.endswith(".zip")}
    print(f"  {len(raw_files)} raw ZIP(s) in item metadata")

    by_kind: dict[str, list[str]] = {}
    for name in sorted(raw_files):
        # raw/Empresas3.zip → kind=Empresas
        leaf = name.removeprefix("raw/").removesuffix(".zip").rstrip("0123456789")
        by_kind.setdefault(leaf or "?", []).append(name)
    for kind, names in sorted(by_kind.items()):
        total_mb = sum(int(raw_files[n].get("size") or 0) for n in names) / (1024**2)
        print(f"    {kind:18s}  {len(names):>2} ZIP(s)  {total_mb:>9.1f} MB")

    if not raw_files:
        return {"count": 0}

    # HEAD the smallest non-Estabelecimento zip first (lookups), or the
    # first one alphabetically as fallback. We want to confirm IA serves
    # Range requests on the ZIPs.
    probe_name = sorted(raw_files, key=lambda n: int(raw_files[n].get("size") or 0))[0]
    probe_url = f"{base}/{probe_name}"
    h = head(probe_url)
    print(f"  HEAD {probe_name}: status={h.get('status')}  size={h.get('size',0)/(1024**2):.2f} MB  ranges={h.get('accept_ranges','?')}")

    # IA serves transparent unzip via `<zip>/<member>` paths. Try
    # listing members by hitting the directory-like URL (returns HTML)
    # — we just check status + content-type.
    listing_url = f"{probe_url}/"
    h2 = head(listing_url)
    print(f"  HEAD {probe_name}/ (unzip listing): status={h2.get('status')}  content_type={h2.get('content_type')}")

    return {
        "count": len(raw_files),
        "by_kind": {k: len(v) for k, v in by_kind.items()},
        "probe": {"name": probe_name, "head": h, "listing_head": h2},
    }


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

        # Footer-only read — top-level columns via DESCRIBE (binds to
        # read_parquet's projection, so children of LIST/STRUCT aren't
        # exposed as separate rows the way parquet_schema() would).
        t0 = time.monotonic()
        try:
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()
            entry["schema_elapsed_s"] = round(time.monotonic() - t0, 3)
            # DESCRIBE columns: (column_name, column_type, null, key, default, extra)
            entry["columns"] = sorted({c[0] for c in cols})
        except Exception as exc:
            entry["schema_error"] = str(exc)
            print(f"  {name}: DESCRIBE FAILED — {exc}")
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
        expected = MINIMAL_REQUIRED_COLUMNS.get(name, set())
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

    try:
        report["raw_zips"] = probe_raw_zips(month, files)
    except Exception as exc:
        log.exception("probe_raw_zips failed")
        print(f"::error::probe_raw_zips failed: {exc}")
        failures.append("raw_zips")
        report["raw_zips"] = {"error": str(exc)}

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
