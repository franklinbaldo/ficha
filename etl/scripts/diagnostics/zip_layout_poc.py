"""ZIP layout POC: STORED vs DEFLATED, flat vs foldered.

Generates six ZIPs from the same 10k-empresa protobuf sample and reports
size + central-directory overhead for each:

  1. PB  flat       STORED      <cnpj_base>.pb
  2. PB  flat       DEFLATED    <cnpj_base>.pb
  3. PB  foldered2  STORED      <XX>/<cnpj_base>.pb   (2-digit prefix)
  4. PB  foldered2  DEFLATED    <XX>/<cnpj_base>.pb
  5. JSON flat      STORED      <cnpj_base>.json      (baseline)
  6. JSON flat      DEFLATED    <cnpj_base>.json

Folders inside the ZIP are pure naming convention — entries still live
in a single flat central directory. The point is to know whether IA's
transparent-unzip listing renders nicer with a folder split, and whether
deflate buys anything once protobuf is already dense.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from doc_format_poc import fetch_sample, to_protobuf  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_MONTH = "2026-04"
DEFAULT_UF = "RR"
DEFAULT_N = 10_000


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def parse_eocd(buf: bytes) -> dict:
    """Parse the End-of-Central-Directory record to recover the central
    directory's size + offset. Works for ZIP32 (last 22 B contain EOCD);
    we scan back up to 64 KB for the signature like every real ZIP
    reader does."""
    sig = b"\x50\x4b\x05\x06"
    idx = buf.rfind(sig, max(0, len(buf) - 65536))
    if idx < 0:
        return {}
    # EOCD layout: sig(4) disk(2) cd_disk(2) entries_this_disk(2)
    # entries_total(2) cd_size(4) cd_offset(4) comment_len(2)
    import struct

    (entries_total, cd_size, cd_offset) = struct.unpack_from("<HII", buf, idx + 10)
    return {
        "entries_total": entries_total,
        "cd_size": cd_size,
        "cd_offset": cd_offset,
        "local_headers_size": cd_offset,
    }


def build_zip(
    payloads: dict[str, bytes],
    compression: int,
    foldered: bool,
    extension: str,
) -> bytes:
    """payloads: cnpj_base → payload bytes. Returns the full ZIP bytes."""
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compression, compresslevel=6) as zf:
        for cnpj_base, payload in payloads.items():
            if foldered:
                name = f"{cnpj_base[:2]}/{cnpj_base}.{extension}"
            else:
                name = f"{cnpj_base}.{extension}"
            zf.writestr(name, payload)
    return buf.getvalue()


def main() -> int:
    month = os.environ.get("MONTH", "").strip() or DEFAULT_MONTH
    uf = os.environ.get("SAMPLE_UF", "").strip() or DEFAULT_UF
    n = int(os.environ.get("SAMPLE_SIZE", str(DEFAULT_N)))

    section(f"zip_layout_poc — month={month} uf={uf} n={n}")
    docs = fetch_sample(month, uf, n)
    if not docs:
        print("::error::no docs returned from sample query")
        return 1

    # Build payloads keyed by cnpj_base (we want unique-per-raiz, so dedupe
    # collapses estabelecimentos to their raiz).
    seen: set[str] = set()
    pb_payloads: dict[str, bytes] = {}
    json_payloads: dict[str, bytes] = {}
    for d in docs:
        cb = d.get("cnpj_base") or ""
        if cb in seen or not cb:
            continue
        seen.add(cb)
        pb_payloads[cb] = to_protobuf(d)
        json_payloads[cb] = json.dumps(d, default=str, separators=(",", ":")).encode()
    n_uniq = len(pb_payloads)

    section("Sample shape")
    print(f"  empresas (estabelecimentos): {len(docs):,}")
    print(f"  cnpj_base únicos (raízes):   {n_uniq:,}")
    pb_total = sum(len(p) for p in pb_payloads.values())
    json_total = sum(len(p) for p in json_payloads.values())
    print(
        f"  payloads raw PB:             {pb_total / 1024:7.1f} KB  ({pb_total / n_uniq:.0f} B/doc)"
    )
    print(
        f"  payloads raw JSON:           {json_total / 1024:7.1f} KB  ({json_total / n_uniq:.0f} B/doc)"
    )

    section("Build ZIPs")
    variants = [
        ("PB   flat       STORED  ", pb_payloads, zipfile.ZIP_STORED, False, "pb"),
        ("PB   flat       DEFLATE ", pb_payloads, zipfile.ZIP_DEFLATED, False, "pb"),
        ("PB   foldered2  STORED  ", pb_payloads, zipfile.ZIP_STORED, True, "pb"),
        ("PB   foldered2  DEFLATE ", pb_payloads, zipfile.ZIP_DEFLATED, True, "pb"),
        ("JSON flat       STORED  ", json_payloads, zipfile.ZIP_STORED, False, "json"),
        ("JSON flat       DEFLATE ", json_payloads, zipfile.ZIP_DEFLATED, False, "json"),
    ]
    results = []
    for label, payloads, comp, foldered, ext in variants:
        t0 = time.monotonic()
        zbuf = build_zip(payloads, comp, foldered, ext)
        build_s = time.monotonic() - t0
        eocd = parse_eocd(zbuf)
        results.append(
            {
                "label": label,
                "size": len(zbuf),
                "cd_size": eocd.get("cd_size", 0),
                "local_size": eocd.get("local_headers_size", 0),
                "entries": eocd.get("entries_total", 0),
                "build_s": build_s,
            }
        )
        log.info("%s: %.2f MB built in %.2fs", label.strip(), len(zbuf) / (1024 * 1024), build_s)

    section("ZIP size breakdown (sample, 10k empresas)")
    print(f"  {'variant':30s}  {'total':>10s}  {'CD':>9s}  {'payload+hdr':>14s}  {'build':>7s}")
    base_size = results[0]["size"]
    for r in results:
        total_mb = r["size"] / (1024 * 1024)
        cd_kb = r["cd_size"] / 1024
        local_mb = r["local_size"] / (1024 * 1024)
        pct = r["size"] / base_size * 100
        print(
            f"  {r['label']:30s}  {total_mb:6.2f} MB  {cd_kb:6.1f} KB  "
            f"{local_mb:9.2f} MB    {r['build_s']:5.2f}s   ({pct:5.1f}%)"
        )

    section("Per-entry overhead (ZIP plumbing per file)")
    for r in results:
        per_entry = (
            r["cd_size"]
            + r["local_size"]
            - sum(len(p) for p in (pb_payloads if "PB" in r["label"] else json_payloads).values())
        ) / max(r["entries"], 1)
        print(f"  {r['label']:30s}  {per_entry:6.1f} B/entry")

    section("Extrapolation to 67M cnpj_base (raízes)")
    full = 67_635_384
    scale = full / n_uniq
    print(f"  scale factor: {scale:,.1f}× (sample={n_uniq:,}, full={full:,})")
    for r in results:
        ext_gb = r["size"] * scale / (1024**3)
        cd_gb = r["cd_size"] * scale / (1024**3)
        print(f"  {r['label']:30s}  total≈ {ext_gb:6.2f} GB  CD≈ {cd_gb:5.2f} GB")

    section("Verdict")
    pb_st = next(r for r in results if r["label"].startswith("PB   flat       STORED"))
    pb_df = next(r for r in results if r["label"].startswith("PB   flat       DEFLATE"))
    json_st = next(r for r in results if r["label"].startswith("JSON flat       STORED"))
    json_df = next(r for r in results if r["label"].startswith("JSON flat       DEFLATE"))
    deflate_savings_pb = (pb_st["size"] - pb_df["size"]) / pb_st["size"] * 100
    deflate_savings_json = (json_st["size"] - json_df["size"]) / json_st["size"] * 100
    print(f"  DEFLATE saves {deflate_savings_pb:5.1f}% on PB   (protobuf is already dense)")
    print(f"  DEFLATE saves {deflate_savings_json:5.1f}% on JSON (lots of repeated keys)")
    pb_vs_json_st = pb_st["size"] / json_st["size"] * 100
    pb_vs_json_df = pb_df["size"] / json_df["size"] * 100
    print(f"  PB STORED  is {pb_vs_json_st:5.1f}% of JSON STORED  size")
    print(f"  PB DEFLATE is {pb_vs_json_df:5.1f}% of JSON DEFLATE size")

    print("::notice::zip layout POC complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
