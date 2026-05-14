"""ZIP layout POC: STORED vs DEFLATED, flat vs foldered.

Generates eight ZIPs from the same 10k-empresa protobuf sample and reports
size + central-directory overhead for each:

  1. PB  flat       STORED      <cnpj_base>.pb
  2. PB  flat       DEFLATED    <cnpj_base>.pb
  3. PB  fold2      STORED      <XX>/<cnpj_base>.pb       (2-digit prefix)
  4. PB  fold2      DEFLATED    <XX>/<cnpj_base>.pb
  5. PB  cnpjpath   STORED      <XX>/<XXX>/<XXX>.pb       (mirrors 00.000.000 punctuation)
  6. PB  cnpjpath   DEFLATED    <XX>/<XXX>/<XXX>.pb
  7. JSON flat      STORED      <cnpj_base>.json          (baseline)
  8. JSON flat      DEFLATED    <cnpj_base>.json

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

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
from doc_format_poc import _pick_mem_gb  # noqa: E402

from ficha_etl.pack import cnpjpath, row_to_company

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_MONTH = "2026-04"
DEFAULT_UF = "RR"
DEFAULT_N = 10_000


def fetch_company_sample(month: str, uf: str, n: int) -> list[dict]:
    """Pull a sample of N empresas (cnpj_base) from IA, returning rows
    in the shape pack.row_to_company expects: top-level Company fields
    plus `estabelecimentos` (list of struct) and `socios` (list of
    struct) nested.

    Strategy: pick N distinct cnpj_base from the chosen uf, then join
    raizes/socios and group estabelecimentos into a list per cnpj_base.
    """
    base = f"https://archive.org/download/ficha-{month}"
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_progress_bar=false;")
    con.execute(f"SET memory_limit='{_pick_mem_gb()}GB';")

    log.info("fetching %d empresas (company-shaped) from uf=%s on %s", n, uf, month)
    t0 = time.monotonic()
    rows = con.execute(
        """
        WITH bases AS (
            SELECT DISTINCT cnpj_base
            FROM read_parquet(?)
            WHERE uf = ?
            LIMIT ?
        ),
        estabs AS (
            SELECT
                cnpj_base,
                list({
                    'cnpj_ordem': cnpj_ordem,
                    'cnpj_dv': cnpj_dv,
                    'identificador_matriz_filial': identificador_matriz_filial,
                    'nome_fantasia': nome_fantasia,
                    'situacao_cadastral': situacao_cadastral,
                    'data_situacao_cadastral': data_situacao_cadastral,
                    'motivo_situacao_cadastral_codigo': motivo_situacao_cadastral_codigo,
                    'situacao_especial': situacao_especial,
                    'data_situacao_especial': data_situacao_especial,
                    'data_inicio_atividade': data_inicio_atividade,
                    'cnae_principal_codigo': cnae_principal_codigo,
                    'cnaes_secundarios_codigos': cnae_secundario_codigos,
                    'tipo_logradouro': tipo_logradouro,
                    'logradouro': logradouro,
                    'numero': numero,
                    'complemento': complemento,
                    'bairro': bairro,
                    'cep': cep,
                    'uf': uf,
                    'municipio_codigo': municipio_codigo,
                    'nome_cidade_exterior': nome_cidade_exterior,
                    'pais_codigo': pais_codigo,
                    'ddd_1': ddd_1,
                    'telefone_1': telefone_1,
                    'ddd_2': ddd_2,
                    'telefone_2': telefone_2,
                    'ddd_fax': ddd_fax,
                    'fax': fax,
                    'correio_eletronico': correio_eletronico,
                    'opcao_simples': opcao_simples,
                    'data_opcao_simples': data_opcao_simples,
                    'data_exclusao_simples': data_exclusao_simples,
                    'opcao_mei': opcao_mei,
                    'data_opcao_mei': data_opcao_mei,
                    'data_exclusao_mei': data_exclusao_mei
                }) AS estabelecimentos
            FROM read_parquet(?)
            WHERE cnpj_base IN (SELECT cnpj_base FROM bases)
            GROUP BY cnpj_base
        ),
        sos AS (
            SELECT
                cnpj_base,
                list({
                    'tipo': tipo,
                    'nome_socio_razao_social': nome_socio_razao_social,
                    'cpf_mascarado': cpf_mascarado,
                    'cnpj_socio': cnpj_socio,
                    'qualificacao_codigo': qualificacao_codigo,
                    'data_entrada_sociedade': data_entrada_sociedade,
                    'pais_codigo': pais_codigo,
                    'faixa_etaria': faixa_etaria,
                    'representante_legal_cpf': representante_legal_cpf,
                    'representante_legal_nome': representante_legal_nome,
                    'representante_legal_qualificacao_codigo': representante_legal_qualificacao_codigo
                }) AS socios
            FROM read_parquet(?)
            WHERE cnpj_base IN (SELECT cnpj_base FROM bases)
            GROUP BY cnpj_base
        )
        SELECT
            b.cnpj_base,
            r.razao_social,
            r.razao_social_normalizada,
            r.natureza_juridica_codigo,
            r.porte_empresa,
            r.capital_social,
            r.ente_federativo_responsavel,
            r.qtd_estabelecimentos,
            r.qtd_estabelecimentos_ativos,
            e.estabelecimentos AS estabelecimentos,
            s.socios AS socios
        FROM bases b
        LEFT JOIN read_parquet(?) r USING (cnpj_base)
        LEFT JOIN estabs e USING (cnpj_base)
        LEFT JOIN sos s USING (cnpj_base)
        """,
        [
            f"{base}/cnpjs.parquet",  # bases CTE
            uf,
            n,  # LIMIT
            f"{base}/cnpjs.parquet",  # estabs CTE
            f"{base}/socios.parquet",
            f"{base}/raizes.parquet",
        ],
    ).fetchall()
    cols = [d[0] for d in con.description]
    log.info("fetched %d company-shaped rows in %.1fs", len(rows), time.monotonic() - t0)
    return [dict(zip(cols, r)) for r in rows]


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


def _entry_name(cnpj_base: str, layout: str, extension: str) -> str:
    """Layouts:
    - flat:     12345678.pb
    - fold2:    12/12345678.pb               (2-digit prefix folder)
    - cnpjpath: 12/345/678.pb                (mirrors the 00.000.000 split)
                                              uses pack.cnpjpath() — canonical
    """
    if layout == "flat":
        return f"{cnpj_base}.{extension}"
    if layout == "fold2":
        return f"{cnpj_base[:2]}/{cnpj_base}.{extension}"
    if layout == "cnpjpath":
        # strip extension from pack.cnpjpath and add the right one
        base = cnpjpath(int(cnpj_base)).removesuffix(".pb")
        return f"{base}.{extension}"
    raise ValueError(f"unknown layout: {layout}")


def build_zip(
    payloads: dict[str, bytes],
    compression: int,
    layout: str,
    extension: str,
) -> bytes:
    """payloads: cnpj_base → payload bytes. Returns the full ZIP bytes."""
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compression, compresslevel=6) as zf:
        for cnpj_base, payload in payloads.items():
            zf.writestr(_entry_name(cnpj_base, layout, extension), payload)
    return buf.getvalue()


def main() -> int:
    month = os.environ.get("MONTH", "").strip() or DEFAULT_MONTH
    uf = os.environ.get("SAMPLE_UF", "").strip() or DEFAULT_UF
    n = int(os.environ.get("SAMPLE_SIZE", str(DEFAULT_N)))

    section(f"zip_layout_poc — month={month} uf={uf} n={n}")
    # fetch_company_sample returns one row per cnpj_base with nested
    # estabelecimentos[] + socios[] — matching the shape row_to_company
    # expects. Previous version used doc_format_poc.fetch_sample which
    # is estabelecimento-shaped (flat) and dropped all estab fields from
    # the PB payload, biasing the size comparison (Codex P1 on PR #41).
    docs = fetch_company_sample(month, uf, n)
    if not docs:
        print("::error::no docs returned from sample query")
        return 1

    # Build payloads keyed by cnpj_base string (zero-padded 8 digits).
    # Uses the real schema from pack.row_to_company — same code path the
    # ETL will use in production.
    pb_payloads: dict[str, bytes] = {}
    json_payloads: dict[str, bytes] = {}
    for d in docs:
        cb = str(d.get("cnpj_base") or "").zfill(8)
        if cb == "00000000" or cb in pb_payloads:
            continue
        company = row_to_company(d)
        pb_payloads[cb] = company.SerializeToString()
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
        ("PB   flat       STORED  ", pb_payloads, zipfile.ZIP_STORED, "flat", "pb"),
        ("PB   flat       DEFLATE ", pb_payloads, zipfile.ZIP_DEFLATED, "flat", "pb"),
        ("PB   fold2      STORED  ", pb_payloads, zipfile.ZIP_STORED, "fold2", "pb"),
        ("PB   fold2      DEFLATE ", pb_payloads, zipfile.ZIP_DEFLATED, "fold2", "pb"),
        ("PB   cnpjpath   STORED  ", pb_payloads, zipfile.ZIP_STORED, "cnpjpath", "pb"),
        ("PB   cnpjpath   DEFLATE ", pb_payloads, zipfile.ZIP_DEFLATED, "cnpjpath", "pb"),
        ("JSON flat       STORED  ", json_payloads, zipfile.ZIP_STORED, "flat", "json"),
        ("JSON flat       DEFLATE ", json_payloads, zipfile.ZIP_DEFLATED, "flat", "json"),
    ]
    results = []
    for label, payloads, comp, layout, ext in variants:
        t0 = time.monotonic()
        zbuf = build_zip(payloads, comp, layout, ext)
        build_s = time.monotonic() - t0
        eocd = parse_eocd(zbuf)
        # Sum actual compressed bytes per entry (Codex P2: local_size
        # mixes compressed payload + headers; subtracting uncompressed
        # payload gives bogus / negative "overhead" for DEFLATE).
        import io as _io

        with zipfile.ZipFile(_io.BytesIO(zbuf), "r") as _zf:
            compressed_total = sum(info.compress_size for info in _zf.infolist())
        results.append(
            {
                "label": label,
                "size": len(zbuf),
                "cd_size": eocd.get("cd_size", 0),
                "local_size": eocd.get("local_headers_size", 0),
                "compressed_total": compressed_total,
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
    # Overhead = total ZIP size - actual compressed payload bytes.
    # Includes local file headers, central directory entries, EOCD, and
    # any per-entry extra fields. Independent of compression because
    # compressed_total tracks what's actually on disk per entry.
    for r in results:
        plumbing = r["size"] - r["compressed_total"]
        per_entry = plumbing / max(r["entries"], 1)
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
