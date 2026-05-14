"""Doc-format POC: per-company JSON vs MessagePack vs Protobuf.

Goal: decide which wire format to use if/when we publish one document
per company on Internet Archive (sharded ZIPs by cnpj_base prefix).

Pulls a 10k-empresa sample from `ficha-{MONTH}/cnpjs.parquet` on IA
(joined with raizes + socios on cnpj_base), builds one document per
company, serializes each in three formats, builds one ZIP per format,
and reports:

  - per-doc wire size  (uncompressed bytes per format)
  - aggregate ZIP size (stored + deflate; what we'd actually publish)
  - decode wall-time   (Python decoder over all 10k docs)

Sample strategy: take the smallest `uf` bucket (Roraima ≈ 60k empresas)
and LIMIT 10k. Filtering by uf pushes down to parquet row-group stats
since cnpjs.parquet is sorted by cnpj_base — DuckDB skips most groups
when a uf has clustered presence. Selecting a small UF keeps the
joined raiz/socio scans cheap too.
"""

from __future__ import annotations

import io
import json

import logging
import os
import statistics
import sys
import time
import zipfile
from dataclasses import dataclass, field
from typing import Annotated

import duckdb
import msgpack
from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_MONTH = "2026-04"
SAMPLE_SIZE = 10_000
# Roraima — smallest UF by empresa count, keeps the httpfs scans cheap
# without distorting the doc-size distribution (per-company structure
# is uf-invariant).
SAMPLE_UF = "RR"


# ----------------------------------------------------------------------
# Protobuf schema (pure-protobuf, no codegen) — wire-format compatible
# with `protoc`-generated Python. Field numbers are stable; renumbering
# breaks decoders. Keep aligned with web/src/schemas/v1/.
# ----------------------------------------------------------------------


@dataclass
class Socio(BaseMessage):
    nome: Annotated[str, Field(1)] = ""
    qualificacao_codigo: Annotated[int, Field(2)] = 0
    qualificacao_descricao: Annotated[str, Field(3)] = ""
    cpf_mascarado: Annotated[str, Field(4)] = ""
    cnpj_socio: Annotated[str, Field(5)] = ""
    data_entrada_sociedade: Annotated[str, Field(6)] = ""
    tipo: Annotated[int, Field(7)] = 0
    faixa_etaria: Annotated[int, Field(8)] = 0
    pais_codigo: Annotated[str, Field(9)] = ""
    representante_legal_cpf: Annotated[str, Field(10)] = ""
    representante_legal_nome: Annotated[str, Field(11)] = ""
    representante_legal_qualificacao_codigo: Annotated[int, Field(12)] = 0


@dataclass
class Raiz(BaseMessage):
    razao_social: Annotated[str, Field(1)] = ""
    natureza_juridica_codigo: Annotated[int, Field(2)] = 0
    porte_empresa: Annotated[int, Field(3)] = 0
    capital_social: Annotated[float, Field(4)] = 0.0
    qtd_estabelecimentos: Annotated[int, Field(5)] = 0
    qtd_estabelecimentos_ativos: Annotated[int, Field(6)] = 0
    uf_matriz: Annotated[str, Field(7)] = ""
    municipio_matriz_codigo: Annotated[int, Field(8)] = 0
    data_inicio_atividade_matriz: Annotated[str, Field(9)] = ""


@dataclass
class Company(BaseMessage):
    cnpj: Annotated[str, Field(1)] = ""
    cnpj_base: Annotated[str, Field(2)] = ""
    razao_social: Annotated[str, Field(3)] = ""
    nome_fantasia: Annotated[str, Field(4)] = ""
    situacao_cadastral: Annotated[int, Field(5)] = 0
    data_inicio_atividade: Annotated[str, Field(6)] = ""
    cnae_principal_codigo: Annotated[int, Field(7)] = 0
    uf: Annotated[str, Field(8)] = ""
    municipio_codigo: Annotated[int, Field(9)] = 0
    logradouro: Annotated[str, Field(10)] = ""
    numero: Annotated[str, Field(11)] = ""
    bairro: Annotated[str, Field(12)] = ""
    cep: Annotated[str, Field(13)] = ""
    opcao_simples: Annotated[bool, Field(14)] = False
    opcao_mei: Annotated[bool, Field(15)] = False
    capital_social: Annotated[float, Field(16)] = 0.0
    raiz: Annotated[Raiz, Field(17)] = field(default_factory=Raiz)
    socios: Annotated[list[Socio], Field(18)] = field(default_factory=list)


# ----------------------------------------------------------------------
# Sample fetch
# ----------------------------------------------------------------------


def fetch_sample(month: str, uf: str, n: int) -> list[dict]:
    base = f"https://archive.org/download/ficha-{month}"
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_progress_bar=false;")
    con.execute(f"SET memory_limit='{_pick_mem_gb()}GB';")

    log.info("fetching %d empresas from uf=%s on %s", n, uf, month)
    t0 = time.monotonic()
    rows = con.execute(
        f"""
        WITH s AS (
            SELECT * FROM read_parquet(?)
            WHERE uf = ?
            LIMIT {n}
        ),
        r AS (
            SELECT * FROM read_parquet(?)
            WHERE cnpj_base IN (SELECT DISTINCT cnpj_base FROM s)
        ),
        so AS (
            SELECT
                cnpj_base,
                list({{
                    'nome':                                     nome_socio_razao_social,
                    'qualificacao_codigo':                      qualificacao_codigo,
                    'qualificacao_descricao':                   qualificacao_descricao,
                    'cpf_mascarado':                            cpf_mascarado,
                    'cnpj_socio':                               cnpj_socio,
                    'data_entrada_sociedade':                   data_entrada_sociedade,
                    'tipo':                                     tipo,
                    'faixa_etaria':                             faixa_etaria,
                    'pais_codigo':                              pais_codigo,
                    'representante_legal_cpf':                  representante_legal_cpf,
                    'representante_legal_nome':                 representante_legal_nome,
                    'representante_legal_qualificacao_codigo':  representante_legal_qualificacao_codigo,
                }}) AS socios
            FROM read_parquet(?)
            WHERE cnpj_base IN (SELECT DISTINCT cnpj_base FROM s)
            GROUP BY cnpj_base
        )
        SELECT
            s.cnpj, s.cnpj_base, s.razao_social, s.nome_fantasia,
            s.situacao_cadastral, s.data_inicio_atividade,
            s.cnae_principal_codigo, s.uf, s.municipio_codigo,
            s.logradouro, s.numero, s.bairro, s.cep,
            s.opcao_simples, s.opcao_mei, s.capital_social,
            {{
                'razao_social':                  r.razao_social,
                'natureza_juridica_codigo':      r.natureza_juridica_codigo,
                'porte_empresa':                 r.porte_empresa,
                'capital_social':                r.capital_social,
                'qtd_estabelecimentos':          r.qtd_estabelecimentos,
                'qtd_estabelecimentos_ativos':   r.qtd_estabelecimentos_ativos,
                'uf_matriz':                     r.uf_matriz,
                'municipio_matriz_codigo':       r.municipio_matriz_codigo,
                'data_inicio_atividade_matriz':  r.data_inicio_atividade_matriz,
            }} AS raiz,
            so.socios AS socios
        FROM s
        LEFT JOIN r  USING (cnpj_base)
        LEFT JOIN so USING (cnpj_base)
        """,
        [
            f"{base}/cnpjs.parquet",
            uf,
            f"{base}/raizes.parquet",
            f"{base}/socios.parquet",
        ],
    ).fetchall()
    cols = [d[0] for d in con.description]
    elapsed = time.monotonic() - t0
    log.info("fetched %d rows in %.1fs", len(rows), elapsed)

    out: list[dict] = []
    for r in rows:
        d = dict(zip(cols, r))
        # DuckDB hands us a struct as a dict already; nothing else to do.
        # Coerce dates/decimals to JSON-friendly strings/floats.
        for k, v in list(d.items()):
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif hasattr(v, "to_eng_string"):
                d[k] = float(v)
        if d.get("raiz"):
            for k, v in list(d["raiz"].items()):
                if hasattr(v, "isoformat"):
                    d["raiz"][k] = v.isoformat()
                elif hasattr(v, "to_eng_string"):
                    d["raiz"][k] = float(v)
        for s in d.get("socios") or []:
            for k, v in list(s.items()):
                if hasattr(v, "isoformat"):
                    s[k] = v.isoformat()
                elif hasattr(v, "to_eng_string"):
                    s[k] = float(v)
        out.append(d)
    return out


def _pick_mem_gb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return max(2, min(int(kb / 1024 / 1024 * 0.65), int(kb / 1024 / 1024) - 6))
    except Exception:
        pass
    return 4


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def to_protobuf(doc: dict) -> bytes:
    raiz = doc.get("raiz") or {}
    socios = doc.get("socios") or []
    msg = Company(
        cnpj=doc.get("cnpj") or "",
        cnpj_base=doc.get("cnpj_base") or "",
        razao_social=doc.get("razao_social") or "",
        nome_fantasia=doc.get("nome_fantasia") or "",
        situacao_cadastral=int(doc.get("situacao_cadastral") or 0),
        data_inicio_atividade=str(doc.get("data_inicio_atividade") or ""),
        cnae_principal_codigo=int(doc.get("cnae_principal_codigo") or 0),
        uf=doc.get("uf") or "",
        municipio_codigo=int(doc.get("municipio_codigo") or 0),
        logradouro=doc.get("logradouro") or "",
        numero=doc.get("numero") or "",
        bairro=doc.get("bairro") or "",
        cep=doc.get("cep") or "",
        opcao_simples=bool(doc.get("opcao_simples")),
        opcao_mei=bool(doc.get("opcao_mei")),
        capital_social=float(doc.get("capital_social") or 0.0),
        raiz=Raiz(
            razao_social=raiz.get("razao_social") or "",
            natureza_juridica_codigo=int(raiz.get("natureza_juridica_codigo") or 0),
            porte_empresa=int(raiz.get("porte_empresa") or 0),
            capital_social=float(raiz.get("capital_social") or 0.0),
            qtd_estabelecimentos=int(raiz.get("qtd_estabelecimentos") or 0),
            qtd_estabelecimentos_ativos=int(raiz.get("qtd_estabelecimentos_ativos") or 0),
            uf_matriz=raiz.get("uf_matriz") or "",
            municipio_matriz_codigo=int(raiz.get("municipio_matriz_codigo") or 0),
            data_inicio_atividade_matriz=str(raiz.get("data_inicio_atividade_matriz") or ""),
        ),
        socios=[
            Socio(
                nome=s.get("nome") or "",
                qualificacao_codigo=int(s.get("qualificacao_codigo") or 0),
                qualificacao_descricao=s.get("qualificacao_descricao") or "",
                cpf_mascarado=s.get("cpf_mascarado") or "",
                cnpj_socio=s.get("cnpj_socio") or "",
                data_entrada_sociedade=str(s.get("data_entrada_sociedade") or ""),
                tipo=int(s.get("tipo") or 0),
                faixa_etaria=int(s.get("faixa_etaria") or 0),
                pais_codigo=s.get("pais_codigo") or "",
                representante_legal_cpf=s.get("representante_legal_cpf") or "",
                representante_legal_nome=s.get("representante_legal_nome") or "",
                representante_legal_qualificacao_codigo=int(
                    s.get("representante_legal_qualificacao_codigo") or 0
                ),
            )
            for s in socios
        ],
    )
    return msg.dumps()


def build_payloads(docs: list[dict]) -> dict[str, list[bytes]]:
    log.info("encoding %d docs in 3 formats", len(docs))
    json_payloads = [json.dumps(d, default=str, separators=(",", ":")).encode() for d in docs]
    msgpack_payloads = [msgpack.packb(d, use_bin_type=True, datetime=False) for d in docs]
    proto_payloads = [to_protobuf(d) for d in docs]
    return {
        "json": json_payloads,
        "msgpack": msgpack_payloads,
        "protobuf": proto_payloads,
    }


def build_zip(payloads: list[bytes], compression: int) -> int:
    """Build an in-memory ZIP with one entry per payload (named by index).
    Return the resulting ZIP size in bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compression, compresslevel=6) as zf:
        for i, p in enumerate(payloads):
            zf.writestr(f"{i:06d}", p)
    return buf.tell()


# ----------------------------------------------------------------------
# Decode benchmark
# ----------------------------------------------------------------------


def bench_decode(payloads: dict[str, list[bytes]]) -> dict[str, float]:
    out: dict[str, float] = {}

    t0 = time.monotonic()
    for p in payloads["json"]:
        json.loads(p)
    out["json"] = time.monotonic() - t0

    t0 = time.monotonic()
    for p in payloads["msgpack"]:
        msgpack.unpackb(p, raw=False)
    out["msgpack"] = time.monotonic() - t0

    t0 = time.monotonic()
    for p in payloads["protobuf"]:
        Company.loads(p)
    out["protobuf"] = time.monotonic() - t0

    return out


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main() -> int:
    month = os.environ.get("MONTH", "").strip() or DEFAULT_MONTH
    uf = os.environ.get("SAMPLE_UF", "").strip() or SAMPLE_UF
    n = int(os.environ.get("SAMPLE_SIZE", str(SAMPLE_SIZE)))

    section(f"doc_format_poc — month={month} uf={uf} n={n}")
    raw_docs = fetch_sample(month, uf, n)
    if not raw_docs:
        print("::error::no docs returned from sample query")
        return 1

    # Codex P1 on PR #41: fetch_sample's SQL is `LIMIT n` over estabelecimentos,
    # so empresas with multiple estabs appear N times — each copy carrying
    # the same socios list. That double-counts payload bytes in the
    # format comparison and makes the 70M extrapolation off. Dedupe in
    # Python by cnpj_base, keeping the first row seen per empresa.
    seen: set = set()
    docs: list = []
    for d in raw_docs:
        cb = d.get("cnpj_base")
        if not cb or cb in seen:
            continue
        seen.add(cb)
        docs.append(d)
    log.info("deduplicated %d -> %d distinct cnpj_base", len(raw_docs), len(docs))
    if not docs:
        print("::error::no distinct cnpj_base in sample")
        return 1

    n_socios = [len(d.get("socios") or []) for d in docs]
    section("Sample shape")
    print(f"  empresas:          {len(docs):,}")
    print(
        f"  socios/empresa:    mean={statistics.mean(n_socios):.2f}  "
        f"p50={statistics.median(n_socios):.0f}  "
        f"p95={statistics.quantiles(n_socios, n=20)[-1] if len(n_socios) >= 20 else max(n_socios)}  "
        f"max={max(n_socios)}"
    )
    print(f"  total socios:      {sum(n_socios):,}")

    payloads = build_payloads(docs)

    section("Wire size (uncompressed, sum over all 10k docs)")
    sizes = {fmt: sum(len(p) for p in ps) for fmt, ps in payloads.items()}
    medians = {fmt: statistics.median(len(p) for p in ps) for fmt, ps in payloads.items()}
    for fmt in ("json", "msgpack", "protobuf"):
        total_mb = sizes[fmt] / (1024 * 1024)
        med_b = medians[fmt]
        ratio = sizes[fmt] / sizes["json"]
        print(
            f"  {fmt:10s}  total={total_mb:7.2f} MB  median/doc={med_b:6.0f} B  ({ratio * 100:5.1f}% of JSON)"
        )

    section("ZIP size (one entry per company)")
    zip_sizes = {}
    for fmt in ("json", "msgpack", "protobuf"):
        stored = build_zip(payloads[fmt], zipfile.ZIP_STORED)
        deflated = build_zip(payloads[fmt], zipfile.ZIP_DEFLATED)
        zip_sizes[fmt] = {"stored": stored, "deflated": deflated}
        stored_mb = stored / (1024 * 1024)
        deflated_mb = deflated / (1024 * 1024)
        ratio_stored = stored / zip_sizes["json"]["stored"]
        ratio_def = deflated / zip_sizes["json"]["deflated"]
        print(
            f"  {fmt:10s}  ZIP_STORED={stored_mb:7.2f} MB ({ratio_stored * 100:5.1f}%)  "
            f"ZIP_DEFLATED={deflated_mb:7.2f} MB ({ratio_def * 100:5.1f}%)"
        )

    section("Decode wall-time (Python decoders, all 10k docs)")
    times = bench_decode(payloads)
    for fmt in ("json", "msgpack", "protobuf"):
        s = times[fmt]
        ratio = s / times["json"]
        print(
            f"  {fmt:10s}  {s * 1000:7.0f} ms  ({ratio * 100:5.1f}% of JSON time)  "
            f"= {len(docs) / s:,.0f} docs/s"
        )

    # Extrapolation: what would this look like for the full 70M empresas?
    section("Extrapolation to 70M empresas")
    full = 70_145_911
    scale = full / len(docs)
    print(f"  scale factor: {scale:.1f}×  (sample={len(docs):,}, full={full:,})")
    for fmt in ("json", "msgpack", "protobuf"):
        total_gb = sizes[fmt] * scale / (1024**3)
        zip_def_gb = zip_sizes[fmt]["deflated"] * scale / (1024**3)
        print(f"  {fmt:10s}  raw={total_gb:6.2f} GB  ZIP_DEFLATED={zip_def_gb:6.2f} GB")

    section("Verdict")
    json_size = zip_sizes["json"]["deflated"]
    best_fmt = min(("json", "msgpack", "protobuf"), key=lambda f: zip_sizes[f]["deflated"])
    savings = (json_size - zip_sizes[best_fmt]["deflated"]) / json_size * 100
    print(f"::notice::smallest ZIP-DEFLATED: {best_fmt} ({savings:.1f}% smaller than JSON)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
