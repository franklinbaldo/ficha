"""Empacotador de fichas — produz companies.zip com um .pb por cnpj_base.

Layout do ZIP:
  _schema.desc              FileDescriptorSet binário (~3 KB)
  _schema.proto             source .proto pra debug humano
  _meta.json                { schema_version, schema_sha256, snapshot_month, count }
  _lookups/cnaes.pb         LookupFile repeated
  _lookups/municipios.pb
  _lookups/naturezas.pb
  _lookups/motivos.pb
  _lookups/paises.pb
  _lookups/qualificacoes.pb
  XX/XXX/XXX.pb             Company por cnpj_base (cnpjpath layout)

Uso típico:
  from ficha_etl.pack import pack_companies
  pack_companies(con, month="2026-04", output_path=Path("companies.zip"))
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import zipfile
from pathlib import Path
from typing import Iterator

import duckdb
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import FileDescriptor

from ficha_etl import mirror
from ficha_etl.proto.ficha.v1.company_pb2 import (
    Company,
    Estabelecimento,
    FaixaEtaria,
    LookupEntry,
    LookupFile,
    Porte,
    Socio,
    TipoEstabelecimento,
    TipoSocio,
    DESCRIPTOR as _COMPANY_FILE_DESCRIPTOR,
)

log = logging.getLogger(__name__)
_PROTO_DIR = Path(__file__).parent.parent.parent.parent / "proto"
_PROTO_PATH = _PROTO_DIR / "ficha" / "v1" / "company.proto"

SCHEMA_VERSION = "1.0.0"

# Lookup kind names — must match upload.py _LOOKUP_KINDS
LOOKUP_KINDS = ["cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]


# ---- helpers: type coercion ----------------------------------------


def _int(v) -> int:
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def _str(v) -> str:
    return str(v).strip() if v is not None else ""


def _date(v) -> int:
    """Convert date-like value to YYYYMMDD uint32. Returns 0 for null/invalid."""
    if v is None:
        return 0
    s = str(v).replace("-", "").strip()
    if len(s) == 8 and s.isdigit() and s != "00000000":
        return int(s)
    return 0


def _bool(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return str(v).strip().upper() in ("S", "SIM", "1", "TRUE", "T")


def _cpf_meio(v) -> int:
    """Extract the 6 middle digits from a masked CPF like ***123456**."""
    s = _str(v).replace(".", "").replace("-", "").replace("*", " ").strip()
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) == 6:
        return int(digits)
    m = re.search(r"\d{6}", _str(v))
    return int(m.group()) if m else 0


def _porte(v) -> int:
    code = _int(v)
    mapping = {
        1: Porte.NAO_INFORMADO,
        2: Porte.MICRO_EMPRESA,
        3: Porte.PEQUENO_PORTE,
        5: Porte.DEMAIS,
    }
    return mapping.get(code, Porte.PORTE_UNSPECIFIED)


def _tipo_estab(v) -> int:
    code = _int(v)
    if code == 1:
        return TipoEstabelecimento.MATRIZ
    if code == 2:
        return TipoEstabelecimento.FILIAL
    return TipoEstabelecimento.TIPO_ESTAB_UNSPECIFIED


def _tipo_socio(v) -> int:
    code = _int(v)
    mapping = {1: TipoSocio.PESSOA_JURIDICA, 2: TipoSocio.PESSOA_FISICA, 3: TipoSocio.ESTRANGEIRO}
    return mapping.get(code, TipoSocio.TIPO_SOCIO_UNSPECIFIED)


def _faixa_etaria(v) -> int:
    code = _int(v)
    if 1 <= code <= 10:
        return code
    return FaixaEtaria.FAIXA_ETARIA_UNSPECIFIED


# ---- schema artifacts -----------------------------------------------


def _schema_desc_bytes() -> bytes:
    """Serialise FileDescriptorSet for company.proto (self-contained)."""
    fds = descriptor_pb2.FileDescriptorSet()
    # Collect transitive dependencies (proto3 well-known types etc.)
    seen: set[str] = set()

    def _add(fd: FileDescriptor) -> None:
        if fd.name in seen:
            return
        seen.add(fd.name)
        for dep in fd.dependencies:
            _add(dep)
        fd.CopyToProto(fds.file.add())

    _add(_COMPANY_FILE_DESCRIPTOR)
    return fds.SerializeToString()


def _schema_proto_text() -> bytes:
    if _PROTO_PATH.exists():
        return _PROTO_PATH.read_bytes()
    return b"# source not bundled"


# ---- row → protobuf ------------------------------------------------


def row_to_company(row: dict) -> Company:
    """Convert a joined DuckDB row (cnpjs ⊕ raizes ⊕ socios) to Company."""
    c = Company()
    c.cnpj_base = _int(row.get("cnpj_base"))
    c.razao_social = _str(row.get("razao_social"))
    c.razao_social_normalizada = _str(row.get("razao_social_normalizada"))
    c.natureza_juridica_codigo = _int(row.get("natureza_juridica_codigo"))
    c.porte_empresa = _porte(row.get("porte_empresa"))
    c.capital_social = float(row.get("capital_social") or 0.0)
    c.ente_federativo_responsavel = _str(row.get("ente_federativo_responsavel"))
    c.qtd_estabelecimentos = _int(row.get("qtd_estabelecimentos"))
    c.qtd_estabelecimentos_ativos = _int(row.get("qtd_estabelecimentos_ativos"))

    for estab_dict in row.get("estabelecimentos") or []:
        e = Estabelecimento()
        e.cnpj_ordem = _int(estab_dict.get("cnpj_ordem"))
        e.cnpj_dv = _int(estab_dict.get("cnpj_dv"))
        e.tipo = _tipo_estab(estab_dict.get("identificador_matriz_filial"))
        e.nome_fantasia = _str(estab_dict.get("nome_fantasia"))
        e.situacao_cadastral = _int(estab_dict.get("situacao_cadastral"))
        e.data_situacao_cadastral = _date(estab_dict.get("data_situacao_cadastral"))
        e.motivo_situacao_cadastral_codigo = _int(
            estab_dict.get("motivo_situacao_cadastral_codigo")
        )
        e.situacao_especial = _str(estab_dict.get("situacao_especial"))
        e.data_situacao_especial = _date(estab_dict.get("data_situacao_especial"))
        e.data_inicio_atividade = _date(estab_dict.get("data_inicio_atividade"))
        e.cnae_principal_codigo = _int(estab_dict.get("cnae_principal_codigo"))
        for code in estab_dict.get("cnaes_secundarios_codigos") or []:
            if code:
                e.cnaes_secundarios_codigos.append(_int(code))
        e.tipo_logradouro = _str(estab_dict.get("tipo_logradouro"))
        e.logradouro = _str(estab_dict.get("logradouro"))
        e.numero = _str(estab_dict.get("numero"))
        e.complemento = _str(estab_dict.get("complemento"))
        e.bairro = _str(estab_dict.get("bairro"))
        e.cep = _int(estab_dict.get("cep"))
        e.uf = _str(estab_dict.get("uf"))
        e.municipio_codigo = _int(estab_dict.get("municipio_codigo"))
        e.nome_cidade_exterior = _str(estab_dict.get("nome_cidade_exterior"))
        e.pais_codigo = _int(estab_dict.get("pais_codigo"))
        e.ddd_1 = _str(estab_dict.get("ddd_1"))
        e.telefone_1 = _str(estab_dict.get("telefone_1"))
        e.ddd_2 = _str(estab_dict.get("ddd_2"))
        e.telefone_2 = _str(estab_dict.get("telefone_2"))
        e.ddd_fax = _str(estab_dict.get("ddd_fax"))
        e.fax = _str(estab_dict.get("fax"))
        e.correio_eletronico = _str(estab_dict.get("correio_eletronico"))
        e.opcao_simples = _bool(estab_dict.get("opcao_simples"))
        e.data_opcao_simples = _date(estab_dict.get("data_opcao_simples"))
        e.data_exclusao_simples = _date(estab_dict.get("data_exclusao_simples"))
        e.opcao_mei = _bool(estab_dict.get("opcao_mei"))
        e.data_opcao_mei = _date(estab_dict.get("data_opcao_mei"))
        e.data_exclusao_mei = _date(estab_dict.get("data_exclusao_mei"))
        c.estabelecimentos.append(e)

    for socio_dict in row.get("socios") or []:
        s = Socio()
        s.tipo = _tipo_socio(socio_dict.get("tipo"))
        s.nome_socio_razao_social = _str(socio_dict.get("nome_socio_razao_social"))
        s.cpf_mascarado_meio = _cpf_meio(socio_dict.get("cpf_mascarado"))
        s.cnpj_socio = _int(socio_dict.get("cnpj_socio"))
        s.qualificacao_codigo = _int(socio_dict.get("qualificacao_codigo"))
        s.data_entrada_sociedade = _date(socio_dict.get("data_entrada_sociedade"))
        s.pais_codigo = _int(socio_dict.get("pais_codigo"))
        s.faixa_etaria = _faixa_etaria(socio_dict.get("faixa_etaria"))
        s.representante_legal_cpf_meio = _cpf_meio(socio_dict.get("representante_legal_cpf"))
        s.representante_legal_nome = _str(socio_dict.get("representante_legal_nome"))
        s.representante_legal_qualificacao_codigo = _int(
            socio_dict.get("representante_legal_qualificacao_codigo")
        )
        c.socios.append(s)

    return c


def cnpjpath(cnpj_base: int) -> str:
    """'12/345/678.pb' — mirrors the 00.000.000 punctuation of CNPJ."""
    s = str(cnpj_base).zfill(8)
    return f"{s[0:2]}/{s[2:5]}/{s[5:8]}.pb"


# ---- lookup serialisation ------------------------------------------


def build_lookup_pb(kind: str, rows: list[dict]) -> bytes:
    lf = LookupFile(kind=kind)
    for r in rows:
        lf.entries.append(
            LookupEntry(codigo=_int(r.get("codigo")), descricao=_str(r.get("descricao")))
        )
    return lf.SerializeToString()


# ---- main entry point ----------------------------------------------


def pack_companies(
    rows: Iterator[dict],
    lookup_rows: dict[str, list[dict]],
    output_path: Path,
    snapshot_month: str,
) -> dict:
    """Write companies.zip.

    Args:
        rows: iterator of joined company dicts (cnpjs ⊕ raizes ⊕ socios).
        lookup_rows: { kind: [{codigo, descricao}, ...] }
        output_path: destination path for the ZIP.
        snapshot_month: "YYYY-MM" string.

    Returns:
        { count, size_bytes, schema_sha256 }
    """
    schema_desc = _schema_desc_bytes()
    schema_sha256 = hashlib.sha256(schema_desc).hexdigest()
    snapshot_yyyymm = int(snapshot_month.replace("-", ""))

    # Fail fast if the caller didn't provide every required lookup kind
    # (Codex P2 on PR #41). The package contract is that every published
    # companies.zip is queryable end-to-end — if a kind is missing the
    # frontend can't decode codes of that kind and there's no recovery
    # short of republishing.
    missing_kinds = sorted(set(LOOKUP_KINDS) - set(lookup_rows.keys()))
    if missing_kinds:
        raise ValueError(
            f"lookup_rows missing required kinds: {missing_kinds} (expected all of: {LOOKUP_KINDS})"
        )

    count = 0
    # O(1) duplicate guard: requires input sorted by cnpj_base (pack_from_parquets
    # guarantees this via ORDER BY; external callers must sort too). A full set
    # would grow to tens of millions of entries on production snapshots.
    prev_cnpj_base: int | None = None
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        # schema artifacts
        zf.writestr("_schema.desc", schema_desc)
        zf.writestr("_schema.proto", _schema_proto_text())

        # lookups
        for kind, lrows in lookup_rows.items():
            pb_bytes = build_lookup_pb(kind, lrows)
            zf.writestr(f"_lookups/{kind}.pb", pb_bytes)

        # company docs
        for row in rows:
            company = row_to_company(row)
            # Enforce strictly-increasing cnpj_base. Two reasons:
            #   1. Duplicates: zipfile silently writes both entries under the
            #      same name and different ZIP readers resolve conflicts
            #      differently — safer to fail at pack time.
            #   2. Sorted-input contract: the adjacent-only duplicate check
            #      above would miss non-adjacent duplicates in unsorted input.
            #      Requiring strictly-increasing keys makes the guard complete.
            if prev_cnpj_base is not None and company.cnpj_base <= prev_cnpj_base:
                if company.cnpj_base == prev_cnpj_base:
                    raise ValueError(
                        f"duplicate cnpj_base in input rows: {company.cnpj_base:08d} "
                        f"— caller must sort and deduplicate by cnpj_base before packing"
                    )
                raise ValueError(
                    f"unsorted input rows: cnpj_base {company.cnpj_base:08d} "
                    f"< previous {prev_cnpj_base:08d} — caller must sort by cnpj_base"
                )
            prev_cnpj_base = company.cnpj_base
            company.snapshot_yyyymm = snapshot_yyyymm
            pb_bytes = company.SerializeToString()
            zf.writestr(cnpjpath(company.cnpj_base), pb_bytes)
            count += 1

        # meta (written last so count is accurate)
        meta = {
            "schema_version": SCHEMA_VERSION,
            "schema_sha256": schema_sha256,
            "snapshot_month": snapshot_month,
            "count": count,
        }
        zf.writestr("_meta.json", json.dumps(meta, indent=2))

    size = output_path.stat().st_size
    return {"count": count, "size_bytes": size, "schema_sha256": schema_sha256}


# ---- read from parquets (IA or local) ----------------------------------------

_COMPANIES_SQL = """
SELECT
    r.cnpj_base,
    r.razao_social,
    r.razao_social_normalizada,
    r.natureza_juridica_codigo,
    r.porte_empresa,
    r.capital_social,
    r.ente_federativo_responsavel,
    r.qtd_estabelecimentos,
    r.qtd_estabelecimentos_ativos,
    e.estabelecimentos,
    s.socios
FROM read_parquet(?) r
LEFT JOIN (
    SELECT cnpj_base,
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
           } ORDER BY cnpj_ordem, cnpj_dv) AS estabelecimentos
    FROM read_parquet(?)
    GROUP BY cnpj_base
) e USING (cnpj_base)
LEFT JOIN (
    SELECT cnpj_base,
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
           } ORDER BY qualificacao_codigo,
                      nome_socio_razao_social,
                      cnpj_socio,
                      cpf_mascarado,
                      data_entrada_sociedade,
                      tipo,
                      pais_codigo,
                      faixa_etaria,
                      representante_legal_cpf,
                      representante_legal_nome,
                      representante_legal_qualificacao_codigo) AS socios
    FROM read_parquet(?)
    GROUP BY cnpj_base
) s USING (cnpj_base)
ORDER BY r.cnpj_base
"""


def pack_from_parquets(
    month: str,
    output_path: Path,
    *,
    parquets_base: str | None = None,
    batch_size: int = 10_000,
    memory_limit_gb: float | None = None,
) -> dict:
    """Build companies.zip by reading parquets from IA (or a local directory).

    Args:
        month: snapshot in YYYY-MM format.
        output_path: destination path for companies.zip.
        parquets_base: URL prefix or local directory path containing the parquets.
            Defaults to the IA item URL for the given month.
        batch_size: rows to fetch per DuckDB fetchmany() call.
        memory_limit_gb: optional DuckDB memory cap in GB.

    Returns:
        { count, size_bytes, schema_sha256 }
    """
    if parquets_base is None:
        # mirror.item_root honors FICHA_IA_BASE_URL — same source of truth as
        # the rest of the pipeline (tests/staging/self-hosted mirrors).
        parquets_base = mirror.item_root(month)

    raizes_url = f"{parquets_base}/raizes.parquet"
    cnpjs_url = f"{parquets_base}/cnpjs.parquet"
    socios_url = f"{parquets_base}/socios.parquet"

    con = duckdb.connect()
    if parquets_base.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    if memory_limit_gb is not None:
        con.execute(f"SET memory_limit='{memory_limit_gb}GB'")

    log.info("pack_from_parquets: reading lookups from %s", parquets_base)
    lookup_rows: dict[str, list[dict]] = {}
    for kind in LOOKUP_KINDS:
        lk_url = f"{parquets_base}/lookups/{kind}.parquet"
        rows = con.execute("SELECT codigo, descricao FROM read_parquet(?)", [lk_url]).fetchall()
        lookup_rows[kind] = [{"codigo": r[0], "descricao": r[1]} for r in rows]
        log.info("  lookup %s: %d entries", kind, len(rows))

    log.info("pack_from_parquets: querying company rows")
    cur = con.execute(_COMPANIES_SQL, [raizes_url, cnpjs_url, socios_url])
    cols = [d[0] for d in cur.description]

    def _row_iter() -> Iterator[dict]:
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                yield dict(zip(cols, row))

    return pack_companies(_row_iter(), lookup_rows, output_path, snapshot_month=month)
