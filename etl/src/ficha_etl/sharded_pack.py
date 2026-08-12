"""Pack shardado de ``companies`` com identidade lógica explícita (#161).

Este módulo deliberadamente NÃO faz rede e NÃO decide reuse/upload. A unidade
local precisa estar completa e semanticamente identificada antes que outra
camada possa decidir se um objeto remoto é reutilizável.

A geometria (2 ou 3 dígitos) é parâmetro explícito. Ela só será fixada no
contrato público depois da medição #159/#160; transformar um palpite em
constante global aqui repetiria o erro documentado na primeira versão da #147.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping

import duckdb

from ficha_etl import mirror
from ficha_etl.materialization import MaterializationSpec, ShardRange
from ficha_etl.pack import (  # noqa: PLC2701 -- composição interna do mesmo pacote
    LOOKUP_KINDS,
    SCHEMA_VERSION,
    _COMPANIES_SQL,
    _membro,
    _schema_desc_bytes,
    _schema_proto_text,
    build_lookup_pb,
    cnpjpath,
    data_canonica_do_snapshot,
    row_to_company,
)
from ficha_etl.spooling_zip import SpoolingZipFile

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ShardGeometry:
    """Geometria candidata para prefixos de ``cnpj_base``.

    Só 2 e 3 dígitos são aceitos porque são as duas alternativas que chegaram
    à etapa de medição da RFC #147. Um dígito já OOMou em escala real; larguras
    maiores não foram propostas nem medidas.
    """

    prefix_digits: int

    def __post_init__(self) -> None:
        if self.prefix_digits not in (2, 3):
            raise ValueError("prefix_digits must be 2 or 3")

    @property
    def count(self) -> int:
        return 10**self.prefix_digits

    def prefixes(self) -> Iterator[str]:
        for value in range(self.count):
            yield f"{value:0{self.prefix_digits}d}"

    def validate_prefix(self, prefix: str) -> str:
        if not isinstance(prefix, str):
            raise TypeError("shard prefix must be str")
        if len(prefix) != self.prefix_digits or not prefix.isascii() or not prefix.isdigit():
            raise ValueError(
                f"shard prefix must be exactly {self.prefix_digits} ASCII digits, got {prefix!r}"
            )
        return prefix

    def range_bounds(self, prefix: str) -> tuple[str, str]:
        prefix = self.validate_prefix(prefix)
        value = int(prefix)
        suffix = "0" * (8 - self.prefix_digits)
        lower = prefix + suffix
        upper = (
            f"{value + 1:0{self.prefix_digits}d}{suffix}"
            if value < self.count - 1
            else "A0000000"
        )
        return lower, upper

    def shard_of(self, cnpj_base: int | str) -> str:
        if isinstance(cnpj_base, bool):
            raise TypeError("cnpj_base must be an 8-digit root, not bool")
        if isinstance(cnpj_base, int):
            if not 0 <= cnpj_base <= 99_999_999:
                raise ValueError(f"cnpj_base out of 8-digit range: {cnpj_base}")
            normalized = f"{cnpj_base:08d}"
        elif isinstance(cnpj_base, str):
            if len(cnpj_base) != 8 or not cnpj_base.isascii() or not cnpj_base.isdigit():
                raise ValueError(f"cnpj_base string must contain exactly 8 ASCII digits: {cnpj_base!r}")
            normalized = cnpj_base
        else:
            raise TypeError(f"cnpj_base must be int or str, got {type(cnpj_base).__name__}")
        return normalized[: self.prefix_digits]

    def shard_name(self, prefix: str) -> str:
        return f"companies-{self.validate_prefix(prefix)}.zip"


@dataclass(frozen=True)
class PackedShard:
    """Um único artefato local completo.

    ``path`` pertence somente a esta unidade e pode ser removido pelo chamador
    depois de confirmar durabilidade remota. Não existe lista acumulada de paths
    que possam ficar mortos após upload/cleanup.
    """

    prefix: str
    path: Path
    count: int
    size_bytes: int
    schema_sha256: str
    materialization_id: str


class ShardPackSession:
    """Sessão que reaproveita DuckDB/lookups entre shards e fecha explicitamente.

    Use como context manager. O upload deve acontecer fora deste objeto; uma
    falha de rede não é uma falha de materialização local e não deve ficar
    escondida dentro de callback do pack.
    """

    def __init__(
        self,
        month: str,
        geometry: ShardGeometry,
        *,
        parquets_base: str | None = None,
        batch_size: int = 10_000,
        memory_limit_gb: float | None = None,
    ) -> None:
        self.month = month
        self.geometry = geometry
        self.parquets_base = parquets_base or mirror.item_root(month)
        self.batch_size = batch_size
        self.memory_limit_gb = memory_limit_gb
        self._con: duckdb.DuckDBPyConnection | None = None
        self._lookups: dict[str, list[dict]] | None = None
        self._urls: dict[str, str] | None = None
        self._schema_desc = _schema_desc_bytes()
        self.schema_sha256 = hashlib.sha256(self._schema_desc).hexdigest()

    def __enter__(self) -> ShardPackSession:
        con = duckdb.connect()
        try:
            if self.parquets_base.startswith("http"):
                con.execute("INSTALL httpfs; LOAD httpfs;")
            if self.memory_limit_gb is not None:
                con.execute(f"SET memory_limit='{self.memory_limit_gb}GB'")

            lookups: dict[str, list[dict]] = {}
            log.info("sharded_pack: reading lookups from %s", self.parquets_base)
            for kind in LOOKUP_KINDS:
                url = f"{self.parquets_base}/lookups/{kind}.parquet"
                rows = con.execute("SELECT codigo, descricao FROM read_parquet(?)", [url]).fetchall()
                lookups[kind] = [{"codigo": row[0], "descricao": row[1]} for row in rows]

            self._con = con
            self._lookups = lookups
            self._urls = {
                kind: f"{self.parquets_base}/{kind}.parquet"
                for kind in ("raizes", "cnpjs", "socios")
            }
            return self
        except Exception:
            con.close()
            raise

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._con is not None:
            self._con.close()
        self._con = None
        self._lookups = None
        self._urls = None

    def materialization_spec(
        self,
        prefix: str,
        *,
        input_sha1s: Mapping[str, str],
    ) -> MaterializationSpec:
        """Constrói a identidade semântica antes de gerar o shard."""
        prefix = self.geometry.validate_prefix(prefix)
        return MaterializationSpec(
            snapshot=self.month,
            shard_range=ShardRange(value=prefix),
            inputs=input_sha1s,
            descriptor_sha256=self.schema_sha256,
        )

    def pack(
        self,
        prefix: str,
        output_dir: Path,
        *,
        materialization: MaterializationSpec,
    ) -> PackedShard:
        """Materializa exatamente um shard e só publica o nome final ao concluir."""
        con, lookups, urls = self._require_open()
        prefix = self.geometry.validate_prefix(prefix)
        self._validate_materialization(prefix, materialization)
        lower, upper = self.geometry.range_bounds(prefix)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / self.geometry.shard_name(prefix)

        log.info(
            "sharded_pack: %s range=[%s,%s)",
            output_path.name,
            lower,
            upper,
        )
        rows = _iter_rows_for_range(
            con,
            raizes_url=urls["raizes"],
            cnpjs_url=urls["cnpjs"],
            socios_url=urls["socios"],
            lower=lower,
            upper=upper,
            batch_size=self.batch_size,
        )
        count, size = _pack_rows(
            rows,
            lookups,
            output_path,
            snapshot_month=self.month,
            materialization=materialization,
            schema_desc=self._schema_desc,
            schema_sha256=self.schema_sha256,
        )
        return PackedShard(
            prefix=prefix,
            path=output_path,
            count=count,
            size_bytes=size,
            schema_sha256=self.schema_sha256,
            materialization_id=materialization.materialization_id(),
        )

    def _require_open(
        self,
    ) -> tuple[duckdb.DuckDBPyConnection, dict[str, list[dict]], dict[str, str]]:
        if self._con is None or self._lookups is None or self._urls is None:
            raise RuntimeError("ShardPackSession must be used inside a with block")
        return self._con, self._lookups, self._urls

    def _validate_materialization(self, prefix: str, spec: MaterializationSpec) -> None:
        if spec.snapshot != self.month:
            raise ValueError(
                f"materialization snapshot {spec.snapshot!r} != session month {self.month!r}"
            )
        if spec.shard_range.kind != "cnpj_base_prefix" or spec.shard_range.value != prefix:
            raise ValueError(
                "materialization range does not match shard: "
                f"{spec.shard_range.as_document()!r} vs {prefix!r}"
            )
        if spec.descriptor_sha256 != self.schema_sha256:
            raise ValueError(
                "materialization descriptor_sha256 does not match the schema being packed"
            )


def _iter_rows_for_range(
    con: duckdb.DuckDBPyConnection,
    *,
    raizes_url: str,
    cnpjs_url: str,
    socios_url: str,
    lower: str,
    upper: str,
    batch_size: int,
) -> Iterator[dict]:
    cur = con.execute(
        _COMPANIES_SQL,
        [
            raizes_url,
            lower,
            upper,
            cnpjs_url,
            lower,
            upper,
            socios_url,
            lower,
            upper,
        ],
    )
    columns = [description[0] for description in cur.description]
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            return
        for row in batch:
            yield dict(zip(columns, row))


def _pack_rows(
    rows: Iterator[dict],
    lookup_rows: dict[str, list[dict]],
    output_path: Path,
    *,
    snapshot_month: str,
    materialization: MaterializationSpec,
    schema_desc: bytes,
    schema_sha256: str,
) -> tuple[int, int]:
    missing_kinds = sorted(set(LOOKUP_KINDS) - set(lookup_rows))
    if missing_kinds:
        raise ValueError(f"lookup_rows missing required kinds: {missing_kinds}")

    snapshot_yyyymm = int(snapshot_month.replace("-", ""))
    zip_date = data_canonica_do_snapshot(snapshot_month)
    previous: int | None = None
    count = 0

    partial = output_path.with_name(output_path.name + ".part")
    partial.unlink(missing_ok=True)

    with SpoolingZipFile(partial, "w", compression=8, compresslevel=6, allowZip64=True) as zf:
        zf.writestr(_membro(zf, "_schema.desc", zip_date), schema_desc)
        zf.writestr(_membro(zf, "_schema.proto", zip_date), _schema_proto_text())
        for kind, lookup in lookup_rows.items():
            zf.writestr(
                _membro(zf, f"_lookups/{kind}.pb", zip_date),
                build_lookup_pb(kind, lookup),
            )

        for row in rows:
            company = row_to_company(row)
            if previous is not None and company.cnpj_base <= previous:
                if company.cnpj_base == previous:
                    raise ValueError(f"duplicate cnpj_base in shard input: {company.cnpj_base:08d}")
                raise ValueError(
                    f"unsorted shard input: {company.cnpj_base:08d} < {previous:08d}"
                )
            previous = company.cnpj_base
            company.snapshot_yyyymm = snapshot_yyyymm
            zf.writestr(
                _membro(zf, cnpjpath(company.cnpj_base), zip_date),
                company.SerializeToString(),
            )
            count += 1

        meta = {
            "schema_version": SCHEMA_VERSION,
            "schema_sha256": schema_sha256,
            "snapshot_month": snapshot_month,
            "count": count,
            **materialization.meta_payload(),
        }
        zf.writestr(
            _membro(zf, "_meta.json", zip_date),
            json.dumps(meta, indent=2, sort_keys=True),
        )

    partial.replace(output_path)
    return count, output_path.stat().st_size
