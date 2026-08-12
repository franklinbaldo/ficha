"""Classificação fail-closed de shards remotos de ``companies`` (#163).

A presença de ``companies-NN.zip`` não é prova de reuse. Um shard só é
reutilizável quando o metadata do item é observável **e** o ``_meta.json``
interno declara exatamente o ``MaterializationSpec`` esperado.

``pending_tasks`` é deliberadamente ignorado: o probe real 31582216723 tornou
``_meta.json`` legível enquanto o item ainda reportava tarefas pendentes.
"""

from __future__ import annotations

import enum
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from . import mirror
from .materialization import MaterializationSpec
from .pack import LOOKUP_KINDS
from .sharded_pack import ShardGeometry
from .upload_identity import files_list

log = logging.getLogger(__name__)

PUBLIC_COMPANIES_GEOMETRY = ShardGeometry(2)
"""Geometria pública escolhida após a varredura controlada #159/#160."""

SHARD_INPUT_NAMES = (
    "cnpjs.parquet",
    "raizes.parquet",
    "socios.parquet",
    *(f"lookups/{kind}.parquet" for kind in LOOKUP_KINDS),
)
"""Inputs semânticos que determinam o conteúdo de cada shard."""

_TRANSIENT_META_STATUS = frozenset({404, 429, 500, 502, 503, 504})
_META_ATTEMPTS = 5
_META_BACKOFF_S = 10.0
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)


class ShardReuseState(enum.StrEnum):
    ABSENT = "absent"
    REUSABLE = "reusable"
    MISMATCH = "mismatch"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ShardReuseVerdict:
    name: str
    state: ShardReuseState
    detail: str
    size: int | None = None
    sha1: str | None = None

    @property
    def may_skip(self) -> bool:
        return self.state is ShardReuseState.REUSABLE


class MaterializationInputsUnavailable(RuntimeError):
    """Os inputs remotos não têm identidade suficiente para calcular reuse."""


RemoteMetaFetch = Callable[[str], object | None]


def _valid_sha1(value: object) -> str | None:
    if not isinstance(value, str) or len(value) != 40:
        return None
    lowered = value.lower()
    if any(char not in "0123456789abcdef" for char in lowered):
        return None
    return lowered


def materialization_input_sha1s(metadata: dict | None) -> dict[str, str]:
    """Extrai os SHA-1 dos nove inputs que realmente entram no shard.

    Falha alto se o metadata estiver ambíguo, o arquivo não existir, tiver
    tamanho não positivo ou não expuser SHA-1 comparável. Sem essa prova não é
    possível construir a identidade lógica esperada com segurança.
    """
    if metadata is None:
        raise MaterializationInputsUnavailable("metadata do item indisponível")
    files = files_list(metadata)
    if files is None:
        raise MaterializationInputsUnavailable("metadata sem lista files utilizável")
    by_name = {entry.get("name"): entry for entry in files}

    result: dict[str, str] = {}
    for name in SHARD_INPUT_NAMES:
        entry = by_name.get(name)
        if entry is None:
            raise MaterializationInputsUnavailable(f"input remoto ausente: {name}")
        try:
            size = int(entry.get("size"))
        except (TypeError, ValueError):
            size = 0
        sha1 = _valid_sha1(entry.get("sha1"))
        if size <= 0 or sha1 is None:
            raise MaterializationInputsUnavailable(
                f"input remoto sem identidade comparável: {name} "
                f"size={entry.get('size')!r} sha1={entry.get('sha1')!r}"
            )
        result[name] = sha1
    return result


def classify_remote_shard(
    prefix: str,
    expected: MaterializationSpec,
    metadata: dict | None,
    *,
    geometry: ShardGeometry = PUBLIC_COMPANIES_GEOMETRY,
    fetch_meta: RemoteMetaFetch,
) -> ShardReuseVerdict:
    """Classifica um shard remoto sem jamais transformar ambiguidade em skip."""
    prefix = geometry.validate_prefix(prefix)
    name = geometry.shard_name(prefix)

    if metadata is None:
        return ShardReuseVerdict(name, ShardReuseState.UNKNOWN, "metadata indisponível")
    files = files_list(metadata)
    if files is None:
        return ShardReuseVerdict(name, ShardReuseState.UNKNOWN, "lista files ilegível")

    entry = next((candidate for candidate in files if candidate.get("name") == name), None)
    if entry is None:
        return ShardReuseVerdict(name, ShardReuseState.ABSENT, "ausente em lista files válida")

    try:
        size = int(entry.get("size"))
    except (TypeError, ValueError):
        size = 0
    sha1 = _valid_sha1(entry.get("sha1"))
    if size <= 0 or sha1 is None:
        return ShardReuseVerdict(
            name,
            ShardReuseState.UNKNOWN,
            "arquivo presente sem size+sha1 comparáveis",
            size=size or None,
            sha1=sha1,
        )

    try:
        payload = fetch_meta(name)
    except Exception as exc:  # noqa: BLE001 — falha de observação é UNKNOWN
        return ShardReuseVerdict(
            name,
            ShardReuseState.UNKNOWN,
            f"_meta.json ilegível: {type(exc).__name__}: {exc}",
            size=size,
            sha1=sha1,
        )
    if not isinstance(payload, dict):
        return ShardReuseVerdict(
            name,
            ShardReuseState.UNKNOWN,
            "_meta.json não é objeto JSON",
            size=size,
            sha1=sha1,
        )

    materialization = payload.get("materialization")
    if not isinstance(materialization, dict):
        return ShardReuseVerdict(
            name,
            ShardReuseState.UNKNOWN,
            "_meta.json sem bloco materialization",
            size=size,
            sha1=sha1,
        )

    expected_id = expected.materialization_id()
    if materialization.get("id") != expected_id or materialization.get("spec") != expected.as_document():
        return ShardReuseVerdict(
            name,
            ShardReuseState.MISMATCH,
            f"materialização remota diverge da esperada {expected_id}",
            size=size,
            sha1=sha1,
        )

    return ShardReuseVerdict(
        name,
        ShardReuseState.REUSABLE,
        f"materialization_id={expected_id}",
        size=size,
        sha1=sha1,
    )


def fetch_remote_shard_meta(
    month: str,
    name: str,
    *,
    attempts: int = _META_ATTEMPTS,
    backoff_s: float = _META_BACKOFF_S,
    sleep: Callable[[float], None] = time.sleep,
    client: httpx.Client | None = None,
) -> object | None:
    """Lê ``_meta.json`` pelo unzip transparente com janela > probe observado.

    A medição canônica observou T2 em algum ponto de ``(39,8s, 79,8s]``.
    Cinco tentativas com backoff 10/20/30/40s cobrem até ~100s sem depender de
    ``pending_tasks``. 404/503 são transitórios aqui porque o metadata pode já
    listar o ZIP antes de o membro interno ficar disponível.
    """
    url = f"{mirror.item_root(month)}/{name}/_meta.json"
    owns_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        for attempt in range(1, attempts + 1):
            try:
                response = client.get(url)
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError:
                        return None
                if response.status_code not in _TRANSIENT_META_STATUS:
                    return None
            except httpx.HTTPError:
                pass
            if attempt < attempts:
                sleep(backoff_s * attempt)
        return None
    finally:
        if owns_client:
            client.close()
