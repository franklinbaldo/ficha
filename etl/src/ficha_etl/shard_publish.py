"""Publicação retomável de shards de ``companies`` (#165/#175/#180/#185).

Um checkpoint é completo quando duas identidades independentes concordam:

1. ``size + sha1`` do ZIP remoto são iguais aos bytes enviados/observados;
2. ``_meta.json`` declara exatamente o ``MaterializationSpec`` pinado.

O SHA-1 aqui é checksum operacional fornecido pelo próprio Internet Archive,
não assinatura criptográfica. O ``materialization_id`` identifica a semântica;
``size + sha1`` identificam os bytes observados. Nada aqui faz replace do ZIP.
``UNKNOWN`` e ``MISMATCH`` abortam antes de escrita.

O catálogo ``/metadata`` do IA é eventualmente consistente e já permaneceu
stale por mais de seis minutos após um PUT aceito. Quando ele diz que o shard
está ausente, fazemos uma reconciliação direta e somente-leitura do URL do ZIP:
404 prova ausência; 200 é hasheado em streaming e cotejado com o ``_meta.json``.
Qualquer outra resposta ou falha de rede é ambiguidade e falha fechado.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

import httpx

from .mirror import item_root
from .shard_remote import (
    PUBLIC_COMPANIES_GEOMETRY,
    ShardReuseState,
    ShardReuseVerdict,
    classify_remote_shard,
    materialization_input_sha1s,
)
from .sharded_pack import ShardPackSession
from .upload_identity import LocalIdentity, confirm_remote_identity, files_list

log = logging.getLogger(__name__)

MetadataFetch = Callable[[], dict | None]
RemoteMetaFetch = Callable[[str], object | None]
ShardUpload = Callable[[str, Path], None]
DirectArtifactFetch = Callable[[str], LocalIdentity | None]

# O item real ficha-2026-05 excedeu a antiga janela de ~132 s: um ZIP aceito
# pelo IA só apareceu no metadata cerca de 5m44s depois. Com backoff linear de
# 2s*n, 20 observações cobrem 380 s sem usar pending_tasks como gate.
_CONFIRM_ATTEMPTS = 20
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)
_TRANSIENT_DIRECT_STATUS = frozenset({429, 500, 502, 503, 504})


class ShardPublishAction(enum.StrEnum):
    SKIPPED = "skipped"
    UPLOADED = "uploaded"


@dataclass(frozen=True)
class ShardPublishResult:
    prefix: str
    name: str
    action: ShardPublishAction
    materialization_id: str
    size: int
    sha1: str


class ShardPublishError(RuntimeError):
    """Estado remoto ou pós-condição não permite continuar com segurança."""


def pin_materialization_inputs(fetch_metadata: MetadataFetch) -> dict[str, str]:
    return materialization_input_sha1s(fetch_metadata())


def _metadata_still_matches(
    pinned: Mapping[str, str],
    fetch_metadata: MetadataFetch,
) -> dict:
    metadata = fetch_metadata()
    current = materialization_input_sha1s(metadata)
    if dict(current) != dict(pinned):
        raise ShardPublishError(
            "inputs remotos mudaram durante a publicação; abortando antes de misturar materializações"
        )
    assert metadata is not None
    return metadata


def _local_identity(path: Path) -> LocalIdentity:
    sha1 = hashlib.sha1(usedforsecurity=False)
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            sha1.update(chunk)
    return LocalIdentity(size=size, sha1=sha1.hexdigest())


def fetch_direct_artifact_identity(
    month: str,
    name: str,
    *,
    client: httpx.Client | None = None,
) -> LocalIdentity | None:
    """Observa o objeto pelo URL de download, independente do catálogo do IA.

    ``None`` é reservado exclusivamente a HTTP 404. Um 200 é lido em streaming
    para produzir a mesma identidade ``size + sha1`` usada pelo catálogo. Erros
    transitórios, de transporte ou status inesperados são ambíguos e portanto
    levantam ``ShardPublishError`` em vez de autorizarem um PUT.
    """
    url = f"{item_root(month)}/{name}"
    owns_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        try:
            with client.stream("GET", url) as response:
                if response.status_code == 404:
                    return None
                if response.status_code != 200:
                    kind = (
                        "transitório"
                        if response.status_code in _TRANSIENT_DIRECT_STATUS
                        else "inesperado"
                    )
                    raise ShardPublishError(
                        f"{name}: GET direto {kind}: HTTP {response.status_code}"
                    )

                sha1 = hashlib.sha1(usedforsecurity=False)
                size = 0
                for chunk in response.iter_bytes(1024 * 1024):
                    size += len(chunk)
                    sha1.update(chunk)
                if size <= 0:
                    raise ShardPublishError(f"{name}: GET direto retornou objeto vazio")
                return LocalIdentity(size=size, sha1=sha1.hexdigest())
        except httpx.HTTPError as exc:
            raise ShardPublishError(
                f"{name}: GET direto falhou: {type(exc).__name__}: {exc}"
            ) from exc
    finally:
        if owns_client:
            client.close()


def _metadata_with_direct_identity(
    metadata: dict,
    name: str,
    identity: LocalIdentity,
) -> dict:
    """Projeta uma observação direta no catálogo sem alterar os inputs pinados."""
    files = files_list(metadata)
    if files is None:
        raise ShardPublishError(f"{name}: lista files ilegível durante reconciliação direta")
    projected = dict(metadata)
    projected_files = [dict(entry) for entry in files if entry.get("name") != name]
    projected_files.append({"name": name, "size": str(identity.size), "sha1": identity.sha1})
    projected["files"] = projected_files
    return projected


def _reconcile_catalog_absent(
    prefix: str,
    expected,
    metadata: dict,
    *,
    fetch_meta: RemoteMetaFetch,
    fetch_direct: DirectArtifactFetch,
    geometry=PUBLIC_COMPANIES_GEOMETRY,
) -> ShardReuseVerdict | None:
    """Resolve ``ABSENT`` do catálogo contra o objeto real sem escrever nada.

    Retorna ``None`` somente quando o URL direto também responde 404. Se os
    bytes existem, eles só autorizam reuse quando ``size + sha1`` e a
    materialização interna são exatamente os esperados. Todo estado diferente
    de ``REUSABLE`` é uma ambiguidade/divergência que bloqueia o PUT.
    """
    name = geometry.shard_name(prefix)
    direct = fetch_direct(name)
    if direct is None:
        return None

    projected = _metadata_with_direct_identity(metadata, name, direct)
    verdict = classify_remote_shard(
        prefix,
        expected,
        projected,
        geometry=geometry,
        fetch_meta=fetch_meta,
    )
    if verdict.state is not ShardReuseState.REUSABLE:
        raise ShardPublishError(
            f"{name}: objeto direto existe mas não é reutilizável: {verdict.state}: {verdict.detail}"
        )
    assert verdict.size == direct.size and verdict.sha1 == direct.sha1
    log.info(
        "%s reconciliado diretamente enquanto ausente do catálogo — %d bytes sha1=%s",
        name,
        direct.size,
        direct.sha1,
    )
    return verdict


def _reuse_result(prefix: str, name: str, materialization_id: str, size: int, sha1: str):
    return ShardPublishResult(
        prefix=prefix,
        name=name,
        action=ShardPublishAction.SKIPPED,
        materialization_id=materialization_id,
        size=size,
        sha1=sha1,
    )


def publish_one_shard(
    session: ShardPackSession,
    prefix: str,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    upload: ShardUpload,
    fetch_direct: DirectArtifactFetch | None = None,
    confirm_attempts: int = _CONFIRM_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> ShardPublishResult:
    if session.geometry != PUBLIC_COMPANIES_GEOMETRY:
        raise ShardPublishError(
            f"geometria de publicação deve ser {PUBLIC_COMPANIES_GEOMETRY.prefix_digits} dígitos"
        )

    prefix = session.geometry.validate_prefix(prefix)
    expected = session.materialization_spec(prefix, input_sha1s=pinned_inputs)
    expected_id = expected.materialization_id()
    if fetch_direct is None:
        fetch_direct = lambda name: fetch_direct_artifact_identity(session.month, name)

    metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    verdict = classify_remote_shard(
        prefix,
        expected,
        metadata,
        geometry=session.geometry,
        fetch_meta=fetch_meta,
    )

    if verdict.state is ShardReuseState.REUSABLE:
        assert verdict.size is not None and verdict.sha1 is not None
        return _reuse_result(prefix, verdict.name, expected_id, verdict.size, verdict.sha1)
    if verdict.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")

    direct_before_pack = _reconcile_catalog_absent(
        prefix,
        expected,
        metadata,
        fetch_meta=fetch_meta,
        fetch_direct=fetch_direct,
        geometry=session.geometry,
    )
    if direct_before_pack is not None:
        assert direct_before_pack.size is not None and direct_before_pack.sha1 is not None
        return _reuse_result(
            prefix,
            direct_before_pack.name,
            expected_id,
            direct_before_pack.size,
            direct_before_pack.sha1,
        )

    artifact = session.pack(prefix, output_dir, materialization=expected)
    local = _local_identity(artifact.path)

    # Segunda observação imediatamente antes do PUT: outra execução pode ter
    # publicado o mesmo shard enquanto fazíamos o pack. Se o catálogo ainda
    # estiver stale, a reconciliação direta fecha a mesma corrida sem overwrite.
    pre_upload_metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    pre_upload = classify_remote_shard(
        prefix,
        expected,
        pre_upload_metadata,
        geometry=session.geometry,
        fetch_meta=fetch_meta,
    )
    if pre_upload.state is ShardReuseState.REUSABLE:
        assert pre_upload.size is not None and pre_upload.sha1 is not None
        artifact.path.unlink(missing_ok=True)
        return _reuse_result(prefix, pre_upload.name, expected_id, pre_upload.size, pre_upload.sha1)
    if pre_upload.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(
            f"{pre_upload.name}: estado mudou antes do PUT: {pre_upload.state}: {pre_upload.detail}"
        )

    direct_before_put = _reconcile_catalog_absent(
        prefix,
        expected,
        pre_upload_metadata,
        fetch_meta=fetch_meta,
        fetch_direct=fetch_direct,
        geometry=session.geometry,
    )
    if direct_before_put is not None:
        assert direct_before_put.size is not None and direct_before_put.sha1 is not None
        artifact.path.unlink(missing_ok=True)
        return _reuse_result(
            prefix,
            direct_before_put.name,
            expected_id,
            direct_before_put.size,
            direct_before_put.sha1,
        )

    upload(pre_upload.name, artifact.path)

    if not confirm_remote_identity(
        pre_upload.name,
        local,
        fetch_metadata=fetch_metadata,
        attempts=confirm_attempts,
        sleep=sleep,
    ):
        # O PUT pode ter sido aceito mesmo que o catálogo continue stale. Antes
        # de declarar falha, observamos os bytes reais. Isso não autoriza novo
        # PUT: só converte a pós-condição em sucesso quando bytes + spec batem.
        stale_metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
        stale_verdict = classify_remote_shard(
            prefix,
            expected,
            stale_metadata,
            geometry=session.geometry,
            fetch_meta=fetch_meta,
        )
        if stale_verdict.state is ShardReuseState.ABSENT:
            direct_after_put = _reconcile_catalog_absent(
                prefix,
                expected,
                stale_metadata,
                fetch_meta=fetch_meta,
                fetch_direct=fetch_direct,
                geometry=session.geometry,
            )
            if direct_after_put is not None:
                assert direct_after_put.size is not None and direct_after_put.sha1 is not None
                if direct_after_put.size != local.size or direct_after_put.sha1 != local.sha1:
                    raise ShardPublishError(
                        f"{pre_upload.name}: objeto direto após PUT divergiu dos bytes locais: "
                        f"size={direct_after_put.size}/{local.size} "
                        f"sha1={direct_after_put.sha1}/{local.sha1}"
                    )
                artifact.path.unlink(missing_ok=True)
                return ShardPublishResult(
                    prefix=prefix,
                    name=direct_after_put.name,
                    action=ShardPublishAction.UPLOADED,
                    materialization_id=expected_id,
                    size=direct_after_put.size,
                    sha1=direct_after_put.sha1,
                )
        raise ShardPublishError(
            f"{pre_upload.name}: PUT retornou mas size+sha1 remoto não confirmou"
        )

    # A confirmação dos bytes não basta: `_meta.json` também deve declarar a
    # materialização esperada. Isso impede reutilizar bytes de outro conjunto
    # de inputs mesmo que o nome do shard seja o mesmo.
    final_metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    final = classify_remote_shard(
        prefix,
        expected,
        final_metadata,
        geometry=session.geometry,
        fetch_meta=fetch_meta,
    )
    if final.state is not ShardReuseState.REUSABLE:
        raise ShardPublishError(
            f"{pre_upload.name}: bytes confirmados, mas materialização final não: "
            f"{final.state}: {final.detail}"
        )
    assert final.size is not None and final.sha1 is not None
    if final.size != local.size or final.sha1 != local.sha1:
        raise ShardPublishError(
            f"{pre_upload.name}: identidade final divergiu após confirmação: "
            f"size={final.size}/{local.size} sha1={final.sha1}/{local.sha1}"
        )

    artifact.path.unlink(missing_ok=True)
    return ShardPublishResult(
        prefix=prefix,
        name=final.name,
        action=ShardPublishAction.UPLOADED,
        materialization_id=expected_id,
        size=final.size,
        sha1=final.sha1,
    )


def publish_shards(
    session: ShardPackSession,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    upload: ShardUpload,
    fetch_direct: DirectArtifactFetch | None = None,
    prefixes: Iterable[str] | None = None,
    confirm_attempts: int = _CONFIRM_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> list[ShardPublishResult]:
    selected = prefixes if prefixes is not None else session.geometry.prefixes()
    results: list[ShardPublishResult] = []
    for prefix in selected:
        result = publish_one_shard(
            session,
            prefix,
            output_dir,
            pinned_inputs=pinned_inputs,
            fetch_metadata=fetch_metadata,
            fetch_meta=fetch_meta,
            upload=upload,
            fetch_direct=fetch_direct,
            confirm_attempts=confirm_attempts,
            sleep=sleep,
        )
        results.append(result)
        log.info(
            "shard %s %s — %d bytes sha1=%s materialization=%s",
            result.prefix,
            result.action,
            result.size,
            result.sha1,
            result.materialization_id,
        )
    return results
