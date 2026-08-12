"""Publicação retomável de shards de ``companies`` (#165/#175/#180).

Um checkpoint é completo quando duas identidades independentes concordam:

1. ``size + sha1`` do ZIP remoto são iguais aos bytes enviados;
2. ``_meta.json`` declara exatamente o ``MaterializationSpec`` pinado.

O SHA-1 aqui é checksum operacional fornecido pelo próprio Internet Archive,
não assinatura criptográfica. O ``materialization_id`` identifica a semântica;
``size + sha1`` identificam os bytes observados. Nada aqui faz replace do ZIP.
``UNKNOWN`` e ``MISMATCH`` abortam antes de escrita.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .shard_remote import (
    PUBLIC_COMPANIES_GEOMETRY,
    ShardReuseState,
    classify_remote_shard,
    materialization_input_sha1s,
)
from .sharded_pack import ShardPackSession
from .upload_identity import LocalIdentity, confirm_remote_identity

log = logging.getLogger(__name__)

MetadataFetch = Callable[[], dict | None]
RemoteMetaFetch = Callable[[str], object | None]
ShardUpload = Callable[[str, Path], None]

# O item real ficha-2026-05 excedeu a antiga janela de ~132 s: um ZIP aceito
# pelo IA só apareceu no metadata cerca de 5m44s depois. Com backoff linear de
# 2s*n, 20 observações cobrem 380 s sem usar pending_tasks como gate.
_CONFIRM_ATTEMPTS = 20


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

    artifact = session.pack(prefix, output_dir, materialization=expected)
    local = _local_identity(artifact.path)

    # Segunda observação imediatamente antes do PUT: outra execução pode ter
    # publicado o mesmo shard enquanto fazíamos o pack. Se já for exatamente a
    # materialização esperada, descartamos o arquivo local e reutilizamos.
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

    upload(pre_upload.name, artifact.path)

    if not confirm_remote_identity(
        pre_upload.name,
        local,
        fetch_metadata=fetch_metadata,
        attempts=confirm_attempts,
        sleep=sleep,
    ):
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
