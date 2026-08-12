"""Publicação retomável de shards de ``companies`` (#165).

A unidade de sucesso é um shard remoto que satisfaz duas provas independentes:

1. ``size + sha1`` do objeto remoto são iguais aos bytes locais enviados;
2. ``_meta.json`` remoto declara exatamente o ``MaterializationSpec`` pinado.

Nada aqui faz replace. ``UNKNOWN`` e ``MISMATCH`` abortam antes de escrita.

O SHA-256 público do artefato é uma terceira identidade e não pertence ao
``MaterializationSpec``. Ele será persistido em sidecar própria (#167), porque
um hash do ZIP não pode ser embutido no próprio ``_meta.json`` sem virar uma
autorreferência e precisa sobreviver aos reruns que pulam shards já duráveis.
"""

from __future__ import annotations

import enum
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
from .upload_identity import confirm_remote_identity, local_identity

log = logging.getLogger(__name__)

MetadataFetch = Callable[[], dict | None]
RemoteMetaFetch = Callable[[str], object | None]
ShardUpload = Callable[[str, Path], None]

# 12 observações com o backoff linear interno de upload_identity (2s × n)
# cobrem 132 s. O probe canônico viu a adição ficar observável em algum ponto
# de (39,8s, 79,8s], então há margem sem usar pending_tasks como gate.
_CONFIRM_ATTEMPTS = 12


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
    """Fixa a identidade dos inputs uma vez, antes do primeiro shard."""
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
    assert metadata is not None  # materialization_input_sha1s já falhou se None
    return metadata


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
    """Garante um shard remoto reutilizável, sem sobrescrever mismatch."""
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
    log.info("shard %s: %s — %s", prefix, verdict.state, verdict.detail)

    if verdict.state is ShardReuseState.REUSABLE:
        assert verdict.size is not None and verdict.sha1 is not None
        return ShardPublishResult(
            prefix,
            verdict.name,
            ShardPublishAction.SKIPPED,
            expected_id,
            verdict.size,
            verdict.sha1,
        )
    if verdict.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")

    artifact = session.pack(prefix, output_dir, materialization=expected)
    local = local_identity(artifact.path)

    # Revalida imediatamente antes do PUT. Se outro processo publicou o mesmo
    # shard entre discovery e pack, só um match semântico exato autoriza skip.
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
        return ShardPublishResult(
            prefix,
            pre_upload.name,
            ShardPublishAction.SKIPPED,
            expected_id,
            pre_upload.size,
            pre_upload.sha1,
        )
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

    artifact.path.unlink(missing_ok=True)
    assert final.size is not None and final.sha1 is not None
    return ShardPublishResult(
        prefix,
        final.name,
        ShardPublishAction.UPLOADED,
        expected_id,
        final.size,
        final.sha1,
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
    """Publica sequencialmente; cada resultado já é um checkpoint durável."""
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
            "shard %s %s — %d bytes sha1=%s",
            result.prefix,
            result.action,
            result.size,
            result.sha1,
        )
    return results
