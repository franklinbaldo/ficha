"""Publicação retomável de shards de ``companies`` (#165/#167/#175/#176).

Um checkpoint só é completo quando três identidades concordam:

1. ``size + sha1`` do ZIP remoto são iguais aos bytes enviados;
2. ``_meta.json`` declara exatamente o ``MaterializationSpec`` pinado;
3. a sidecar criptográfica, lida diretamente, preserva o SHA-256 dos bytes.

Nada aqui faz replace do ZIP. ``UNKNOWN`` e ``MISMATCH`` abortam antes de
escrita. A sidecar também falha fechada: 404 é ausência observada; falha de
rede/5xx é estado desconhecido e nunca autoriza PUT.
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
from .shard_sidecar import (
    ArtifactIdentity,
    ShardSidecar,
    SidecarObservationError,
    artifact_identity,
    parse_sidecar,
    sidecar_matches,
    sidecar_name,
    write_sidecar,
)
from .sharded_pack import ShardPackSession
from .upload_identity import LocalIdentity, confirm_remote_identity

log = logging.getLogger(__name__)

MetadataFetch = Callable[[], dict | None]
RemoteMetaFetch = Callable[[str], object | None]
SidecarFetch = Callable[[str], object | None]
RemoteArtifactHash = Callable[[str, int, str], ArtifactIdentity]
ShardUpload = Callable[[str, Path], None]

# O item real ficha-2026-05 excedeu a antiga janela de ~132 s: um ZIP aceito
# pelo IA só apareceu no metadata cerca de 5m44s depois. Com backoff linear de
# 2s*n, 20 observações cobrem 380 s sem usar pending_tasks como gate.
_CONFIRM_ATTEMPTS = 20
_CONFIRM_BACKOFF_S = 2.0


class ShardPublishAction(enum.StrEnum):
    SKIPPED = "skipped"
    UPLOADED = "uploaded"
    SIDECAR_REPAIRED = "sidecar-repaired"


@dataclass(frozen=True)
class ShardPublishResult:
    prefix: str
    name: str
    action: ShardPublishAction
    materialization_id: str
    size: int
    sha1: str
    sha256: str


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


def _validate_sidecar_payload(
    payload: object,
    *,
    snapshot: str,
    prefix: str,
    materialization_id: str,
    artifact_name: str,
    remote_size: int,
    remote_sha1: str,
) -> ShardSidecar:
    parsed = parse_sidecar(payload)
    if parsed is None or not sidecar_matches(
        parsed,
        snapshot=snapshot,
        prefix=prefix,
        materialization_id=materialization_id,
        artifact_name=artifact_name,
        remote_size=remote_size,
        remote_sha1=remote_sha1,
    ):
        raise ShardPublishError(f"{artifact_name}: sidecar ausente, inválida ou divergente")
    return parsed


def _ensure_sidecar(
    session: ShardPackSession,
    prefix: str,
    output_dir: Path,
    *,
    materialization_id: str,
    artifact_name: str,
    remote_size: int,
    remote_sha1: str,
    fetch_sidecar: SidecarFetch,
    hash_remote: RemoteArtifactHash,
    upload: ShardUpload,
    local_artifact: ArtifactIdentity | None,
    confirm_attempts: int,
    sleep: Callable[[float], None],
) -> tuple[ArtifactIdentity, bool]:
    """Garante sidecar por observação direta, sem depender de metadata.files."""
    identity_name = sidecar_name(session.geometry, prefix)

    # Discovery direto: uma sidecar servível já é observável mesmo que o
    # catálogo do IA ainda não a tenha indexado. UNKNOWN propaga e aborta antes
    # de qualquer escrita; somente 404 confirmado (payload=None) permite reparo.
    payload = fetch_sidecar(identity_name)
    if payload is not None:
        parsed = _validate_sidecar_payload(
            payload,
            snapshot=session.month,
            prefix=prefix,
            materialization_id=materialization_id,
            artifact_name=artifact_name,
            remote_size=remote_size,
            remote_sha1=remote_sha1,
        )
        return parsed.artifact, False

    if (
        local_artifact is not None
        and local_artifact.size == remote_size
        and local_artifact.sha1 == remote_sha1
    ):
        identity = local_artifact
    else:
        identity = hash_remote(artifact_name, remote_size, remote_sha1)
        if identity.size != remote_size or identity.sha1 != remote_sha1:
            raise ShardPublishError(
                f"{artifact_name}: hash remoto recuperado não bate com metadata"
            )

    sidecar = ShardSidecar(
        snapshot=session.month,
        shard=prefix,
        materialization_id=materialization_id,
        artifact_name=artifact_name,
        artifact=identity,
    )
    path = output_dir / identity_name
    write_sidecar(path, sidecar)
    upload(identity_name, path)

    # Depois do PUT, retentar UNKNOWN/404 é seguro porque não há nova escrita.
    # O sucesso vem do GET direto + payload exato; a indexação em metadata.files
    # pode atrasar vários minutos e não é requisito de durabilidade da sidecar.
    last_detail = "GET direto ainda não observou a sidecar"
    for attempt in range(1, confirm_attempts + 1):
        try:
            payload = fetch_sidecar(identity_name)
        except SidecarObservationError as exc:
            last_detail = str(exc)
        else:
            if payload is not None:
                parsed = _validate_sidecar_payload(
                    payload,
                    snapshot=session.month,
                    prefix=prefix,
                    materialization_id=materialization_id,
                    artifact_name=artifact_name,
                    remote_size=remote_size,
                    remote_sha1=remote_sha1,
                )
                if parsed != sidecar:
                    raise ShardPublishError(
                        f"{identity_name}: sidecar servida diverge dos bytes acabados de publicar"
                    )
                path.unlink(missing_ok=True)
                return identity, True
            last_detail = "HTTP 404"
        if attempt < confirm_attempts:
            sleep(_CONFIRM_BACKOFF_S * attempt)

    raise ShardPublishError(
        f"{identity_name}: PUT retornou mas GET direto não confirmou após "
        f"{confirm_attempts} observações: {last_detail}"
    )


def publish_one_shard(
    session: ShardPackSession,
    prefix: str,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    fetch_sidecar: SidecarFetch,
    hash_remote: RemoteArtifactHash,
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
        identity, repaired = _ensure_sidecar(
            session,
            prefix,
            output_dir,
            materialization_id=expected_id,
            artifact_name=verdict.name,
            remote_size=verdict.size,
            remote_sha1=verdict.sha1,
            fetch_sidecar=fetch_sidecar,
            hash_remote=hash_remote,
            upload=upload,
            local_artifact=None,
            confirm_attempts=confirm_attempts,
            sleep=sleep,
        )
        return ShardPublishResult(
            prefix,
            verdict.name,
            ShardPublishAction.SIDECAR_REPAIRED if repaired else ShardPublishAction.SKIPPED,
            expected_id,
            identity.size,
            identity.sha1,
            identity.sha256,
        )
    if verdict.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")

    artifact = session.pack(prefix, output_dir, materialization=expected)
    local_artifact = artifact_identity(artifact.path)
    local = LocalIdentity(size=local_artifact.size, sha1=local_artifact.sha1)

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
        identity, repaired = _ensure_sidecar(
            session,
            prefix,
            output_dir,
            materialization_id=expected_id,
            artifact_name=pre_upload.name,
            remote_size=pre_upload.size,
            remote_sha1=pre_upload.sha1,
            fetch_sidecar=fetch_sidecar,
            hash_remote=hash_remote,
            upload=upload,
            local_artifact=local_artifact,
            confirm_attempts=confirm_attempts,
            sleep=sleep,
        )
        artifact.path.unlink(missing_ok=True)
        return ShardPublishResult(
            prefix,
            pre_upload.name,
            ShardPublishAction.SIDECAR_REPAIRED if repaired else ShardPublishAction.SKIPPED,
            expected_id,
            identity.size,
            identity.sha1,
            identity.sha256,
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
    assert final.size is not None and final.sha1 is not None

    identity, _ = _ensure_sidecar(
        session,
        prefix,
        output_dir,
        materialization_id=expected_id,
        artifact_name=final.name,
        remote_size=final.size,
        remote_sha1=final.sha1,
        fetch_sidecar=fetch_sidecar,
        hash_remote=hash_remote,
        upload=upload,
        local_artifact=local_artifact,
        confirm_attempts=confirm_attempts,
        sleep=sleep,
    )
    artifact.path.unlink(missing_ok=True)
    return ShardPublishResult(
        prefix,
        final.name,
        ShardPublishAction.UPLOADED,
        expected_id,
        identity.size,
        identity.sha1,
        identity.sha256,
    )


def publish_shards(
    session: ShardPackSession,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    fetch_sidecar: SidecarFetch,
    hash_remote: RemoteArtifactHash,
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
            fetch_sidecar=fetch_sidecar,
            hash_remote=hash_remote,
            upload=upload,
            confirm_attempts=confirm_attempts,
            sleep=sleep,
        )
        results.append(result)
        log.info(
            "shard %s %s — %d bytes sha1=%s sha256=%s",
            result.prefix,
            result.action,
            result.size,
            result.sha1,
            result.sha256,
        )
    return results
