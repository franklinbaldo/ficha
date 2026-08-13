"""Fases explícitas para produzir, submeter e verificar ``companies`` shards.

A identidade nasce na produção. ``prepare_one_shard`` materializa bytes locais e
fecha ``size + sha1 + materialization_id`` sem rede de escrita. A orquestração
pode persistir esse recibo antes de chamar ``submit_prepared_shard``.

A submissão é a única fase com capacidade de escrita e preserva o preflight
fail-closed: um PUT só é permitido quando catálogo e observação direta concordam
que o nome ainda não existe. A verificação é estritamente read-only e usa
``size + sha1`` do catálogo mais o ``_meta.json`` semântico.
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .shard_publish import (
    DirectArtifactFetch,
    MetadataFetch,
    RemoteMetaFetch,
    ShardPublishError,
    ShardUpload,
    _local_identity,
    _metadata_still_matches,
    _reconcile_catalog_absent,
    fetch_direct_artifact_identity,
)
from .shard_remote import PUBLIC_COMPANIES_GEOMETRY, ShardReuseState, classify_remote_shard
from .sharded_pack import ShardPackSession
from .upload_identity import LocalIdentity


class ShardTransferAction(enum.StrEnum):
    SUBMITTED = "submitted"
    VERIFIED = "verified"


@dataclass(frozen=True)
class ShardTransferResult:
    prefix: str
    name: str
    action: ShardTransferAction
    materialization_id: str
    size: int
    sha1: str


@dataclass(frozen=True)
class PreparedShard:
    """Recibo da identidade local fechado antes de qualquer PUT."""

    prefix: str
    name: str
    path: Path
    materialization_id: str
    size: int
    sha1: str


def _validate_session(session: ShardPackSession) -> None:
    if session.geometry != PUBLIC_COMPANIES_GEOMETRY:
        raise ShardPublishError(
            f"geometria de publicação deve ser {PUBLIC_COMPANIES_GEOMETRY.prefix_digits} dígitos"
        )


def _direct_fetcher(
    session: ShardPackSession,
    fetch_direct: DirectArtifactFetch | None,
) -> DirectArtifactFetch:
    if fetch_direct is not None:
        return fetch_direct

    def fetch(name: str) -> LocalIdentity | None:
        return fetch_direct_artifact_identity(session.month, name)

    return fetch


def _result_from_verified(prefix: str, materialization_id: str, verdict) -> ShardTransferResult:
    assert verdict.size is not None and verdict.sha1 is not None
    return ShardTransferResult(
        prefix=prefix,
        name=verdict.name,
        action=ShardTransferAction.VERIFIED,
        materialization_id=materialization_id,
        size=verdict.size,
        sha1=verdict.sha1,
    )


def _classify(
    session: ShardPackSession,
    prefix: str,
    expected,
    metadata: dict,
    fetch_meta: RemoteMetaFetch,
):
    return classify_remote_shard(
        prefix,
        expected,
        metadata,
        geometry=session.geometry,
        fetch_meta=fetch_meta,
    )


def _verify_from_metadata(
    session: ShardPackSession,
    prefix: str,
    *,
    pinned_inputs: Mapping[str, str],
    metadata: dict,
    fetch_meta: RemoteMetaFetch,
) -> ShardTransferResult:
    prefix = session.geometry.validate_prefix(prefix)
    expected = session.materialization_spec(prefix, input_sha1s=pinned_inputs)
    expected_id = expected.materialization_id()
    verdict = _classify(session, prefix, expected, metadata, fetch_meta)
    if verdict.state is ShardReuseState.REUSABLE:
        return _result_from_verified(prefix, expected_id, verdict)
    if verdict.state is ShardReuseState.ABSENT:
        raise ShardPublishError(
            f"{verdict.name}: ainda não verificável no metadata do Internet Archive"
        )
    raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")


def prepare_one_shard(
    session: ShardPackSession,
    prefix: str,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
) -> PreparedShard:
    """Produz um shard e fecha sua identidade sem observar/escrever o remoto."""
    _validate_session(session)
    prefix = session.geometry.validate_prefix(prefix)
    expected = session.materialization_spec(prefix, input_sha1s=pinned_inputs)
    artifact = session.pack(prefix, output_dir, materialization=expected)
    local = _local_identity(artifact.path)
    if local.size != artifact.size_bytes:
        raise ShardPublishError(
            f"{artifact.path.name}: size mudou após pack: {local.size} != {artifact.size_bytes}"
        )
    return PreparedShard(
        prefix=prefix,
        name=session.geometry.shard_name(prefix),
        path=artifact.path,
        materialization_id=expected.materialization_id(),
        size=local.size,
        sha1=local.sha1,
    )


def _validate_prepared(
    session: ShardPackSession,
    prepared: PreparedShard,
    *,
    pinned_inputs: Mapping[str, str],
) -> tuple[str, object, LocalIdentity]:
    prefix = session.geometry.validate_prefix(prepared.prefix)
    expected_name = session.geometry.shard_name(prefix)
    if prepared.name != expected_name:
        raise ShardPublishError(f"prepared name {prepared.name!r} != {expected_name!r}")
    expected = session.materialization_spec(prefix, input_sha1s=pinned_inputs)
    expected_id = expected.materialization_id()
    if prepared.materialization_id != expected_id:
        raise ShardPublishError(
            f"{expected_name}: prepared materialization_id diverge de {expected_id}"
        )
    if not prepared.path.exists():
        raise ShardPublishError(f"{expected_name}: prepared file ausente: {prepared.path}")
    local = _local_identity(prepared.path)
    if local.size != prepared.size or local.sha1 != prepared.sha1:
        raise ShardPublishError(
            f"{expected_name}: bytes locais mudaram após o recibo de produção: "
            f"size={local.size}/{prepared.size} sha1={local.sha1}/{prepared.sha1}"
        )
    return prefix, expected, local


def _require_same_bytes(name: str, verdict, prepared: PreparedShard) -> None:
    if verdict.size != prepared.size or verdict.sha1 != prepared.sha1:
        raise ShardPublishError(
            f"{name}: materialização remota é semanticamente reutilizável, mas os bytes "
            f"divergem do recibo de produção: size={verdict.size}/{prepared.size} "
            f"sha1={verdict.sha1}/{prepared.sha1}"
        )


def submit_prepared_shard(
    session: ShardPackSession,
    prepared: PreparedShard,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    upload: ShardUpload,
    fetch_direct: DirectArtifactFetch | None = None,
) -> ShardTransferResult:
    """Submete exatamente os bytes já registrados no recibo de produção.

    Se um objeto semanticamente equivalente já existir, ele só é reutilizado
    quando ``size + sha1`` também forem idênticos ao recibo. A identidade
    esperada nunca é aprendida do remoto e nenhum estado ambíguo autoriza PUT.
    """
    _validate_session(session)
    prefix, expected, _local = _validate_prepared(session, prepared, pinned_inputs=pinned_inputs)
    expected_id = expected.materialization_id()
    fetch_direct = _direct_fetcher(session, fetch_direct)

    metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    verdict = _classify(session, prefix, expected, metadata, fetch_meta)
    if verdict.state is ShardReuseState.REUSABLE:
        _require_same_bytes(verdict.name, verdict, prepared)
        return _result_from_verified(prefix, expected_id, verdict)
    if verdict.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")

    direct = _reconcile_catalog_absent(
        prefix,
        expected,
        metadata,
        fetch_meta=fetch_meta,
        fetch_direct=fetch_direct,
        geometry=session.geometry,
    )
    if direct is not None:
        _require_same_bytes(direct.name, direct, prepared)
        return _result_from_verified(prefix, expected_id, direct)

    # Preflight final imediatamente antes do PUT: outro job pode ter criado o
    # nome enquanto este shard era materializado/persistido como artifact.
    pre_upload_metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    pre_upload = _classify(session, prefix, expected, pre_upload_metadata, fetch_meta)
    if pre_upload.state is ShardReuseState.REUSABLE:
        _require_same_bytes(pre_upload.name, pre_upload, prepared)
        return _result_from_verified(prefix, expected_id, pre_upload)
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
        _require_same_bytes(direct_before_put.name, direct_before_put, prepared)
        return _result_from_verified(prefix, expected_id, direct_before_put)

    upload(prepared.name, prepared.path)
    return ShardTransferResult(
        prefix=prefix,
        name=prepared.name,
        action=ShardTransferAction.SUBMITTED,
        materialization_id=expected_id,
        size=prepared.size,
        sha1=prepared.sha1,
    )


def submit_one_shard(
    session: ShardPackSession,
    prefix: str,
    output_dir: Path,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
    upload: ShardUpload,
    fetch_direct: DirectArtifactFetch | None = None,
) -> ShardTransferResult:
    """Compatibilidade: produz e submete em uma única chamada.

    O pipeline mensal novo usa ``prepare_one_shard`` + ``submit_prepared_shard``
    para conseguir persistir o recibo entre as duas fases.
    """
    _validate_session(session)
    prefix = session.geometry.validate_prefix(prefix)
    expected = session.materialization_spec(prefix, input_sha1s=pinned_inputs)
    expected_id = expected.materialization_id()
    fetch_direct = _direct_fetcher(session, fetch_direct)

    metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    verdict = _classify(session, prefix, expected, metadata, fetch_meta)
    if verdict.state is ShardReuseState.REUSABLE:
        return _result_from_verified(prefix, expected_id, verdict)
    if verdict.state is not ShardReuseState.ABSENT:
        raise ShardPublishError(f"{verdict.name}: {verdict.state}: {verdict.detail}")

    direct = _reconcile_catalog_absent(
        prefix,
        expected,
        metadata,
        fetch_meta=fetch_meta,
        fetch_direct=fetch_direct,
        geometry=session.geometry,
    )
    if direct is not None:
        return _result_from_verified(prefix, expected_id, direct)

    artifact = session.pack(prefix, output_dir, materialization=expected)
    local = _local_identity(artifact.path)

    pre_upload_metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    pre_upload = _classify(session, prefix, expected, pre_upload_metadata, fetch_meta)
    if pre_upload.state is ShardReuseState.REUSABLE:
        artifact.path.unlink(missing_ok=True)
        return _result_from_verified(prefix, expected_id, pre_upload)
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
        artifact.path.unlink(missing_ok=True)
        return _result_from_verified(prefix, expected_id, direct_before_put)

    upload(pre_upload.name, artifact.path)
    artifact.path.unlink(missing_ok=True)
    return ShardTransferResult(
        prefix=prefix,
        name=pre_upload.name,
        action=ShardTransferAction.SUBMITTED,
        materialization_id=expected_id,
        size=local.size,
        sha1=local.sha1,
    )


def verify_one_shard(
    session: ShardPackSession,
    prefix: str,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
) -> ShardTransferResult:
    """Verifica um checkpoint somente pelo contrato observável do IA."""
    _validate_session(session)
    metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)
    return _verify_from_metadata(
        session,
        prefix,
        pinned_inputs=pinned_inputs,
        metadata=metadata,
        fetch_meta=fetch_meta,
    )


def verify_all_shards(
    session: ShardPackSession,
    *,
    pinned_inputs: Mapping[str, str],
    fetch_metadata: MetadataFetch,
    fetch_meta: RemoteMetaFetch,
) -> tuple[ShardTransferResult, ...]:
    """Verifica toda a geometria pública com uma única leitura do catálogo IA."""
    _validate_session(session)
    metadata = _metadata_still_matches(pinned_inputs, fetch_metadata)

    results: list[ShardTransferResult] = []
    problems: list[str] = []
    for prefix in session.geometry.prefixes():
        try:
            result = _verify_from_metadata(
                session,
                prefix,
                pinned_inputs=pinned_inputs,
                metadata=metadata,
                fetch_meta=fetch_meta,
            )
        except ShardPublishError as exc:
            problems.append(str(exc))
        else:
            results.append(result)

    if problems:
        raise ShardPublishError(
            f"{len(problems)}/{session.geometry.count} shards não verificáveis via metadata do IA: "
            + "; ".join(problems)
        )

    return tuple(results)
