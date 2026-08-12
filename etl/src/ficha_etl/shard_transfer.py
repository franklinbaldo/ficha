"""Duas fases explícitas para transferir ``companies`` shards (#189).

``submit_one_shard`` é a única fase com capacidade de escrita. Ela preserva o
preflight fail-closed do publisher forte: um PUT só é permitido quando catálogo
e observação direta concordam que o nome ainda não existe. Depois que o PUT
retorna, o resultado é deliberadamente ``SUBMITTED`` — não um checkpoint.

A verificação é estritamente read-only e usa o contrato publicado pelo próprio
Internet Archive: ``size + sha1`` vêm do metadata do item e o ``_meta.json``
interno declara exatamente o ``MaterializationSpec`` esperado. O VERIFY não
baixa novamente o ZIP inteiro. A observação direta dos bytes permanece apenas
no preflight de escrita, onde protege contra overwrite quando o catálogo está
stale.

A separação impede que a consistência eventual do Internet Archive serialize a
submissão de nomes independentes sem enfraquecer a pós-condição que alimenta o
manifesto público.
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
    """Submete no máximo um objeto e nunca espera sua visibilidade pós-PUT.

    ``SUBMITTED`` significa apenas que ``upload`` retornou com sucesso para os
    bytes locais informados no resultado. O chamador deve executar uma fase de
    verificação via metadata antes de considerar o checkpoint completo.
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

    # Repetimos todo o preflight imediatamente antes do PUT. Outra execução
    # pode ter criado esse nome enquanto o shard era empacotado.
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
    """Verifica um checkpoint somente pelo contrato observável do IA.

    O metadata do item fornece ``size + sha1`` do objeto e o unzip transparente
    fornece o pequeno ``_meta.json`` interno. Não há GET integral do ZIP nesta
    fase; se o catálogo ainda não lista o shard, ele simplesmente ainda não pode
    ser promovido.
    """
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
    """Verifica toda a geometria pública com uma única leitura do catálogo IA.

    Cada shard ainda tem seu ``_meta.json`` semântico validado individualmente,
    mas ``/metadata`` é lido exatamente uma vez. Isso evita 100 jobs e 100
    downloads integrais só para concluir a pós-condição de publicação.
    """
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
