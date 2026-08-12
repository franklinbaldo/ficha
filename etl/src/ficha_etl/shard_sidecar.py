"""Sidecar de identidade criptográfica para shards publicados (#167/#176).

``MaterializationSpec`` identifica a semântica do shard; esta sidecar identifica
os bytes publicados. Separar as duas coisas evita a autorreferência impossível
de guardar o SHA-256 do ZIP dentro do próprio ``_meta.json``.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from . import mirror
from .materialization import canonical_json
from .sharded_pack import ShardGeometry

SIDECAR_VERSION = 1
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)
_TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})


class SidecarObservationError(RuntimeError):
    """A sidecar não pôde ser classificada com segurança como presente/ausente."""


@dataclass(frozen=True)
class ArtifactIdentity:
    size: int
    sha1: str
    sha256: str

    def as_document(self) -> dict:
        return {"size": self.size, "sha1": self.sha1, "sha256": self.sha256}


@dataclass(frozen=True)
class ShardSidecar:
    snapshot: str
    shard: str
    materialization_id: str
    artifact_name: str
    artifact: ArtifactIdentity
    sidecar_version: int = SIDECAR_VERSION

    def as_document(self) -> dict:
        return {
            "sidecar_version": self.sidecar_version,
            "snapshot": self.snapshot,
            "range": {"kind": "cnpj_base_prefix", "value": self.shard},
            "materialization_id": self.materialization_id,
            "artifact": {"name": self.artifact_name, **self.artifact.as_document()},
        }

    def canonical_bytes(self) -> bytes:
        return canonical_json(self.as_document()) + b"\n"


def sidecar_name(geometry: ShardGeometry, prefix: str) -> str:
    prefix = geometry.validate_prefix(prefix)
    return f"companies-{prefix}.identity.json"


def artifact_identity(path: Path) -> ArtifactIdentity:
    sha1 = hashlib.sha1(usedforsecurity=False)
    sha256 = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            sha1.update(chunk)
            sha256.update(chunk)
    return ArtifactIdentity(size=size, sha1=sha1.hexdigest(), sha256=sha256.hexdigest())


def write_sidecar(path: Path, sidecar: ShardSidecar) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    partial.unlink(missing_ok=True)
    partial.write_bytes(sidecar.canonical_bytes())
    partial.replace(path)


def parse_sidecar(payload: object) -> ShardSidecar | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("sidecar_version") != SIDECAR_VERSION:
        return None
    range_doc = payload.get("range")
    artifact_doc = payload.get("artifact")
    if not isinstance(range_doc, dict) or not isinstance(artifact_doc, dict):
        return None
    if range_doc.get("kind") != "cnpj_base_prefix":
        return None
    try:
        size = int(artifact_doc["size"])
        parsed = ShardSidecar(
            snapshot=str(payload["snapshot"]),
            shard=str(range_doc["value"]),
            materialization_id=str(payload["materialization_id"]),
            artifact_name=str(artifact_doc["name"]),
            artifact=ArtifactIdentity(
                size=size,
                sha1=str(artifact_doc["sha1"]),
                sha256=str(artifact_doc["sha256"]),
            ),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if parsed.artifact.size <= 0:
        return None
    if len(parsed.artifact.sha1) != 40 or any(
        c not in "0123456789abcdef" for c in parsed.artifact.sha1
    ):
        return None
    if len(parsed.artifact.sha256) != 64 or any(
        c not in "0123456789abcdef" for c in parsed.artifact.sha256
    ):
        return None
    return parsed


def sidecar_matches(
    sidecar: ShardSidecar,
    *,
    snapshot: str,
    prefix: str,
    materialization_id: str,
    artifact_name: str,
    remote_size: int,
    remote_sha1: str,
) -> bool:
    return (
        sidecar.snapshot == snapshot
        and sidecar.shard == prefix
        and sidecar.materialization_id == materialization_id
        and sidecar.artifact_name == artifact_name
        and sidecar.artifact.size == remote_size
        and sidecar.artifact.sha1 == remote_sha1
    )


def fetch_remote_sidecar(
    month: str,
    name: str,
    *,
    attempts: int = 5,
    backoff_s: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    client: httpx.Client | None = None,
) -> object | None:
    """Observa sidecar por GET direto com semântica fail-closed.

    Retorna ``None`` somente quando **todas** as observações foram 404. Um
    429/5xx, erro de rede, resposta vazia ou JSON inválido significa estado
    remoto desconhecido e levanta ``SidecarObservationError``. Assim uma falha
    de observabilidade nunca vira autorização para sobrescrever uma sidecar.
    """
    if attempts <= 0:
        raise ValueError("attempts must be positive")

    url = f"{mirror.item_root(month)}/{name}"
    owns_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    all_observations_are_404 = True
    last_detail = "sem observação"
    try:
        for attempt in range(1, attempts + 1):
            try:
                response = client.get(url)
            except httpx.HTTPError as exc:
                all_observations_are_404 = False
                last_detail = f"erro de rede: {exc}"
            else:
                if response.status_code == 200:
                    if not response.content:
                        raise SidecarObservationError(f"{name}: GET 200 sem conteúdo")
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise SidecarObservationError(f"{name}: JSON remoto inválido") from exc
                if response.status_code == 404:
                    last_detail = "HTTP 404"
                elif response.status_code in _TRANSIENT_STATUS:
                    all_observations_are_404 = False
                    last_detail = f"HTTP {response.status_code}"
                else:
                    raise SidecarObservationError(
                        f"{name}: resposta remota não classificável: HTTP {response.status_code}"
                    )
            if attempt < attempts:
                sleep(backoff_s * attempt)

        if all_observations_are_404:
            return None
        raise SidecarObservationError(
            f"{name}: observação remota permaneceu ambígua após {attempts} tentativa(s): "
            f"{last_detail}"
        )
    finally:
        if owns_client:
            client.close()


def hash_remote_artifact(
    month: str,
    name: str,
    *,
    expected_size: int,
    expected_sha1: str,
    client: httpx.Client | None = None,
) -> ArtifactIdentity:
    """Recupera SHA-256 de um ZIP sem sidecar, validando o stream contra IA."""
    url = f"{mirror.item_root(month)}/{name}"
    owns_client = client is None
    client = client or httpx.Client(timeout=None, follow_redirects=True)
    sha1 = hashlib.sha1(usedforsecurity=False)
    sha256 = hashlib.sha256()
    size = 0
    try:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes(1024 * 1024):
                size += len(chunk)
                sha1.update(chunk)
                sha256.update(chunk)
    finally:
        if owns_client:
            client.close()

    identity = ArtifactIdentity(size=size, sha1=sha1.hexdigest(), sha256=sha256.hexdigest())
    if identity.size != expected_size or identity.sha1 != expected_sha1:
        raise RuntimeError(
            f"remote artifact changed while hashing {name}: "
            f"size={identity.size}/{expected_size} sha1={identity.sha1}/{expected_sha1}"
        )
    return identity
