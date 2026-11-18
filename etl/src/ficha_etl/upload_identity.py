"""Identidade local↔remota para upload idempotente (#132 slice 2).

Motivação (#110): o upload de `companies.zip` foi interrompido a 3%. Não havia
como o pipeline distinguir "já subiu" de "subiu pela metade" de "nunca subiu" —
e reenviar tudo custa horas. Este módulo dá o primitivo que falta.

## Fronteira entre os dois hashes

- **`sha1`** — usado aqui. É *fingerprint de identidade operacional*: responde
  "estes dois objetos são os mesmos bytes?". Não faz parte de contrato nenhum.
- **`sha256`** — do manifesto público, calculado na promoção por
  `manifest.build_snapshot_entry()`. **Não é tocado por este módulo.**

O metadata do IA expõe `sha1` mas nunca `sha256`, então usar sha1 evita baixar
o objeto remoto só para comparar. Probe real contra `ia:ficha-2026-05`
(`lookups/motivos.parquet`, 2743 bytes) confirmou que o `sha1` do metadata é o
SHA-1 dos bytes originais.

## Estado conhecido vs estado desconhecido

A distinção que governa o desenho:

- `MISMATCH` é estado **conhecido** e recuperável — os bytes remotos existem e
  são comprovadamente outros. Substituir é correto.
- `UNAVAILABLE` é estado **desconhecido** — não conseguimos ler o metadata.
  Substituir aqui significaria sobrescrever um objeto remoto por causa de uma
  falha momentânea de observabilidade. **Nunca** se faz replace por
  indisponibilidade; retenta e, persistindo, devolve ambiguidade.

E em nenhuma hipótese ambiguidade vira `SKIP`.

## Semântica de replace no Internet Archive

Verificada na fonte de `internetarchive.item.Item.upload_file`: não existe flag
de overwrite. O PUT vai para `s3.us.archive.org/{identifier}/{key}` e um novo
PUT no mesmo key substitui o objeto. A lib oferece `checksum=True` para pular
uploads, mas ele compara **md5** e só age quando `not self.tasks` — isto é, não
pula enquanto o item tem tarefas pendentes de derive. Por isso a decisão é
tomada aqui, de forma explícita e observável, em vez de delegada.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

_CHUNK_SIZE = 1024 * 1024  # 1 MiB — companies.zip tem 21,8 GiB; nada em RAM.

# O metadata do IA fica atrás do PUT (derive assíncrono). A janela abaixo é
# provisória e deliberadamente NÃO calibrada pelo pior caso observado.
#
# O run 31502999943 mostrou que, ao criar um item NOVO, o arquivo não apareceu
# em `files` nem 79 s depois do PUT. Mas esse é um caso distinto do de produção,
# e transformar aquele número em constante global seria calibrar pelo cenário
# errado. Os quatro casos precisam ser medidos separadamente:
#
#   1. item novo                      — observado: >79 s, e não é caso de produção
#   2. item existente, objeto novo    — não medido
#   3. overwrite em item existente    — não medido (probe de #140 pendente)
#   4. item com derive pendente       — não medido
#
# Produção acontece em (2) e (3): `ficha-YYYY-MM` já existe quando o pipeline
# escreve. Enquanto (2)/(3) não forem medidos, a janela fica curta e ambos
# `confirm_attempts` e `sleep` seguem injetáveis, para que o call site escolha
# com base em evidência em vez de herdar um default arbitrário.
_CONFIRM_ATTEMPTS = 5
_CONFIRM_BACKOFF_S = 2.0


class RemoteIdentityState(enum.StrEnum):
    MISSING = "missing"
    IDENTICAL = "identical"
    MISMATCH = "mismatch"
    UNAVAILABLE = "unavailable"


class UploadAction(enum.StrEnum):
    UPLOAD = "upload"
    SKIP = "skip"
    REPLACE = "replace"
    RETRY_FAIL = "retry-fail"


@dataclass(frozen=True)
class LocalIdentity:
    size: int
    sha1: str


@dataclass(frozen=True)
class IdentityDecision:
    name: str
    state: RemoteIdentityState
    action: UploadAction
    detail: str

    def log_line(self) -> str:
        """Uma linha factual por artefato, no espírito do log do mirror."""
        label = {
            UploadAction.UPLOAD: "UPLOAD  missing",
            UploadAction.SKIP: "SKIP    identical",
            UploadAction.REPLACE: "REPLACE mismatch",
            UploadAction.RETRY_FAIL: "RETRY/FAIL metadata unavailable",
        }[self.action]
        return f"  {label:32} {self.name:28} {self.detail}"


class IdentityNotConfirmed(RuntimeError):
    """Upload terminou mas a identidade remota não pôde ser confirmada.

    Levantada em vez de devolver sucesso: foi exatamente um upload interrompido
    que criou o caso de recuperação de #110, e o retorno do uploader sozinho não
    prova durabilidade.
    """


def sha1_of_file(path: Path, *, chunk_size: int = _CHUNK_SIZE) -> str:
    """SHA-1 em streaming — nunca carrega o arquivo inteiro em memória."""
    digest = hashlib.sha1()  # noqa: S324 — fingerprint operacional, não contrato
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def local_identity(path: Path, *, chunk_size: int = _CHUNK_SIZE) -> LocalIdentity:
    return LocalIdentity(size=path.stat().st_size, sha1=sha1_of_file(path, chunk_size=chunk_size))


def files_list(metadata: dict) -> list[dict] | None:
    """Lista `files` do metadata, ou `None` se estruturalmente inutilizável.

    `{}`, `{"metadata": {...}}`, `files: null` e `files` não-lista significam
    **ausência de observação**, não observação de ausência. Confundir os dois
    faria um metadata parcial virar `MISSING` → `UPLOAD`, isto é, uma escrita
    remota autorizada por falta de informação.
    """
    files = metadata.get("files")
    if not isinstance(files, list):
        return None
    if not all(isinstance(entry, dict) for entry in files):
        return None
    return files


def remote_entry(name: str, metadata: dict | None) -> dict | None:
    """Entrada do arquivo, ou `None` quando ausente de uma lista válida.

    Só distingue ausência dentro de uma lista utilizável; a distinção entre
    "não está na lista" e "não há lista" é feita em `decide()`, porque as duas
    levam a ações diferentes.
    """
    if metadata is None:
        return None
    files = files_list(metadata)
    if files is None:
        return None
    for entry in files:
        if entry.get("name") == name:
            return entry
    return None


def decide(name: str, local: LocalIdentity, metadata: dict | None) -> IdentityDecision:
    """Função pura: identidade local + metadata do item → o que fazer."""
    if metadata is None:
        return IdentityDecision(
            name,
            RemoteIdentityState.UNAVAILABLE,
            UploadAction.RETRY_FAIL,
            "metadata do item indisponível — estado remoto desconhecido",
        )

    files = files_list(metadata)
    if files is None:
        # Metadata chegou, mas sem lista `files` utilizável. Não é evidência de
        # ausência — é ausência de evidência, e não autoriza escrita.
        return IdentityDecision(
            name,
            RemoteIdentityState.UNAVAILABLE,
            UploadAction.RETRY_FAIL,
            "metadata sem lista `files` utilizável — ausência não observada",
        )

    matching = [candidate for candidate in files if candidate.get("name") == name]
    if len(matching) > 1:
        # O catálogo não garante unicidade. Mesmo duplicatas iguais não podem
        # autorizar SKIP ou REPLACE com base na ordem das entradas.
        return IdentityDecision(
            name,
            RemoteIdentityState.UNAVAILABLE,
            UploadAction.RETRY_FAIL,
            f"metadata com {len(matching)} entradas para o mesmo nome — identidade ambígua",
        )

    if not matching:
        # Lista válida e o arquivo não está nela: ausência **observada**.
        return IdentityDecision(
            name, RemoteIdentityState.MISSING, UploadAction.UPLOAD, "ausente no item remoto"
        )

    entry = matching[0]
    raw_size, remote_sha1 = entry.get("size"), entry.get("sha1")
    # int(float) truncates and int(bool) accepts True/False. Neither proves
    # the byte size reported by the archive. Only decimal strings or genuine
    # nonnegative integers are comparable.
    if type(raw_size) is int and raw_size >= 0:
        remote_size = raw_size
    elif isinstance(raw_size, str) and raw_size.isascii() and raw_size.isdecimal():
        try:
            remote_size = int(raw_size)
        except ValueError:  # limite de dígitos de int em Python
            remote_size = None
    else:
        remote_size = None

    if (
        remote_size is None
        or not isinstance(remote_sha1, str)
        or re.fullmatch(r"[0-9a-fA-F]{40}", remote_sha1) is None
    ):
        # Entrada existe mas sem identidade comparável: desconhecido, não
        # divergente. Substituir aqui seria destruir por falta de observabilidade.
        return IdentityDecision(
            name,
            RemoteIdentityState.UNAVAILABLE,
            UploadAction.RETRY_FAIL,
            f"metadata sem identidade comparável (size={raw_size!r}, sha1={remote_sha1!r})",
        )

    remote_sha1 = remote_sha1.lower()
    if remote_size == local.size and remote_sha1 == local.sha1:
        return IdentityDecision(
            name,
            RemoteIdentityState.IDENTICAL,
            UploadAction.SKIP,
            f"size={local.size} sha1={local.sha1}",
        )

    return IdentityDecision(
        name,
        RemoteIdentityState.MISMATCH,
        UploadAction.REPLACE,
        f"local size={local.size} sha1={local.sha1} · remoto size={remote_size} sha1={remote_sha1}",
    )


MetadataFetch = Callable[[], dict | None]
Uploader = Callable[[], None]


def confirm_remote_identity(
    name: str,
    local: LocalIdentity,
    *,
    fetch_metadata: MetadataFetch,
    attempts: int = _CONFIRM_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Relê o metadata até observar `size` e `sha1` iguais aos locais.

    O metadata do IA fica atrás do PUT por alguns segundos, então "ainda não
    bateu" não é o mesmo que "não bate".
    """
    for attempt in range(1, attempts + 1):
        decision = decide(name, local, fetch_metadata())
        if decision.state is RemoteIdentityState.IDENTICAL:
            return True
        if attempt == attempts:
            log.warning(
                "identidade de %s não confirmada após %d leituras: %s",
                name,
                attempts,
                decision.detail,
            )
            return False
        sleep(_CONFIRM_BACKOFF_S * attempt)
    return False


def ensure_uploaded(
    name: str,
    path: Path,
    *,
    upload: Uploader,
    fetch_metadata: MetadataFetch,
    confirm_attempts: int = _CONFIRM_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> IdentityDecision:
    """Garante que o objeto remoto tem os mesmos bytes do local.

    Devolve a decisão tomada. Levanta `IdentityNotConfirmed` quando um upload
    ou replace ocorreu mas a identidade remota não pôde ser confirmada — o
    retorno do uploader não é aceito como prova de durabilidade.
    """
    local = local_identity(path)
    decision = decide(name, local, fetch_metadata())
    log.info("%s", decision.log_line())

    if decision.action is UploadAction.SKIP:
        return decision

    if decision.action is UploadAction.RETRY_FAIL:
        # Estado desconhecido nunca autoriza escrita nem skip.
        raise IdentityNotConfirmed(f"{name}: {decision.detail}")

    upload()

    if not confirm_remote_identity(
        name, local, fetch_metadata=fetch_metadata, attempts=confirm_attempts, sleep=sleep
    ):
        raise IdentityNotConfirmed(
            f"{name}: upload concluído mas identidade remota não confirmada "
            f"(esperado size={local.size} sha1={local.sha1})"
        )

    return decision
