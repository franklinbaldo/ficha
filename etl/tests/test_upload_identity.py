"""Testes do primitivo de upload idempotente (#132 slice 2).

Sem rede: uploader e leitura de metadata são injetados. O SHA-1 é calculado
sobre arquivos reais em `tmp_path`, inclusive com chunk menor que o arquivo,
para provar o streaming.
"""

from __future__ import annotations

import hashlib

import pytest

from ficha_etl.upload_identity import (
    IdentityNotConfirmed,
    RemoteIdentityState,
    UploadAction,
    confirm_remote_identity,
    decide,
    ensure_uploaded,
    local_identity,
    sha1_of_file,
)

NAME = "cnpjs.parquet"


def _file(tmp_path, content: bytes):
    path = tmp_path / NAME
    path.write_bytes(content)
    return path


def _metadata(*entries: dict) -> dict:
    return {"files": list(entries)}


def _entry(content: bytes, *, name: str = NAME) -> dict:
    return {
        "name": name,
        "size": str(len(content)),
        "sha1": hashlib.sha1(content).hexdigest(),  # noqa: S324
    }


# --- SHA-1 local em streaming ------------------------------------------------


def test_sha1_matches_hashlib(tmp_path) -> None:
    content = b"conteudo qualquer"
    path = _file(tmp_path, content)
    assert sha1_of_file(path) == hashlib.sha1(content).hexdigest()  # noqa: S324


def test_sha1_is_computed_in_chunks(tmp_path) -> None:
    """Chunk menor que o arquivo: prova que não depende de ler tudo de uma vez."""
    content = b"x" * 100_000
    path = _file(tmp_path, content)
    assert sha1_of_file(path, chunk_size=997) == hashlib.sha1(content).hexdigest()  # noqa: S324


def test_local_identity_reports_size_and_sha1(tmp_path) -> None:
    content = b"abc" * 10
    identity = local_identity(_file(tmp_path, content))
    assert identity.size == len(content)
    assert identity.sha1 == hashlib.sha1(content).hexdigest()  # noqa: S324


# --- classificação -----------------------------------------------------------


def test_missing_remote_leads_to_upload(tmp_path) -> None:
    local = local_identity(_file(tmp_path, b"dados"))
    decision = decide(NAME, local, _metadata())
    assert decision.state is RemoteIdentityState.MISSING
    assert decision.action is UploadAction.UPLOAD


def test_identical_remote_leads_to_skip(tmp_path) -> None:
    content = b"dados"
    local = local_identity(_file(tmp_path, content))
    decision = decide(NAME, local, _metadata(_entry(content)))
    assert decision.state is RemoteIdentityState.IDENTICAL
    assert decision.action is UploadAction.SKIP


def test_different_size_is_mismatch(tmp_path) -> None:
    local = local_identity(_file(tmp_path, b"dados completos"))
    entry = _entry(b"dados truncados demais")
    decision = decide(NAME, local, _metadata(entry))
    assert decision.state is RemoteIdentityState.MISMATCH
    assert decision.action is UploadAction.REPLACE


def test_same_size_different_sha1_is_mismatch(tmp_path) -> None:
    """Tamanho igual não é identidade — é exatamente onde o sha1 ganha o dia."""
    local = local_identity(_file(tmp_path, b"AAAAA"))
    entry = _entry(b"BBBBB")
    assert entry["size"] == "5"
    decision = decide(NAME, local, _metadata(entry))
    assert decision.state is RemoteIdentityState.MISMATCH
    assert decision.action is UploadAction.REPLACE


def test_metadata_without_sha1_is_unavailable_not_mismatch(tmp_path) -> None:
    """Sem identidade comparável o estado é desconhecido, não divergente."""
    content = b"dados"
    local = local_identity(_file(tmp_path, content))
    decision = decide(NAME, local, _metadata({"name": NAME, "size": str(len(content))}))
    assert decision.state is RemoteIdentityState.UNAVAILABLE
    assert decision.action is UploadAction.RETRY_FAIL


def test_metadata_unavailable_is_never_skip_and_never_replace(tmp_path) -> None:
    local = local_identity(_file(tmp_path, b"dados"))
    decision = decide(NAME, local, None)
    assert decision.state is RemoteIdentityState.UNAVAILABLE
    assert decision.action is UploadAction.RETRY_FAIL
    assert decision.action is not UploadAction.SKIP
    assert decision.action is not UploadAction.REPLACE


# --- pós-condição ------------------------------------------------------------


def test_upload_is_confirmed_by_rereading_metadata(tmp_path) -> None:
    content = b"dados"
    path = _file(tmp_path, content)
    states = [_metadata(), _metadata(_entry(content))]
    uploaded: list[str] = []

    decision = ensure_uploaded(
        NAME,
        path,
        upload=lambda: uploaded.append("put"),
        fetch_metadata=lambda: states.pop(0) if states else _metadata(_entry(content)),
        sleep=lambda _: None,
    )

    assert decision.action is UploadAction.UPLOAD
    assert uploaded == ["put"]


def test_stale_metadata_is_retried_until_it_catches_up(tmp_path) -> None:
    """Metadata do IA fica atrás do PUT: 'ainda não bateu' != 'não bate'."""
    content = b"dados"
    path = _file(tmp_path, content)
    old = _entry(b"versao antiga diferente")
    sequence = [_metadata(old), _metadata(old), _metadata(old), _metadata(_entry(content))]
    slept: list[float] = []

    decision = ensure_uploaded(
        NAME,
        path,
        upload=lambda: None,
        fetch_metadata=lambda: sequence.pop(0),
        sleep=slept.append,
    )

    assert decision.action is UploadAction.REPLACE
    assert slept, "deveria ter esperado o metadata alcançar o PUT"


def test_upload_that_never_converges_raises(tmp_path) -> None:
    """O retorno do uploader não é prova de durabilidade."""
    content = b"dados"
    path = _file(tmp_path, content)
    stale = _metadata(_entry(b"outros bytes"))

    with pytest.raises(IdentityNotConfirmed, match="não confirmada"):
        ensure_uploaded(
            NAME,
            path,
            upload=lambda: None,
            fetch_metadata=lambda: stale,
            confirm_attempts=3,
            sleep=lambda _: None,
        )


def test_replace_converges_to_the_new_identity(tmp_path) -> None:
    content = b"conteudo novo e correto"
    path = _file(tmp_path, content)
    remote = {"value": _metadata(_entry(b"conteudo velho"))}

    def upload() -> None:
        remote["value"] = _metadata(_entry(content))

    decision = ensure_uploaded(
        NAME,
        path,
        upload=upload,
        fetch_metadata=lambda: remote["value"],
        sleep=lambda _: None,
    )

    assert decision.state is RemoteIdentityState.MISMATCH
    assert decision.action is UploadAction.REPLACE


def test_skip_does_not_call_the_uploader(tmp_path) -> None:
    content = b"dados"
    path = _file(tmp_path, content)

    def upload() -> None:
        raise AssertionError("SKIP não pode escrever no remoto")

    decision = ensure_uploaded(
        NAME,
        path,
        upload=upload,
        fetch_metadata=lambda: _metadata(_entry(content)),
        sleep=lambda _: None,
    )
    assert decision.action is UploadAction.SKIP


def test_unavailable_metadata_never_triggers_an_upload(tmp_path) -> None:
    """Não sobrescrever objeto remoto por falha momentânea de observabilidade."""
    path = _file(tmp_path, b"dados")

    def upload() -> None:
        raise AssertionError("estado desconhecido não autoriza escrita")

    with pytest.raises(IdentityNotConfirmed):
        ensure_uploaded(
            NAME, path, upload=upload, fetch_metadata=lambda: None, sleep=lambda _: None
        )


def test_confirm_remote_identity_reports_failure_without_raising(tmp_path) -> None:
    local = local_identity(_file(tmp_path, b"dados"))
    assert not confirm_remote_identity(
        NAME, local, fetch_metadata=lambda: _metadata(), attempts=2, sleep=lambda _: None
    )


# --- observabilidade ---------------------------------------------------------


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (UploadAction.UPLOAD, "UPLOAD  missing"),
        (UploadAction.SKIP, "SKIP    identical"),
        (UploadAction.REPLACE, "REPLACE mismatch"),
        (UploadAction.RETRY_FAIL, "RETRY/FAIL metadata unavailable"),
    ],
)
def test_log_line_distinguishes_every_action(tmp_path, action, expected) -> None:
    from ficha_etl.upload_identity import IdentityDecision

    decision = IdentityDecision(NAME, RemoteIdentityState.MISSING, action, "detalhe")
    assert expected in decision.log_line()
    assert NAME in decision.log_line()


def test_mismatch_log_carries_both_identities(tmp_path) -> None:
    """Divergência precisa ser auditável, não só corrigida."""
    local = local_identity(_file(tmp_path, b"AAAAA"))
    decision = decide(NAME, local, _metadata(_entry(b"BBBBB")))
    assert local.sha1 in decision.detail
    assert hashlib.sha1(b"BBBBB").hexdigest() in decision.detail  # noqa: S324


# --- ausência observada vs ausência de observação ----------------------------
# Só ausência OBSERVADA autoriza UPLOAD. Metadata estruturalmente incompleto não
# é evidência de ausência, e autorizar escrita nele seria escrever por falta de
# informação — o oposto da regra do slice.


@pytest.mark.parametrize(
    ("metadata", "rotulo"),
    [
        ({}, "objeto vazio"),
        ({"metadata": {"identifier": "ficha-2026-05"}}, "sem chave files"),
        ({"files": None}, "files=None"),
        ({"files": "cnpjs.parquet"}, "files não-lista (str)"),
        ({"files": {"name": NAME}}, "files não-lista (dict)"),
        ({"files": ["cnpjs.parquet"]}, "files com entradas não-dict"),
    ],
)
def test_structurally_incomplete_metadata_is_unavailable(tmp_path, metadata, rotulo) -> None:
    local = local_identity(_file(tmp_path, b"dados"))
    decision = decide(NAME, local, metadata)
    assert decision.state is RemoteIdentityState.UNAVAILABLE, rotulo
    assert decision.action is UploadAction.RETRY_FAIL, rotulo


def test_empty_but_valid_files_list_is_missing(tmp_path) -> None:
    """Lista válida e vazia é ausência observada — aí UPLOAD é correto."""
    local = local_identity(_file(tmp_path, b"dados"))
    decision = decide(NAME, local, {"files": []})
    assert decision.state is RemoteIdentityState.MISSING
    assert decision.action is UploadAction.UPLOAD


def test_valid_list_without_the_target_is_missing(tmp_path) -> None:
    local = local_identity(_file(tmp_path, b"dados"))
    metadata = _metadata(
        _entry(b"outro", name="raizes.parquet"), _entry(b"x", name="socios.parquet")
    )
    decision = decide(NAME, local, metadata)
    assert decision.state is RemoteIdentityState.MISSING
    assert decision.action is UploadAction.UPLOAD


def test_incomplete_metadata_never_triggers_a_write(tmp_path) -> None:
    """Fecha a brecha: metadata parcial não pode virar UPLOAD."""
    path = _file(tmp_path, b"dados")

    def upload() -> None:
        raise AssertionError("ausência não observada não autoriza escrita")

    for metadata in ({}, {"metadata": {}}, {"files": None}, {"files": 42}):
        with pytest.raises(IdentityNotConfirmed):
            ensure_uploaded(
                NAME, path, upload=upload, fetch_metadata=lambda m=metadata: m, sleep=lambda _: None
            )
