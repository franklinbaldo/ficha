"""Probe de semântica de overwrite no Internet Archive (#132 slice 2).

A leitura da fonte de `internetarchive.item.Item.upload_file` diz que não existe
flag de overwrite e que um PUT no mesmo key substitui o objeto. Isso é evidência
de código, não de execução — e `REPLACE` só deve virar contrato depois de
observado.

Sequência, registrando tudo:

    A: gera bytes, sha1/size, upload, metadata confirma A
    B: gera bytes **do mesmo tamanho** e sha1 diferente, upload no mesmo nome,
       metadata confirma B, medindo quanto tempo leva para convergir

A e B terem o mesmo tamanho é deliberado: exercita exatamente o caso em que
`size` sozinho diria "idêntico" e só o `sha1` detecta a troca.

Roda num item descartável — nunca no item público de uma competência — e apaga
os arquivos ao final. Não escreve em `ficha-YYYY-MM`: o item de `2026-05` está
recebendo `companies.zip` neste momento e não deve receber tarefas extras.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import sys
import time
from pathlib import Path

import internetarchive as ia

IDENTIFIER = "ficha-overwrite-probe-132"
FILE_NAME = "overwrite-probe.bin"
PAYLOAD_BYTES = 64 * 1024

CONFIRM_ATTEMPTS = 12
CONFIRM_BACKOFF_S = 5.0


def _sha1(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()  # noqa: S324 — fingerprint, não contrato


def _remote_identity(identifier: str, name: str) -> tuple[int | None, str | None]:
    item = ia.get_item(identifier)
    item.refresh()
    for entry in item.item_metadata.get("files", []):
        if entry.get("name") == name:
            raw = entry.get("size")
            return (int(raw) if raw is not None else None, entry.get("sha1"))
    return (None, None)


def _wait_for(identifier: str, name: str, *, size: int, sha1: str, label: str) -> float | None:
    """Espera o metadata refletir (size, sha1). Devolve segundos até convergir."""
    started = time.monotonic()
    for attempt in range(1, CONFIRM_ATTEMPTS + 1):
        remote_size, remote_sha1 = _remote_identity(identifier, name)
        elapsed = time.monotonic() - started
        print(
            f"  [{label}] tentativa {attempt}: remoto size={remote_size} sha1={remote_sha1}",
            flush=True,
        )
        if remote_size == size and remote_sha1 == sha1:
            print(f"  [{label}] convergiu em {elapsed:.1f}s", flush=True)
            return elapsed
        if attempt < CONFIRM_ATTEMPTS:
            time.sleep(CONFIRM_BACKOFF_S)
    print(f"  [{label}] NÃO convergiu após {CONFIRM_ATTEMPTS} leituras", file=sys.stderr)
    return None


def _upload(path: Path, *, access_key: str, secret_key: str) -> None:
    responses = ia.upload(
        IDENTIFIER,
        files={FILE_NAME: str(path)},
        metadata={
            "title": "FICHA — probe de semântica de overwrite (#132)",
            "mediatype": "data",
            "collection": "opensource",
        },
        access_key=access_key,
        secret_key=secret_key,
        retries=3,
        retries_sleep=10,
        verbose=True,
    )
    bad = [r for r in responses if r is not None and r.status_code not in (200, 201)]
    if bad:
        raise RuntimeError(f"upload rejeitado: {[r.status_code for r in bad]}")


def main() -> int:
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if not access_key or not secret_key:
        print("error: IA_ACCESS_KEY e IA_SECRET_KEY são obrigatórios", file=sys.stderr)
        return 1

    path = Path(FILE_NAME)
    results: dict[str, object] = {}

    try:
        # --- versão A -------------------------------------------------------
        payload_a = secrets.token_bytes(PAYLOAD_BYTES)
        sha1_a = _sha1(payload_a)
        path.write_bytes(payload_a)
        print(f"A: size={len(payload_a)} sha1={sha1_a}", flush=True)

        _upload(path, access_key=access_key, secret_key=secret_key)
        results["convergencia_a_s"] = _wait_for(
            IDENTIFIER, FILE_NAME, size=len(payload_a), sha1=sha1_a, label="A"
        )
        if results["convergencia_a_s"] is None:
            print("error: metadata nunca confirmou A", file=sys.stderr)
            return 1

        # --- versão B: MESMO tamanho, sha1 diferente ------------------------
        payload_b = secrets.token_bytes(PAYLOAD_BYTES)
        sha1_b = _sha1(payload_b)
        assert len(payload_b) == len(payload_a), "B deve ter o mesmo tamanho de A"
        assert sha1_b != sha1_a
        path.write_bytes(payload_b)
        print(f"\nB: size={len(payload_b)} sha1={sha1_b} (mesmo size de A)", flush=True)

        _upload(path, access_key=access_key, secret_key=secret_key)
        results["convergencia_b_s"] = _wait_for(
            IDENTIFIER, FILE_NAME, size=len(payload_b), sha1=sha1_b, label="B"
        )
        if results["convergencia_b_s"] is None:
            print(
                "error: PUT no mesmo nome NÃO substituiu o objeto dentro da janela observada.\n"
                "       REPLACE não pode ser codificado como contrato com esta semântica.",
                file=sys.stderr,
            )
            return 1

        # --- estado final: nenhuma duplicata -------------------------------
        item = ia.get_item(IDENTIFIER)
        item.refresh()
        originais = [
            entry["name"]
            for entry in item.item_metadata.get("files", [])
            if entry.get("source") == "original"
        ]
        print(f"\narquivos 'original' no item: {originais}", flush=True)
        if originais.count(FILE_NAME) != 1:
            print(f"error: esperado exatamente 1 {FILE_NAME}, achei {originais}", file=sys.stderr)
            return 1

        print(
            "\nOK — PUT no mesmo nome substituiu o objeto; sem duplicata.\n"
            f"convergência A: {results['convergencia_a_s']:.1f}s · "
            f"B: {results['convergencia_b_s']:.1f}s",
            flush=True,
        )
        return 0
    finally:
        path.unlink(missing_ok=True)
        try:
            item = ia.get_item(IDENTIFIER)
            item.get_file(FILE_NAME).delete(
                access_key=access_key, secret_key=secret_key, cascade_delete=True
            )
            print(f"objeto de probe removido de ia:{IDENTIFIER}", flush=True)
        except Exception as exc:  # limpeza não deve derrubar a medição
            print(f"warning: falha ao remover {FILE_NAME}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
