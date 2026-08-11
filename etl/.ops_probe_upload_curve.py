"""Curva de throughput de upload no Internet Archive por tamanho de objeto (#133).

Decide a arquitetura de #110/#136: o upload de `companies.zip` (21,8 GiB) roda a
~1,6 MiB/s sustentados, enquanto uma sonda de 512 MiB no mesmo item deu
>28 MiB/s. Se a queda for função do tamanho do objeto, shardar o artefato é a
correção; se não for, sharding não resolve o transporte e o gargalo é outro.

Mede duas coisas, em item descartável:

  1. varredura sequencial de tamanhos — 64 MiB, 256 MiB, 1 GiB, 2 GiB;
  2. concorrência — 4 x 256 MiB simultâneos, para saber se a banda agregada
     escala ou se o limite é por conta/item.

Nenhum objeto é sobrescrito: cada PUT usa nome único. Os objetos são apagados
ao final.
"""

from __future__ import annotations

import os
import secrets
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import internetarchive as ia

IDENTIFIER = "ficha-upload-curve-133"
SEQUENTIAL_MIB = (64, 256, 1024, 2048)
PARALLEL_MIB = 256
PARALLEL_N = 4


def _make(path: Path, mib: int) -> None:
    """Bytes incompressíveis — mede a rede, não a compressão."""
    with path.open("wb") as fh:
        for _ in range(mib):
            fh.write(secrets.token_bytes(1024 * 1024))


def _upload(name: str, path: Path, access_key: str, secret_key: str) -> float:
    started = time.monotonic()
    responses = ia.upload(
        IDENTIFIER,
        files={name: str(path)},
        metadata={
            "title": "FICHA — curva de throughput de upload (#133)",
            "mediatype": "data",
            "collection": "opensource",
        },
        access_key=access_key,
        secret_key=secret_key,
        retries=2,
        retries_sleep=10,
        verbose=False,
    )
    elapsed = time.monotonic() - started
    bad = [r for r in responses if r is not None and r.status_code not in (200, 201)]
    if bad:
        raise RuntimeError(f"{name}: PUT rejeitado {[r.status_code for r in bad]}")
    return elapsed


def main() -> int:
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if not access_key or not secret_key:
        print("error: IA_ACCESS_KEY e IA_SECRET_KEY são obrigatórios", file=sys.stderr)
        return 1

    criados: list[str] = []
    try:
        print("=== 1. varredura sequencial de tamanhos ===", flush=True)
        print(f"{'tamanho':>10} {'tempo':>10} {'MiB/s':>9}", flush=True)
        for mib in SEQUENTIAL_MIB:
            name = f"seq-{mib}mib.bin"
            path = Path(name)
            _make(path, mib)
            try:
                elapsed = _upload(name, path, access_key, secret_key)
                criados.append(name)
                print(f"{mib:>7} MiB {elapsed:>9.1f}s {mib / elapsed:>9.2f}", flush=True)
            finally:
                path.unlink(missing_ok=True)

        print(f"\n=== 2. concorrência: {PARALLEL_N} x {PARALLEL_MIB} MiB ===", flush=True)
        paths = []
        for i in range(PARALLEL_N):
            name = f"par-{i}-{PARALLEL_MIB}mib.bin"
            path = Path(name)
            _make(path, PARALLEL_MIB)
            paths.append((name, path))

        started = time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=PARALLEL_N) as pool:
                futures = [
                    pool.submit(_upload, name, path, access_key, secret_key)
                    for name, path in paths
                ]
                individuais = [f.result() for f in futures]
            parede = time.monotonic() - started
            criados.extend(name for name, _ in paths)
            total = PARALLEL_MIB * PARALLEL_N
            print(f"tempos individuais: {[f'{d:.1f}s' for d in individuais]}", flush=True)
            print(f"parede: {parede:.1f}s para {total} MiB", flush=True)
            print(f"AGREGADO: {total / parede:.2f} MiB/s", flush=True)
        finally:
            for _, path in paths:
                path.unlink(missing_ok=True)

        return 0
    finally:
        print("\n=== limpeza ===", flush=True)
        item = ia.get_item(IDENTIFIER)
        for name in criados:
            try:
                item.get_file(name).delete(
                    access_key=access_key, secret_key=secret_key, cascade_delete=True
                )
                print(f"removido {name}", flush=True)
            except Exception as exc:
                print(f"warning: falha ao remover {name}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
