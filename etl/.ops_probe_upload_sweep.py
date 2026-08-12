"""Varredura controlada de throughput de upload no Internet Archive (#159).

Decide a geometria de shard de #147 sem tocar o item de produção. O ponto de
64 MiB de #146 foi o primeiro PUT do item e, portanto, não separa tamanho de
objeto de custo de criação/aquecimento. Este probe faz um warm-up explícito,
exclui-o da amostra e mede 32/64/128/256/320/440 MiB em duas ordens opostas.

A geração dos bytes acontece fora do cronômetro. Todos os PUTs são sequenciais,
com nomes únicos por run; nada é sobrescrito. A limpeza é best-effort e não é
tratada como evidência de throughput.
"""

from __future__ import annotations

import json
import os
import secrets
import statistics
import sys
import time
from pathlib import Path

import internetarchive as ia

IDENTIFIER = "ficha-upload-sweep-159"
SIZES_MIB = (32, 64, 128, 256, 320, 440)
WARMUP_MIB = 64
RESULTS_PATH = Path("upload-sweep-results.json")


def _make(path: Path, mib: int) -> None:
    """Gera bytes incompressíveis; o custo fica fora do cronômetro do PUT."""
    with path.open("wb") as fh:
        for _ in range(mib):
            fh.write(secrets.token_bytes(1024 * 1024))


def _upload(name: str, path: Path, access_key: str, secret_key: str) -> float:
    started = time.monotonic()
    responses = ia.upload(
        IDENTIFIER,
        files={name: str(path)},
        metadata={
            "title": "FICHA — sweep controlado de upload (#159)",
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


def _cleanup(names: list[str], access_key: str, secret_key: str) -> None:
    if not names:
        return
    print("\n=== limpeza best-effort ===", flush=True)
    item = ia.get_item(IDENTIFIER)
    for name in names:
        try:
            item.get_file(name).delete(
                access_key=access_key,
                secret_key=secret_key,
                cascade_delete=True,
            )
            print(f"removido {name}", flush=True)
        except Exception as exc:
            print(f"warning: falha ao remover {name}: {exc}", file=sys.stderr)


def main() -> int:
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if not access_key or not secret_key:
        print("error: IA_ACCESS_KEY e IA_SECRET_KEY são obrigatórios", file=sys.stderr)
        return 1

    run_token = os.environ.get("GITHUB_RUN_ID") or secrets.token_hex(6)
    created: list[str] = []
    observations: list[dict[str, float | int | str]] = []

    try:
        print("=== warm-up explícito — excluído da amostra ===", flush=True)
        warm_name = f"{run_token}-warmup-{WARMUP_MIB}mib.bin"
        warm_path = Path(warm_name)
        _make(warm_path, WARMUP_MIB)
        try:
            warm_elapsed = _upload(warm_name, warm_path, access_key, secret_key)
            created.append(warm_name)
            print(
                f"warm-up {WARMUP_MIB} MiB: {warm_elapsed:.1f}s "
                f"({WARMUP_MIB / warm_elapsed:.2f} MiB/s) — EXCLUÍDO",
                flush=True,
            )
        finally:
            warm_path.unlink(missing_ok=True)

        passes = (SIZES_MIB, tuple(reversed(SIZES_MIB)))
        for pass_index, sizes in enumerate(passes, start=1):
            direction = "asc" if pass_index == 1 else "desc"
            print(f"\n=== passagem {pass_index} ({direction}) ===", flush=True)
            print(f"{'tamanho':>10} {'tempo':>10} {'MiB/s':>9}", flush=True)
            for sequence, mib in enumerate(sizes, start=1):
                name = f"{run_token}-p{pass_index}-{sequence}-{mib}mib.bin"
                path = Path(name)
                _make(path, mib)
                try:
                    elapsed = _upload(name, path, access_key, secret_key)
                    created.append(name)
                    rate = mib / elapsed
                    observations.append(
                        {
                            "pass": pass_index,
                            "direction": direction,
                            "sequence": sequence,
                            "mib": mib,
                            "elapsed_s": elapsed,
                            "mib_s": rate,
                        }
                    )
                    print(f"{mib:>7} MiB {elapsed:>9.1f}s {rate:>9.2f}", flush=True)
                finally:
                    path.unlink(missing_ok=True)

        summary = []
        for mib in SIZES_MIB:
            rates = [float(o["mib_s"]) for o in observations if o["mib"] == mib]
            elapsed = [float(o["elapsed_s"]) for o in observations if o["mib"] == mib]
            if len(rates) != 2:
                raise RuntimeError(f"{mib} MiB: esperado 2 observações, obtido {len(rates)}")
            summary.append(
                {
                    "mib": mib,
                    "median_mib_s": statistics.median(rates),
                    "rates_mib_s": rates,
                    "elapsed_s": elapsed,
                }
            )

        result = {
            "experiment": 159,
            "identifier": IDENTIFIER,
            "run_token": run_token,
            "warmup_mib": WARMUP_MIB,
            "warmup_elapsed_s": warm_elapsed,
            "warmup_mib_s": WARMUP_MIB / warm_elapsed,
            "observations": observations,
            "summary": summary,
        }
        RESULTS_PATH.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")

        print("\n=== mediana das duas passagens ===", flush=True)
        print(f"{'tamanho':>10} {'mediana MiB/s':>15} {'obs.':>22}", flush=True)
        for row in summary:
            rates = row["rates_mib_s"]
            print(
                f"{row['mib']:>7} MiB {row['median_mib_s']:>15.2f} "
                f"{rates[0]:>9.2f}, {rates[1]:>9.2f}",
                flush=True,
            )
        return 0
    finally:
        _cleanup(created, access_key, secret_key)


if __name__ == "__main__":
    sys.exit(main())
