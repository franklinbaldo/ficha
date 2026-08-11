"""Sonda de throughput de PUT no Internet Archive — gate de #133 para o backfill de #110.

O run 31450937194 morreu no `timeout-minutes: 350` durante o upload de
`companies.zip` (21,8 GiB) a ~1,1 MiB/s. Nessa taxa o objeto sozinho levaria
5-8h e nenhum job do Actions consegue publicá-lo.

Este script mede a taxa real antes de gastar as ~3h de `pack`: sobe um objeto
de teste para o item real do mês, mede MiB/s sustentado e apaga o objeto. Se a
taxa não permitir concluir `companies.zip` dentro do orçamento restante do job,
sai com erro — falhar em 10 minutos é muito melhor que falhar em 4 horas.
"""

from __future__ import annotations

import os
import secrets
import sys
import time
from pathlib import Path

import internetarchive as ia

IDENTIFIER = "ficha-2026-05"
PROBE_NAME = "probe-throughput-110.bin"
PROBE_MIB = 512

# Tamanho real do companies.zip produzido pelo run 31450937194.
COMPANIES_MIB = 22349

# Piso de taxa para o upload caber no orçamento do job junto com download
# (~20 min), pack (~190 min observados) e fase de manifest (~30 min).
MIN_RATE_MIB_S = 5.0


def main() -> int:
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if not access_key or not secret_key:
        print("error: IA_ACCESS_KEY e IA_SECRET_KEY são obrigatórios", file=sys.stderr)
        return 1

    probe = Path(PROBE_NAME)
    # Bytes aleatórios: dados compressíveis mediriam a compressão, não a rede.
    print(f"gerando objeto de sonda de {PROBE_MIB} MiB…", flush=True)
    with probe.open("wb") as fh:
        for _ in range(PROBE_MIB):
            fh.write(secrets.token_bytes(1024 * 1024))

    try:
        print(f"subindo {PROBE_NAME} → ia:{IDENTIFIER}…", flush=True)
        started = time.monotonic()
        responses = ia.upload(
            IDENTIFIER,
            files={PROBE_NAME: str(probe)},
            access_key=access_key,
            secret_key=secret_key,
            retries=2,
            retries_sleep=15,
            verbose=True,
        )
        elapsed = time.monotonic() - started

        bad = [r for r in responses if r is not None and r.status_code not in (200, 201)]
        if bad:
            print(
                f"error: sonda rejeitada pelo IA: {[r.status_code for r in bad]}",
                file=sys.stderr,
            )
            return 1
    finally:
        probe.unlink(missing_ok=True)

    rate = PROBE_MIB / elapsed
    eta_min = COMPANIES_MIB / rate / 60
    print(
        f"\nthroughput medido: {rate:.2f} MiB/s "
        f"({PROBE_MIB} MiB em {elapsed:.1f}s)\n"
        f"ETA para companies.zip ({COMPANIES_MIB} MiB): {eta_min:.0f} min",
        flush=True,
    )

    # Limpeza: o objeto de sonda não faz parte do contrato do snapshot e não
    # deve sobrar no item público.
    try:
        item = ia.get_item(IDENTIFIER)
        item.get_file(PROBE_NAME).delete(
            access_key=access_key,
            secret_key=secret_key,
            cascade_delete=True,
        )
        print(f"objeto de sonda removido de ia:{IDENTIFIER}", flush=True)
    except Exception as exc:  # a limpeza não deve derrubar a medição
        print(f"warning: falha ao remover {PROBE_NAME}: {exc}", file=sys.stderr)

    if rate < MIN_RATE_MIB_S:
        print(
            f"\nerror: {rate:.2f} MiB/s < piso de {MIN_RATE_MIB_S} MiB/s.\n"
            f"       companies.zip levaria ~{eta_min:.0f} min e não cabe no job.\n"
            f"       Abortando antes do pack — ver #133 (uploader multipart/paralelo).",
            file=sys.stderr,
        )
        return 1

    print("\nOK — taxa suficiente, seguindo para download + pack.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
