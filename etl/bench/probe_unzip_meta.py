"""Probe: `_meta.json` remoto serve como identidade de materialização? (#147)

O desenho de retomada por shard depende de responder, **antes de gerar**, se um
shard remoto pertence aos inputs atuais. O mecanismo proposto é ler apenas o
membro `_meta.json` de dentro do ZIP remoto, pelo unzip transparente do IA.

Isso pressupõe três coisas que **não** estão medidas:

1. que ler um membro não baixe o ZIP inteiro;
2. que o membro fique legível logo após o upload, e não só depois da fila de
   `derive` — se depender de `derive`, a máquina real não é
   `CONFIRM → DURÁVEL` e sim `CONFIRM → WAIT → READ → DURÁVEL`;
3. que os casos degenerados (membro inexistente, ZIP truncado) falhem de forma
   distinguível de "ainda não indexado".

Se (2) falhar, o checkpoint imediato não existe e o desenho de #147 muda.

Segurança: item descartável, nome único por execução, nunca `ficha-YYYY-MM`.
Tudo é apagado no `finally`. Nenhuma escrita em produção.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile
from dataclasses import dataclass, field

import httpx
from internetarchive import get_item, get_session

ITEM = "ficha-probe-unzip-meta-147"
BASE = "https://archive.org/download"
META_MEMBRO = "_meta.json"


@dataclass
class Evento:
    momento: float
    o_que: str
    detalhe: dict = field(default_factory=dict)


EVENTOS: list[Evento] = []


def registra(t0: float, o_que: str, **detalhe) -> None:
    ev = Evento(round(time.time() - t0, 2), o_que, detalhe)
    EVENTOS.append(ev)
    print(f"[{ev.momento:8.2f}s] {o_que}: {json.dumps(detalhe, default=str)}", flush=True)


def zip_de_teste(*, membros: int, meta: dict) -> bytes:
    """ZIP com a mesma forma de um shard: membros + `_meta.json`."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for i in range(membros):
            zf.writestr(f"00/000/{i:03d}.pb", bytes(range(256)) * 8)
        zf.writestr(META_MEMBRO, json.dumps(meta, indent=2))
    return buf.getvalue()


def le_membro(nome_zip: str, membro: str, *, timeout: float = 60.0) -> tuple[int, int, float, str]:
    """GET de um único membro pelo unzip transparente. Devolve (status, bytes, s, corpo)."""
    url = f"{BASE}/{ITEM}/{nome_zip}/{membro}"
    t = time.time()
    try:
        r = httpx.get(url, follow_redirects=True, timeout=timeout)
        corpo = r.text[:200]
        return r.status_code, len(r.content), time.time() - t, corpo
    except httpx.HTTPError as exc:
        return -1, 0, time.time() - t, f"{type(exc).__name__}: {exc}"


def pending_tasks() -> bool | None:
    try:
        r = httpx.get(f"https://archive.org/metadata/{ITEM}", follow_redirects=True, timeout=30)
        return r.json().get("pending_tasks")
    except (httpx.HTTPError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--membros", type=int, default=2000, help="membros no ZIP de teste")
    ap.add_argument("--janela", type=int, default=900, help="segundos observando disponibilidade")
    args = ap.parse_args()

    access = os.environ["IA_ACCESS_KEY"]
    secret = os.environ["IA_SECRET_KEY"]
    sessao = get_session(config={"s3": {"access": access, "secret": secret}})

    identidade = {
        "packer_format_version": 1,
        "month": "2026-05",
        "shard": {"prefix": "00", "prefix_len": 2},
        "probe": True,
    }
    bom = zip_de_teste(membros=args.membros, meta=identidade)
    # ZIP truncado: os primeiros 60% dos bytes. O central directory fica no fim,
    # então o arquivo é irrecuperável — é o que um upload interrompido produz.
    truncado = bom[: int(len(bom) * 0.6)]

    t0 = time.time()
    registra(t0, "inicio", zip_bytes=len(bom), membros=args.membros)

    subidos: list[str] = []
    try:
        item = get_item(ITEM, archive_session=sessao)
        for nome, dados in (("bom.zip", bom), ("truncado.zip", truncado)):
            item.upload(
                {nome: io.BytesIO(dados)},
                access_key=access,
                secret_key=secret,
                retries=3,
                verify=True,
            )
            subidos.append(nome)
            registra(t0, "upload concluido", arquivo=nome, bytes=len(dados))

        registra(t0, "pending_tasks apos upload", valor=pending_tasks())

        # --- 2: quando o membro fica legivel? ---------------------------------
        primeira_leitura_ok: float | None = None
        espera = 5
        while time.time() - t0 < args.janela:
            status, n, dur, corpo = le_membro("bom.zip", META_MEMBRO)
            registra(
                t0,
                "GET _meta.json",
                status=status,
                bytes=n,
                segundos=round(dur, 2),
                pending=pending_tasks(),
            )
            if status == 200 and n:
                primeira_leitura_ok = time.time() - t0
                registra(t0, "MEMBRO LEGIVEL", apos_segundos=round(primeira_leitura_ok, 1))
                break
            time.sleep(espera)
            espera = min(espera * 2, 60)

        if primeira_leitura_ok is None:
            registra(t0, "MEMBRO NUNCA FICOU LEGIVEL", janela=args.janela)
        else:
            # --- 1: ler o membro baixa o ZIP inteiro? -------------------------
            status, n, dur, corpo = le_membro("bom.zip", META_MEMBRO)
            registra(
                t0,
                "custo de ler so o membro",
                bytes_do_membro=n,
                bytes_do_zip=len(bom),
                fracao=round(n / len(bom), 5),
                segundos=round(dur, 2),
                conteudo_bate=corpo.strip().startswith("{"),
            )

            # --- 3: casos degenerados ----------------------------------------
            registra(t0, "membro inexistente", resultado=le_membro("bom.zip", "_nao_existe.json"))
            registra(t0, "zip truncado", resultado=le_membro("truncado.zip", META_MEMBRO))
            registra(t0, "zip inexistente", resultado=le_membro("nao_existe.zip", META_MEMBRO))

        print("\n=== RESUMO ===", flush=True)
        print(json.dumps([ev.__dict__ for ev in EVENTOS], indent=2, default=str), flush=True)
        return 0
    finally:
        for nome in subidos:
            try:
                get_item(ITEM, archive_session=sessao).get_file(nome).delete(
                    access_key=access, secret_key=secret, cascade_delete=True
                )
                print(f"limpeza: {nome} removido", flush=True)
            except Exception as exc:  # noqa: BLE001 — limpeza é best-effort
                print(f"limpeza FALHOU para {nome}: {exc}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
