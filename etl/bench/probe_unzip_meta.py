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

## Duas fases, e por quê

O primeiro run (`31558838717`) confundiu três coisas diferentes: **criação do
item**, **quietude do item** e **legibilidade do membro**. O gate leu
`pending_tasks=None` e concluiu "quieto", quando `None` ali significava *"o
item não existe"* — e como o primeiro PUT também cria o item, T1 e T2 sairiam
contaminados pela latência de bootstrap.

Por isso o probe é explicitamente bifásico:

- **preparação** — garante que o item exista, esteja identificável, esteja
  quieto e esteja limpo. Nada aqui é cronometrado.
- **medição** — só então T0 começa, e o que se mede é o comportamento de um
  **objeto novo dentro de um item já existente**, que é a situação real de um
  shard sendo publicado.

Segurança: item descartável, nunca `ficha-YYYY-MM`. Os objetos de payload são
apagados no `finally`. Nenhuma escrita em produção.
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
from requests.exceptions import HTTPError

ITEM = "ficha-probe-unzip-meta-147"
BASE = "https://archive.org/download"
META_MEMBRO = "_meta.json"
PAYLOAD = ("bom.zip", "truncado.zip")
MARCADOR = "_probe_marker.txt"
"""Arquivo mínimo que faz o item existir.

Criar o item é pré-condição, não medição. O marcador **não** é apagado no
`finally`: ele mantém o item existindo entre execuções, para que runs futuros
não paguem de novo o bootstrap — que é exatamente o custo que contaminaria T1.
"""


@dataclass
class Evento:
    momento: float
    o_que: str
    detalhe: dict = field(default_factory=dict)


EVENTOS: list[Evento] = []


def registra(t0: float | None, o_que: str, **detalhe) -> None:
    momento = round(time.time() - t0, 2) if t0 is not None else -1.0
    EVENTOS.append(Evento(momento, o_que, detalhe))
    prefixo = f"[{momento:8.2f}s]" if t0 is not None else "[  prep  ]"
    print(f"{prefixo} {o_que}: {json.dumps(detalhe, default=str)}", flush=True)


# ---- leitura remota ---------------------------------------------------------


def metadata() -> dict | None:
    """Metadata do item, ou `None` se a leitura não for confiável.

    `None` aqui significa **não sei**, e nunca deve ser lido como "quieto" —
    foi exatamente essa confusão que invalidou o primeiro run.
    """
    try:
        r = httpx.get(f"https://archive.org/metadata/{ITEM}", follow_redirects=True, timeout=30)
        r.raise_for_status()
        return r.json()
    except (httpx.HTTPError, ValueError):
        return None


def le_membro(nome_zip: str, membro: str, *, timeout: float = 60.0) -> tuple[int, int, float, str]:
    """GET de um único membro pelo unzip transparente. Devolve (status, bytes, s, corpo)."""
    url = f"{BASE}/{ITEM}/{nome_zip}/{membro}"
    t = time.time()
    try:
        r = httpx.get(url, follow_redirects=True, timeout=timeout)
        return r.status_code, len(r.content), time.time() - t, r.text[:200]
    except httpx.HTTPError as exc:
        return -1, 0, time.time() - t, f"{type(exc).__name__}: {exc}"


def estado_remoto(nome_zip: str) -> dict:
    """Os eventos observados de forma independente, num único instante.

    `metadata do arquivo visível` e `membro legível` são duas observações
    distintas: a primeira **não** implica a segunda, e descobrir qual delas
    autoriza `DURÁVEL PARA REUSE` é a pergunta do probe.
    """
    md = metadata() or {}
    arquivos = md.get("files")
    entrada = None
    if isinstance(arquivos, list):
        entrada = next((f for f in arquivos if f.get("name") == nome_zip), None)
    status, n, dur, _ = le_membro(nome_zip, META_MEMBRO)
    return {
        "metadata_do_arquivo_visivel": entrada is not None,
        "size_declarado": (entrada or {}).get("size"),
        "sha1_declarado": (entrada or {}).get("sha1"),
        "membro_legivel": status == 200 and n > 0,
        "membro_status": status,
        "membro_bytes": n,
        "membro_segundos": round(dur, 2),
        "pending_tasks": md.get("pending_tasks"),
    }


# ---- escrita ----------------------------------------------------------------


def apaga(sessao, nome: str) -> None:
    """Apaga um objeto do item, com o identifier declarado explicitamente.

    `File.identifier` é lido de `item_metadata["metadata"]["identifier"]`
    (internetarchive 5.8.0, `files.py:80`). Num item recém-criado o metadata
    ainda é `{}`, o identifier sai `None`, e o DELETE vai parar em
    `https://s3.us.archive.org/None/<nome>` — 403, que foi o que aconteceu no
    run `31558838717`. Fixar o identifier a partir da constante torna a limpeza
    imune à latência do metadata.
    """
    arquivo = get_item(ITEM, archive_session=sessao).get_file(nome)
    arquivo.identifier = ITEM
    arquivo.delete(
        access_key=os.environ["IA_ACCESS_KEY"],
        secret_key=os.environ["IA_SECRET_KEY"],
        cascade_delete=True,
    )


def sobe(sessao, nome: str, dados: bytes) -> tuple[bool, dict]:
    """PUT de um objeto. A rejeição é resultado, não exceção a propagar."""
    try:
        get_item(ITEM, archive_session=sessao).upload(
            {nome: io.BytesIO(dados)},
            access_key=os.environ["IA_ACCESS_KEY"],
            secret_key=os.environ["IA_SECRET_KEY"],
            retries=3,
            verify=True,
        )
    except HTTPError as exc:
        resp = getattr(exc, "response", None)
        return False, {"status": getattr(resp, "status_code", None), "motivo": str(exc)[:300]}
    return True, {}


# ---- fase 1: preparação (não cronometrada) ----------------------------------


def precondicao(md: dict | None) -> tuple[bool, str]:
    """As condições que tornam o item mensurável.

    `ABSENT` é estado próprio: a ausência do item não é quietude.
    """
    if md is None:
        return False, "metadata ilegível"
    if not md:
        return False, "ABSENT — o item não existe"
    if md.get("metadata", {}).get("identifier") != ITEM:
        return False, f"identifier inesperado: {md.get('metadata', {}).get('identifier')!r}"
    if not isinstance(md.get("files"), list):
        return False, "files não é uma lista válida"
    if md.get("pending_tasks") is True:
        return False, "pending_tasks=True"
    residuo = [f.get("name") for f in md["files"] if f.get("name") in PAYLOAD]
    if residuo:
        return False, f"resíduo de payload presente: {residuo}"
    return True, "ok"


def _residuo(md: dict | None) -> list[str]:
    if not md or not isinstance(md.get("files"), list):
        return []
    return [f.get("name") for f in md["files"] if f.get("name") in PAYLOAD]


def prepara(sessao, *, limite: float) -> bool:
    """Deixa o item existente, identificável e quieto — sem curar resíduo.

    Resolve **bootstrap** e **consistência eventual**, que são estados normais:
    o item pode não existir ainda, e o metadata pode demorar a refletir a
    realidade. Exige duas leituras consecutivas satisfeitas, porque uma leitura
    isolada pode pegar uma janela transitória.

    O que ela **não** faz é apagar payload residual. Um `bom.zip` ou
    `truncado.zip` sobrando é evidência de que a execução anterior não fechou
    corretamente — quase sempre uma falha de cleanup. Se cada run apagasse isso
    em silêncio, uma falha recorrente de limpeza ficaria invisível justamente
    porque o probe a estaria curando. Então aqui a resposta é **falhar fechado**
    e dizer quais objetos existem; remover é ato deliberado, via
    `--reparar-residuo`.
    """
    fim = time.time() + limite
    estaveis = 0
    criou = False

    while time.time() < fim:
        md = metadata()

        # Falha fechada e imediata: esperar não resolve resíduo, e insistir até
        # o timeout só esconderia a causa atrás de "precondição não atingida".
        sobras = _residuo(md)
        if sobras:
            registra(
                None,
                "ABORTANDO: payload residual de execucao anterior",
                objetos=sobras,
                acao="rode com --reparar-residuo para remover deliberadamente",
            )
            return False

        ok, motivo = precondicao(md)
        if ok:
            estaveis += 1
            registra(None, "precondicao satisfeita", leituras_estaveis=estaveis)
            if estaveis >= 2:
                return True
            time.sleep(10)
            continue

        estaveis = 0
        registra(None, "precondicao pendente", motivo=motivo)

        if md is not None and not md and not criou:
            # Bootstrap é preparação legítima: o item precisa existir ANTES de
            # T0, senão sua criação entra na latência do primeiro objeto.
            ok_put, det = sobe(sessao, MARCADOR, b"probe #147 - marcador de existencia\n")
            registra(None, "criando o item pelo marcador", sucesso=ok_put, **det)
            if not ok_put:
                return False
            criou = True

        time.sleep(15)

    registra(None, "PRECONDICAO NAO ATINGIDA", limite_segundos=limite)
    return False


def repara_residuo(sessao, *, limite: float) -> bool:
    """Remove payload residual de uma execução anterior. **Ato deliberado.**

    Separado de `prepara()` de propósito: apagar resíduo é reparação de uma
    falha passada, não preparação de um experimento novo. Exige pedido
    explícito, apaga com identifier explícito, confirma a ausência por polling e
    espera `pending_tasks` estabilizar.
    """
    md = metadata()
    sobras = _residuo(md)
    if not sobras:
        registra(
            None, "nada a reparar", arquivos=[f.get("name") for f in (md or {}).get("files", [])]
        )
        return True

    registra(None, "REPARANDO residuo", objetos=sobras)
    for nome in sobras:
        try:
            apaga(sessao, nome)
            registra(None, "DELETE emitido", arquivo=nome)
        except Exception as exc:  # noqa: BLE001
            registra(None, "DELETE FALHOU", arquivo=nome, erro=str(exc)[:300])
            return False

    fim = time.time() + limite
    while time.time() < fim:
        md = metadata()
        sobras = _residuo(md)
        quieto = md is not None and md and md.get("pending_tasks") is not True
        registra(
            None,
            "aguardando ausencia",
            residuo=sobras,
            pending_tasks=(md or {}).get("pending_tasks"),
        )
        if not sobras and quieto:
            registra(None, "REPARO CONFIRMADO — item limpo e quieto")
            return True
        time.sleep(20)

    registra(None, "REPARO NAO CONFIRMADO no limite", limite_segundos=limite)
    return False


# ---- fase 2: medição --------------------------------------------------------


def zip_de_teste(*, membros: int, meta: dict) -> bytes:
    """ZIP com a mesma forma de um shard: membros + `_meta.json`."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for i in range(membros):
            zf.writestr(f"00/000/{i:03d}.pb", bytes(range(256)) * 8)
        zf.writestr(META_MEMBRO, json.dumps(meta, indent=2))
    return buf.getvalue()


def mede(sessao, *, membros: int, janela: int) -> int:
    identidade = {
        "packer_format_version": 1,
        "month": "2026-05",
        "shard": {"prefix": "00", "prefix_len": 2},
        "probe": True,
    }
    bom = zip_de_teste(membros=membros, meta=identidade)
    # ZIP truncado: os primeiros 60% dos bytes. O central directory fica no fim,
    # então o arquivo é irrecuperável — é o que um upload interrompido produz.
    truncado = bom[: int(len(bom) * 0.6)]

    truncado_estado = "nao_testado"
    truncado_detalhe: dict = {}
    subidos: list[str] = []

    t0 = time.time()
    registra(t0, "T0 — inicio da medicao", zip_bytes=len(bom), membros=membros)

    try:
        ok, det = sobe(sessao, "bom.zip", bom)
        if not ok:
            registra(t0, "ABORTANDO: PUT de bom.zip rejeitado", **det)
            return 1
        subidos.append("bom.zip")
        registra(t0, "T0 — PUT de bom.zip aceito", bytes=len(bom))

        # O truncado é caso degenerado e NÃO pode bloquear a medição principal.
        # A rejeição no PUT já é resultado empírico: um ZIP truncado não se passa
        # silenciosamente por objeto válido no caminho testado.
        ok_t, det_t = sobe(sessao, "truncado.zip", truncado)
        if ok_t:
            subidos.append("truncado.zip")
            truncado_estado = "aceito_no_put"
            registra(t0, "T0b — PUT de truncado.zip aceito", bytes=len(truncado))
        else:
            truncado_estado = "rejeitado_no_put"
            truncado_detalhe = det_t
            registra(t0, "T0b — PUT de truncado.zip REJEITADO", bytes=len(truncado), **det_t)

        marcos: dict[str, float] = {}
        legivel_em: float | None = None
        pending_anterior: object = "<nao observado>"
        espera = 5
        while time.time() - t0 < janela:
            est = estado_remoto("bom.zip")
            registra(t0, "observacao", **est)

            if est["metadata_do_arquivo_visivel"] and "T1_metadata_visivel" not in marcos:
                marcos["T1_metadata_visivel"] = round(time.time() - t0, 1)
            if est["pending_tasks"] != pending_anterior:
                marcos[f"pending_tasks={est['pending_tasks']}"] = round(time.time() - t0, 1)
                pending_anterior = est["pending_tasks"]
            if est["membro_legivel"]:
                marcos["T2_membro_legivel"] = round(time.time() - t0, 1)
                legivel_em = time.time() - t0
                break

            time.sleep(espera)
            espera = min(espera * 2, 60)

        registra(t0, "ORDEM DOS EVENTOS", **marcos)

        if legivel_em is None:
            registra(t0, "T2 NUNCA ACONTECEU", janela=janela)
        else:
            status, n, dur, corpo = le_membro("bom.zip", META_MEMBRO)
            registra(
                t0,
                "custo de ler so o membro",
                bytes_do_membro=n,
                bytes_do_zip=len(bom),
                fracao=round(n / len(bom), 5),
                segundos=round(dur, 2),
                conteudo_bate=corpo.strip().startswith("{"),
                status=status,
            )
            registra(t0, "membro inexistente", resultado=le_membro("bom.zip", "_nao_existe.json"))
            registra(t0, "zip inexistente", resultado=le_membro("nao_existe.zip", META_MEMBRO))

            if truncado_estado == "aceito_no_put":
                st, nb, dt, _ = le_membro("truncado.zip", META_MEMBRO)
                truncado_estado = (
                    "aceito_e_legivel" if (st == 200 and nb > 0) else "aceito_mas_ilegivel"
                )
                truncado_detalhe = {"status": st, "bytes": nb, "segundos": round(dt, 2)}

        registra(t0, "ZIP TRUNCADO", estado=truncado_estado, **truncado_detalhe)
        print("\n=== RESUMO ===", flush=True)
        print(json.dumps([ev.__dict__ for ev in EVENTOS], indent=2, default=str), flush=True)
        return 0
    finally:
        # O marcador NÃO é apagado: mantém o item existindo para o próximo run.
        for nome in subidos:
            try:
                apaga(sessao, nome)
                registra(t0, "limpeza: removido", arquivo=nome)
            except Exception as exc:  # noqa: BLE001 — limpeza é best-effort
                registra(t0, "limpeza FALHOU", arquivo=nome, erro=str(exc)[:200])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--membros", type=int, default=2000, help="membros no ZIP de teste")
    ap.add_argument("--janela", type=int, default=900, help="segundos observando disponibilidade")
    ap.add_argument("--preparo", type=int, default=600, help="segundos para a fase de preparação")
    ap.add_argument(
        "--reparar-residuo",
        action="store_true",
        help="remove deliberadamente payload residual de execução anterior e sai",
    )
    args = ap.parse_args()

    # Defesa em profundidade: o workflow já valida, mas o script também roda a
    # mão. Sem limite, `membros` alto constrói um ZIP enorme em memória antes de
    # qualquer upload, e `janela` alta deixa o processo ocioso indefinidamente.
    if not 1 <= args.membros <= 50_000:
        ap.error(f"--membros={args.membros} fora da faixa [1, 50000]")
    if not 60 <= args.janela <= 1_800:
        ap.error(f"--janela={args.janela} fora da faixa [60, 1800]")
    if not 60 <= args.preparo <= 1_800:
        ap.error(f"--preparo={args.preparo} fora da faixa [60, 1800]")

    sessao = get_session(
        config={
            "s3": {"access": os.environ["IA_ACCESS_KEY"], "secret": os.environ["IA_SECRET_KEY"]}
        }
    )

    if args.reparar_residuo:
        return 0 if repara_residuo(sessao, limite=args.preparo) else 1

    if not prepara(sessao, limite=args.preparo):
        print("::error::precondicao nao atingida — nada foi medido", flush=True)
        return 1

    return mede(sessao, membros=args.membros, janela=args.janela)


if __name__ == "__main__":
    sys.exit(main())
