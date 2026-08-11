"""Reconhecimento de outputs derivados já duráveis no IA (#132 slice 1).

Motivação (#110): o run 31450937194 gastou ~2h30 em download+transform, tornou
todos os Parquets derivados duráveis em `ia:ficha-2026-05`, e morreu depois. A
retomada teve que ser feita à mão — `curl` no metadata do item e reconstrução da
linha do tempo — quando o pipeline tinha toda a informação para decidir sozinho.

O mirror de ZIPs brutos já faz exatamente isto (`skipping N/37 ZIPs already on
ia:.../raw/`). Este módulo estende o mesmo princípio aos artefatos derivados.

## O que este gate decide — e o que não decide

Decide **"preciso recomputar?"**. Não decide "posso promover?".

A promoção continua exclusivamente com `build_snapshot_entry()` +
`verify_snapshot_files()`, que recalculam `sha256` a partir dos bytes. Nada aqui
lê o manifesto promovido, então não há circularidade: reuse nunca depende de um
hash que só existiria *depois* da publicação.

## Evidência usada

O metadata do IA expõe `size`, `md5`, `sha1` e `crc32` — nunca `sha256`. Mas
para decidir recomputação não é preciso identidade criptográfica: basta evidência
estrutural de que o objeto remoto é uma materialização completa e coerente.

1. objeto presente no item;
2. `size > 0`;
3. metadata legível;
4. **footer do Parquet legível remotamente** — um arquivo truncado ou meio
   escrito não tem footer válido, e a leitura falha;
5. **colunas mínimas esperadas presentes** — footer legível sozinho provaria só
   que o arquivo não está truncado, não que é o artefato certo;
6. `row_count > 0`, obtido do próprio footer (sem baixar o arquivo).

As colunas mínimas não são inventadas: vêm de contratos que já existem no
repositório — as chaves de `sort` declaradas em `manifest.py` para os artefatos
que as declaram, e as colunas que `pack._COMPANIES_SQL` de fato lê para os
demais. Nenhuma contagem específica de mês é fixada em lugar nenhum.

`lookups.json` não tem footer, mas também não é aprovado por tamanho: ele é
baixado (~272 KB), parseado e checado contra as chaves que
`transform.write_lookups_json()` sempre emite. **Nenhum artefato do contrato é
considerado reutilizável apenas por presença e tamanho.**

Qualquer ambiguidade resulta em recomputação. Corrupção não vira erro
irrecuperável: o pipeline sabe reconstruir o artefato.
"""

from __future__ import annotations

import enum
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import duckdb
import httpx

from .mirror import item_root
from .sources import is_valid_month

log = logging.getLogger(__name__)

_METADATA_URL = "https://archive.org/metadata/{identifier}"
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)

# Retry curto e explícito: o custo de tratar um timeout momentâneo como
# ambiguidade é ~2h30 de transform desnecessário.
_METADATA_ATTEMPTS = 3
_METADATA_BACKOFF_S = 2.0
_TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})


class ReuseState(enum.StrEnum):
    """Os três resultados conceituais do reconhecimento."""

    REUSABLE = "reusable"
    """Materialização remota completa e coerente — pode ser reutilizada."""

    ABSENT = "absent"
    """Não existe no item remoto."""

    INVALID = "invalid"
    """Existe mas é inutilizável ou ambíguo: truncado, schema errado, vazio,
    metadata ilegível. Indistinguível de ausente para fins de decisão."""


@dataclass(frozen=True)
class ReuseVerdict:
    name: str
    state: ReuseState
    detail: str
    size: int | None = None
    row_count: int | None = None

    @property
    def must_recompute(self) -> bool:
        """`absent` e `invalid` levam à recomputação — nunca a uma falha dura."""
        return self.state is not ReuseState.REUSABLE


@dataclass(frozen=True)
class ParquetProbeResult:
    columns: frozenset[str]
    row_count: int


ParquetProbe = Callable[[str], ParquetProbeResult]
JsonFetch = Callable[[str], object]


# Colunas mínimas por artefato. Origem de cada conjunto:
#   - cnpj_cnaes / enderecos / pessoas: chaves de `sort` já declaradas em
#     manifest.build_snapshot_entry();
#   - cnpjs / raizes / socios: colunas que pack._COMPANIES_SQL lê;
#   - cnpj_contatos: chave de junção do contrato.
# `None` = artefato não-Parquet: só presença e tamanho são verificáveis.
REQUIRED_COLUMNS: dict[str, frozenset[str] | None] = {
    "cnpjs.parquet": frozenset(
        {"cnpj_base", "cnpj_ordem", "cnpj_dv", "situacao_cadastral", "uf", "municipio_codigo"}
    ),
    "raizes.parquet": frozenset(
        {"cnpj_base", "razao_social", "qtd_estabelecimentos", "qtd_estabelecimentos_ativos"}
    ),
    "socios.parquet": frozenset({"cnpj_base"}),
    "cnpj_contatos.parquet": frozenset({"cnpj_base"}),
    "cnpj_cnaes.parquet": frozenset({"cnae_codigo", "posicao", "cnpj_base"}),
    "enderecos.parquet": frozenset({"uf", "municipio_codigo", "logradouro_normalizado", "numero"}),
    "pessoas.parquet": frozenset({"cpf_mascarado", "nome_normalizado"}),
    # Não-Parquet: validado por `_classify_lookups_json`, não por footer.
    "lookups.json": None,
}

# Chaves que `transform.write_lookups_json()` sempre emite (schema
# `web/src/schemas/v1/lookups.ts`). Presença + tamanho autorizariam um JSON
# truncado, um `{}` ou uma página de erro HTML a passar por materialização
# válida — daí a validação estrutural. O arquivo tem ~272 KB, então baixá-lo
# é mais barato que qualquer alternativa indireta.
LOOKUPS_JSON_REQUIRED_KEYS = frozenset(
    {
        "schema_version",
        "snapshot_date",
        "cnaes",
        "motivos_situacao_cadastral",
        "municipios",
        "naturezas_juridicas",
        "paises",
        "qualificacoes_socio",
    }
)

_LOOKUP_KINDS = ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")

for _kind in _LOOKUP_KINDS:
    REQUIRED_COLUMNS[f"lookups/{_kind}.parquet"] = frozenset({"codigo", "descricao"})


def _load_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """Carrega httpfs, instalando só se ainda não estiver disponível.

    `INSTALL` pode resolver/baixar a extensão de um repositório remoto. Fazer
    isso uma vez por artefato acrescentaria N dependências externas justamente
    no mecanismo de recuperação — o oposto do que este módulo existe para
    oferecer. `LOAD` sozinho basta quando a extensão já está instalada, que é o
    caso no runner e no ambiente de dev.
    """
    try:
        con.execute("LOAD httpfs;")
    except duckdb.Error:
        con.execute("INSTALL httpfs; LOAD httpfs;")


def duckdb_parquet_probe(url: str) -> ParquetProbeResult:
    """Lê colunas e contagem pelo footer do Parquet, sem baixar o arquivo.

    Medido contra `ia:ficha-2026-05`: 3,0 s para os 5,98 GB de `cnpjs.parquet`.
    """
    con = duckdb.connect()
    try:
        if url.startswith("http"):
            _load_httpfs(con)
        cursor = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [url])
        columns = frozenset(description[0] for description in cursor.description)
        row_count = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [url]).fetchone()[0]
        return ParquetProbeResult(columns=columns, row_count=int(row_count))
    finally:
        con.close()


def fetch_item_metadata(
    month: str,
    *,
    client: httpx.Client | None = None,
    attempts: int = _METADATA_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> dict | None:
    """Metadata do item do mês, ou `None` quando indisponível/ilegível.

    Não levanta: indisponibilidade é ambiguidade, e ambiguidade leva a
    recomputar — não a derrubar o pipeline.

    Faz retry curto **apenas** para falhas transitórias (erro de transporte,
    5xx, 429). Sem isso, um único timeout de `archive.org/metadata` custaria as
    ~2h30 de transform que este módulo existe para evitar. Um 404 é resposta
    estrutural — o item não existe — e não é retentado: insistir nele só
    atrasaria a recomputação legítima.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    url = _METADATA_URL.format(identifier=f"ficha-{month}")
    owns_client = client is None
    client = client or httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        for attempt in range(1, attempts + 1):
            transient: str | None = None
            try:
                response = client.get(url)
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as exc:
                        # JSON inválido é resposta estrutural, não transitória.
                        log.warning("metadata de %s ilegível: %s", url, exc)
                        return None
                if response.status_code in _TRANSIENT_STATUS:
                    transient = f"HTTP {response.status_code}"
                else:
                    log.warning("metadata de %s → HTTP %d", url, response.status_code)
                    return None
            except httpx.HTTPError as exc:
                transient = str(exc)

            if attempt == attempts:
                log.warning(
                    "metadata de %s indisponível após %d tentativas: %s",
                    url,
                    attempts,
                    transient,
                )
                return None
            delay = _METADATA_BACKOFF_S * attempt
            log.warning(
                "metadata de %s falhou (tentativa %d/%d): %s — repetindo em %.0fs",
                url,
                attempt,
                attempts,
                transient,
                delay,
            )
            sleep(delay)
        return None
    finally:
        if owns_client:
            client.close()


def httpx_json_fetch(url: str) -> object:
    """Baixa e parseia um JSON pequeno. `follow_redirects` é obrigatório.

    `archive.org/download/...` redireciona para o nó de armazenamento; sem
    seguir o redirect a resposta chega vazia **sem erro**, e um arquivo vazio
    não pode ser confundido com um arquivo válido.
    """
    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        if not response.content:
            raise ValueError("resposta vazia")
        return response.json()


def _files_by_name(metadata: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in metadata.get("files", []) if "name" in entry}


def _classify_lookups_json(
    name: str,
    *,
    size: int,
    base_url: str,
    fetch: JsonFetch,
) -> ReuseVerdict:
    """Valida estrutura mínima de `lookups.json` — presença/tamanho não bastam.

    Um JSON truncado, um `{}`, uma página de erro HTML ou conteúdo de outro
    formato têm `size > 0` e passariam por materialização válida.
    """
    try:
        payload = fetch(f"{base_url}/{name}")
    except Exception as exc:  # noqa: BLE001 — qualquer falha de leitura é ambiguidade
        return ReuseVerdict(name, ReuseState.INVALID, f"ilegível: {exc}", size=size)

    if not isinstance(payload, dict):
        return ReuseVerdict(
            name, ReuseState.INVALID, f"não é objeto JSON: {type(payload).__name__}", size=size
        )

    missing = LOOKUPS_JSON_REQUIRED_KEYS - payload.keys()
    if missing:
        return ReuseVerdict(
            name, ReuseState.INVALID, f"chaves ausentes: {sorted(missing)}", size=size
        )

    return ReuseVerdict(name, ReuseState.REUSABLE, "JSON com as chaves do schema", size=size)


def _classify_one(
    name: str,
    entry: dict | None,
    *,
    base_url: str,
    probe: ParquetProbe,
    json_fetch: JsonFetch,
) -> ReuseVerdict:
    if entry is None:
        return ReuseVerdict(name, ReuseState.ABSENT, "ausente no item remoto")

    raw_size = entry.get("size")
    try:
        size = int(raw_size)
    except (TypeError, ValueError):
        # Entrada presente mas sem tamanho utilizável: ambíguo, não ausente.
        return ReuseVerdict(name, ReuseState.INVALID, f"size ilegível no metadata: {raw_size!r}")

    if size <= 0:
        return ReuseVerdict(name, ReuseState.INVALID, "size zero", size=size)

    if name == "lookups.json":
        return _classify_lookups_json(name, size=size, base_url=base_url, fetch=json_fetch)

    expected_columns = REQUIRED_COLUMNS.get(name)
    if expected_columns is None:
        return ReuseVerdict(name, ReuseState.INVALID, "artefato sem validação definida", size=size)

    try:
        result = probe(f"{base_url}/{name}")
    except Exception as exc:  # noqa: BLE001 — qualquer falha de leitura é ambiguidade
        return ReuseVerdict(name, ReuseState.INVALID, f"footer ilegível: {exc}", size=size)

    missing = expected_columns - result.columns
    if missing:
        return ReuseVerdict(
            name,
            ReuseState.INVALID,
            f"colunas ausentes: {sorted(missing)}",
            size=size,
            row_count=result.row_count,
        )

    if result.row_count <= 0:
        return ReuseVerdict(
            name, ReuseState.INVALID, "row_count zero", size=size, row_count=result.row_count
        )

    return ReuseVerdict(
        name,
        ReuseState.REUSABLE,
        "footer legível, schema compatível, row_count > 0",
        size=size,
        row_count=result.row_count,
    )


def classify_outputs(
    month: str,
    *,
    metadata: dict | None,
    probe: ParquetProbe = duckdb_parquet_probe,
    json_fetch: JsonFetch = httpx_json_fetch,
) -> dict[str, ReuseVerdict]:
    """Classifica cada output derivado do contrato como reusable/absent/invalid.

    Função de leitura pura: nenhuma escrita local ou remota. `metadata` é
    injetado (ver `fetch_item_metadata`) para manter a decisão testável sem rede.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    if metadata is None:
        return {
            name: ReuseVerdict(name, ReuseState.INVALID, "metadata do item indisponível")
            for name in REQUIRED_COLUMNS
        }

    base_url = item_root(month)
    entries = _files_by_name(metadata)
    return {
        name: _classify_one(
            name, entries.get(name), base_url=base_url, probe=probe, json_fetch=json_fetch
        )
        for name in REQUIRED_COLUMNS
    }


def all_outputs_reusable(verdicts: dict[str, ReuseVerdict]) -> bool:
    """Só reutiliza a materialização se **todo** o contrato estiver íntegro.

    Reuse parcial exigiria recomputar o transform de qualquer forma — ele produz
    os artefatos em conjunto — então não haveria economia.
    """
    return all(not verdict.must_recompute for verdict in verdicts.values())


def format_verdicts(verdicts: dict[str, ReuseVerdict]) -> str:
    """Uma linha factual por artefato, no espírito do log do mirror de ZIPs."""
    return "\n".join(
        f"  {verdict.state.value:9} {name:28} {verdict.detail}"
        for name, verdict in sorted(verdicts.items())
    )
