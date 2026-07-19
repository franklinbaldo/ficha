"""Observabilidade de estágio — Fase 0 da RFC 0001 (baseline real).

RFC 0001 §16 exige que cada estágio do pipeline registre, em formato humano
E JSON: tempo, linhas/bytes lidos e escritos, MB/s, pico de RSS, pico de
`duckdb_tmp`, pico do diretório de trabalho, e versões (código, DuckDB,
schema). Este módulo só existe pra medir -- não decide nada sobre os dados
produzidos (Fase 0 é "sem mudança de comportamento").

Uso típico em transform.py:

    recorder = MetricsRecorder(month=month, schema_version=schema_version)
    ...
    recorder.capture_pragmas(con)  # depois de setar memory_limit/threads
    ...
    with recorder.stage("cnpjs_chunked", duckdb_tmp_dir=tmp, workdir=out) as h:
        write_cnpjs_parquet_chunked(...)
        h.bytes_written = out_path.stat().st_size
        h.rows_written = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    ...
    recorder.write_json(cache_dir / month / "metrics" / "transform_metrics.json")

Falha ao ESCREVER metrics.json vira `log.warning`, nunca exceção -- métricas
são observabilidade, não podem derrubar um job mensal que já produziu dados
corretos (ver `write_json`). Um bug de programação dentro deste módulo (ex.:
TypeError por um tipo errado passado a StageMetrics) continua propagando
normalmente -- isso é bug, não "falha de coleta de métrica em produção".
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import resource
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

# Tipos aceitos no sub-dict `extra` de StageMetrics -- qualquer coisa que
# serialize direto em JSON sem transformação.
ExtraValue = str | int | float | bool


@dataclass(frozen=True)
class ChunkMetrics:
    """Métricas de UM chunk dentro de um writer chunked (ex.: cnpjs_chunked).

    RFC 0001 §16 exige registro por estágio E POR CHUNK -- o agregado de
    `StageMetrics` esconde a variação entre chunks, que é justamente o que
    importa pra achar um chunk fora da curva (o incidente histórico de OOM
    do writer chunked foi isolado no chunk 0/10 -- ver a docstring de
    `write_cnpjs_parquet_chunked` em transform.py -- não visível olhando só
    o total do estágio).

    `status`/`error`: um chunk que lança exceção no meio do processamento
    (ex.: OOM num `COPY`) precisa continuar aparecendo no `metrics.json` --
    é o chunk culpado, o dado mais importante pro diagnóstico. O writer
    invoca `on_chunk` com `status="failed"` e `error=str(exc)` ANTES de
    re-levantar a exceção original (nunca no lugar dela -- reportar a
    métrica não pode suprimir uma falha real do pipeline).
    """

    index: int
    csv_name: str
    wall_seconds: float
    rows_written: int | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    status: str = "ok"
    error: str | None = None

    def to_json_dict(self) -> dict[str, object]:
        return {
            "index": self.index,
            "csv_name": self.csv_name,
            "wall_seconds": round(self.wall_seconds, 3),
            "rows_written": self.rows_written,
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "status": self.status,
            "error": self.error,
        }


@dataclass(frozen=True)
class StageMetrics:
    """Métricas finais e imutáveis de um estágio já concluído.

    Campos e nomes de `to_json_dict()` são o contrato externo de
    `metrics.json` (RFC 0001 §16) -- não renomeie sem atualizar quem lê o
    arquivo.
    """

    name: str
    wall_seconds: float
    rss_peak_mib: float
    rss_peak_delta_mib: float
    started_at: str
    finished_at: str
    rows_read: int | None = None
    rows_written: int | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    duckdb_tmp_peak_mib: float | None = None
    workdir_peak_mib: float | None = None
    filesystem_used_peak_mib: float | None = None
    filesystem_total_mib: float | None = None
    filesystem_used_peak_percent: float | None = None
    chunks: tuple[ChunkMetrics, ...] | None = None
    extra: Mapping[str, ExtraValue] | None = None

    def mb_per_second(self) -> float | None:
        """MB/s derivado de bytes_written/wall_seconds -- None se não houver dado."""
        if self.bytes_written is None or self.wall_seconds <= 0:
            return None
        return round((self.bytes_written / 1024 / 1024) / self.wall_seconds, 2)

    def rows_per_second(self) -> float | None:
        """Linhas/s derivado de rows_written/wall_seconds -- None se não houver dado."""
        if self.rows_written is None or self.wall_seconds <= 0:
            return None
        return round(self.rows_written / self.wall_seconds, 1)

    def to_json_dict(self) -> dict[str, object]:
        """Serialização estável pra `metrics.json`.

        Chaves (contrato externo, RFC 0001 §16):
        stage, wall_seconds, rows_read, rows_written, bytes_read,
        bytes_written, mb_per_second, rows_per_second, rss_peak_mib,
        rss_peak_delta_mib, duckdb_tmp_peak_mib, workdir_peak_mib,
        filesystem_used_peak_mib, filesystem_total_mib,
        filesystem_used_peak_percent, started_at, finished_at, chunks, extra.

        `filesystem_used_peak_percent` é o que a RFC 0001 §19 realmente
        pede ("permanecer abaixo de 80% do filesystem") -- o MiB absoluto
        sozinho não diz se um runner está perto do limite ou não (o mesmo
        valor é folgado num runner de 500 GiB e crítico num de 20 GiB).
        """
        return {
            "stage": self.name,
            "wall_seconds": round(self.wall_seconds, 3),
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "mb_per_second": self.mb_per_second(),
            "rows_per_second": self.rows_per_second(),
            "rss_peak_mib": round(self.rss_peak_mib, 1),
            "rss_peak_delta_mib": round(self.rss_peak_delta_mib, 1),
            "duckdb_tmp_peak_mib": (
                round(self.duckdb_tmp_peak_mib, 1) if self.duckdb_tmp_peak_mib is not None else None
            ),
            "workdir_peak_mib": (
                round(self.workdir_peak_mib, 1) if self.workdir_peak_mib is not None else None
            ),
            "filesystem_used_peak_mib": (
                round(self.filesystem_used_peak_mib, 1)
                if self.filesystem_used_peak_mib is not None
                else None
            ),
            "filesystem_total_mib": (
                round(self.filesystem_total_mib, 1)
                if self.filesystem_total_mib is not None
                else None
            ),
            "filesystem_used_peak_percent": (
                round(self.filesystem_used_peak_percent, 2)
                if self.filesystem_used_peak_percent is not None
                else None
            ),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "chunks": [c.to_json_dict() for c in self.chunks] if self.chunks else [],
            "extra": dict(self.extra) if self.extra else {},
        }


@dataclass
class StageHandle:
    """Handle MUTÁVEL entregue por `MetricsRecorder.stage()`.

    Ao contrário de `StageMetrics` (imutável, resultado final), este objeto
    existe só durante o `with` -- o chamador preenche rows/bytes conforme
    descobre (ex.: depois de escrever um parquet); wall-clock, RSS e pico de
    disco são medidos pelo próprio context manager e não precisam de input.
    """

    name: str
    rows_read: int | None = None
    rows_written: int | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    extra: dict[str, ExtraValue] = field(default_factory=dict)
    # Preenchido por writers chunked via callback `on_chunk` (ex.:
    # `write_cnpjs_parquet_chunked`) -- ver `ChunkMetrics`. `chunks.append`
    # é o próprio callback: qualquer callable `ChunkMetrics -> None` serve.
    chunks: list[ChunkMetrics] = field(default_factory=list)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rss_peak_mib() -> float:
    """Pico de RSS do processo até agora, em MiB.

    `ru_maxrss` é MONOTÔNICO e cumulativo desde o início do processo -- a
    API do SO não devolve "pico só deste período", só "pico corrente". Por
    isso reportamos dois números em `StageMetrics`:

    - `rss_peak_mib`: o pico acumulado (útil pra saber o teto absoluto que
      o processo já atingiu);
    - `rss_peak_delta_mib`: a subida desde o fim do estágio anterior --
      proxy grosseiro de "quanto ESTE estágio pode ter contribuído". Pode
      ficar em zero mesmo que o estágio tenha alocado e liberado bastante
      memória dentro da própria janela (o pico local não empurrou o pico
      global) -- isso é uma limitação conhecida de getrusage, não um bug
      daqui.

    Unidade de `ru_maxrss` varia por plataforma: KiB no Linux, bytes no
    macOS/BSD. Produção roda em Linux, mas dev/testes locais podem rodar em
    macOS -- por isso o branch em `sys.platform` em vez de assumir KiB.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return peak / (1024 * 1024)
    return peak / 1024


def _bytes_to_mib(size_bytes: int | None) -> float | None:
    if size_bytes is None:
        return None
    return size_bytes / (1024 * 1024)


def _iter_file_sizes(path: Path) -> Iterator[int]:
    """Percorre `path` recursivamente e gera o tamanho de cada arquivo.

    Qualquer `OSError` no `stat()` de um arquivo individual -- não só
    `FileNotFoundError`, também `PermissionError`, `NotADirectoryError`,
    um handle NFS caído, etc. -- é ruído de
    infraestrutura da amostragem de disco (concorrência normal com o
    próprio pipeline: partes temporárias de COPY sendo trocadas, spill do
    DuckDB sendo liberado), não um evento de fluxo de negócio: o arquivo é
    pulado, nunca propaga. `os.walk` já ignora por padrão erros ao LISTAR
    um diretório (sem `onerror`, ele pula a entrada problemática sozinho);
    só o `stat()` por arquivo precisa de proteção explícita aqui.

    Isolar isso num gerador mantém os consumidores (`_dir_size_bytes`,
    `_sample_once`) num único `for` + guard clause, sem o `for→for→try`
    aninhado que estaria em cada um deles.
    """
    if not path.exists():
        return
    for root, _dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = Path(root) / filename
            try:
                yield file_path.stat().st_size
            except OSError:
                continue


def _dir_size_bytes(path: Path) -> int:
    """Soma o tamanho de todos os arquivos sob `path` (recursivo)."""
    return sum(_iter_file_sizes(path))


def _filesystem_usage(path: Path) -> shutil._ntuple_diskusage | None:  # noqa: SLF001
    """Uso (total, used, free) do mount que contém `path`, via `shutil.disk_usage`.

    Somar o tamanho de diretórios monitorados por nome (`_dir_size_bytes`
    sobre `duckdb_tmp`/`workdir`) só vê o que o próprio pipeline sabe que
    está escrevendo. Isso deixa de fora, por exemplo,
    `cache_dir/<month>/transform.duckdb` -- o arquivo de banco pode ter
    vários GiB e nunca fica dentro de `duckdb_tmp` nem de `workdir` (é
    irmão do primeiro, não filho). O gate de aceitação da RFC 0001 §19
    ("permanecer abaixo de 80% do filesystem") se refere ao disco REAL, não
    a uma soma parcial de diretórios conhecidos -- por isso esse número vem
    de `disk_usage`, que reporta o mount inteiro, cross-platform, sem
    depender de sabermos nomear cada arquivo grande que o pipeline cria.

    Devolve o `total` JUNTO com o `used` na mesma chamada -- não faça duas
    chamadas de `disk_usage` separadas pra "used" e "total": elas poderiam
    observar estados diferentes do mount se algo escrever entre as duas
    (o `total` da partição não muda de amostra pra amostra, mas o `used`
    sim, e a dupla precisa vir consistente da mesma leitura).

    Sobe pela árvore de diretórios até achar um ancestral existente antes
    de chamar `disk_usage` -- `path` pode ainda não existir no momento em
    que o sampler arranca (ex.: `cache_dir/<month>/` só é criado depois do
    primeiro estágio). Devolve None se nada nunca existir (não deveria
    acontecer -- toda árvore tem "/" como ancestral) ou se `disk_usage`
    falhar por qualquer razão: ruído de infraestrutura da amostragem, não
    deve derrubar o sampler.
    """
    probe = path
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            return None
        probe = parent
    try:
        return shutil.disk_usage(probe)
    except OSError:
        return None


def _percent(part: int | None, whole: int | None) -> float | None:
    """`part / whole * 100`, ou None se algo faltar ou `whole` for zero/None.

    Usado pra derivar `filesystem_used_peak_percent` a partir dos bytes
    brutos (não dos MiB já arredondados) -- calculado uma vez, na hora de
    montar o `StageMetrics`, não guardado por amostra (RFC 0001 §19: o gate
    de 80% é sobre essa fração, não sobre o MiB absoluto, que sozinho não
    diz se um runner está perto do limite ou não).
    """
    if part is None or not whole:
        return None
    return part / whole * 100


class _DiskPeakSampler:
    """Sampler em thread daemon: soma o tamanho de diretórios periodicamente
    e guarda o máximo observado por diretório -- mais, opcionalmente, o pico
    de uso do filesystem inteiro (`_filesystem_usage`) sob as chaves
    especiais `"filesystem"` (pico de bytes usados) e `"filesystem_total"`
    (capacidade total do mount, constante entre amostras do mesmo mount).

    Trade-off de amostragem: o sampler só olha o filesystem a cada
    `interval` segundos (default 5s). Um pico de disco que sobe e desce
    inteiramente ENTRE duas amostras escapa sem ser registrado. Para os
    estágios deste pipeline (minutos, não segundos) isso é aceitável -- o
    objetivo do baseline é a ordem de grandeza do pico sustentado, não o
    instante exato de um spill transitório de milissegundos. Custo por
    amostra é um `os.walk` sobre os diretórios monitorados + uma chamada
    `disk_usage`; desprezível frente à duração típica de um estágio
    (extract/load/write rodam por dezenas de segundos a minutos, não os
    poucos ms que isso custa).
    """

    def __init__(
        self,
        dirs: dict[str, Path],
        *,
        interval: float = 5.0,
        filesystem_path: Path | None = None,
    ) -> None:
        self._dirs = dirs
        self._interval = interval
        self._filesystem_path = filesystem_path
        self._peaks: dict[str, int] = dict.fromkeys(dirs, 0)
        if filesystem_path is not None:
            self._peaks["filesystem"] = 0
            self._peaks["filesystem_total"] = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def _has_anything_to_watch(self) -> bool:
        return bool(self._dirs) or self._filesystem_path is not None

    def start(self) -> None:
        if not self._has_anything_to_watch():
            return
        self._thread = threading.Thread(
            target=self._run, name="ficha-etl-disk-sampler", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        # Amostra imediatamente ao entrar -- sem isso, um estágio mais curto
        # que `interval` (ex.: lookups, <1s) nunca teria nenhuma leitura
        # antes do stop() rodar a amostra final.
        #
        # `_sample_once` já engole todo OSError esperado internamente (via
        # `_iter_file_sizes`); o try/except aqui é uma rede de segurança
        # pra qualquer coisa GENUINAMENTE inesperada (bug de programação,
        # erro que não é OSError). Sem isso, uma exceção nesta thread
        # daemon mata a amostragem silenciosamente -- só apareceria em
        # stderr via threading.excepthook, sem nenhum sinal no
        # metrics.json/log estruturado. Loga UMA vez e encerra o loop (não
        # fica tentando de novo pra não spammar o log a cada `interval`).
        while not self._stop_event.is_set():
            try:
                self._sample_once()
            except Exception as exc:
                log.warning(
                    "metrics: sampler de disco parou de amostrar após erro inesperado: %s", exc
                )
                return
            self._stop_event.wait(self._interval)

    def _sample_once(self) -> None:
        for key, path in self._dirs.items():
            self._update_peak(key, _dir_size_bytes(path))
        if self._filesystem_path is None:
            return
        usage = _filesystem_usage(self._filesystem_path)
        if usage is not None:
            self._record_filesystem_usage(usage)

    def _record_filesystem_usage(self, usage: shutil._ntuple_diskusage) -> None:  # noqa: SLF001
        """Atualiza pico de `used` + `total` observado a partir de UMA amostra.

        `used` e `total` vêm do mesmo objeto `usage` (uma única chamada de
        `disk_usage`, ver `_filesystem_usage`) -- nunca duas leituras
        separadas que poderiam observar o mount em momentos diferentes.
        """
        self._update_peak("filesystem", usage.used)
        with self._lock:
            self._peaks["filesystem_total"] = usage.total

    def _update_peak(self, key: str, size: int) -> None:
        with self._lock:
            if size <= self._peaks[key]:
                return
            self._peaks[key] = size

    def stop(self) -> dict[str, int]:
        if not self._has_anything_to_watch():
            return {}
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1)
            if self._thread.is_alive():
                # Não bloqueia mais -- só avisa. Uma thread daemon presa
                # (ex.: `os.walk` pendurado num mount de rede morto) não
                # pode travar o teardown do estágio; ela morre sozinha
                # quando o processo termina (daemon=True).
                log.warning(
                    "metrics: thread do disk sampler ainda viva após join(timeout=%.1fs) -- "
                    "seguindo sem bloquear",
                    self._interval + 1,
                )
        # Amostra final direto nesta thread -- cobre o tamanho no instante
        # exato do teardown, mesmo que a thread de fundo ainda estivesse
        # dormindo em `wait()` quando paramos.
        self._sample_once()
        with self._lock:
            return dict(self._peaks)


def _package_version() -> str:
    """Versão do pacote ficha-etl instalado, ou 'unknown' fora de um venv com metadata.

    NÃO é a identidade de código do run -- `pyproject.toml` fixa essa versão
    em "0.0.1" e não é bumped por commit, então todo run gera o mesmo valor
    aqui independente de qual implementação rodou. Mantido como campo
    separado (`package_version` no envelope) porque ainda é útil saber qual
    release do pacote está instalada; a identidade real é `_git_sha()`.
    """
    import importlib.metadata

    try:
        return importlib.metadata.version("ficha-etl")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _run_git(*args: str) -> subprocess.CompletedProcess[str] | None:
    """Roda `git <args>` capturando stdout como texto. None se `git` falhar
    por qualquer razão (não instalado, não é um repo git, timeout, etc.) --
    ruído de infraestrutura da coleta de métricas, nunca deve propagar."""
    try:
        return subprocess.run(
            ["git", *args], capture_output=True, text=True, check=False, timeout=10
        )
    except (OSError, subprocess.SubprocessError):
        return None


def _git_tree_is_dirty() -> bool:
    result = _run_git("status", "--porcelain")
    if result is None or result.returncode != 0:
        return False
    return bool(result.stdout.strip())


def _git_sha() -> str:
    """SHA do commit que gerou este run -- a identidade real do código executado.

    `_package_version()` fica travado em "0.0.1", então não diferencia qual
    implementação produziu um `metrics.json` -- esse é o campo de
    identidade que RFC 0001 §16 pede ("versão do código").

    Precedência:
    1. `GITHUB_SHA` -- Actions já injeta isso em todo job, sem precisar
       rodar `git` (mais barato, e funciona mesmo num checkout raso/sem
       histórico completo);
    2. `git rev-parse HEAD` via subprocess -- cobre dev local e backfills
       fora de Actions;
    3. `"unknown"` se nenhum dos dois funcionar (não é um repo git, `git`
       não instalado, etc.) -- observabilidade não pode falhar o pipeline
       por não conseguir identificar o próprio código.

    Quando o SHA vem de `git` (não de `GITHUB_SHA`, que já reflete um
    checkout limpo por definição), sufixa "-dirty" se
    `git status --porcelain` reportar mudanças não commitadas -- distingue
    um run de dev com working tree suja de um run limpo de CI.
    """
    env_sha = os.environ.get("GITHUB_SHA", "").strip()
    if env_sha:
        return env_sha

    result = _run_git("rev-parse", "HEAD")
    if result is None or result.returncode != 0:
        return "unknown"
    sha = result.stdout.strip()
    if not sha:
        return "unknown"
    if _git_tree_is_dirty():
        return f"{sha}-dirty"
    return sha


class MetricsRecorder:
    """Coleta `StageMetrics` de estágios sucessivos e escreve `metrics.json` ao final.

    Um recorder por execução de `transform_snapshot`. Não é thread-safe entre
    estágios concorrentes -- os estágios do pipeline hoje são sequenciais, só
    o sampler de disco interno roda em thread própria.
    """

    def __init__(
        self,
        *,
        month: str,
        schema_version: str,
        filesystem_path: Path | None = None,
    ) -> None:
        """`filesystem_path`: qualquer path dentro do mount cujo pico de uso
        (RFC 0001 §19: gate de 80% do filesystem) deve ser rastreado em
        TODO estágio, além dos picos de diretório específicos que cada
        `stage()` já opta por monitorar via `duckdb_tmp_dir`/`workdir`.
        Tipicamente `cache_dir` (cobre `transform.duckdb`, `duckdb_tmp` e
        os CSVs extraídos, todos sob o mesmo mount). None desativa essa
        amostragem (ex.: em testes unitários de estágios isolados).
        """
        self.month = month
        self.schema_version = schema_version
        self.stages: list[StageMetrics] = []
        self.pragmas: dict[str, str] = {}
        self._last_rss_mib = _rss_peak_mib()
        self._filesystem_path = filesystem_path

    def capture_pragmas(self, con: duckdb.DuckDBPyConnection) -> None:
        """Lê de volta os PRAGMAs que EFETIVAMENTE valeram (RFC 0001 §16).

        Consulta via `current_setting` em vez de confiar no valor que
        pedimos -- em tese o DuckDB poderia arredondar/rejeitar um valor
        silenciosamente, e o ponto de um baseline é registrar o que
        realmente rodou, não o que a chamada pediu.
        """
        try:
            mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        except duckdb.Error as exc:
            log.warning("metrics: falha ao ler PRAGMAs efetivos: %s", exc)
            return
        self.pragmas = {"memory_limit": str(mem), "threads": str(threads)}

    @contextlib.contextmanager
    def stage(
        self,
        name: str,
        *,
        duckdb_tmp_dir: Path | None = None,
        workdir: Path | None = None,
        sample_interval: float = 5.0,
    ) -> Iterator[StageHandle]:
        """Mede wall-clock + pico de RSS (+ pico de disco opcional) de um estágio.

        `duckdb_tmp_dir`/`workdir` mapeiam diretamente pros dois campos
        nomeados de `StageMetrics` (`duckdb_tmp_peak_mib`/`workdir_peak_mib`)
        exigidos pela RFC 0001 §16 -- preferido a um `watch_dirs: dict`
        genérico cruzando a fronteira do módulo sem tipo, o que violaria a
        regra de "nenhum dict solto cruzando fronteira de módulo". Além
        desses dois, se o `MetricsRecorder` foi criado com
        `filesystem_path`, TODO estágio também amostra
        `filesystem_used_peak_mib` (via `shutil.disk_usage` no mount real) --
        essa é a métrica que importa pro gate de 80% do filesystem (RFC 0001
        §19), já que soma de diretórios monitorados por nome pode deixar de
        fora arquivos grandes que o pipeline cria fora deles (ex.:
        `transform.duckdb`).

        O handle devolvido é mutável: o chamador seta rows/bytes conforme
        descobre (ex.: depois do COPY TO PARQUET). Se o corpo do `with`
        levantar uma exceção, essa exceção é a ÚNICA coisa que propaga --
        qualquer falha no teardown de métricas (parar o sampler, montar o
        `StageMetrics`, logar) é blindada em `_finalize_stage` e vira só
        `log.warning`, nunca re-levanta e nunca mascara a exceção original
        do corpo (ver docstring de `_finalize_stage`).
        """
        handle = StageHandle(name=name)
        started_at = _now_iso()
        t0 = time.monotonic()
        sampler = self._start_sampler(duckdb_tmp_dir, workdir, sample_interval)
        try:
            yield handle
        finally:
            self._finalize_stage(handle, started_at=started_at, t0=t0, sampler=sampler)

    def _start_sampler(
        self, duckdb_tmp_dir: Path | None, workdir: Path | None, sample_interval: float
    ) -> _DiskPeakSampler | None:
        watch: dict[str, Path] = {}
        if duckdb_tmp_dir is not None:
            watch["duckdb_tmp"] = duckdb_tmp_dir
        if workdir is not None:
            watch["workdir"] = workdir
        if not watch and self._filesystem_path is None:
            return None
        sampler = _DiskPeakSampler(
            watch, interval=sample_interval, filesystem_path=self._filesystem_path
        )
        sampler.start()
        return sampler

    def _finalize_stage(
        self,
        handle: StageHandle,
        *,
        started_at: str,
        t0: float,
        sampler: _DiskPeakSampler | None,
    ) -> None:
        """Fecha o estágio: pára o sampler, monta e registra o `StageMetrics`.

        Roda dentro do `finally` de `stage()` -- por isso TUDO aqui é
        blindado num único `try/except Exception` que só loga um warning e
        nunca re-levanta. Se o teardown falhar por qualquer razão (ex.: um
        `OSError` genuinamente inesperado que escapou de `_dir_size_bytes`,
        um bug na construção do `StageMetrics`), essa falha NÃO PODE:

        1. mascarar a exceção real do corpo do `with` (se houver) -- em
           Python, uma exceção levantada dentro de um `finally` substitui a
           exceção em voo, que vira só `__context__` e some da vista de
           quem chamou; ou
        2. derrubar um `with` que, do ponto de vista do corpo, terminou
           com sucesso -- ex.: o loop de writers em `transform.py` que
           usa `except NotImplementedError` logo após o `with
           recorder.stage(...)` esperaria terminar normalmente.

        Mesmo padrão de `write_json`: observabilidade nunca é motivo pra
        derrubar (ou mascarar a falha real de) o pipeline.
        """
        try:
            wall = time.monotonic() - t0
            peaks = sampler.stop() if sampler is not None else {}
            rss_now = _rss_peak_mib()
            fs_used = peaks.get("filesystem")
            fs_total = peaks.get("filesystem_total")
            stage_metrics = StageMetrics(
                name=handle.name,
                wall_seconds=wall,
                rows_read=handle.rows_read,
                rows_written=handle.rows_written,
                bytes_read=handle.bytes_read,
                bytes_written=handle.bytes_written,
                rss_peak_mib=rss_now,
                rss_peak_delta_mib=max(0.0, rss_now - self._last_rss_mib),
                duckdb_tmp_peak_mib=_bytes_to_mib(peaks.get("duckdb_tmp")),
                workdir_peak_mib=_bytes_to_mib(peaks.get("workdir")),
                filesystem_used_peak_mib=_bytes_to_mib(fs_used),
                filesystem_total_mib=_bytes_to_mib(fs_total),
                filesystem_used_peak_percent=_percent(fs_used, fs_total),
                chunks=tuple(handle.chunks) if handle.chunks else None,
                started_at=started_at,
                finished_at=_now_iso(),
                extra=dict(handle.extra) if handle.extra else None,
            )
            self._last_rss_mib = rss_now
            self.stages.append(stage_metrics)
            self._log_stage(stage_metrics)
        except Exception as exc:
            log.warning("metrics: falha ao finalizar estágio %r: %s", handle.name, exc)

    def _log_stage(self, m: StageMetrics) -> None:
        """Formato humano compacto, uma linha por estágio (RFC 0001 §16)."""
        parts = [f"[metrics] {m.name:<20s} {m.wall_seconds:7.1f}s"]
        parts.append(f"rows={m.rows_written:,}" if m.rows_written is not None else "rows=-")
        mbps = m.mb_per_second()
        if mbps is not None:
            parts.append(f"{mbps:.1f}MB/s")
        rss_part = f"rss={m.rss_peak_mib:.0f}MiB(+{m.rss_peak_delta_mib:.0f})"
        parts.append(rss_part)
        if m.duckdb_tmp_peak_mib is not None:
            parts.append(f"duckdb_tmp_peak={m.duckdb_tmp_peak_mib:.0f}MiB")
        if m.workdir_peak_mib is not None:
            parts.append(f"workdir_peak={m.workdir_peak_mib:.0f}MiB")
        if m.filesystem_used_peak_mib is not None:
            pct = (
                f"/{m.filesystem_used_peak_percent:.1f}%"
                if m.filesystem_used_peak_percent is not None
                else ""
            )
            parts.append(f"fs_used_peak={m.filesystem_used_peak_mib:.0f}MiB{pct}")
        if m.chunks:
            parts.append(f"chunks={len(m.chunks)}")
        log.info(" ".join(parts))

    def to_envelope(self) -> dict[str, object]:
        """Payload completo de `metrics.json` (RFC 0001 §16/17).

        `code_version` é o SHA do commit (`_git_sha()`) -- a identidade real
        do código que gerou este run. `package_version` é
        `importlib.metadata.version("ficha-etl")`, mantido à parte por ser
        útil saber, mas travado em "0.0.1" no pyproject.toml e por isso não
        serve como identidade (ver docstring de `_git_sha`).
        """
        return {
            "code_version": _git_sha(),
            "package_version": _package_version(),
            "duckdb_version": duckdb.__version__,
            "schema_version": self.schema_version,
            "month": self.month,
            "pragmas": dict(self.pragmas),
            "stages": [s.to_json_dict() for s in self.stages],
        }

    def write_json(self, path: Path) -> None:
        """Escreve o `metrics.json` final.

        Falha de I/O aqui (diretório inexistente e não criável, disco
        cheio, permissão negada) vira `log.warning` e retorna -- métricas
        são observabilidade, não podem fazer um pipeline que já produziu
        dados corretos falhar por não conseguir escrever um arquivo de
        diagnóstico. Bugs de programação (ex.: objeto não serializável em
        `extra`) continuam propagando via TypeError, porque isso é um bug
        deste módulo, não uma falha operacional esperada de infraestrutura.
        """
        payload = self.to_envelope()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        except OSError as exc:
            log.warning(
                "metrics: falha ao escrever %s: %s -- métricas deste run foram perdidas", path, exc
            )


__all__ = ["ChunkMetrics", "MetricsRecorder", "StageHandle", "StageMetrics"]
