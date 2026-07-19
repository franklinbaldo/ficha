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
        started_at, finished_at, extra.
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
            "started_at": self.started_at,
            "finished_at": self.finished_at,
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


class _DiskPeakSampler:
    """Sampler em thread daemon: soma o tamanho de diretórios periodicamente
    e guarda o máximo observado por diretório.

    Trade-off de amostragem: o sampler só olha o filesystem a cada
    `interval` segundos (default 5s). Um pico de disco que sobe e desce
    inteiramente ENTRE duas amostras escapa sem ser registrado. Para os
    estágios deste pipeline (minutos, não segundos) isso é aceitável -- o
    objetivo do baseline é a ordem de grandeza do pico sustentado, não o
    instante exato de um spill transitório de milissegundos. Custo por
    amostra é um `os.walk` sobre os diretórios monitorados; desprezível
    frente à duração típica de um estágio (extract/load/write rodam por
    dezenas de segundos a minutos, não os poucos ms que um walk custa).
    """

    def __init__(self, dirs: dict[str, Path], *, interval: float = 5.0) -> None:
        self._dirs = dirs
        self._interval = interval
        self._peaks: dict[str, int] = dict.fromkeys(dirs, 0)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self._dirs:
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

    def _update_peak(self, key: str, size: int) -> None:
        with self._lock:
            if size <= self._peaks[key]:
                return
            self._peaks[key] = size

    def stop(self) -> dict[str, int]:
        if not self._dirs:
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


def _code_version() -> str:
    """Versão do pacote ficha-etl instalado, ou 'unknown' fora de um venv com metadata."""
    import importlib.metadata

    try:
        return importlib.metadata.version("ficha-etl")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


class MetricsRecorder:
    """Coleta `StageMetrics` de estágios sucessivos e escreve `metrics.json` ao final.

    Um recorder por execução de `transform_snapshot`. Não é thread-safe entre
    estágios concorrentes -- os estágios do pipeline hoje são sequenciais, só
    o sampler de disco interno roda em thread própria.
    """

    def __init__(self, *, month: str, schema_version: str) -> None:
        self.month = month
        self.schema_version = schema_version
        self.stages: list[StageMetrics] = []
        self.pragmas: dict[str, str] = {}
        self._last_rss_mib = _rss_peak_mib()

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
        regra de "nenhum dict solto cruzando fronteira de módulo".

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

    @staticmethod
    def _start_sampler(
        duckdb_tmp_dir: Path | None, workdir: Path | None, sample_interval: float
    ) -> _DiskPeakSampler | None:
        watch: dict[str, Path] = {}
        if duckdb_tmp_dir is not None:
            watch["duckdb_tmp"] = duckdb_tmp_dir
        if workdir is not None:
            watch["workdir"] = workdir
        if not watch:
            return None
        sampler = _DiskPeakSampler(watch, interval=sample_interval)
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
        log.info(" ".join(parts))

    def to_envelope(self) -> dict[str, object]:
        """Payload completo de `metrics.json` (RFC 0001 §16/17)."""
        return {
            "code_version": _code_version(),
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


__all__ = ["MetricsRecorder", "StageHandle", "StageMetrics"]
