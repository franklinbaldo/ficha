"""Fábrica compartilhada de barra de progresso (rich) pro CLI e transform.py.

Sempre desabilitada quando stdout não é um terminal (CI, log capturado em
arquivo) via `disable=`, em vez de depender de detecção implícita do rich —
sem isso, uma pipeline de horas rodando em CI acaba emitindo uma linha nova
a cada refresh automático em vez das mensagens `log.info` já existentes.

Nesse caso (não-terminal), um heartbeat de baixo custo assume o lugar da
barra: loga a descrição do estágio atual uma vez por minuto. Alguns passos
individuais do pipeline (ex.: `cnpj_cnaes.parquet` em ~370s) rodam vários
minutos entre uma linha de log e a próxima sem esse heartbeat — útil pra
saber que o job não travou sem precisar reabrir os logs do Actions toda
hora. Só ativa quando a barra ao vivo está desabilitada: terminal
interativo já mostra progresso ao vivo e não precisa do heartbeat também.
"""

from __future__ import annotations

import logging
import threading
import time

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)

log = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL_S = 60.0


class _HeartbeatProgress(Progress):
    """Progress que também loga um heartbeat periódico quando não-terminal.

    Mesma API do `rich.progress.Progress` (`start`/`stop`/`add_task`/
    `update`) — chamadores existentes não precisam saber que o heartbeat
    existe.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hb_description = ""
        self._hb_start = 0.0
        self._hb_thread: threading.Thread | None = None
        self._hb_stop = threading.Event()

    def start(self) -> None:
        super().start()
        if not self.console.is_terminal and self._hb_thread is None:
            self._hb_start = time.monotonic()
            self._hb_stop.clear()
            self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._hb_thread.start()

    def stop(self) -> None:
        self._hb_stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=2)
            self._hb_thread = None
        super().stop()

    def add_task(self, description: str, **kwargs) -> TaskID:
        self._hb_description = description
        return super().add_task(description, **kwargs)

    def update(self, task_id: TaskID, **kwargs) -> None:
        super().update(task_id, **kwargs)
        description = kwargs.get("description")
        if description:
            self._hb_description = description

    def _heartbeat_loop(self) -> None:
        while not self._hb_stop.wait(_HEARTBEAT_INTERVAL_S):
            elapsed = time.monotonic() - self._hb_start
            log.info("... ainda rodando: %s (%.0fs decorridos)", self._hb_description, elapsed)


_console = Console()


def make_progress() -> Progress:
    return _HeartbeatProgress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=_console,
        disable=not _console.is_terminal,
    )
