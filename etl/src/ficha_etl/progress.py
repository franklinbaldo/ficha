"""Fábrica compartilhada de barra de progresso (rich) pro CLI e transform.py.

Sempre desabilitada quando stdout não é um terminal (CI, log capturado em
arquivo) via `disable=`, em vez de depender de detecção implícita do rich —
sem isso, uma pipeline de horas rodando em CI acaba emitindo uma linha nova
a cada refresh automático em vez das mensagens `log.info` já existentes.
"""

from __future__ import annotations

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

_console = Console()


def make_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=_console,
        disable=not _console.is_terminal,
    )
