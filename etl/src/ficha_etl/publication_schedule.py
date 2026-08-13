"""Seleção sequencial da próxima competência publicável.

`2026-06` é o baseline do pipeline com identidade fechada na produção. O modo
automático nunca escolhe simplesmente o snapshot mais novo do upstream: publica
uma competência por vez e falha se o próximo mês esperado estiver ausente
enquanto meses posteriores já existem.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterable
from pathlib import Path

from .sources import is_valid_month

PUBLICATION_BASELINE = "2026-06"
_MONTH_RE = re.compile(r"\b\d{4}-(?:0[1-9]|1[0-2])\b")


class PublicationGapError(RuntimeError):
    """Há meses posteriores disponíveis, mas a próxima competência está ausente."""


def _next_month(month: str) -> str:
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    year, value = map(int, month.split("-"))
    if value == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{value + 1:02d}"


def parse_available_months(text: str) -> tuple[str, ...]:
    """Extrai e ordena competências de uma saída humana de `list-snapshots`."""
    return tuple(sorted(set(_MONTH_RE.findall(text))))


def next_publication_month(
    current: str | None,
    available: Iterable[str],
    *,
    baseline: str = PUBLICATION_BASELINE,
) -> str | None:
    """Retorna a única competência que o modo automático pode publicar.

    Antes do baseline, a história antiga é deliberadamente ignorada e o alvo é
    o próprio baseline. A partir dele, o alvo é sempre `current + 1`.

    Se o upstream ainda não chegou ao alvo, retorna ``None``. Se o alvo falta
    mas meses posteriores já existem, falha: ausência negativa não autoriza
    pular uma competência.
    """
    if not is_valid_month(baseline):
        raise ValueError(f"baseline must be YYYY-MM, got {baseline!r}")
    if current and not is_valid_month(current):
        raise ValueError(f"current must be YYYY-MM, got {current!r}")

    months = tuple(sorted(set(available)))
    invalid = [month for month in months if not is_valid_month(month)]
    if invalid:
        raise ValueError(f"invalid available months: {invalid}")
    if not months:
        return None

    target = baseline if not current or current < baseline else _next_month(current)
    if target in months:
        return target

    later = [month for month in months if month > target]
    if later:
        raise PublicationGapError(
            f"expected next publication {target}, but upstream already has later months: {later}"
        )
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", default="")
    parser.add_argument("--available-file", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    months = parse_available_months(args.available_file.read_text(encoding="utf-8"))
    target = next_publication_month(args.current or None, months)
    if target is not None:
        print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
