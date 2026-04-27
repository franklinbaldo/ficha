"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import download as download_mod
from . import smoke as smoke_mod
from . import sources


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(prog="ficha-etl", description="FICHA ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo para um mês")
    run.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")

    dl = sub.add_parser("download", help="Apenas baixa os ZIPs de um mês")
    dl.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")
    dl.add_argument(
        "--target",
        type=Path,
        default=Path("./.cache/raw"),
        help="Diretório de destino (default: ./.cache/raw)",
    )

    sm = sub.add_parser(
        "smoke",
        help="HEAD em cada URL para detectar mudanças na fonte sem baixar bytes",
    )
    sm.add_argument(
        "--month",
        help=(
            "Snapshot alvo (YYYY-MM). Se omitido, descobre o mês mais recente "
            "disponível probing os últimos 6 meses."
        ),
    )

    args = parser.parse_args(argv)

    if args.command == "download":
        return _cmd_download(args.month, args.target)
    if args.command == "smoke":
        return _cmd_smoke(args.month)  # may be None → auto-discover
    if args.command == "run":
        raise NotImplementedError(f"Pipeline ainda não implementado (alvo: {args.month})")

    return 0


def _cmd_download(month: str, target: Path) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    files = sources.files_for_month(month)
    target.mkdir(parents=True, exist_ok=True)
    results = download_mod.download_all(files, target)
    total = sum(r.size_bytes for r in results)
    print(f"downloaded {len(results)} files ({total:,} bytes) to {target}")
    return 0


def _cmd_smoke(month: str | None) -> int:
    if month is None:
        print(f"Auto-discovering latest published month (base_url={sources.base_url()})...")
        month = smoke_mod.find_latest_available_month()
        if month is None:
            print(
                "error: no recent month responded — RFB indisponível ou URL mudou",
                file=sys.stderr,
            )
            return 1
        print(f"Latest available month: {month}")
    elif not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2

    print(f"Smoke target: month={month}  base_url={sources.base_url()}")
    files = sources.files_for_month(month)
    results = smoke_mod.smoke_check(files)
    failed = [r for r in results if not r.ok]
    for r in results:
        size_str = f"{r.size:>12,} bytes" if r.size else " " * 18
        if r.ok:
            print(f"  ok  {r.status}  {size_str}  {r.file.name}")
        else:
            err = r.error or f"HTTP {r.status}"
            print(f"FAIL       {size_str}  {r.file.name}  — {err}", file=sys.stderr)
    print(f"\n{len(results) - len(failed)}/{len(results)} URLs OK")

    if failed:
        # Probe pai pra distinguir "RFB fora" de "mês não publicado".
        url, status, err = smoke_mod.diagnose_root(month)
        if err:
            print(f"\nDiagnóstico — pasta do mês: {url}  ERRO: {err}", file=sys.stderr)
        else:
            print(f"\nDiagnóstico — pasta do mês: {url}  HTTP {status}", file=sys.stderr)
            if status == 404:
                print(
                    "  → mês ainda não publicado pelo RFB. Tente um mês anterior.",
                    file=sys.stderr,
                )
            elif status and 500 <= status < 600:
                print(
                    "  → RFB indisponível (5xx). Não bloquear merge — re-rodar depois.",
                    file=sys.stderr,
                )

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
