"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import download as download_mod
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

    args = parser.parse_args(argv)

    if args.command == "download":
        return _cmd_download(args.month, args.target)
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


if __name__ == "__main__":
    sys.exit(main())
