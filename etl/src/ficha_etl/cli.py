"""Entrypoint do CLI: `ficha-etl ...`."""

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ficha-etl", description="FICHA ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo para um mês")
    run.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")

    args = parser.parse_args(argv)

    if args.command == "run":
        raise NotImplementedError(f"Pipeline ainda não implementado (alvo: {args.month})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
