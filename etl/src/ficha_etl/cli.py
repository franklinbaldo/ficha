"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import download as download_mod
from . import mirror, smoke, sources, upstream


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(prog="ficha-etl", description="FICHA ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo para um mês")
    run.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")

    dl = sub.add_parser("download", help="Baixa o release atual da RFB para disco")
    dl.add_argument(
        "--target",
        type=Path,
        default=Path("./.cache/raw"),
        help="Diretório de destino (default: ./.cache/raw)",
    )

    sub.add_parser(
        "smoke",
        help="Valida que upstream RFB e mirror IA estão acessíveis",
    )

    sub.add_parser(
        "discover-token",
        help="Imprime o token Nextcloud da RFB descoberto via env / known / scrape",
    )

    args = parser.parse_args(argv)

    if args.command == "download":
        return _cmd_download(args.target)
    if args.command == "smoke":
        return _cmd_smoke()
    if args.command == "discover-token":
        return _cmd_discover_token()
    if args.command == "run":
        raise NotImplementedError(f"Pipeline ainda não implementado (alvo: {args.month})")

    return 0


def _cmd_download(target: Path) -> int:
    """Baixa todos os ZIPs do release atual da RFB. Token via discover."""
    try:
        tok = upstream.discover_token()
    except upstream.NoTokenFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Using token {tok.token} (source: {tok.source})")
    files = upstream.files_in_share(tok.token)
    target.mkdir(parents=True, exist_ok=True)
    results = download_mod.download_all(files, target)
    total = sum(r.size_bytes for r in results)
    print(f"downloaded {len(results)} files ({total:,} bytes) to {target}")
    return 0


def _cmd_smoke() -> int:
    print("Smoke check — upstream RFB + mirror IA")
    print()
    report = smoke.run_smoke()

    upstream_mark = "✓" if report.upstream_ok else "✗"
    mirror_mark = "✓" if report.mirror_ok else "✗"

    print(f"  {upstream_mark} upstream  {report.upstream_detail}")
    print(f"  {mirror_mark} mirror    {report.mirror_detail}")
    print()

    if report.all_ok:
        print("OK — upstream e mirror estão acessíveis")
        return 0

    if not report.upstream_ok:
        print(
            "WARNING: upstream RFB token discovery falhou.\n"
            "  Operator action: visite "
            "https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/"
            "cadastros/consultas/dados-publicos-cnpj\n"
            "  Encontre o link do share atual (formato "
            "arquivos.receitafederal.gov.br/s/{TOKEN})\n"
            "  Adicione o token novo a KNOWN_TOKENS em etl/src/ficha_etl/upstream.py "
            "via PR.",
            file=sys.stderr,
        )

    if report.blocking_failure:
        print("\nFAIL — mirror IA inacessível (bloqueante)", file=sys.stderr)
        return 1

    print("\nOK — mirror IA acessível (upstream warning não-bloqueante)")
    return 0


def _cmd_discover_token() -> int:
    try:
        result = upstream.discover_token()
    except upstream.NoTokenFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"token={result.token}")
    print(f"source={result.source}")
    return 0


# Re-export for backward compat with anyone importing sources.is_valid_month.
__all__ = ["main"]
_ = (sources, mirror)  # silence unused-import lints in editors


if __name__ == "__main__":
    sys.exit(main())
