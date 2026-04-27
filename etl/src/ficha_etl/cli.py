"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import download as download_mod
from . import fetcher, mirror, smoke, sources, upstream


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(prog="ficha-etl", description="FICHA ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo para um mês")
    run.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")

    dl = sub.add_parser(
        "download",
        help="Baixa todos os ZIPs de um mês via WebDAV pra disco",
    )
    dl.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")
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
        "list-snapshots",
        help="Lista os meses disponíveis no Nextcloud da RFB",
    )

    ls = sub.add_parser("list-files", help="Lista arquivos de um snapshot mensal")
    ls.add_argument("--month", required=True)

    ft = sub.add_parser(
        "fetch",
        help=(
            "Resolve um arquivo via chain: cache local → IA mirror → RFB upstream. "
            "Devolve o caminho local do arquivo após download (se necessário)."
        ),
    )
    ft.add_argument("--month", required=True)
    ft.add_argument("--file", required=True, help="Nome do arquivo (ex.: Empresas0.zip)")
    ft.add_argument(
        "--cache-dir",
        type=Path,
        default=fetcher.DEFAULT_CACHE_DIR,
        help=f"Diretório de cache (default: {fetcher.DEFAULT_CACHE_DIR})",
    )
    ft.add_argument(
        "--no-upstream",
        action="store_true",
        help="Não cair no RFB upstream se cache + IA mirror falharem",
    )

    args = parser.parse_args(argv)

    if args.command == "download":
        return _cmd_download(args.month, args.target)
    if args.command == "smoke":
        return _cmd_smoke()
    if args.command == "list-snapshots":
        return _cmd_list_snapshots()
    if args.command == "list-files":
        return _cmd_list_files(args.month)
    if args.command == "fetch":
        return _cmd_fetch(args.month, args.file, args.cache_dir, args.no_upstream)
    if args.command == "run":
        raise NotImplementedError(f"Pipeline ainda não implementado (alvo: {args.month})")

    return 0


def _cmd_download(month: str, target: Path) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    files = upstream.files_for_month(token, month)
    target.mkdir(parents=True, exist_ok=True)
    auth_headers = _basic_auth_headers(token)
    results = download_mod.download_all(files, target, extra_headers=auth_headers)
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
            "WARNING: upstream RFB inacessível.\n"
            "  Verifique se o token em KNOWN_TOKENS ainda funciona em\n"
            "  https://arquivos.receitafederal.gov.br/index.php/s/{TOKEN}\n"
            "  Se rotacionou, atualize via PR em etl/src/ficha_etl/upstream.py.",
            file=sys.stderr,
        )
    if report.blocking_failure:
        print("\nFAIL — mirror IA inacessível (bloqueante)", file=sys.stderr)
        return 1
    print("\nOK — mirror IA acessível (upstream warning não-bloqueante)")
    return 0


def _cmd_list_snapshots() -> int:
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    snapshots = upstream.list_snapshots(token)
    for s in snapshots:
        print(s)
    print(f"\n{len(snapshots)} snapshots", file=sys.stderr)
    return 0


def _cmd_list_files(month: str) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    files = upstream.list_files(token, month)
    total = 0
    for f in files:
        print(f"{f.size:>14,}  {f.name}")
        total += f.size
    print(f"\n{len(files)} files, {total:,} bytes total", file=sys.stderr)
    return 0


def _cmd_fetch(month: str, filename: str, cache_dir: Path, no_upstream: bool) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    chain = fetcher.default_chain(
        month, cache_dir=cache_dir, include_upstream=not no_upstream
    )
    print(f"Chain: {' → '.join(f.name for f in chain.fetchers)}", file=sys.stderr)
    try:
        path = chain.get(filename)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(path)
    return 0


def _basic_auth_headers(token: str) -> dict[str, str]:
    """Headers Basic auth manuais — usados pra passar pro download.py via httpx."""
    import base64
    raw = base64.b64encode(f"{token}:".encode()).decode()
    return {"Authorization": f"Basic {raw}"}


__all__ = ["main"]
_ = (mirror,)


if __name__ == "__main__":
    sys.exit(main())
