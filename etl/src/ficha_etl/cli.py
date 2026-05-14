"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import download as download_mod
from . import (
    fetcher,
    manifest as manifest_mod,
    mirror,
    pack as pack_mod,
    smoke,
    sources,
    transform,
    upload as upload_mod,
    upstream,
)

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(prog="ficha-etl", description="FICHA ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Executa o pipeline completo para um mês")
    run.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")
    run.add_argument(
        "--cache-dir",
        type=Path,
        default=fetcher.DEFAULT_CACHE_DIR,
        help=f"Cache de ZIPs (default: {fetcher.DEFAULT_CACHE_DIR})",
    )
    run.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Diretório de saída dos parquets (default: <cache-dir>/<month>/output)",
    )
    run.add_argument(
        "--manifest",
        type=Path,
        default=Path("../web/public/manifest.json"),
        help="Caminho do manifest.json a atualizar (default: ../web/public/manifest.json)",
    )
    run.add_argument(
        "--skip-upload",
        action="store_true",
        help="Pula o upload para o IA — útil para smoke/testes locais",
    )
    run.add_argument(
        "--verify",
        action="store_true",
        default=True,
        help="Roda roundtrip-equivalence test após transform (default: ativado)",
    )
    run.add_argument(
        "--no-verify",
        dest="verify",
        action="store_false",
        help="Desativa o roundtrip-equivalence test",
    )
    run.add_argument(
        "--verify-sample-size",
        type=int,
        default=1000,
        help="Quantos CNPJs amostrar no roundtrip (default: 1000)",
    )

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

    tr = sub.add_parser(
        "transform",
        help=(
            "Roda o transform completo de um mês: resolve via chain → "
            "extract → DuckDB → escreve outputs (lookups.json sempre; "
            "parquets quando implementados)."
        ),
    )
    tr.add_argument("--month", required=True)
    tr.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Diretório onde escrever lookups.json + parquets",
    )
    tr.add_argument(
        "--cache-dir",
        type=Path,
        default=fetcher.DEFAULT_CACHE_DIR,
        help=f"Cache de ZIPs (default: {fetcher.DEFAULT_CACHE_DIR})",
    )
    tr.add_argument(
        "--strict",
        action="store_true",
        help="Falha se algum parquet stub não estiver implementado",
    )
    tr.add_argument(
        "--verify",
        action="store_true",
        help=(
            "Roda roundtrip-equivalence test (ADR 0009) após escrever os "
            "parquets — falha se sample de CNPJs divergir do source."
        ),
    )
    tr.add_argument(
        "--verify-sample-size",
        type=int,
        default=100,
        help="Quantos CNPJs amostrar no roundtrip (default: 100)",
    )

    pk = sub.add_parser(
        "pack",
        help="Empacota companies.zip a partir dos parquets do mês (IA ou local)",
    )
    pk.add_argument("--month", required=True, help="Snapshot alvo no formato YYYY-MM")
    pk.add_argument(
        "--output",
        type=Path,
        default=Path("companies.zip"),
        help="Caminho de saída do companies.zip (default: ./companies.zip)",
    )
    pk.add_argument(
        "--local-parquets",
        type=Path,
        default=None,
        help="Diretório local com os parquets; omitir lê do IA via httpfs",
    )
    pk.add_argument(
        "--memory-limit-gb",
        type=float,
        default=None,
        help="Limite de memória DuckDB em GB (default: sem limite explícito)",
    )
    pk.add_argument(
        "--skip-upload",
        action="store_true",
        help="Gera o ZIP mas não faz upload para o IA",
    )

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
    if args.command == "transform":
        return _cmd_transform(
            args.month,
            args.output,
            args.cache_dir,
            args.strict,
            args.verify,
            args.verify_sample_size,
        )
    if args.command == "pack":
        return _cmd_pack(
            args.month,
            output=args.output,
            local_parquets=args.local_parquets,
            memory_limit_gb=args.memory_limit_gb,
            skip_upload=args.skip_upload,
        )
    if args.command == "run":
        return _cmd_run(
            args.month,
            cache_dir=args.cache_dir,
            output_dir=args.output,
            manifest_path=args.manifest,
            skip_upload=args.skip_upload,
            verify=args.verify,
            verify_sample_size=args.verify_sample_size,
        )

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


def _cmd_transform(
    month: str,
    output: Path,
    cache_dir: Path,
    strict: bool,
    verify: bool,
    verify_sample_size: int,
) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    try:
        transform.transform_snapshot(
            month,
            cache_dir=cache_dir,
            output_dir=output,
            skip_unimplemented=not strict,
            verify=verify,
            verify_sample_size=verify_sample_size,
        )
    except (FileNotFoundError, NotImplementedError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"transform OK — outputs em {output}")
    return 0


def _cmd_fetch(month: str, filename: str, cache_dir: Path, no_upstream: bool) -> int:
    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2
    chain = fetcher.default_chain(month, cache_dir=cache_dir, include_upstream=not no_upstream)
    print(f"Chain: {' → '.join(f.name for f in chain.fetchers)}", file=sys.stderr)
    try:
        path = chain.get(filename)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(path)
    return 0


def _cmd_pack(
    month: str,
    *,
    output: Path,
    local_parquets: Path | None,
    memory_limit_gb: float | None,
    skip_upload: bool,
) -> int:
    """Empacota companies.zip e opcionalmente faz upload para o IA."""
    import os

    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2

    parquets_base = str(local_parquets) if local_parquets else None
    log.info("[pack] building companies.zip for %s", month)
    try:
        result = pack_mod.pack_from_parquets(
            month,
            output,
            parquets_base=parquets_base,
            memory_limit_gb=memory_limit_gb,
        )
    except Exception as exc:
        print(f"error: pack failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"pack OK — {result['count']:,} companies, "
        f"{result['size_bytes'] / 1e6:.1f} MB → {output}"
    )

    if not skip_upload:
        access_key = os.environ.get("IA_ACCESS_KEY", "")
        secret_key = os.environ.get("IA_SECRET_KEY", "")
        if not access_key or not secret_key:
            print(
                "error: IA_ACCESS_KEY e IA_SECRET_KEY devem estar definidos para upload\n"
                "       use --skip-upload para rodar sem credenciais",
                file=sys.stderr,
            )
            return 1
        try:
            upload_mod.upload_companies_zip(
                month, output, access_key=access_key, secret_key=secret_key
            )
        except Exception as exc:
            print(f"error: upload companies.zip falhou: {exc}", file=sys.stderr)
            return 1
        print(f"upload OK — ia:ficha-{month}/companies.zip")

    return 0


def _cmd_run(
    month: str,
    *,
    cache_dir: Path,
    output_dir: Path | None,
    manifest_path: Path,
    skip_upload: bool,
    verify: bool,
    verify_sample_size: int,
) -> int:
    """Orquestra: stream ZIPs → IA → transform → upload parquets → pack → manifest.

    Com upload ativado (default):
      1. Stream 37 ZIPs da RFB → IA (zero disco, paralelo)
      2. Transform: baixa do IA (rápido) → DuckDB → parquets
      3. Upload parquets + lookups.json → IA
      4. Pack: parquets locais → companies.zip → upload IA
      5. Atualiza manifest.json

    Com --skip-upload (dry-run):
      1. Transform: baixa da RFB → DuckDB → parquets
      2. Pack: parquets locais → companies.zip (sem upload)
      3. Atualiza manifest.json (sem upload)
    """
    import os

    if not sources.is_valid_month(month):
        print(f"error: month must be YYYY-MM, got {month!r}", file=sys.stderr)
        return 2

    if output_dir is None:
        output_dir = cache_dir / month / "output"

    if not skip_upload:
        access_key = os.environ.get("IA_ACCESS_KEY", "")
        secret_key = os.environ.get("IA_SECRET_KEY", "")
        if not access_key or not secret_key:
            print(
                "error: IA_ACCESS_KEY e IA_SECRET_KEY devem estar definidos para upload\n"
                "       use --skip-upload para rodar sem credenciais",
                file=sys.stderr,
            )
            return 1

        # ── 1. Stream ZIPs → IA (zero disco) ────────────────────────────────
        log.info("[run 1/5] stream ZIPs RFB → IA (zero disco)")
        try:
            upload_mod.stream_raw_zips_to_ia(month, access_key=access_key, secret_key=secret_key)
        except Exception as exc:
            print(f"error: stream ZIPs falhou: {exc}", file=sys.stderr)
            return 1
    else:
        log.info("[run 1/5] stream ignorado (--skip-upload) — transform usará RFB direto")

    # ── 2. Transform ─────────────────────────────────────────────────────────
    # Com upload: fetcher chain encontra ZIPs no IA (rápido).
    # Sem upload: fetcher chain vai direto na RFB.
    log.info("[run 2/5] transform %s → %s", month, output_dir)
    try:
        transform.transform_snapshot(
            month,
            cache_dir=cache_dir,
            output_dir=output_dir,
            skip_unimplemented=False,
            verify=verify,
            verify_sample_size=verify_sample_size,
        )
    except Exception as exc:
        print(f"error: transform failed: {exc}", file=sys.stderr)
        return 1

    # ── 3. Upload parquets ───────────────────────────────────────────────────
    if not skip_upload:
        log.info("[run 3/5] upload outputs (parquets + lookups.json) → IA")
        try:
            upload_mod.upload_outputs(
                month, output_dir, access_key=access_key, secret_key=secret_key
            )
        except Exception as exc:
            print(f"error: upload outputs falhou: {exc}", file=sys.stderr)
            return 1
    else:
        log.info("[run 3/5] upload ignorado (--skip-upload)")

    # ── 4. Pack companies.zip ────────────────────────────────────────────────
    companies_zip = output_dir / "companies.zip"
    log.info("[run 4/5] pack companies.zip ← parquets locais → %s", companies_zip)
    try:
        result = pack_mod.pack_from_parquets(month, companies_zip, parquets_base=str(output_dir))
    except Exception as exc:
        print(f"error: pack companies.zip falhou: {exc}", file=sys.stderr)
        return 1
    log.info(
        "[run 4/5] pack OK — %d companies, %.1f MB",
        result["count"],
        result["size_bytes"] / 1e6,
    )

    if not skip_upload:
        log.info("[run 4/5] upload companies.zip → IA")
        try:
            upload_mod.upload_companies_zip(
                month, companies_zip, access_key=access_key, secret_key=secret_key
            )
        except Exception as exc:
            print(f"error: upload companies.zip falhou: {exc}", file=sys.stderr)
            return 1

    # ── 5. Manifest ─────────────────────────────────────────────────────────
    log.info("[run 5/5] atualizar manifest → %s", manifest_path)
    try:
        entry = manifest_mod.build_snapshot_entry(month, output_dir)
        manifest_mod.update_manifest(manifest_path, entry)
    except Exception as exc:
        print(f"error: manifest update falhou: {exc}", file=sys.stderr)
        return 1

    companies_size_mb = companies_zip.stat().st_size / 1e6 if companies_zip.exists() else 0
    print(
        f"run OK — pipeline {month} concluído. "
        f"companies.zip: {companies_size_mb:.1f} MB. Manifest: {manifest_path}"
    )
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
