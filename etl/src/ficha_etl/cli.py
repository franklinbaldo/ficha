"""Entrypoint do CLI: `ficha-etl ...`."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console
from rich.logging import RichHandler

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
from .progress import make_progress

log = logging.getLogger(__name__)

# Console (not raw print/sys.stdout): rich detects the real console encoding
# instead of trusting the OS default, which is what broke `ficha-etl` on
# Windows before -- CLI output uses non-ASCII (→, —, ✓, ✗) throughout, and
# Windows' default console codepage (cp1252) can't encode those, crashing
# with UnicodeEncodeError. `markup=False` on every call: several messages
# below contain literal `[...]` (e.g. "[run 1/5] ..."), which rich would
# otherwise try to parse as markup tags. Auto-disables ANSI/color when the
# stream isn't a terminal (CI logs, `tee run.log`), matching progress.py's
# existing Console.
_out = Console()
_err = Console(stderr=True)


def _print(*args: object, **kwargs: object) -> None:
    _out.print(*args, markup=False, **kwargs)


def _eprint(*args: object, **kwargs: object) -> None:
    _err.print(*args, markup=False, **kwargs)


app = App(name="ficha-etl", help="FICHA ETL pipeline")


@app.command
def run(
    *,
    month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")],
    cache_dir: Annotated[
        Path, Parameter(help=f"Cache de ZIPs (default: {fetcher.DEFAULT_CACHE_DIR})")
    ] = fetcher.DEFAULT_CACHE_DIR,
    output: Annotated[
        Path | None,
        Parameter(help="Diretório de saída dos parquets (default: <cache-dir>/<month>/output)"),
    ] = None,
    manifest: Annotated[
        Path,
        Parameter(
            help="Caminho do manifest.json a atualizar (default: ../web/public/manifest.json)"
        ),
    ] = Path("../web/public/manifest.json"),
    skip_upload: Annotated[
        bool, Parameter(help="Pula o upload para o IA — útil para smoke/testes locais")
    ] = False,
    verify: Annotated[
        bool, Parameter(help="Roda roundtrip-equivalence test após transform")
    ] = True,
    verify_sample_size: Annotated[
        int, Parameter(help="Quantos CNPJs amostrar no roundtrip")
    ] = 1000,
) -> int:
    """Executa o pipeline completo para um mês."""
    return _cmd_run(
        month,
        cache_dir=cache_dir,
        output_dir=output,
        manifest_path=manifest,
        skip_upload=skip_upload,
        verify=verify,
        verify_sample_size=verify_sample_size,
    )


@app.command
def download(
    *,
    month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")],
    target: Annotated[Path, Parameter(help="Diretório de destino")] = Path("./.cache/raw"),
) -> int:
    """Baixa todos os ZIPs de um mês via WebDAV pra disco."""
    return _cmd_download(month, target)


@app.command(name="smoke")
def smoke_cmd() -> int:
    """Valida que upstream RFB e mirror IA estão acessíveis."""
    return _cmd_smoke()


@app.command(name="list-snapshots")
def list_snapshots() -> int:
    """Lista os meses disponíveis no Nextcloud da RFB."""
    return _cmd_list_snapshots()


@app.command(name="list-files")
def list_files(*, month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")]) -> int:
    """Lista arquivos de um snapshot mensal."""
    return _cmd_list_files(month)


@app.command(name="transform")
def transform_cmd(
    *,
    month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")],
    output: Annotated[Path, Parameter(help="Diretório onde escrever lookups.json + parquets")],
    cache_dir: Annotated[
        Path, Parameter(help=f"Cache de ZIPs (default: {fetcher.DEFAULT_CACHE_DIR})")
    ] = fetcher.DEFAULT_CACHE_DIR,
    strict: Annotated[
        bool, Parameter(help="Falha se algum parquet stub não estiver implementado")
    ] = False,
    verify: Annotated[
        bool,
        Parameter(
            help="Roda roundtrip-equivalence test (ADR 0009) após escrever os parquets — "
            "falha se sample de CNPJs divergir do source."
        ),
    ] = False,
    verify_sample_size: Annotated[int, Parameter(help="Quantos CNPJs amostrar no roundtrip")] = 100,
) -> int:
    """Roda o transform completo de um mês: resolve via chain -> extract -> DuckDB
    -> escreve outputs (lookups.json sempre; parquets quando implementados)."""
    return _cmd_transform(month, output, cache_dir, strict, verify, verify_sample_size)


@app.command(name="pack")
def pack(
    *,
    month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")],
    output: Annotated[Path, Parameter(help="Caminho de saída do companies.zip")] = Path(
        "companies.zip"
    ),
    local_parquets: Annotated[
        Path | None,
        Parameter(help="Diretório local com os parquets; omitir lê do IA via httpfs"),
    ] = None,
    memory_limit_gb: Annotated[
        float | None,
        Parameter(help="Limite de memória DuckDB em GB (default: sem limite explícito)"),
    ] = None,
    skip_upload: Annotated[bool, Parameter(help="Gera o ZIP mas não faz upload para o IA")] = False,
) -> int:
    """Empacota companies.zip a partir dos parquets do mês (IA ou local)."""
    return _cmd_pack(
        month,
        output=output,
        local_parquets=local_parquets,
        memory_limit_gb=memory_limit_gb,
        skip_upload=skip_upload,
    )


@app.command(name="fetch")
def fetch(
    *,
    month: Annotated[str, Parameter(help="Snapshot alvo no formato YYYY-MM")],
    file: Annotated[str, Parameter(help="Nome do arquivo (ex.: Empresas0.zip)")],
    cache_dir: Annotated[
        Path, Parameter(help=f"Diretório de cache (default: {fetcher.DEFAULT_CACHE_DIR})")
    ] = fetcher.DEFAULT_CACHE_DIR,
    no_upstream: Annotated[
        bool, Parameter(help="Não cair no RFB upstream se cache + IA mirror falharem")
    ] = False,
) -> int:
    """Resolve um arquivo via chain: cache local -> IA mirror -> RFB upstream.
    Devolve o caminho local do arquivo após download (se necessário)."""
    return _cmd_fetch(month, file, cache_dir, no_upstream)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=_err, markup=False, show_path=False, show_time=True)],
    )

    result = app(argv, result_action="return_value")
    return result if isinstance(result, int) else 0


def _cmd_download(month: str, target: Path) -> int:
    if not sources.is_valid_month(month):
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
        return 2
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        _eprint(f"error: {exc}")
        return 1
    files = upstream.files_for_month(token, month)
    target.mkdir(parents=True, exist_ok=True)
    auth_headers = _basic_auth_headers(token)
    results = download_mod.download_all(files, target, extra_headers=auth_headers)
    total = sum(r.size_bytes for r in results)
    _print(f"downloaded {len(results)} files ({total:,} bytes) to {target}")
    return 0


def _cmd_smoke() -> int:
    _print("Smoke check — upstream RFB + mirror IA")
    _print()
    report = smoke.run_smoke()
    upstream_mark = "✓" if report.upstream_ok else "✗"
    mirror_mark = "✓" if report.mirror_ok else "✗"
    _print(f"  {upstream_mark} upstream  {report.upstream_detail}")
    _print(f"  {mirror_mark} mirror    {report.mirror_detail}")
    _print()
    if report.all_ok:
        _print("OK — upstream e mirror estão acessíveis")
        return 0
    if not report.upstream_ok:
        _eprint(
            "WARNING: upstream RFB inacessível.\n"
            "  Verifique se o token em KNOWN_TOKENS ainda funciona em\n"
            "  https://arquivos.receitafederal.gov.br/index.php/s/{TOKEN}\n"
            "  Se rotacionou, atualize via PR em etl/src/ficha_etl/upstream.py."
        )
    if report.blocking_failure:
        _eprint("\nFAIL — mirror IA inacessível (bloqueante)")
        return 1
    _print("\nOK — mirror IA acessível (upstream warning não-bloqueante)")
    return 0


def _cmd_list_snapshots() -> int:
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        _eprint(f"error: {exc}")
        return 1
    snapshots = upstream.list_snapshots(token)
    if not snapshots:
        # Lista vazia nunca é normal (upstream tem 35+ meses desde 2023-05).
        # Exit 0 aqui já mascarou dois crons mensais: o workflow lia stdout
        # vazio e falhava adiante com "Invalid month ''" sem apontar a causa.
        _eprint(
            "error: no YYYY-MM folders found on RFB upstream — "
            "PROPFIND respondeu, mas sem pastas de snapshot (layout mudou? resposta truncada?)"
        )
        return 1
    for s in snapshots:
        _print(s)
    _eprint(f"\n{len(snapshots)} snapshots")
    return 0


def _cmd_list_files(month: str) -> int:
    if not sources.is_valid_month(month):
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
        return 2
    try:
        token = upstream.discover_token()
    except upstream.NoTokenError as exc:
        _eprint(f"error: {exc}")
        return 1
    files = upstream.list_files(token, month)
    total = 0
    for f in files:
        _print(f"{f.size:>14,}  {f.name}")
        total += f.size
    _eprint(f"\n{len(files)} files, {total:,} bytes total")
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
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
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
        _eprint(f"error: {exc}")
        return 1
    _print(f"transform OK — outputs em {output}")
    return 0


def _cmd_fetch(month: str, filename: str, cache_dir: Path, no_upstream: bool) -> int:
    if not sources.is_valid_month(month):
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
        return 2
    chain = fetcher.default_chain(month, cache_dir=cache_dir, include_upstream=not no_upstream)
    _eprint(f"Chain: {' → '.join(f.name for f in chain.fetchers)}")
    try:
        path = chain.get(filename)
    except FileNotFoundError as exc:
        _eprint(f"error: {exc}")
        return 1
    _print(path)
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
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
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
        _eprint(f"error: pack failed: {exc}")
        return 1

    _print(
        f"pack OK — {result['count']:,} companies, {result['size_bytes'] / 1e6:.1f} MB → {output}"
    )

    if not skip_upload:
        access_key = os.environ.get("IA_ACCESS_KEY", "")
        secret_key = os.environ.get("IA_SECRET_KEY", "")
        if not access_key or not secret_key:
            _eprint(
                "error: IA_ACCESS_KEY e IA_SECRET_KEY devem estar definidos para upload\n"
                "       use --skip-upload para rodar sem credenciais"
            )
            return 1
        try:
            upload_mod.upload_companies_zip(
                month, output, access_key=access_key, secret_key=secret_key
            )
        except Exception as exc:
            _eprint(f"error: upload companies.zip falhou: {exc}")
            return 1
        _print(f"upload OK — ia:ficha-{month}/companies.zip")

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
        _eprint(f"error: month must be YYYY-MM, got {month!r}")
        return 2

    if output_dir is None:
        output_dir = cache_dir / month / "output"

    progress = make_progress()
    progress.start()
    stage = progress.add_task("run", total=5)

    def _advance(description: str) -> None:
        progress.update(stage, description=description, advance=1)

    try:
        if not skip_upload:
            access_key = os.environ.get("IA_ACCESS_KEY", "")
            secret_key = os.environ.get("IA_SECRET_KEY", "")
            if not access_key or not secret_key:
                _eprint(
                    "error: IA_ACCESS_KEY e IA_SECRET_KEY devem estar definidos para upload\n"
                    "       use --skip-upload para rodar sem credenciais"
                )
                return 1

            # ── 1. Stream ZIPs → IA (zero disco) ────────────────────────
            log.info("[run 1/5] stream ZIPs RFB → IA (zero disco)")
            try:
                upload_mod.stream_raw_zips_to_ia(
                    month, access_key=access_key, secret_key=secret_key
                )
            except Exception as exc:
                _eprint(f"error: stream ZIPs falhou: {exc}")
                return 1
        else:
            log.info("[run 1/5] stream ignorado (--skip-upload) — transform usará RFB direto")
        _advance("[run 2/5] transform")

        # ── 2. Transform ─────────────────────────────────────────────────────
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
                progress=progress,
            )
        except Exception as exc:
            _eprint(f"error: transform failed: {exc}")
            return 1
        _advance("[run 3/5] upload outputs")

        # ── 3. Upload parquets ───────────────────────────────────────────────
        if not skip_upload:
            log.info("[run 3/5] upload outputs (parquets + lookups.json) → IA")
            try:
                upload_mod.upload_outputs(
                    month, output_dir, access_key=access_key, secret_key=secret_key
                )
            except Exception as exc:
                _eprint(f"error: upload outputs falhou: {exc}")
                return 1
        else:
            log.info("[run 3/5] upload ignorado (--skip-upload)")
        _advance("[run 4/5] pack companies.zip")

        # ── 4. Pack companies.zip ─────────────────────────────────────────────
        companies_zip = output_dir / "companies.zip"
        log.info("[run 4/5] pack companies.zip ← parquets locais → %s", companies_zip)
        try:
            result = pack_mod.pack_from_parquets(
                month, companies_zip, parquets_base=str(output_dir)
            )
        except Exception as exc:
            _eprint(f"error: pack companies.zip falhou: {exc}")
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
                _eprint(f"error: upload companies.zip falhou: {exc}")
                return 1
        _advance("[run 5/5] manifest")

        # ── 5. Manifest ───────────────────────────────────────────────────────
        log.info("[run 5/5] atualizar manifest → %s", manifest_path)
        try:
            entry = manifest_mod.build_snapshot_entry(month, output_dir)
        except Exception as exc:
            _eprint(f"error: manifest build falhou: {exc}")
            return 1

        if not skip_upload:
            # Local existence (build_snapshot_entry) e upload HTTP OK
            # (upload_outputs) não garantem que o arquivo segue baixável — IA
            # processa uploads assincronamente. Sem essa checagem o manifest
            # pode acabar publicado com URLs 404 (foi o que aconteceu com
            # cnpj_contatos/cnpj_cnaes em 2026-04). Só faz sentido com
            # --skip-upload desligado: em dry-run local nada foi upado ainda.
            log.info("[run 5/5] verificando que todos os arquivos publicados respondem 200")
            broken = manifest_mod.verify_snapshot_files(entry)
            if broken:
                _eprint(
                    "error: manifest não publicado — arquivos declarados mas inacessíveis no IA:\n"
                    + "\n".join(f"  {u}" for u in broken)
                )
                return 1

        try:
            manifest_mod.update_manifest(manifest_path, entry)
        except Exception as exc:
            _eprint(f"error: manifest update falhou: {exc}")
            return 1

        progress.update(stage, description="[run 5/5] done", completed=5)
    finally:
        progress.stop()

    companies_size_mb = companies_zip.stat().st_size / 1e6 if companies_zip.exists() else 0
    _print(
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
