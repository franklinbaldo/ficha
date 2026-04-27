"""Upload de outputs do ETL pro Internet Archive.

Por snapshot mensal cria/atualiza um item `ficha-YYYY-MM` com:

    raw/Empresas0.zip  ...  (mirror dos ZIPs RFB originais)
    cnpjs.parquet
    raizes.parquet
    socios.parquet
    lookups.json

Usa a biblioteca `internetarchive` (S3-like API). Idempotente: chamadas
repetidas só re-uploadam arquivos que mudaram.

Credenciais via env (IA_ACCESS_KEY / IA_SECRET_KEY) ou config padrão da lib.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from internetarchive import get_session

from . import mirror

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadPlan:
    """O conjunto de arquivos que vão pro item IA, com path remoto relativo."""

    item_id: str
    files: dict[str, Path]  # remote_name → local_path


def build_upload_plan(
    month: str,
    *,
    output_dir: Path,
    raw_dir: Path | None = None,
) -> UploadPlan:
    """Monta o plano de upload: outputs do transform + (opcional) raw mirror."""
    item_id = mirror.item_id(month)
    files: dict[str, Path] = {}
    for name in ("cnpjs.parquet", "raizes.parquet", "socios.parquet", "lookups.json"):
        path = output_dir / name
        if not path.exists():
            raise FileNotFoundError(f"expected output missing: {path}")
        files[name] = path
    if raw_dir is not None:
        if not raw_dir.is_dir():
            raise FileNotFoundError(f"raw dir not found: {raw_dir}")
        for zip_path in sorted(raw_dir.glob("*.zip")):
            files[f"raw/{zip_path.name}"] = zip_path
    return UploadPlan(item_id=item_id, files=files)


def _credentials() -> tuple[str | None, str | None]:
    return (
        os.environ.get("IA_ACCESS_KEY") or None,
        os.environ.get("IA_SECRET_KEY") or None,
    )


def upload_snapshot(
    plan: UploadPlan,
    *,
    metadata: dict[str, str] | None = None,
    verbose: bool = True,
) -> dict[str, str]:
    """Executa o plano. Devolve {remote_name: status} (status = 'uploaded'|'skipped'|status_code).

    Por default, `internetarchive` skipa upload se o arquivo remoto tiver o
    mesmo MD5 — tornando a função idempotente. Em caso de falha de rede,
    cada arquivo retorna o status code da response.
    """
    access_key, secret_key = _credentials()
    if not access_key or not secret_key:
        raise RuntimeError(
            "IA_ACCESS_KEY/IA_SECRET_KEY não configurados (ver "
            "https://archive.org/account/s3.php)"
        )

    md = {
        "title": f"FICHA — Snapshot CNPJ {plan.item_id.split('-', 1)[1]}",
        "mediatype": "data",
        "collection": "opensource_misc",
        "language": "por",
        **(metadata or {}),
    }

    session = get_session(
        config={"s3": {"access": access_key, "secret": secret_key}}
    )
    item = session.get_item(plan.item_id)

    results: dict[str, str] = {}
    for remote_name, local_path in plan.files.items():
        if verbose:
            log.info("uploading %s → %s/%s", local_path, plan.item_id, remote_name)
        responses = item.upload_file(
            str(local_path),
            key=remote_name,
            metadata=md,
            access_key=access_key,
            secret_key=secret_key,
            verify=True,  # checa MD5; skipa se idêntico
            retries=3,
            verbose=verbose,
        )
        # `upload_file` retorna lista de Response (geralmente 1 elemento).
        if isinstance(responses, list) and responses:
            r = responses[0]
            results[remote_name] = (
                "uploaded" if 200 <= r.status_code < 300 else f"http_{r.status_code}"
            )
        else:
            # Quando o IA já tem o arquivo idêntico, a lib retorna [] ou similar.
            results[remote_name] = "skipped"

    return results
