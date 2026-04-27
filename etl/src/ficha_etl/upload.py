"""Upload dos outputs do pipeline FICHA para o Internet Archive.

Cada snapshot mensal vive num item `ficha-YYYY-MM`. Estrutura interna:

    ficha-YYYY-MM/
      raw/            ← 37 ZIPs crus espelhados da RFB
      cnpjs.parquet
      raizes.parquet
      socios.parquet
      lookups.json

Ver ADR 0012 e mirror.py (URLs de leitura).
"""

from __future__ import annotations

import logging
from pathlib import Path

import internetarchive as ia

from .mirror import item_id
from .sources import canonical_inventory, is_valid_month

log = logging.getLogger(__name__)

_RETRIES = 5
_RETRY_SLEEP = 30  # segundos entre tentativas

_IA_METADATA_BASE: dict[str, object] = {
    "mediatype": "data",
    "subject": ["CNPJ", "Receita Federal", "dados abertos", "Brasil"],
    "creator": "franklinbaldo",
    "licenseurl": "https://creativecommons.org/publicdomain/zero/1.0/",
    "language": "por",
}


def _check_responses(responses: list, label: str) -> None:
    """Levanta RuntimeError se algum response indicar falha."""
    for r in responses:
        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"IA upload failed [{label}]: HTTP {r.status_code} — {r.url}"
            )


def upload_outputs(
    month: str,
    output_dir: Path,
    *,
    access_key: str,
    secret_key: str,
) -> None:
    """Faz upload dos 3 parquets + lookups.json para o item IA do mês.

    Args:
        month: snapshot no formato YYYY-MM.
        output_dir: diretório local com os 4 arquivos produzidos pelo transform.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    outputs = {
        "cnpjs.parquet": output_dir / "cnpjs.parquet",
        "raizes.parquet": output_dir / "raizes.parquet",
        "socios.parquet": output_dir / "socios.parquet",
        "lookups.json": output_dir / "lookups.json",
    }

    missing = [name for name, path in outputs.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"outputs missing before upload: {missing}")

    identifier = item_id(month)
    metadata = {
        **_IA_METADATA_BASE,
        "title": f"FICHA CNPJ {month}",
        "description": (
            f"Dados do CNPJ (Cadastro Nacional de Pessoas Jurídicas) da Receita Federal "
            f"do Brasil — snapshot {month}. "
            f"Processado pelo projeto FICHA (https://github.com/franklinbaldo/ficha)."
        ),
    }

    log.info("uploading %d output files to ia:%s", len(outputs), identifier)
    responses = ia.upload(
        identifier,
        files={name: str(path) for name, path in outputs.items()},
        metadata=metadata,
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "outputs")
    log.info("uploaded outputs to ia:%s OK", identifier)


def upload_raw_zips(
    month: str,
    cache_dir: Path,
    *,
    access_key: str,
    secret_key: str,
) -> None:
    """Espelha os ZIPs crus da RFB para ficha-YYYY-MM/raw/.

    Pula ZIPs ausentes no cache (warning) mas falha se nenhum for encontrado.

    Args:
        month: snapshot no formato YYYY-MM.
        cache_dir: raiz do cache local; ZIPs esperados em cache_dir/month/*.zip.
        access_key: IA S3-like access key.
        secret_key: IA S3-like secret key.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    raw_dir = cache_dir / month
    files: dict[str, str] = {}
    for spec in canonical_inventory():
        local = raw_dir / spec.name
        if local.exists() and local.stat().st_size > 0:
            # Remote name com prefixo raw/ cria "pasta" no item IA.
            files[f"raw/{spec.name}"] = str(local)
        else:
            log.warning("raw ZIP not in cache, skipping mirror: %s", local)

    if not files:
        raise FileNotFoundError(
            f"no raw ZIPs found in {raw_dir} — run download first"
        )

    identifier = item_id(month)
    log.info("uploading %d raw ZIPs to ia:%s/raw/", len(files), identifier)
    responses = ia.upload(
        identifier,
        files=files,
        access_key=access_key,
        secret_key=secret_key,
        retries=_RETRIES,
        retries_sleep=_RETRY_SLEEP,
        verbose=True,
    )
    _check_responses(responses, "raw-zips")
    log.info("uploaded %d raw ZIPs to ia:%s OK", len(files), identifier)
