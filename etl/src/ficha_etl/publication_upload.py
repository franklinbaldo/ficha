"""Upload fail-closed dos outputs-base de um snapshot.

O production descriptor é a autoridade sobre a identidade esperada. O remoto
nunca define o hash correto e um nome existente nunca é sobrescrito às cegas:

- catálogo ``size + sha1`` idêntico -> reuse;
- catálogo ``size + sha1`` divergente -> erro duro;
- nome ausente no catálogo + HEAD 404 -> PUT permitido;
- nome servido mas ainda ausente/incompleto no catálogo -> aguarda metadata;
- estado ambíguo -> não escreve.

Isso é o equivalente para Parquets/lookups do preflight já usado nos shards.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
import internetarchive as ia

from .mirror import item_id
from .remote_reuse import fetch_item_metadata
from .sources import is_valid_month

_TRANSIENT_HEAD = frozenset({429, 500, 502, 503, 504})
_HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)

_STANDARD_FILES = {
    "cnpjs": "cnpjs.parquet",
    "cnpj_contatos": "cnpj_contatos.parquet",
    "cnpj_cnaes": "cnpj_cnaes.parquet",
    "raizes": "raizes.parquet",
    "socios": "socios.parquet",
    "enderecos": "enderecos.parquet",
    "pessoas": "pessoas.parquet",
    "lookups": "lookups.json",
}


class OutputPublishError(RuntimeError):
    """O remoto não permite uma escrita fail-closed segura."""


@dataclass(frozen=True)
class OutputUploadPlan:
    upload: tuple[str, ...]
    reuse: tuple[str, ...]
    pending: tuple[str, ...]
    mismatches: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.pending and not self.mismatches


MetadataFetch = Callable[[], dict | None]
HeadStatus = Callable[[str], int]
Sleep = Callable[[float], None]


def descriptor_file_entries(descriptor: dict) -> dict[str, dict]:
    """Achata os outputs-base do descriptor por nome remoto."""
    files = descriptor.get("files")
    lookups = descriptor.get("lookups")
    if not isinstance(files, dict) or not isinstance(lookups, dict):
        raise ValueError("production descriptor missing files/lookups")

    entries: dict[str, dict] = {}
    for key, name in _STANDARD_FILES.items():
        entry = files.get(key)
        if not isinstance(entry, dict):
            raise ValueError(f"production descriptor missing files.{key}")
        entries[name] = entry
    for kind, entry in lookups.items():
        if not isinstance(kind, str) or not isinstance(entry, dict):
            raise ValueError("invalid lookup entry in production descriptor")
        entries[f"lookups/{kind}.parquet"] = entry
    return entries


def _sha1_file(path: Path) -> str:
    h = hashlib.sha1(usedforsecurity=False)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def local_output_paths(output_dir: Path, descriptor: dict) -> dict[str, Path]:
    entries = descriptor_file_entries(descriptor)
    paths: dict[str, Path] = {}
    for name in entries:
        path = output_dir / name
        if not path.exists():
            raise FileNotFoundError(f"output missing before publication: {path}")
        expected_size = int(entries[name].get("size", 0) or 0)
        expected_sha1 = _valid_sha1(entries[name].get("sha1"))
        actual_size = path.stat().st_size
        actual_sha1 = (
            _sha1_file(path) if actual_size == expected_size and expected_size > 0 else None
        )
        if (
            expected_size <= 0
            or expected_sha1 is None
            or actual_size != expected_size
            or actual_sha1 != expected_sha1
        ):
            raise OutputPublishError(
                f"{name}: local bytes changed after production descriptor: "
                f"size={actual_size}/{expected_size} sha1={actual_sha1}/{expected_sha1}"
            )
        paths[name] = path
    return paths


def _valid_sha1(value: object) -> str | None:
    if not isinstance(value, str) or len(value) != 40:
        return None
    lowered = value.lower()
    if any(char not in "0123456789abcdef" for char in lowered):
        return None
    return lowered


def http_head_status(url: str) -> int:
    with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
        try:
            return client.head(url).status_code
        except httpx.HTTPError:
            return 503


def classify_output_upload(
    descriptor: dict,
    metadata: dict | None,
    *,
    head_status: HeadStatus,
) -> OutputUploadPlan:
    """Classifica uma fotografia remota sem fazer escrita."""
    entries = descriptor_file_entries(descriptor)
    if metadata is None or not isinstance(metadata.get("files"), list):
        return OutputUploadPlan((), (), tuple(entries), ())

    by_name = {
        entry.get("name"): entry
        for entry in metadata["files"]
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }

    upload: list[str] = []
    reuse: list[str] = []
    pending: list[str] = []
    mismatches: list[str] = []

    for name, expected in entries.items():
        remote = by_name.get(name)
        expected_size = int(expected.get("size", 0) or 0)
        expected_sha1 = _valid_sha1(expected.get("sha1"))
        if expected_size <= 0 or expected_sha1 is None:
            raise ValueError(f"{name}: invalid identity in production descriptor")

        if remote is not None:
            try:
                remote_size = int(remote.get("size", 0) or 0)
            except (TypeError, ValueError):
                remote_size = 0
            remote_sha1 = _valid_sha1(remote.get("sha1"))
            if remote_size <= 0 or remote_sha1 is None:
                pending.append(name)
                continue
            if remote_size == expected_size and remote_sha1 == expected_sha1:
                reuse.append(name)
            else:
                mismatches.append(
                    f"{name}: remote size={remote_size}/{expected_size} "
                    f"sha1={remote_sha1}/{expected_sha1}"
                )
            continue

        status = head_status(str(expected.get("url", "")))
        if status == 404:
            upload.append(name)
        elif status == 200 or status in _TRANSIENT_HEAD:
            # 200 sem entrada no metadata é exatamente a janela de consistência
            # eventual vista no backfill de 2026-05. Não autoriza overwrite.
            pending.append(name)
        else:
            pending.append(f"{name} (HEAD {status})")

    return OutputUploadPlan(
        tuple(sorted(upload)),
        tuple(sorted(reuse)),
        tuple(sorted(pending)),
        tuple(sorted(mismatches)),
    )


def wait_for_safe_output_plan(
    descriptor: dict,
    *,
    fetch_metadata: MetadataFetch,
    head_status: HeadStatus = http_head_status,
    attempts: int = 30,
    interval_s: float = 10.0,
    sleep: Sleep = time.sleep,
) -> OutputUploadPlan:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    for attempt in range(1, attempts + 1):
        plan = classify_output_upload(
            descriptor,
            fetch_metadata(),
            head_status=head_status,
        )
        if plan.mismatches:
            raise OutputPublishError(
                "remote output identity diverges from production descriptor:\n"
                + "\n".join(plan.mismatches)
            )
        if not plan.pending:
            return plan
        if attempt == attempts:
            raise OutputPublishError(
                f"remote output state still ambiguous after {attempts} attempts: {plan.pending}"
            )
        sleep(interval_s)
    raise AssertionError("unreachable")


def submit_outputs_fail_closed(
    month: str,
    output_dir: Path,
    descriptor: dict,
    *,
    access_key: str,
    secret_key: str,
    fetch_metadata: MetadataFetch | None = None,
    head_status: HeadStatus = http_head_status,
    attempts: int = 30,
    interval_s: float = 10.0,
) -> OutputUploadPlan:
    """Submete somente nomes comprovadamente ausentes.

    A função valida que os arquivos locais ainda têm o tamanho fechado no
    descriptor. Após o PUT, o job seguinte confirma ``size + sha1`` pelo
    catálogo antes de qualquer pack/promoção.
    """
    if not is_valid_month(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    paths = local_output_paths(output_dir, descriptor)
    fetch_metadata = fetch_metadata or (lambda: fetch_item_metadata(month))
    plan = wait_for_safe_output_plan(
        descriptor,
        fetch_metadata=fetch_metadata,
        head_status=head_status,
        attempts=attempts,
        interval_s=interval_s,
    )
    if not plan.upload:
        return plan

    responses = ia.upload(
        item_id(month),
        files={name: str(paths[name]) for name in plan.upload},
        access_key=access_key,
        secret_key=secret_key,
        retries=5,
        retries_sleep=30,
        verbose=True,
    )
    bad: list[str] = []
    for response in responses:
        status = getattr(response, "status_code", None)
        if status not in (200, 201):
            bad.append(f"HTTP {status}: {getattr(response, 'url', '<unknown>')}")
    if bad:
        raise OutputPublishError("IA rejected derived outputs: " + "; ".join(bad))
    return plan
