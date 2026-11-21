"""Offline regression probes for partial IA raw catalogs from an older generation.

Both strict XFAIL cases invoke the real upload entrypoint with fake upstream,
staging and IA boundaries. XFAIL is a measured defect, not a safe publication.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory

MONTH = "2026-07"


class RemoteDigestMismatch(AssertionError):
    """The old remote objects conflict with the current source pins."""


def require(condition: bool, message: str) -> None:
    """Retain all witness checks when running optimized Python."""
    if not condition:
        raise AssertionError(message)


def _valid_zip(name: str, generation: str) -> bytes:
    """Same-size A/B archives with valid central directory and CRC."""
    buffer = io.BytesIO()
    member = name.removesuffix(".zip") + ".csv"
    payload = f"{generation}-{name}".encode()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(member, payload)
    data = buffer.getvalue()
    with zipfile.ZipFile(io.BytesIO(data)) as check:
        require(check.testzip() is None, "synthetic ZIP CRC is invalid")
    return data


@pytest.mark.parametrize("remote_count", [1, 18], ids=["one-stale", "eighteen-stale"])
@pytest.mark.xfail(
    strict=True,
    raises=RemoteDigestMismatch,
    reason="#433: existing raw name+size is not tied to current pinned SHA-256",
)
def test_partial_catalog_cannot_mix_remote_a_with_source_b(
    monkeypatch, tmp_path: Path, remote_count: int
) -> None:
    """The uploader must refuse a mixed generation before any PUT."""
    specs = list(canonical_inventory())
    require(len(specs) == 37, "wrong source inventory")
    a = {spec.name: _valid_zip(spec.name, "A") for spec in specs}
    b = {spec.name: _valid_zip(spec.name, "B") for spec in specs}
    for spec in specs:
        name = spec.name
        require(len(a[name]) == len(b[name]), f"different lengths: {name}")
        require(
            hashlib.sha256(a[name]).digest() != hashlib.sha256(b[name]).digest(),
            f"equal digests: {name}",
        )

    old_specs = specs[:remote_count]
    remote = {f"raw/{spec.name}": a[spec.name] for spec in old_specs}
    original_remote = dict(remote)
    staged_calls: list[int] = []
    puts: list[tuple[str, bytes]] = []

    def existing(identifier: str) -> set[str]:
        require(identifier == "ficha-2026-07", "wrong IA item")
        return set(remote)

    def stage_all(inventory, month, token, dest):
        require(month == MONTH and token == "synthetic-token", "wrong source")
        observed_names = [spec.name for spec in inventory]
        expected_names = [spec.name for spec in specs]
        require(observed_names == expected_names, "not 37")
        staged_calls.append(1)
        staged = {}
        for spec in inventory:
            path = dest / spec.name
            payload = b[spec.name]
            path.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            staged[spec.name] = (path, len(payload), digest)
        return staged

    def put(identifier, key, chunks, *, content_length, **kwargs):
        require(identifier == "ficha-2026-07", "wrong IA item for PUT")
        payload = b"".join(chunks)
        require(len(payload) == int(content_length), "wrong PUT length")
        require(key.startswith("raw/"), "wrong IA prefix")
        name = key.removeprefix("raw/")
        require(payload == b[name], "unexpected source bytes for PUT")
        puts.append((key, payload))
        remote[key] = payload

    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", existing)
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "synthetic-token")
    monkeypatch.setattr(upload.raw_preflight, "stage_all", stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", put)

    try:
        upload.stream_raw_zips_to_ia(
            MONTH, access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path
        )
    except RuntimeError as exc:
        # A future producer can reject catalog/source disagreement; require
        # a contextual, pre-PUT refusal rather than accepting any fixture error.
        message = str(exc).lower()
        require(
            any(term in message for term in ("remote", "catalog", "digest", "source version")),
            f"unrelated runtime error: {exc}",
        )
        require(not puts, "some objects were PUT before refusal")
        return

    # Current-code witness: stage 37 current B files and PUT only the missing
    # names, retaining older A files reported as present by the IA catalog.
    require(staged_calls == [1], "the 37-source preflight was not called exactly once")
    require(len(puts) == 37 - remote_count, "unexpected count of missing-source PUTs")
    require(len(remote) == 37, "synthetic remote is not complete")
    require(
        all(remote[key] == payload for key, payload in original_remote.items()),
        "the pre-existing old bytes changed unexpectedly",
    )
    for spec in specs[remote_count:]:
        key = f"raw/{spec.name}"
        require(remote[key] == b[spec.name], f"wrong new-generation bytes: {key}")
    raise RemoteDigestMismatch(
        f"partial catalog yielded {remote_count} old A and {37 - remote_count} new B ZIPs"
    )


def test_optimized_python_replays_partial_catalog_negatives() -> None:
    """Keep typed strict XFAILs live under python -O."""
    source = str(Path(__file__).resolve())
    selector = f"{source}::test_partial_catalog_cannot_mix_remote_a_with_source_b"
    completed = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", selector],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    report = completed.stdout + "\n" + completed.stderr
    require(completed.returncode == 0, report)
    require("2 xfailed" in completed.stdout, report)
