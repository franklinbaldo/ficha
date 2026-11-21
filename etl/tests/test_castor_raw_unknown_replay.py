"""Offline contract probes for raw UNKNOWN replay and catalog-only completion.

Both strict XFAIL tests exercise the real upload entrypoint with fake source
and network boundaries. XFAIL means the producer is still unsafe, not fixed.
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory


MONTH = "2026-07"


class RawContractDefect(AssertionError):
    """Only a specifically observed safety gap is an expected failure."""


def require(condition: bool, message: str) -> None:
    """Retain negative assertions under optimized Python."""
    if not condition:
        raise AssertionError(message)


@pytest.mark.xfail(
    strict=True,
    raises=RawContractDefect,
    reason="#433: UNKNOWN replay not fenced",
)
def test_ambiguous_put_replay_changes_bytes(monkeypatch, tmp_path):
    """A/503 followed by B under the same key must not issue a second PUT."""
    specs = list(canonical_inventory())
    require(len(specs) == 37, "bad inventory")
    versions = []
    puts = []

    def stage_all(inventory, month, token, dest):
        require(month == MONTH, "wrong month")
        require(token == "offline-token", "wrong token")
        require(len(inventory) == 37, "wrong inventory")
        version = b"A" if not versions else b"B"
        versions.append(version)
        staged = {}
        for i, spec in enumerate(inventory):
            payload = b"PK\x03\x04" + version + bytes([i]) + b"fixed-size"
            path = dest / spec.name
            path.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            staged[spec.name] = (path, len(payload), digest)
        return staged

    def put(identifier, name, chunks, *, content_length, **kwargs):
        data = b"".join(chunks)
        require(identifier == "ficha-2026-07", "wrong item")
        require(len(data) == int(content_length), "wrong content length")
        puts.append((name, data))
        if len(puts) == 1:
            raise upload._IAS3Error(503, "https://s3.us.archive.org/fake")

    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _: set())
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "offline-token")
    monkeypatch.setattr(upload.raw_preflight, "stage_all", stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", put)

    def invoke():
        upload.stream_raw_zips_to_ia(
            MONTH, access_key="fake", secret_key="fake", spool_dir=tmp_path
        )

    with pytest.raises(upload._IAS3Error, match="503"):
        invoke()
    require(len(puts) == 1, "expected first attempted PUT")
    try:
        invoke()
    except RuntimeError:
        pass

    first_key = f"raw/{specs[0].name}"
    matching = [data for name, data in puts if name == first_key]
    if len(matching) > 1:
        require(len(matching[0]) == len(matching[1]), "size not held equal")
        require(matching[0] != matching[1], "bytes did not change")
    if len(matching) != 1:
        raise RawContractDefect("unknown key was replayed")


@pytest.mark.xfail(
    strict=True,
    raises=RawContractDefect,
    reason="#433: catalog alone is not byte proof",
)
def test_catalog_only_37_names_must_fail_closed(monkeypatch):
    """A metadata-only no-op is not a verified remote completion."""
    names = {f"raw/{spec.name}" for spec in canonical_inventory()}
    require(len(names) == 37, "wrong catalog")
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _: names)

    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected upstream or PUT call")

    monkeypatch.setattr(upload.upstream, "discover_token", forbidden)
    monkeypatch.setattr(upload.raw_preflight, "stage_all", forbidden)
    monkeypatch.setattr(upload, "_ia_s3_put", forbidden)
    try:
        upload.stream_raw_zips_to_ia(MONTH, access_key="fake", secret_key="fake")
    except RuntimeError:
        return
    raise RawContractDefect("catalog-only no-op was reported as completion")


def test_optimized_python_executes_both_negative_contract_probes():
    """Replay expected failures under -O rather than silently skip checks."""
    source = str(Path(__file__).resolve())
    selectors = [
        f"{source}::test_ambiguous_put_replay_changes_bytes",
        f"{source}::test_catalog_only_37_names_must_fail_closed",
    ]
    completed = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    report = completed.stdout + "\n" + completed.stderr
    require(completed.returncode == 0, report)
    require("2 xfailed" in completed.stdout, report)
