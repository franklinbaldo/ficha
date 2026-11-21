"""Adversarial, expected-failing contract probes for raw upload #433.

These tests call the real production entrypoint with an offline source-stage
adapter and fake IA PUT.  Strict XFAIL documents *unfixed* safety defects:
do not interpret green CI, XFAIL, or this module as a repaired uploader.
No RFB/IA network request, ETL run, manifest promotion, or persistent upload.
"""

from __future__ import annotations

import hashlib

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory


def _require(condition: bool, message: str) -> None:
    # Active with python -O, unlike a bare assert statement.
    if not condition:
        raise AssertionError(message)


_MONTH = "2026-07"


@pytest.mark.xfail(
    strict=True,
    reason="#433: an ambiguous raw PUT can be replayed after restart with changed bytes",
)
def test_ambiguous_put_must_fence_changed_same_key_after_restart(monkeypatch, tmp_path):
    """A/503, then B/201 under the same key must never become two PUTs.

    This is a direct call to stream_raw_zips_to_ia twice, not a model of a
    journal or of eventual IA processing.  The source adapter stages 37
    same-length local fixtures and changes all their bytes on invocation 2.
    """
    specs = list(canonical_inventory())
    _require(len(specs) == 37, "canonical inventory must contain 37 entries")
    stage_calls = []
    puts = []

    def fake_stage_all(all_specs, month, token, dest):
        _require(month == _MONTH, "wrong month")
        _require(token == "offline-token", "wrong upstream token")
        _require([s.name for s in all_specs] == [s.name for s in specs], "wrong inventory")
        version = b"A" if not stage_calls else b"B"
        stage_calls.append(version)
        staged = {}
        for index, spec in enumerate(all_specs):
            data = b"PK\x03\x04" + version + index.to_bytes(2, "big") + b"fixed-size"
            path = dest / spec.name
            path.write_bytes(data)
            staged[spec.name] = (path, len(data), hashlib.sha256(data).hexdigest())
        return staged

    def fake_put(identifier, remote_name, body_iter, *, content_length, **kwargs):
        data = b"".join(body_iter)
        _require(identifier == "ficha-2026-07", "wrong item")
        _require(int(content_length) == len(data), "PUT content length differs")
        puts.append((remote_name, data))
        if len(puts) == 1:
            # A 503 is ambiguous: the server could have accepted the body.
            raise upload._IAS3Error(503, "https://s3.us.archive.org/fixture")

    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _identifier: set())
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "offline-token")
    monkeypatch.setattr(upload.raw_preflight, "stage_all", fake_stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", fake_put)

    def run():
        upload.stream_raw_zips_to_ia(
            _MONTH, access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path
        )

    with pytest.raises(upload._IAS3Error, match="503"):
        run()
    _require(len(puts) == 1, "first invocation should issue exactly one PUT")
    # A new process would also lack any durable journal in this implementation.
    # An explicit second invocation stands in for that restart.  A future
    # fail-closed implementation may refuse it; such refusal is acceptable.
    try:
        run()
    except RuntimeError:
        pass

    key = f"raw/{specs[0].name}"
    same_key = [data for name, data in puts if name == key]
    if len(same_key) > 1:
        _require(len(same_key[0]) == len(same_key[1]), "replayed body must keep length")
        _require(same_key[0] != same_key[1], "replayed body must differ")
    _require(len(same_key) == 1, "UNKNOWN must not be replayed to same mutable key")


@pytest.mark.xfail(
    strict=True,
    reason="#433: 37/37 catalog names and sizes can return success without byte readback",
)
def test_catalog_only_37_of_37_must_not_complete_as_verified(monkeypatch):
    """The metadata-only skip path cannot be a verified completion receipt."""
    names = {f"raw/{spec.name}" for spec in canonical_inventory()}
    _require(len(names) == 37, "canonical catalog must have 37 entries")
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _identifier: names)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("catalog-only path attempted unrelated source or IA PUT")

    monkeypatch.setattr(upload.upstream, "discover_token", forbidden)
    monkeypatch.setattr(upload.raw_preflight, "stage_all", forbidden)
    monkeypatch.setattr(upload, "_ia_s3_put", forbidden)

    # No spool/readback proof was supplied; success would be only a name
    # observation.  A correct fail-closed implementation must refuse.
    with pytest.raises(RuntimeError):
        upload.stream_raw_zips_to_ia(_MONTH, access_key="fake", secret_key="fake")

def test_optimized_python_replays_raw_unknown_contract_probes():
    """Run the two strict-XFAIL tests under -O and demand real XFAIL results."""
    import subprocess
    import sys
    from pathlib import Path

    source = str(Path(__file__).resolve())
    selectors = [
        f"{source}::test_ambiguous_put_must_fence_changed_same_key_after_restart",
        f"{source}::test_catalog_only_37_of_37_must_not_complete_as_verified",
    ]
    run = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    report = run.stdout + "\n" + run.stderr
    _require(run.returncode == 0, report)
    _require("2 xfailed" in run.stdout, report)
