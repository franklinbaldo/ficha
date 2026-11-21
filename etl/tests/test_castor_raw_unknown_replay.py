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
    assert len(specs) == 37
    stage_calls = []
    puts = []

    def fake_stage_all(all_specs, month, token, dest):
        assert month == _MONTH
        assert token == "offline-token"
        assert [s.name for s in all_specs] == [s.name for s in specs]
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
        assert identifier == "ficha-2026-07"
        assert int(content_length) == len(data)
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
    assert len(puts) == 1
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
        assert len(same_key[0]) == len(same_key[1])
        assert same_key[0] != same_key[1]
    assert len(same_key) == 1, "UNKNOWN must not be replayed to same mutable key"


@pytest.mark.xfail(
    strict=True,
    reason="#433: 37/37 catalog names and sizes can return success without byte readback",
)
def test_catalog_only_37_of_37_must_not_complete_as_verified(monkeypatch):
    """The metadata-only skip path cannot be a verified completion receipt."""
    names = {f"raw/{spec.name}" for spec in canonical_inventory()}
    assert len(names) == 37
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
