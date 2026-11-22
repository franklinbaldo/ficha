"""Offline replay contract for the real Ficha raw uploader (Ficha #433).

All RFB, staging, and IA boundaries are fake. A completed fake PUT body followed
by a synthetic HTTP 503 is ambiguous: a later invocation must not blindly PUT
again, even if the source pathname now resolves to a new same-size ZIP.
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


class AmbiguousReplay(AssertionError):
    """Second fake transport attempt occurred without independent readback."""


def _zip_bytes(name: str, version: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(
            zipfile.ZipInfo("source.csv", date_time=(1980, 1, 1, 0, 0, 0)),
            (version + ":" + name).encode("ascii"),
        )
    data = buf.getvalue()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if archive.testzip() is not None:
            raise AssertionError("invalid test ZIP")
    return data


@pytest.mark.parametrize("second_version", ["A", "B"])
@pytest.mark.xfail(
    strict=True,
    raises=AmbiguousReplay,
    reason="#433: ambiguous PUT followed by new invocation can replay without readback",
)
def test_unknown_after_503_prevents_restart_replay(
    monkeypatch, tmp_path: Path, second_version: str
):
    specs = list(canonical_inventory())
    if len(specs) != 37 or len({spec.name for spec in specs}) != 37:
        raise AssertionError("fixture must have exactly 37 distinct sources")

    first_name = specs[0].name
    first_a = _zip_bytes(first_name, "A")
    first_b = _zip_bytes(first_name, "B")
    if len(first_a) != len(first_b) or (
        hashlib.sha256(first_a).digest() == hashlib.sha256(first_b).digest()
    ):
        raise AssertionError("A/B must have equal length but distinct SHA-256")

    attempts: list[tuple[str, bytes]] = []
    generation = 0

    # Deliberately weak metadata observation: an ambiguous failed PUT need not
    # be present in a listing. This is NOT an IA live observation.
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _identifier: set())
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "offline-token")

    def stage_all(inventory, month, token, dest):
        nonlocal generation
        if [spec.name for spec in inventory] != [spec.name for spec in specs]:
            raise AssertionError("unexpected canonical inventory")
        if month != "2026-07" or token != "offline-token":
            raise AssertionError("unexpected source selection")
        generation += 1
        version = "A" if generation == 1 else second_version
        staged = {}
        for spec in inventory:
            payload = _zip_bytes(spec.name, version)
            path = dest / spec.name
            path.write_bytes(payload)
            staged[spec.name] = (path, len(payload), hashlib.sha256(payload).hexdigest())
        return staged

    def fake_put(identifier, remote_name, chunks, *, content_length, **_kwargs):
        if identifier != "ficha-2026-07" or not remote_name.startswith("raw/"):
            raise AssertionError("unexpected IA destination")
        body = b"".join(chunks)
        if len(body) != int(content_length):
            raise AssertionError("truncated synthetic body")
        attempts.append((remote_name, body))  # Count before response/error.
        if len(attempts) == 1:
            raise upload._IAS3Error(503, "https://s3.us.archive.org/offline")

    monkeypatch.setattr(upload.raw_preflight, "stage_all", stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", fake_put)

    kwargs = dict(access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path)
    with pytest.raises(upload._IAS3Error) as first_error:
        upload.stream_raw_zips_to_ia("2026-07", **kwargs)
    if first_error.value.status != 503:
        raise AssertionError("fixture must produce HTTP 503 only after fake wire body")
    if len(attempts) != 1 or attempts[0] != (f"raw/{first_name}", first_a):
        raise AssertionError("first attempt did not transmit admitted A")

    # Simulates a later process invocation: local TemporaryDirectory from the
    # first call is gone; no independent remote digest/readback has occurred.
    try:
        upload.stream_raw_zips_to_ia("2026-07", **kwargs)
    except (RuntimeError, ValueError):
        # A future fail-closed journal gate may reject the second invocation.
        pass

    if len(attempts) > 1:
        second_name, second_body = attempts[1]
        if second_name != f"raw/{first_name}":
            raise AssertionError("unexpected second destination")
        expected = first_a if second_version == "A" else first_b
        if second_body != expected:
            raise AssertionError("fixture failed to represent second generation")
        raise AmbiguousReplay(
            f"second PUT without readback (A/B differ={second_version == 'B'}); "
            f"wire attempts={len(attempts)}"
        )


def test_unknown_restart_probe_in_optimized_python():
    path = Path(__file__).resolve()
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{path}::test_unknown_after_503_prevents_restart_replay",
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=180,
    )
    output = result.stdout + "\n" + result.stderr
    if result.returncode != 0 or "2 xfailed" not in result.stdout:
        raise AssertionError(output)
