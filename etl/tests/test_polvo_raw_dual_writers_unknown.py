"""Characterize simultaneous raw writers without a durable UNKNOWN intent (#433).

Run the real streaming entrypoint concurrently in two threads, replacing only
remote discovery, staging and the IA sink with synthetic boundaries. This is
not a live service test or evidence that any raw archive was published.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory


class ConcurrentRawWrite(AssertionError):
    """Both invocations reached the fake PUT for the same remote object."""


def _payload(name: str, generation: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(
            zipfile.ZipInfo("source.csv", date_time=(1980, 1, 1, 0, 0, 0)),
            (generation + ":" + name).encode("ascii"),
        )
    return buffer.getvalue()


@pytest.mark.xfail(
    strict=True,
    raises=ConcurrentRawWrite,
    reason="#433: independent writers have no durable singleflight/UNKNOWN intent",
)
def test_two_writers_must_not_put_the_same_raw_name_without_singleflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    specs = list(canonical_inventory())
    if len(specs) != 37 or len({spec.name for spec in specs}) != 37:
        raise AssertionError("fixture must contain 37 unique canonical ZIPs")

    first_name = specs[0].name
    a = _payload(first_name, "A")
    b = _payload(first_name, "B")
    if len(a) != len(b) or hashlib.sha256(a).digest() == hashlib.sha256(b).digest():
        raise AssertionError("synthetic A/B require equal lengths and distinct hashes")

    # Two attempts for the same month and destination, with distinct bytes.
    # Calls use separate TemporaryDirectories under one operator-selected scratch.
    identity = threading.local()
    simultaneous_start = threading.Barrier(2, timeout=15)
    fake_wire_barrier = threading.Barrier(2, timeout=6)
    observed: list[tuple[str, bytes]] = []
    observed_lock = threading.Lock()

    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _identifier: set())
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "offline-token")

    def fake_stage_all(inventory, month, token, dest):
        if [spec.name for spec in inventory] != [spec.name for spec in specs]:
            raise AssertionError("unexpected canonical inventory")
        if (month, token) != ("2026-07", "offline-token"):
            raise AssertionError("unexpected staging identity")
        generation = identity.generation
        result = {}
        for spec in inventory:
            body = _payload(spec.name, generation)
            path = dest / spec.name
            path.write_bytes(body)
            result[spec.name] = (path, len(body), hashlib.sha256(body).hexdigest())
        return result

    def fake_put(identifier, remote_name, chunks, *, content_length, **_kwargs):
        if (identifier, remote_name) != ("ficha-2026-07", f"raw/{first_name}"):
            raise AssertionError("unexpected fake IA object")
        body = b"".join(chunks)
        if len(body) != int(content_length):
            raise AssertionError("synthetic PUT body truncated")
        if body not in (a, b):
            raise AssertionError("synthetic PUT is neither pinned A nor B")
        with observed_lock:
            observed.append((identity.generation, body))
        # A future singleflight gate permits just one writer through. The first
        # PUT times out only at this *fake* barrier and yields an ambiguous 503.
        try:
            fake_wire_barrier.wait()
        except threading.BrokenBarrierError:
            pass
        raise upload._IAS3Error(503, "https://s3.us.archive.org/offline")

    monkeypatch.setattr(upload.raw_preflight, "stage_all", fake_stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", fake_put)

    def run_one(generation: str) -> BaseException | None:
        identity.generation = generation
        simultaneous_start.wait()
        try:
            upload.stream_raw_zips_to_ia(
                "2026-07",
                access_key="fake",
                secret_key="fake",
                workers=1,
                spool_dir=tmp_path,
            )
        except Exception as exc:
            return exc
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        fa = executor.submit(run_one, "A")
        fb = executor.submit(run_one, "B")
        outcomes = [fa.result(timeout=45), fb.result(timeout=45)]

    if len(observed) == 2:
        if {generation for generation, _ in observed} != {"A", "B"}:
            raise AssertionError("fake PUTs were not issued by independent invocations")
        if not all(isinstance(outcome, upload._IAS3Error) for outcome in outcomes):
            raise AssertionError("fake PUT failure should be an ambiguous HTTP 503")
        raise ConcurrentRawWrite("two writers sent A and B to the same raw object without a fence")

    # Future acceptance: one fake PUT plus an explicit intent/inflight refusal.
    # Generic file errors or silent no-op do not satisfy the custody contract.
    if len(observed) != 1:
        raise AssertionError(f"expected one fenced write, got {len(observed)}")
    if sum(isinstance(outcome, upload._IAS3Error) for outcome in outcomes) != 1:
        raise AssertionError("the one observed fake PUT must return HTTP 503")
    refusals = [outcome for outcome in outcomes if not isinstance(outcome, upload._IAS3Error)]
    if len(refusals) != 1 or not isinstance(refusals[0], RuntimeError):
        raise AssertionError("second writer must refuse explicitly")
    refusal = str(refusals[0]).lower()
    if not any(
        word in refusal
        for word in ("unknown", "journal", "intent", "singleflight", "inflight", "lock", "busy")
    ):
        raise AssertionError("second writer did not report an explicit raw authority refusal")


def test_two_writer_regression_in_optimized_python() -> None:
    target = (
        f"{Path(__file__).resolve()}"
        "::test_two_writers_must_not_put_the_same_raw_name_without_singleflight"
    )
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", target],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if child.returncode or "1 xfailed" not in child.stdout:
        raise AssertionError(child.stdout + "\n" + child.stderr)
