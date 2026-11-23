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
import tempfile
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


def _is_authority_refusal(exc: BaseException | None) -> bool:
    # Proposed typed U boundary; absent today. Generic errors are not evidence.
    refusal = getattr(upload, "RawAuthorityRefusal", None)
    return isinstance(refusal, type) and isinstance(exc, refusal)


def _check_outcomes(
    attempts: list[str],
    observed: list[tuple[str, bytes]],
    outcomes: dict[str, BaseException | None],
    expected: dict[str, bytes],
) -> None:
    writers = [generation for generation, _ in observed]
    if sorted(attempts) != sorted(writers) or len(set(writers)) != len(writers):
        raise AssertionError("attempts and complete bodies must correspond one-to-one")
    if set(outcomes) != set(expected):
        raise AssertionError("missing writer outcome")
    for generation, body in observed:
        if generation not in expected or body != expected[generation]:
            raise AssertionError("wire body must match its writer's generation")
        outcome = outcomes[generation]
        if not isinstance(outcome, upload._IAS3Error) or outcome.status != 503:
            raise AssertionError("observed writer must receive its own HTTP 503")
    if len(writers) == 2:
        raise ConcurrentRawWrite("two writers sent A and B to the same raw object without a fence")
    if len(writers) != 1:
        raise AssertionError(f"expected one fenced write, got {len(writers)}")
    other = next(generation for generation in expected if generation not in writers)
    if not _is_authority_refusal(outcomes[other]):
        raise AssertionError("second writer must report an explicit raw authority refusal")


@pytest.mark.parametrize(
    ("observed", "status_a", "status_b", "generic_refusal", "attempts"),
    [
        ([("A", b"A")], 503, None, False, ["A"]),
        ([("A", b"B")], 503, None, False, ["A"]),
        ([("A", b"A")], None, 503, False, ["A"]),
        ([("A", b"A")], 401, None, False, ["A"]),
        ([("A", b"A")], 503, None, True, ["A"]),
        ([("A", b"A")], 503, None, False, ["A", "B"]),
        ([("A", b"A"), ("B", b"B")], 503, 401, False, ["A", "B"]),
    ],
)
def test_dual_writer_oracle_mutations(
    monkeypatch, observed, status_a, status_b, generic_refusal, attempts
):
    class RawAuthorityRefusal(RuntimeError):
        pass

    monkeypatch.setattr(upload, "RawAuthorityRefusal", RawAuthorityRefusal, raising=False)

    def outcome(status):
        if status is not None:
            return upload._IAS3Error(status, "https://s3.us.archive.org/offline")
        return RuntimeError("database locked") if generic_refusal else RawAuthorityRefusal()

    args = (
        attempts,
        observed,
        {"A": outcome(status_a), "B": outcome(status_b)},
        {"A": b"A", "B": b"B"},
    )
    if observed == [("A", b"A")] and status_a == 503 and not generic_refusal and attempts == ["A"]:
        _check_outcomes(*args)
    else:
        with pytest.raises(AssertionError) as error:
            _check_outcomes(*args)
        if isinstance(error.value, ConcurrentRawWrite):
            raise AssertionError("mutation must not be mistaken for the real counterexample")


def _run_dual_writers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, expected_optimize: int
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
    attempts: list[str] = []
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
        with observed_lock:
            attempts.append(identity.generation)
        body = b"".join(chunks)
        if len(body) != int(content_length):
            raise AssertionError("synthetic PUT body truncated")
        if body != {"A": a, "B": b}[identity.generation]:
            raise AssertionError("synthetic PUT does not match its writer generation")
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
        if sys.flags.optimize != expected_optimize:
            raise AssertionError("worker optimization mode differs from parent")
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
        outcomes = {"A": fa.result(timeout=45), "B": fb.result(timeout=45)}

    _check_outcomes(attempts, observed, outcomes, {"A": a, "B": b})


@pytest.mark.xfail(
    strict=True,
    raises=ConcurrentRawWrite,
    reason="#433: independent writers have no durable singleflight/UNKNOWN intent",
)
def test_two_writers_must_not_put_the_same_raw_name_without_singleflight() -> None:
    # External watchdog bounds even ThreadPoolExecutor.__exit__ deadlocks.
    command = [sys.executable, *(["-O"] * sys.flags.optimize)]
    child = subprocess.run(
        [*command, str(Path(__file__).resolve()), "--offline-dual-worker", str(sys.flags.optimize)],
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    if child.returncode == 42:
        raise ConcurrentRawWrite("offline worker observed two exact writes and HTTP 503 outcomes")
    if child.returncode != 0:
        raise AssertionError(child.stdout + "\n" + child.stderr)


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


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--offline-dual-worker":
        raise SystemExit("offline dual worker only")
    expected_optimize = int(sys.argv[2])
    if sys.flags.optimize != expected_optimize:
        raise AssertionError("incorrect worker optimization mode")
    with tempfile.TemporaryDirectory() as scratch, pytest.MonkeyPatch.context() as patch:
        try:
            _run_dual_writers(patch, Path(scratch), expected_optimize)
        except ConcurrentRawWrite:
            raise SystemExit(42) from None
