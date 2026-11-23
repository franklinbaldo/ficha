"""Offline process-restart oracle for the Ficha #433 raw UNKNOWN contract.

Two distinct Python interpreters call the real stream_raw_zips_to_ia entrypoint.
The first process either receives HTTP 503, a response ReadTimeout after
all request bytes were consumed by the fake transport, or is killed while
a fake transport holds a partial body. Every RFB/IA boundary is synthetic.
No network, ETL or publishing occurs.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest

from ficha_etl.sources import canonical_inventory


class AmbiguousReplay(AssertionError):
    """The second process reached a fake PUT before independent readback."""


class ReplayAttempt(Exception):
    """Stop the second fake PUT immediately after observing its body."""


def _payload(name: str, generation: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(
            zipfile.ZipInfo("source.csv", date_time=(1980, 1, 1, 0, 0, 0)),
            (generation + ":" + name).encode("ascii"),
        )
    return output.getvalue()


def _worker(scratch: Path, phase: str, failure: str) -> int:
    # Imports and patches live inside the child, not in the pytest parent.
    from ficha_etl import upload

    specs = list(canonical_inventory())
    if len(specs) != 37 or len({spec.name for spec in specs}) != 37:
        raise AssertionError("expected canonical 37/37 inventory")
    if phase not in ("first", "second") or failure not in (
        "http503",
        "timeout",
        "kill-midput",
    ):
        raise AssertionError("invalid offline phase or failure")

    generation = "A" if phase == "first" else "B"
    upload._existing_raw_files_on_ia = lambda _identifier: set()
    upload.upstream.discover_token = lambda: "offline-token"

    def fake_stage(inventory, month, token, dest):
        if [spec.name for spec in inventory] != [spec.name for spec in specs]:
            raise AssertionError("unexpected source inventory")
        if (month, token) != ("2026-07", "offline-token"):
            raise AssertionError("unexpected stage selection")
        staged = {}
        for spec in inventory:
            body = _payload(spec.name, generation)
            path = dest / spec.name
            path.write_bytes(body)
            staged[spec.name] = (path, len(body), hashlib.sha256(body).hexdigest())
        return staged

    def fake_put(identifier, remote_name, chunks, *, content_length, **_kwargs):
        if identifier != "ficha-2026-07" or remote_name != f"raw/{specs[0].name}":
            raise AssertionError("unexpected fake remote target")
        if phase == "first" and failure == "kill-midput":
            # The fake socket has accepted only a prefix of one chunk, not
            # a complete body. The parent must kill this *actual process*
            # while it is inside the fake transport, before any response.
            first_chunk = next(iter(chunks), b"")
            if len(first_chunk) < 2:
                raise AssertionError("expected nonempty partial fake wire body")
            prefix = first_chunk[: len(first_chunk) // 2]
            (scratch / "wire-first.bin").write_bytes(prefix)
            (scratch / "wire-ready.flag").write_text("partial-body-awaiting-kill")
            time.sleep(90)  # Parent kills the child; timeout is only a failsafe.
            raise AssertionError("first child was not killed")
        body = b"".join(chunks)
        if len(body) != int(content_length):
            raise AssertionError("truncated fake wire body")
        (scratch / f"wire-{phase}.bin").write_bytes(body)
        if phase == "first":
            # The transport consumed the entire body; the response is ambiguous.
            if failure == "timeout":
                # Request body fully consumed; the response is not observed.
                raise upload.httpx.ReadTimeout("offline response timeout")
            raise upload._IAS3Error(503, "https://s3.us.archive.org/offline")
        raise ReplayAttempt("second fake PUT reached after process restart")

    upload.raw_preflight.stage_all = fake_stage
    upload._ia_s3_put = fake_put

    try:
        upload.stream_raw_zips_to_ia(
            "2026-07", access_key="fake", secret_key="fake", workers=1, spool_dir=scratch
        )
    except upload._IAS3Error as exc:
        if phase == "first" and exc.status == 503:
            return 53
        raise
    except upload.httpx.ReadTimeout:
        if phase == "first" and failure == "timeout":
            return 54
        raise
    except ReplayAttempt:
        return 42
    except Exception as exc:
        # A future implementation may fail closed only for an explicit UNKNOWN
        # state. Arbitrary exceptions must not count as a successful fence.
        evidence = f"{type(exc).__name__}: {exc}".lower()
        if phase == "second" and any(
            term in evidence for term in ("unknown", "journal", "ambiguous", "reconcil")
        ):
            return 0
        raise
    # A silent no-op does not prove recovery from the ambiguous intent.
    return 7


def _child(scratch: Path, phase: str, failure: str) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--offline-worker",
        str(scratch),
        phase,
        failure,
    ]
    if phase == "first" and failure == "kill-midput":
        # Cross-platform parent-directed process kill, gated on a filesystem
        # handshake that the child is *inside* the fake partial PUT.
        # No timing assumption about child start-up or its Python imports.
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as child:
            deadline = time.monotonic() + 30
            marker = scratch / "wire-ready.flag"
            try:
                while not marker.exists() and time.monotonic() < deadline:
                    if child.poll() is not None:
                        raise AssertionError(
                            f"child exited before partial fake PUT: {child.returncode}"
                        )
                    time.sleep(0.02)
                if not marker.exists() or child.poll() is not None:
                    raise AssertionError("fake PUT never reached the kill barrier")
            finally:
                if child.poll() is None:
                    child.kill()  # SIGKILL (POSIX); hard terminate (Windows).
                out, err = child.communicate(timeout=15)
            return subprocess.CompletedProcess(command, child.returncode, out, err)
    return subprocess.run(
        command, capture_output=True, text=True, check=False, timeout=90
    )


@pytest.mark.parametrize(
    ("failure", "first_exit"),
    [("http503", 53), ("timeout", 54), ("kill-midput", None)],
    ids=("http-503", "read-timeout-after-body", "hard-kill-during-partial-put"),
)
@pytest.mark.xfail(
    strict=True,
    raises=AmbiguousReplay,
    reason="#433: true process restart must not replay an ambiguous raw PUT",
)
def test_unknown_after_real_process_restart(
    tmp_path: Path, failure: str, first_exit: int | None
):
    specs = list(canonical_inventory())
    first = specs[0].name
    a = _payload(first, "A")
    b = _payload(first, "B")
    if len(a) != len(b) or hashlib.sha256(a).digest() == hashlib.sha256(b).digest():
        raise AssertionError("A/B must collide in length but not SHA-256")

    one = _child(tmp_path, "first", failure)
    first_wire = tmp_path / "wire-first.bin"
    if failure == "kill-midput":
        if (
            one.returncode == 0
            or not (tmp_path / "wire-ready.flag").exists()
            or not first_wire.exists()
            or not (0 < len(first_wire.read_bytes()) < len(a))
            or first_wire.read_bytes() != a[: len(first_wire.read_bytes())]
        ):
            raise AssertionError(f"child was not killed inside partial fake PUT: {one}")
    elif (
        one.returncode != first_exit
        or not first_wire.exists()
        or first_wire.read_bytes() != a
    ):
        raise AssertionError(f"first process did not send A then fail ambiguously: {one}")
    # The interpreter has exited. No in-memory UNKNOWN can survive this line.
    two = _child(tmp_path, "second", failure)
    second_wire = tmp_path / "wire-second.bin"
    if two.returncode == 0 and not second_wire.exists():
        return  # A future durable, explicit fail-closed guard is sufficient.
    if two.returncode != 42 or not second_wire.exists():
        raise AssertionError(f"unexpected second-process result: {two}")
    if second_wire.read_bytes() != b:
        raise AssertionError("second attempt did not transmit B")
    raise AmbiguousReplay("fresh process replayed B without readback of A's unknown PUT")


def test_unknown_process_restart_in_optimized_python():
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            f"{Path(__file__).resolve()}::test_unknown_after_real_process_restart",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=240,
    )
    if result.returncode != 0 or "3 xfailed" not in result.stdout:
        raise AssertionError(result.stdout + "\n" + result.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 5 or sys.argv[1] != "--offline-worker":
        raise SystemExit(
            "offline worker only: --offline-worker SCRATCH first|second http503|timeout|kill-midput"
        )
    raise SystemExit(_worker(Path(sys.argv[2]), sys.argv[3], sys.argv[4]))
