"""Offline raw-transport boundary probes for Ficha #433.

Imports the real entrypoint; only catalog, source staging, and IA PUT are fake.
A synthetic 37th-source seal failure tests call order, NOT fs-verity support.
Strict XFAILs document same-inode mutation and post-staging ZIP 37 open failure.
"""

from __future__ import annotations

import errno
import hashlib
import io
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory

MONTH = "2026-07"


class EscapedInodeMutation(AssertionError):
    """Specific defect: admitted A differs from body delivered by fake PUT."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def make_zip(name: str, version: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(name.removesuffix(".zip") + ".csv", (version + ":" + name).encode())
    payload = buffer.getvalue()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        require(archive.testzip() is None, "invalid test ZIP")
    return payload


def arrange(monkeypatch, tmp_path: Path, *, scenario: str):
    specs = list(canonical_inventory())
    require(len(specs) == len({s.name for s in specs}) == 37, "not 37 unique sources")
    first = specs[0].name
    originals = {spec.name: make_zip(spec.name, "A") for spec in specs}
    replacement = make_zip(first, "B")
    require(len(replacement) == len(originals[first]), "A/B unequal length")
    require(
        hashlib.sha256(replacement).digest() != hashlib.sha256(originals[first]).digest(),
        "A/B equal digest",
    )
    state = {
        "attempts": [],
        "received": [],
        "receipts": [],
        "writer": None,
        "write_ok": False,
        "staged": 0,
    }

    def stage_all(inventory, month, token, dest):
        require([s.name for s in inventory] == [s.name for s in specs], "inventory mismatch")
        require(month == MONTH and token == "offline-token", "unexpected source")
        staged = {}
        for spec in inventory:
            path = dest / spec.name
            data = originals[spec.name]
            path.write_bytes(data)
            staged[spec.name] = (path, len(data), hashlib.sha256(data).hexdigest())
            state["staged"] += 1
            if spec.name == first and scenario == "mutate":
                state["writer"] = path.open("r+b")  # Open before entrypoint's last SHA.
        if scenario == "seal37":
            # Simulates a failure before returning an admitted batch. No seal
            # is actually attempted: current production has no seal interface.
            raise OSError(errno.ETXTBSY, "synthetic 37th-source seal failure")
        return staged

    def put(identifier, remote_name, chunks, *, content_length, **kwargs):
        # MUST count at invocation, not after successful body/read/response.
        require(identifier == "ficha-2026-07", "unexpected item")
        require(remote_name.startswith("raw/"), "unexpected namespace")
        name = remote_name.removeprefix("raw/")
        state["attempts"].append(name)
        if scenario == "mutate" and name == first:
            writer = state["writer"]
            require(writer is not None, "missing pre-opened writer")
            try:
                writer.seek(0)
                writer.write(replacement)
                writer.flush()
                os.fsync(writer.fileno())
                state["write_ok"] = True
            except OSError:
                pass  # A physical seal might safely reject the write.
            finally:
                writer.close()  # Release before TemporaryDirectory cleanup on Windows.
                state["writer"] = None
        payload = b"".join(chunks)
        require(len(payload) == int(content_length), "body differs from Content-Length")
        state["received"].append((name, payload))  # Before any simulated HTTP error.
        if scenario == "post503":
            raise upload._IAS3Error(503, "https://s3.us.archive.org/offline")
        state["receipts"].append(name)  # Application-level fake response only.

    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _: set())
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "offline-token")
    monkeypatch.setattr(upload.raw_preflight, "stage_all", stage_all)
    monkeypatch.setattr(upload, "_ia_s3_put", put)

    def invoke():
        try:
            return upload.stream_raw_zips_to_ia(
                MONTH, access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path
            )
        finally:
            if state["writer"] is not None:
                state["writer"].close()
                state["writer"] = None

    return invoke, state, originals


def test_all_37_sources_reach_fake_transport_unchanged(monkeypatch, tmp_path):
    invoke, state, originals = arrange(monkeypatch, tmp_path, scenario="control")
    result = invoke()
    require(result == upload.RawStreamOutcome.SENT_UNVERIFIED, "unexpected result")
    require(len(state["attempts"]) == len(state["received"]) == 37, "incomplete batch")
    require(state["staged"] == 37, "incomplete staging")
    require(
        all(payload == originals[name] for name, payload in state["received"]),
        "changed source in control",
    )


def test_synthetic_37th_gate_failure_causes_zero_put_attempts(monkeypatch, tmp_path):
    invoke, state, _ = arrange(monkeypatch, tmp_path, scenario="seal37")
    with pytest.raises(OSError, match="synthetic 37th-source seal failure"):
        invoke()
    require(state["staged"] == 37, "failure did not occur after source 37")
    require(state["attempts"] == state["received"] == [], "egress before global gate")


def test_http_503_after_body_is_not_pre_call_refusal(monkeypatch, tmp_path):
    invoke, state, originals = arrange(monkeypatch, tmp_path, scenario="post503")
    with pytest.raises(upload._IAS3Error, match="503"):
        invoke()
    first = canonical_inventory()[0].name
    require(state["attempts"] == [first], "missing or repeated attempt")
    require(state["received"] == [(first, originals[first])], "body not recorded")
    require(state["receipts"] == [], "unexpected successful receipt")
    # Zero receipts is not equivalent to zero attempts or zero transmitted bytes.


@pytest.mark.xfail(
    strict=True,
    raises=EscapedInodeMutation,
    reason="#433: pre-opened writer can mutate same inode after last SHA",
)
def test_preopened_writer_cannot_change_transmitted_bytes(monkeypatch, tmp_path):
    invoke, state, originals = arrange(monkeypatch, tmp_path, scenario="mutate")
    try:
        invoke()
    except (RuntimeError, OSError):
        # A pre-transport refusal is safe only if *no attempt* was made.
        if not state["attempts"]:
            return
        # After entry into transport, an error alone cannot certify no egress.
    for name, payload in state["received"]:
        if payload != originals[name]:
            require(state["write_ok"], "bytes changed without simulated writer")
            require(len(payload) == len(originals[name]), "not same-length mutation")
            raise EscapedInodeMutation("fake receiver got B despite admitted SHA(A)")
    require(not state["write_ok"], "unblocked writer but no mutation detected")


class PartialBatchExposure(AssertionError):
    """A late local read failure cannot undo earlier fake IA PUT attempts."""


@pytest.mark.xfail(
    strict=True,
    raises=PartialBatchExposure,
    reason="#433: sequential reopening can send 36 ZIPs before last FD fails",
)
def test_zip37_open_failure_cannot_follow_any_put_attempt(monkeypatch, tmp_path):
    # Uses the REAL stream_raw_zips_to_ia entrypoint; only its boundaries
    # are synthetic, including this deterministic EMFILE fault injection.
    invoke, state, _ = arrange(monkeypatch, tmp_path, scenario="control")
    specs = list(canonical_inventory())
    last = specs[-1].name
    native_open = Path.open

    def deny_last_source(path, mode="r", *args, **kwargs):
        if path.name == last and mode == "rb":
            raise OSError(errno.EMFILE, "injected ZIP 37 FD exhaustion")
        return native_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_last_source)
    with pytest.raises(OSError, match="injected ZIP 37 FD exhaustion"):
        invoke()
    require(state["staged"] == 37, "failure preceded full staging")
    if state["attempts"]:
        require(state["attempts"] == [spec.name for spec in specs[:-1]], "wrong attempt order")
        require(len(state["received"]) == len(state["receipts"]) == 36, "wrong body/receipt counts")
        raise PartialBatchExposure("36 fake PUT attempts preceded ZIP 37 EMFILE")
    require(state["received"] == state["receipts"] == [], "unexpected body or receipt")


def test_replay_boundary_probes_under_python_optimized():
    source = str(Path(__file__).resolve())
    cases = [
        "test_all_37_sources_reach_fake_transport_unchanged",
        "test_synthetic_37th_gate_failure_causes_zero_put_attempts",
        "test_http_503_after_body_is_not_pre_call_refusal",
        "test_preopened_writer_cannot_change_transmitted_bytes",
        "test_zip37_open_failure_cannot_follow_any_put_attempt",
    ]
    completed = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *(f"{source}::{case}" for case in cases)],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    report = completed.stdout + "\n" + completed.stderr
    require(completed.returncode == 0, report)
    require("3 passed" in completed.stdout and "2 xfailed" in completed.stdout, report)


# R159: real Linux FIFO swapped in after all 37 sources have been staged.
# A per-source blocking open must never follow earlier PUT attempts.
class FifoBlockedAfterEgress(AssertionError):
    """A blocking ZIP-37 FIFO followed 36 fake PUT attempts."""


class FifoOpenDeadline(RuntimeError):
    """The kernel blocked an O_RDONLY open of a FIFO without a writer."""


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(os, "mkfifo"),
    reason="requires POSIX FIFO and SIGALRM",
)
@pytest.mark.xfail(
    strict=True,
    raises=FifoBlockedAfterEgress,
    reason="#433: ZIP37 FIFO blocks after 36 local PUT attempts",
)
def test_zip37_fifo_must_be_rejected_before_any_put(monkeypatch, tmp_path):
    import signal

    invoke, state, _ = arrange(monkeypatch, tmp_path, scenario="control")
    names = [s.name for s in canonical_inventory()]
    last = names[-1]
    stage_original = upload.raw_preflight.stage_all

    def stage_fifo(*args, **kwargs):
        staged = stage_original(*args, **kwargs)
        path = staged[last][0]
        path.unlink()
        os.mkfifo(path)  # Real FIFO inode, no writer: O_RDONLY blocks.
        return staged

    monkeypatch.setattr(upload.raw_preflight, "stage_all", stage_fifo)
    prior_handler = signal.getsignal(signal.SIGALRM)

    def interrupt_blocked_open(_signum, _frame):
        raise FifoOpenDeadline("ZIP37 open blocked on FIFO")

    signal.signal(signal.SIGALRM, interrupt_blocked_open)
    timed_out = False
    try:
        signal.setitimer(signal.ITIMER_REAL, 1.5)
        try:
            invoke()
        except FifoOpenDeadline:
            timed_out = True
        except (OSError, RuntimeError):
            # Prompt refusal before egress is acceptable.
            pass
        else:
            raise AssertionError("FIFO was accepted as raw ZIP data")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior_handler)

    require(state["staged"] == 37, "FIFO injection preceded complete staging")
    if state["attempts"]:
        require(timed_out, "unexpected partial transmission without FIFO block")
        require(state["attempts"] == names[:-1], "unexpected fake PUT order")
        require(len(state["received"]) == len(state["receipts"]) == 36, "wrong fake receipts")
        raise FifoBlockedAfterEgress("36 fake PUTs precede real ZIP37 FIFO block")
    require(not timed_out, "FIFO block is unsafe even without a PUT")
    require(state["received"] == state["receipts"] == [], "unexpected synthetic egress")


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(os, "mkfifo"),
    reason="requires POSIX FIFO and SIGALRM",
)
def test_fifo_failure_replayed_under_python_optimized():
    source = str(Path(__file__).resolve())
    test = f"{source}::test_zip37_fifo_must_be_rejected_before_any_put"
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", test],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    require(child.returncode == 0, child.stdout + "\n" + child.stderr)
    require("1 xfailed" in child.stdout, child.stdout + "\n" + child.stderr)


# R160: reproduce the kernel's real EMFILE during entrypoint ZIP 37 open.
# The wrapper observes the syscall error and releases borrowed FDs; it never
# invents EMFILE. This lets TemporaryDirectory clean up on error.
@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux RLIMIT_NOFILE")
@pytest.mark.xfail(
    strict=True,
    raises=PartialBatchExposure,
    reason="#433: real ZIP37 EMFILE after 36 fake PUT callbacks",
)
def test_real_kernel_emfile_after_fake_put36(monkeypatch, tmp_path):
    import resource

    original_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if original_limit[0] < 80:
        pytest.skip("insufficient descriptors for 37-ZIP control")
    invoke, state, _ = arrange(monkeypatch, tmp_path, scenario="control")
    names = [spec.name for spec in canonical_inventory()]
    real_open = Path.open
    original_put = upload._ia_s3_put
    held = []
    observed = []

    def release():
        while held:
            os.close(held.pop())

    def trace_kernel_open(path, mode="r", *args, **kwargs):
        try:
            return real_open(path, mode, *args, **kwargs)
        except OSError as exc:
            if path.name == names[-1] and mode == "rb" and exc.errno == errno.EMFILE:
                observed.append(exc.errno)
                release()
            raise

    def saturate_after_fake_put(*args, **kwargs):
        result = original_put(*args, **kwargs)
        if len(state["attempts"]) == 36:
            while True:
                try:
                    held.append(os.open(os.devnull, os.O_RDONLY))
                except OSError as exc:
                    require(exc.errno == errno.EMFILE, "unexpected FD failure")
                    break
        elif len(state["attempts"]) == 37:
            release()
        return result

    monkeypatch.setattr(Path, "open", trace_kernel_open)
    monkeypatch.setattr(upload, "_ia_s3_put", saturate_after_fake_put)
    caught = None
    try:
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (min(128, original_limit[0]), original_limit[1]),
        )
        try:
            invoke()
        except OSError as exc:
            caught = exc
    finally:
        release()
        resource.setrlimit(resource.RLIMIT_NOFILE, original_limit)

    require(state["staged"] == 37, "did not stage all ZIPs")
    if observed:
        require(observed == [errno.EMFILE], "ZIP37 EMFILE not unique")
        require(caught is not None and caught.errno == errno.EMFILE, "wrong error")
        require(state["attempts"] == names[:-1], "unexpected fake PUT order")
        require(
            len(state["received"]) == len(state["receipts"]) == 36,
            "wrong fake body count",
        )
        raise PartialBatchExposure("36 fake PUTs preceded real ZIP37 EMFILE")
    if caught is not None:
        require(not state["attempts"], "unexpected egress after local error")
        require(state["received"] == state["receipts"] == [], "partial egress")
    else:
        require(state["attempts"] == names, "successful call must cover all 37")


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux RLIMIT_NOFILE")
def test_real_kernel_emfile_replay_python_optimized():
    source = str(Path(__file__).resolve())
    target = f"{source}::test_real_kernel_emfile_after_fake_put36"
    child = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", target],
        capture_output=True,
        text=True,
        check=False,
        timeout=45,
    )
    require(child.returncode == 0, child.stdout + "\n" + child.stderr)
    require("1 xfailed" in child.stdout, child.stdout + "\n" + child.stderr)
