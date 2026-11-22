"""Offline raw-transport boundary probes for Ficha #433.

Imports the real entrypoint; only catalog, source staging, and IA PUT are fake.
A synthetic 37th-source seal failure tests call order, NOT fs-verity support.
The strict XFAIL documents a remaining same-inode mutation vulnerability.
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
    require(hashlib.sha256(replacement).digest() != hashlib.sha256(originals[first]).digest(),
            "A/B equal digest")
    state = {"attempts": [], "received": [], "receipts": [], "writer": None,
             "write_ok": False, "staged": 0}

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
    require(all(payload == originals[name] for name, payload in state["received"]),
            "changed source in control")


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


def test_replay_boundary_probes_under_python_optimized():
    source = str(Path(__file__).resolve())
    cases = [
        "test_all_37_sources_reach_fake_transport_unchanged",
        "test_synthetic_37th_gate_failure_causes_zero_put_attempts",
        "test_http_503_after_body_is_not_pre_call_refusal",
        "test_preopened_writer_cannot_change_transmitted_bytes",
    ]
    completed = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *(f"{source}::{case}" for case in cases)],
        capture_output=True, text=True, check=False, timeout=180,
    )
    report = completed.stdout + "\n" + completed.stderr
    require(completed.returncode == 0, report)
    require("3 passed" in completed.stdout and "1 xfailed" in completed.stdout, report)
