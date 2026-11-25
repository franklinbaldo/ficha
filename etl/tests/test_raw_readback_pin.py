"""Offline one-pin readback contract; no Internet Archive requests."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from xml.etree import ElementTree

import httpx
import pytest

from ficha_etl.raw_readback import (
    RawPin,
    RawPinObservation,
    RawReadbackError,
    readback_raw_batch,
    readback_raw_pin,
)
from ficha_etl.sources import canonical_inventory


def require(condition: bool) -> None:
    if not condition:
        raise AssertionError("offline one-pin contract failed")


def fixture_pins() -> tuple[tuple[RawPin, ...], dict[str, bytes]]:
    bodies = {spec.name: spec.name.encode() for spec in canonical_inventory()}
    pins = tuple(
        RawPin(name, len(data), hashlib.sha256(data).hexdigest()) for name, data in bodies.items()
    )
    return pins, bodies


def test_single_pin_without_rest_of_cohort():
    pins, bodies = fixture_pins()
    pin = min(pins, key=lambda p: p.name)
    seen = []

    def handler(request):
        seen.append(request)
        name = request.url.path.rsplit("/", 1)[-1]
        if name != pin.name:
            return httpx.Response(404)
        return httpx.Response(200, stream=httpx.ByteStream(bodies[name]))

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        result = readback_raw_pin(client, month="2026-07", pin=pin)
        require(type(result) is RawPinObservation)
        require(result.month == "2026-07")
        require(result.object.name == pin.name)
        require(result.object.size == pin.size and result.object.sha256 == pin.sha256)
        require(result.status == "R_MATCH_AT_T_ONLY")
        require(not result.publication_authorized)
        require(not result.source_cohort_proven)
        require(not result.future_writes_fenced)
        require(len(seen) == 1 and seen[0].method == "GET")
        require(not hasattr(result, "objects") and not hasattr(result, "token"))
        with pytest.raises(RawReadbackError, match="exactly 37"):
            readback_raw_batch(client, month="2026-07", pins=(pin,))
        require(len(seen) == 1)
        with pytest.raises(RawReadbackError, match="complete HTTP 200"):
            readback_raw_batch(client, month="2026-07", pins=pins)
        require(len(seen) == 3)


@pytest.mark.parametrize("month", ["2026-07", "2026-08"])
def test_pin_observes_exact_month_key(month):
    body = b"identical bytes across months"
    pin = RawPin("Cnaes.zip", len(body), hashlib.sha256(body).hexdigest())
    expected = f"https://archive.org/download/ficha-{month}/raw/Cnaes.zip"
    seen = []

    def handler(request):
        seen.append((request.method, str(request.url)))
        return httpx.Response(200, stream=httpx.ByteStream(body))

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        result = readback_raw_pin(client, month=month, pin=pin)
    require(seen == [("GET", expected)])
    require(result.month == month)
    require(result.object.request_url == expected and result.object.final_url == expected)
    require(result.object.sha256 == pin.sha256 and result.object.size == pin.size)


@pytest.mark.parametrize(
    "month, changes, error",
    [
        ("２０２６-07", {}, "invalid month"),
        ("٢٠٢٦-07", {}, "invalid month"),
        ("2026-13", {}, "invalid month"),
        ("2026-07", {"name": "../Empresas0.zip"}, "canonical ZIP pin"),
        ("2026-07", {"name": "empresas0.zip"}, "canonical ZIP pin"),
        ("2026-07", {"name": []}, "canonical ZIP pin"),
        ("2026-07", {"size": True}, "invalid pinned size"),
        ("2026-07", {"size": 0}, "invalid pinned size"),
        ("2026-07", {"size": 2**63}, "invalid pinned size"),
        ("2026-07", {"size": "1" * 64, "sha256": 42}, "invalid pinned size"),
        ("2026-07", {"sha256": "A" * 64}, "lowercase SHA-256"),
        ("2026-07", {"sha256": "a" * 63}, "lowercase SHA-256"),
    ],
    ids=[
        "fullwidth-month",
        "arabic-month",
        "invalid-month",
        "traversal",
        "wrong-case",
        "list-name",
        "bool-size",
        "zero-size",
        "oversize",
        "wrong-types",
        "uppercase-sha",
        "short-sha",
    ],
)
def test_pin_identity_rejected_before_io(month, changes, error):
    pin = replace(fixture_pins()[0][0], **changes)

    def handler(_request):
        raise AssertionError("invalid pin reached transport")

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        with pytest.raises(RawReadbackError, match=error):
            readback_raw_pin(client, month=month, pin=pin)


def test_pin_reuses_client_gate():
    pin = fixture_pins()[0][0]

    def handler(_request):
        raise AssertionError("unsafe client reached transport")

    with httpx.Client(
        transport=httpx.MockTransport(handler), trust_env=False, params={"x": "dummy"}
    ) as client:
        with pytest.raises(RawReadbackError, match="unauthenticated"):
            readback_raw_pin(client, month="2026-07", pin=pin)


def test_pin_reuses_cross_key_redirect_gate():
    pin = fixture_pins()[0][0]
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(
            302, headers={"Location": "https://archive.org/download/ficha-2026-06/raw/Cnaes.zip"}
        )

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        with pytest.raises(RawReadbackError, match="changes item or file key"):
            readback_raw_pin(client, month="2026-07", pin=pin)
    require(len(seen) == 1)


@pytest.mark.parametrize("mode", ["normal", "truncated", "eof-error", "close-error"])
def test_pin_requires_normal_eof_and_cleanup(mode):
    body = b"x" * 65536
    pin = RawPin("Cnaes.zip", len(body), hashlib.sha256(body).hexdigest())
    seen = []

    class Stream(httpx.SyncByteStream):
        def __init__(self):
            self.closed_normally = False

        def __iter__(self):
            yield body[:-1] if mode == "truncated" else body
            if mode == "eof-error":
                raise httpx.ReadError("synthetic failure after last byte")

        def close(self):
            if mode == "close-error":
                raise httpx.CloseError("synthetic cleanup failure")
            self.closed_normally = True

    stream = Stream()

    def handler(request):
        seen.append(request)
        return httpx.Response(200, stream=stream)

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
        if mode == "normal":
            result = readback_raw_pin(client, month="2026-07", pin=pin)
            require(result.object.sha256 == pin.sha256 and stream.closed_normally)
        else:
            error = "body differs" if mode == "truncated" else "transport/URL failed"
            with pytest.raises(RawReadbackError, match=error):
                readback_raw_pin(client, month="2026-07", pin=pin)
    require(len(seen) == 1)


# Independent of the child's collection and any selection inherited from the parent.
_EXPECTED_OPTIMIZED = {
    "test_single_pin_without_rest_of_cohort",
    "test_pin_observes_exact_month_key[2026-07]",
    "test_pin_observes_exact_month_key[2026-08]",
    "test_pin_reuses_client_gate",
    "test_pin_reuses_cross_key_redirect_gate",
    "test_pin_identity_rejected_before_io[fullwidth-month]",
    "test_pin_identity_rejected_before_io[arabic-month]",
    "test_pin_identity_rejected_before_io[invalid-month]",
    "test_pin_identity_rejected_before_io[traversal]",
    "test_pin_identity_rejected_before_io[wrong-case]",
    "test_pin_identity_rejected_before_io[list-name]",
    "test_pin_identity_rejected_before_io[bool-size]",
    "test_pin_identity_rejected_before_io[zero-size]",
    "test_pin_identity_rejected_before_io[oversize]",
    "test_pin_identity_rejected_before_io[wrong-types]",
    "test_pin_identity_rejected_before_io[uppercase-sha]",
    "test_pin_identity_rejected_before_io[short-sha]",
    "test_pin_requires_normal_eof_and_cleanup[normal]",
    "test_pin_requires_normal_eof_and_cleanup[truncated]",
    "test_pin_requires_normal_eof_and_cleanup[eof-error]",
    "test_pin_requires_normal_eof_and_cleanup[close-error]",
}


def _require_pass_report(report: Path, expected: set[str]) -> None:
    cases = ElementTree.parse(report).findall(".//testcase")
    names = [case.get("name") for case in cases]
    if len(names) != len(expected) or set(names) != expected:
        raise AssertionError(f"unexpected optimized cases: {names!r}")
    for case in cases:
        wrong_class = case.get("classname", "").split(".")[-1] != Path(__file__).stem
        not_passed = any(case.find(tag) is not None for tag in ("skipped", "failure", "error"))
        if wrong_class or not_passed:
            raise AssertionError(f"optimized case did not pass: {case.get('name')}")


def test_pin_optimized_contract_subprocess(tmp_path):
    if os.environ.get("FICHA_PIN_READBACK_OPTIMIZED_CHILD") == "1":
        require(sys.flags.optimize >= 1)
        return

    def run_child(env, report, *selection):
        require(not report.exists())
        proc = subprocess.run(
            [
                sys.executable,
                "-O",
                "-m",
                "pytest",
                "-q",
                "-o",
                "addopts=",
                f"--junitxml={report}",
                str(Path(__file__).resolve()),
                *selection,
            ],
            env=env,
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
        if proc.returncode:
            raise AssertionError(proc.stdout[-2000:] + proc.stderr[-2000:])

    env = dict(os.environ)
    env["FICHA_PIN_READBACK_OPTIMIZED_CHILD"] = "1"
    # Negative control: the old harness accepted this exit 0 despite no readback tests.
    env["PYTEST_ADDOPTS"] = "-k test_pin_optimized_contract_subprocess"
    filtered = tmp_path / "filtered.xml"
    run_child(env, filtered)
    _require_pass_report(filtered, {"test_pin_optimized_contract_subprocess"})
    with pytest.raises(AssertionError, match="unexpected optimized cases"):
        _require_pass_report(filtered, _EXPECTED_OPTIMIZED)

    # The actual replay must be immune to the inherited selection.
    env.pop("PYTEST_ADDOPTS", None)
    complete = tmp_path / "complete.xml"
    run_child(env, complete, "-k", "not test_pin_optimized_contract_subprocess")
    _require_pass_report(complete, _EXPECTED_OPTIMIZED)
