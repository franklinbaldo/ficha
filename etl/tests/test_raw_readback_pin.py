"""Offline one-pin readback contract; no Internet Archive requests."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

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


def test_pin_optimized_contract_subprocess():
    if os.environ.get("FICHA_PIN_READBACK_OPTIMIZED_CHILD") == "1":
        require(sys.flags.optimize >= 1)
        return
    env = dict(os.environ)
    env["FICHA_PIN_READBACK_OPTIMIZED_CHILD"] = "1"
    proc = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", str(Path(__file__).resolve())],
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    if proc.returncode:
        raise AssertionError(proc.stdout[-2000:] + proc.stderr[-2000:])
