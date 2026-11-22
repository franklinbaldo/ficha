"""No Internet Archive traffic: all full-body readbacks use httpx.MockTransport."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import httpx
import pytest

from ficha_etl.raw_readback import RawPin, RawReadbackError, readback_raw_batch
from ficha_etl.sources import canonical_inventory


def require(condition: bool, message: str = "offline check failed") -> None:
    if not condition:
        raise AssertionError(message)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixture_pins(letter: bytes = b"A") -> tuple[tuple[RawPin, ...], dict[str, bytes]]:
    bodies = {spec.name: letter * 5 + spec.name.encode() for spec in canonical_inventory()}
    return tuple(RawPin(name, len(data), sha(data)) for name, data in bodies.items()), bodies


def harness(handler, *, pins=None, month="2026-07", **client_options):
    pins = fixture_pins()[0] if pins is None else pins
    with httpx.Client(transport=httpx.MockTransport(handler), **client_options) as client:
        return readback_raw_batch(client, pins=pins, month=month)


def test_all_37_full_reads_and_non_authorizing_receipt():
    pins, bodies = fixture_pins()
    seen = []

    def handler(request):
        seen.append(request)
        require(request.headers["Accept-Encoding"] == "identity")
        require(request.headers["Cache-Control"] == "no-cache")
        require("authorization" not in request.headers and "cookie" not in request.headers)
        name = request.url.path.rsplit("/", 1)[-1]
        require(request.url.host == "archive.org")
        return httpx.Response(
            200,
            stream=httpx.ByteStream(bodies[name]),
            headers={"Content-Length": str(len(bodies[name]))},
        )

    receipt = harness(handler, pins=pins)
    require(len(seen) == len(receipt.objects) == 37)
    require(receipt.total_size == sum(pin.size for pin in pins))
    require(receipt.status == "R_MATCH_AT_T_ONLY")
    require(not receipt.publication_authorized)
    require(not receipt.source_cohort_proven)
    require(not receipt.future_writes_fenced)
    require(
        all(
            obj.status == "R_MATCH_AT_T_ONLY" and not obj.publication_authorized
            for obj in receipt.objects
        )
    )


def test_redirect_to_trusted_archive_cdn_and_relative_location():
    pins, bodies = fixture_pins()
    seen = []

    def handler(request):
        seen.append(str(request.url))
        name = request.url.path.rsplit("/", 1)[-1]
        if request.url.host == "archive.org":
            return httpx.Response(
                302,
                headers={
                    "Location": f"https://dn1.us.archive.org/0/items/ficha-2026-07/raw/{name}"
                },
            )
        if request.url.host == "dn1.us.archive.org" and request.url.path.startswith("/0/items/"):
            return httpx.Response(307, headers={"Location": f"/download/ficha-2026-07/raw/{name}"})
        return httpx.Response(200, stream=httpx.ByteStream(bodies[name]))

    receipt = harness(handler, pins=pins)
    require(len(seen) == 111)
    require(all(obj.redirects == 2 for obj in receipt.objects))
    require(all(obj.final_url.startswith("https://dn1.us.archive.org/") for obj in receipt.objects))


def test_37th_bad_body_yields_no_success_receipt():
    pins, bodies = fixture_pins()
    ordered = sorted(bodies)
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        name = request.url.path.rsplit("/", 1)[-1]
        data = bodies[name]
        body = b"B" + data[1:] if name == ordered[-1] else data
        return httpx.Response(200, stream=httpx.ByteStream(body))

    with pytest.raises(RawReadbackError, match="differs"):
        harness(handler, pins=pins)
    require(calls == 37)


@pytest.mark.parametrize(
    "bad",
    [
        "missing",
        "extra",
        "duplicate",
        "wrong_size_type",
        "wrong_sha",
        "bad_month",
    ],
)
def test_bad_inventory_refused_before_any_get(bad):
    pins, _ = fixture_pins()
    edited = list(pins)
    month = "2026-07"
    if bad == "missing":
        edited.pop()
    elif bad == "extra":
        edited.append(RawPin("Other.zip", 3, sha(b"abc")))
    elif bad == "duplicate":
        edited[-1] = edited[0]
    elif bad == "wrong_size_type":
        edited[0] = replace(edited[0], size=True)
    elif bad == "wrong_sha":
        edited[0] = replace(edited[0], sha256="1" * 63)
    elif bad == "bad_month":
        month = "2026-13"

    def fail_handler(_request):
        pytest.fail("invalid inventory triggered HTTP GET")

    with pytest.raises(RawReadbackError):
        harness(fail_handler, pins=tuple(edited), month=month)


@pytest.mark.parametrize("status", [206, 204, 304, 401, 503])
def test_no_partial_no_unexpected_status(status):
    with pytest.raises(RawReadbackError, match="complete HTTP 200"):
        harness(lambda _: httpx.Response(status, content=b"abc"))


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Encoding": "gzip"},
        {"Content-Encoding": "br"},
        {"Content-Range": "bytes 0-9/10"},
        {"Content-Length": "1, 1"},
        {"Content-Length": "0"},
        {"Content-Length": "+1"},
    ],
)
def test_malicious_headers_fail_closed(headers):
    pins, bodies = fixture_pins()
    name = pins[0].name

    def handler(_request):
        return httpx.Response(200, headers=headers, content=bodies[name])

    with pytest.raises(RawReadbackError):
        harness(handler, pins=pins)


@pytest.mark.parametrize(
    "location",
    [
        "http://archive.org/download/ficha-2026-07/raw/Cnaes.zip",
        "https://evil.example/download/ficha-2026-07/raw/Cnaes.zip",
        "https://archive.org.evil.example/download/a",
        "https://user:secret@archive.org/download/a",
        "https://archive.org:444/download/a",
        "https://archive.org/download/a?token=secret",
    ],
)
def test_reject_redirects_before_following(location):
    count = 0

    def handler(_request):
        nonlocal count
        count += 1
        return httpx.Response(302, headers={"Location": location})

    with pytest.raises(RawReadbackError):
        harness(handler)
    require(count == 1)


def test_redirect_loop_and_missing_location():
    with pytest.raises(RawReadbackError, match="redirect loop"):
        harness(lambda req: httpx.Response(302, headers={"Location": str(req.url)}))
    with pytest.raises(RawReadbackError, match="redirect"):
        harness(lambda _: httpx.Response(302))


def test_no_automatic_retries_after_503():
    seen = 0

    def handler(_request):
        nonlocal seen
        seen += 1
        return httpx.Response(503)

    with pytest.raises(RawReadbackError):
        harness(handler)
    require(seen == 1)


def test_wrong_bytes_same_size_not_confirmed_and_late_write_not_fenced():
    pins, bodies = fixture_pins()
    first = pins[0]
    rewritten = b"B" * len(bodies[first.name])
    require(len(rewritten) == first.size and sha(rewritten) != first.sha256)

    def original_handler(request):
        name = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, stream=httpx.ByteStream(bodies[name]))

    require(not harness(original_handler, pins=pins).future_writes_fenced)

    def later_handler(request):
        name = request.url.path.rsplit("/", 1)[-1]
        body = rewritten if name == first.name else bodies[name]
        return httpx.Response(200, stream=httpx.ByteStream(body))

    with pytest.raises(RawReadbackError, match="differs"):
        harness(later_handler, pins=pins)


def test_credentials_refused_without_transport_access():
    def fail_handler(_request):
        pytest.fail("authenticated client triggered HTTP GET")

    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(fail_handler, headers={"Authorization": "Bearer dummy"})
    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(fail_handler, cookies={"session": "dummy"})
