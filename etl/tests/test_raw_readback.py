"""No Internet Archive traffic: all full-body readbacks use httpx.MockTransport."""

from __future__ import annotations

import hashlib
import os
import socket
import ssl
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from ficha_etl.raw_readback import RawPin, RawReadbackError, _check_client, readback_raw_batch
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
    with httpx.Client(
        transport=httpx.MockTransport(handler), trust_env=False, **client_options
    ) as client:
        return readback_raw_batch(client, pins=pins, month=month)


def test_all_37_full_reads_and_non_authorizing_receipt():
    pins, bodies = fixture_pins()
    seen = []

    def handler(request):
        seen.append(request)
        require(request.headers["Accept-Encoding"] == "identity")
        require(request.headers["Cache-Control"] == "no-cache")
        require(not request.url.query)
        require("authorization" not in request.headers and "cookie" not in request.headers)
        name = request.url.path.rsplit("/", 1)[-1]
        require(request.url.host == "archive.org")
        return httpx.Response(
            200,
            stream=httpx.ByteStream(bodies[name]),
            headers={"Content-Length": str(len(bodies[name]))},
        )

    receipt = harness(handler, pins=pins, params={})
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
                    "Location": f"https://ia600107.us.archive.org/34/items/ficha-2026-07/raw/{name}"
                },
            )
        if request.url.host == "ia600107.us.archive.org" and request.url.path.startswith(
            "/34/items/"
        ):
            return httpx.Response(307, headers={"Location": f"/35/items/ficha-2026-07/raw/{name}"})
        return httpx.Response(200, stream=httpx.ByteStream(bodies[name]))

    receipt = harness(handler, pins=pins)
    require(len(seen) == 111)
    require(all(obj.redirects == 2 for obj in receipt.objects))
    require(
        all(
            obj.final_url.startswith("https://ia600107.us.archive.org/35/items/")
            for obj in receipt.objects
        )
    )


@pytest.mark.parametrize(
    "cdn_path",
    [
        "/34/items/ficha-unrelated/raw/{name}",
        "/34/items/ficha-2026-06/raw/{name}",
        "/34/items/ficha-2026-07/raw/Other-{name}",
        "/34/items/ficha-2026-07/raw/%43{name_tail}",
        "/34/items/ficha-2026-07/raw/{name}%2Fextra",
        "/34/items/ficha-2026-07/raw/%252e%252e/{name}",
        "/34/items/ficha-2026-07/raw/{name}/extra",
        "/34/items/ficha-2026-07/raw/{name}?query=changed",
        "/download/ficha-2026-07/raw/Other-{name}",
    ],
)
def test_cross_key_redirect_is_refused_before_second_get(cdn_path):
    pins, _bodies = fixture_pins()
    first = min(pin.name for pin in pins)
    location = "https://ia600107.us.archive.org" + cdn_path.format(name=first, name_tail=first[1:])
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"Location": location})

    with pytest.raises(RawReadbackError):
        harness(handler, pins=pins)
    require(len(seen) == 1, "cross-key redirect must not reach a storage node")


@pytest.mark.parametrize("hook_kind", ["request", "response"])
def test_client_hooks_refused_before_any_get(hook_kind):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, content=b"unexpected")

    def inject_authorization(request):
        request.headers["Authorization"] = "Bearer dummy"

    # Request hooks can add secrets *after* an ordinary client header preflight.
    # Response hooks are likewise disallowed to keep the readback surface fixed.
    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(handler, event_hooks={hook_kind: [inject_authorization]})
    require(len(seen) == 0, "unsafe event hook must be rejected without transport IO")


def test_client_default_params_refused_before_any_get():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, content=b"unexpected")

    # HTTPX applies client.params after a caller validates the original URL.
    # Refuse even synthetic query parameters before the first transport call.
    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(handler, params={"token": "dummy"})
    require(not seen, "client params must be rejected before transport I/O")


@pytest.mark.parametrize(
    "bad_host",
    ["dn1.us.archive.org", "foo.archive.org", "ia600107.us.archive.org.evil.example"],
)
def test_unrecognised_cdn_host_is_refused_before_following(bad_host):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(
            302,
            headers={
                "Location": (
                    f"https://{bad_host}/34/items/ficha-2026-07/raw/"
                    f"{min(pin.name for pin in fixture_pins()[0])}"
                )
            },
        )

    with pytest.raises(RawReadbackError):
        harness(handler)
    require(len(seen) == 1)


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


def test_ambient_httpx_configuration_refused_without_transport_access():
    pins, _ = fixture_pins()
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=b"unexpected")

    with httpx.Client(transport=httpx.MockTransport(handler), trust_env=True) as client:
        with pytest.raises(RawReadbackError, match="unauthenticated"):
            readback_raw_batch(client, month="2026-07", pins=pins)
    require(requests == [], "ambient HTTPX configuration must never touch transport")


def test_credentials_refused_without_transport_access():
    def fail_handler(_request):
        pytest.fail("authenticated client triggered HTTP GET")

    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(fail_handler, headers={"Authorization": "Bearer dummy"})
    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(fail_handler, cookies={"session": "dummy"})


@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "other-virtual-host.example"},
        {"Host": "archive.org"},
        {"X-Api-Key": "DUMMY-NOT-A-SECRET"},
        {"X-Auth-Token": "DUMMY-NOT-A-SECRET"},
    ],
)
def test_custom_host_or_header_rejected_without_any_transport(headers):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, content=b"unexpected")

    with pytest.raises(RawReadbackError, match="unauthenticated"):
        harness(handler, headers=headers)
    require(not seen, "caller-supplied Host or auth-like header must never reach transport")


def test_set_cookie_on_http_200_refused_before_next_object():
    seen = []
    pins, bodies = fixture_pins()

    def handler(request):
        seen.append(request)
        name = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            content=bodies[name],
            headers={"Set-Cookie": "probe=synthetic; Domain=archive.org; Path=/; Secure"},
        )

    with pytest.raises(RawReadbackError, match="cookie"):
        harness(handler, pins=pins)
    require(len(seen) == 1, "Set-Cookie 200 must prevent the second GET")
    require("cookie" not in seen[0].headers)


def test_set_cookie_on_redirect_refused_before_cdn_get():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(
            302,
            headers={
                "Location": "https://ia600107.us.archive.org/34/items/ficha-2026-07/raw/Cnaes.zip",
                "Set-Cookie": "probe=synthetic; Domain=archive.org; Path=/; Secure",
            },
        )

    with pytest.raises(RawReadbackError, match="cookie"):
        harness(handler)
    require(len(seen) == 1, "Set-Cookie redirect must not reach the CDN")
    require("cookie" not in seen[0].headers)


def test_redirect_host_matches_url_and_headers_do_not_leak():
    pins, bodies = fixture_pins()
    seen = []

    def handler(request):
        seen.append(request)
        require(request.headers["host"] == request.url.host)
        require(set(request.headers) == {"host", "accept-encoding", "cache-control"})
        require("cookie" not in request.headers)
        require("x-api-key" not in request.headers)
        name = request.url.path.rsplit("/", 1)[-1]
        if request.url.host == "archive.org":
            return httpx.Response(
                302,
                headers={
                    "Location": (
                        "https://ia600107.us.archive.org/34/items/ficha-2026-07/raw/" + name
                    )
                },
            )
        return httpx.Response(200, stream=httpx.ByteStream(bodies[name]))

    receipt = harness(handler, pins=pins, headers={"User-Agent": "DUMMY-SYNTHETIC"})
    require(len(seen) == 74)
    require(len(receipt.objects) == 37)
    require(all(obj.redirects == 1 for obj in receipt.objects))
    require(all("user-agent" not in request.headers for request in seen))


@pytest.mark.parametrize("proxy_configuration", ["proxy", "transport", "mounts"])
def test_explicit_authenticated_proxy_rejected_with_zero_socket_io(
    proxy_configuration, monkeypatch
):
    """Even a synthetic CONNECT must never be attempted by this observer."""
    proxy_url = "http://demo_user:demo_pass@127.0.0.1:9876"
    if proxy_configuration == "proxy":
        options = {"proxy": proxy_url}
    elif proxy_configuration == "transport":
        options = {"transport": httpx.HTTPTransport(proxy=proxy_url, trust_env=False)}
    else:
        options = {"mounts": {"https://": httpx.HTTPTransport(proxy=proxy_url, trust_env=False)}}

    def forbid_connect(*_args, **_kwargs):
        raise AssertionError("raw readback attempted proxy/socket I/O")

    pins, _ = fixture_pins()
    with httpx.Client(trust_env=False, **options) as client:
        # No real or loopback connection may be made, even to test credentials.
        monkeypatch.setattr(socket.socket, "connect", forbid_connect)
        with pytest.raises(RawReadbackError, match="unauthenticated"):
            readback_raw_batch(client, month="2026-07", pins=pins)


def test_unrecognised_transport_rejected_before_mock_transport_io():
    class CustomTransport(httpx.BaseTransport):
        def handle_request(self, _request):
            raise AssertionError("custom transport must not be used")

    pins, _ = fixture_pins()
    with httpx.Client(transport=CustomTransport(), trust_env=False) as client:
        with pytest.raises(RawReadbackError, match="unauthenticated"):
            readback_raw_batch(client, month="2026-07", pins=pins)



@pytest.mark.parametrize("tls_setting", ["verify_false", "cert_none", "hostname_off"])
def test_unverified_tls_rejected_before_any_socket_io(tls_setting, monkeypatch):
    """A canonical HTTPS URL cannot compensate for TLS peer checks being off."""
    if tls_setting == "verify_false":
        verify = False
    else:
        verify = ssl.create_default_context()
        verify.check_hostname = False
        if tls_setting == "cert_none":
            verify.verify_mode = ssl.CERT_NONE

    def forbid_connect(*_args, **_kwargs):
        raise AssertionError("raw readback attempted socket I/O without TLS verification")

    pins, _ = fixture_pins()
    with httpx.Client(trust_env=False, verify=verify) as client:
        monkeypatch.setattr(socket.socket, "connect", forbid_connect)
        with pytest.raises(RawReadbackError, match="unauthenticated"):
            readback_raw_batch(client, month="2026-07", pins=pins)


def test_stock_direct_verified_tls_client_preflight_has_no_socket_io(monkeypatch):
    def forbid_connect(*_args, **_kwargs):
        raise AssertionError("client preflight attempted socket I/O")

    with httpx.Client(trust_env=False, verify=True) as client:
        monkeypatch.setattr(socket.socket, "connect", forbid_connect)
        _check_client(client)

def test_optimized_contract_subprocess():
    """Run this exact checked-out module and test file under Python -O in CI."""
    if os.environ.get("FICHA_READBACK_OPTIMIZED_CHILD") == "1":
        return
    env = dict(os.environ)
    env["FICHA_READBACK_OPTIMIZED_CHILD"] = "1"
    proc = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-q",
            str(Path(__file__).resolve()),
            "-k",
            "not optimized_contract_subprocess",
        ],
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    require(
        proc.returncode == 0,
        "Python -O readback tests failed:\\n" + proc.stdout[-2000:] + proc.stderr[-2000:],
    )
