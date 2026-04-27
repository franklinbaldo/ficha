import httpx

from ficha_etl import smoke
from ficha_etl.sources import RemoteFile


def _files() -> list[RemoteFile]:
    return [
        RemoteFile(name="A.zip", url="https://x/A.zip", kind="cnaes"),
        RemoteFile(name="B.zip", url="https://x/B.zip", kind="paises"),
    ]


def test_smoke_all_ok():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-length": "1234"})

    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    httpx.Client = patched  # type: ignore[assignment]
    try:
        results = smoke.smoke_check(_files())
    finally:
        httpx.Client = original  # type: ignore[assignment]

    assert all(r.ok for r in results)
    assert all(r.size == 1234 for r in results)


def test_smoke_reports_404_as_failure():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    httpx.Client = patched  # type: ignore[assignment]
    try:
        results = smoke.smoke_check(_files())
    finally:
        httpx.Client = original  # type: ignore[assignment]

    assert not any(r.ok for r in results)
    assert all(r.status == 404 for r in results)


def test_smoke_captures_network_error():
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    httpx.Client = patched  # type: ignore[assignment]
    try:
        results = smoke.smoke_check(_files())
    finally:
        httpx.Client = original  # type: ignore[assignment]

    assert not any(r.ok for r in results)
    assert all(r.status is None for r in results)
    assert all(r.error and "boom" in r.error for r in results)
