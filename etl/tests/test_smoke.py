import httpx

from ficha_etl import smoke


def _patch_client(monkeypatch, handler):
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched)


def test_smoke_all_ok(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is True
    assert report.mirror_ok is True
    assert report.all_ok is True
    assert report.blocking_failure is False


def test_smoke_upstream_failure_is_warning_not_blocking(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            return httpx.Response(200)
        return httpx.Response(503)  # upstream down

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is False
    assert report.mirror_ok is True
    assert report.all_ok is False
    assert report.blocking_failure is False


def test_smoke_mirror_failure_is_blocking(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            raise httpx.ConnectError("simulated outage")
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is True
    assert report.mirror_ok is False
    assert report.all_ok is False
    assert report.blocking_failure is True
    assert "simulated outage" in report.mirror_detail


def test_smoke_upstream_network_error_reported(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            return httpx.Response(200)
        raise httpx.ConnectError("rfb dns fail")

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is False
    assert "rfb dns fail" in report.upstream_detail
    assert report.mirror_ok is True
    assert report.blocking_failure is False
