import httpx

from ficha_etl import smoke, upstream


def _patch_client(monkeypatch, handler):
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched)


def test_smoke_all_ok(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is True
    assert report.mirror_ok is True
    assert report.all_ok is True
    assert report.blocking_failure is False


def test_smoke_upstream_failure_is_warning_not_blocking(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            return httpx.Response(200)
        return httpx.Response(404)  # all token strategies fail

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is False
    assert report.mirror_ok is True
    assert report.all_ok is False
    assert report.blocking_failure is False


def test_smoke_mirror_failure_is_blocking(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")

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
