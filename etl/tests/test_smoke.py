import httpx

from ficha_etl import smoke, upstream


PROPFIND_ROOT_XML = (
    b'<?xml version="1.0"?>'
    b'<d:multistatus xmlns:d="DAV:">'
    b"<d:response><d:href>/public.php/webdav/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-03/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-04/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"</d:multistatus>"
)


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
        if request.method == "PROPFIND":
            return httpx.Response(207, content=PROPFIND_ROOT_XML)
        return httpx.Response(200)  # archive.org HEAD

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is True
    assert report.mirror_ok is True
    assert report.all_ok is True
    assert report.blocking_failure is False
    assert "snapshots=2" in report.upstream_detail


def test_smoke_upstream_failure_is_warning_not_blocking(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PROPFIND":
            return httpx.Response(401)  # token rejected
        return httpx.Response(200)  # archive.org

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is False
    assert report.mirror_ok is True
    assert report.all_ok is False
    assert report.blocking_failure is False


def test_smoke_mirror_failure_is_blocking(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)  # não dorme o backoff

    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            raise httpx.ConnectError("simulated outage")
        return httpx.Response(207, content=PROPFIND_ROOT_XML)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is True
    assert report.mirror_ok is False
    assert report.all_ok is False
    assert report.blocking_failure is True
    assert "simulated outage" in report.mirror_detail


def test_smoke_mirror_transient_503_recovers(monkeypatch):
    # 503 transitório do IA (derive/rate-limit) seguido de 200 → mirror OK.
    # Sem retry isto virava falha de smoke bloqueante por um soluço externo.
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)
    calls = {"archive": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            calls["archive"] += 1
            return httpx.Response(200 if calls["archive"] >= 2 else 503)
        return httpx.Response(207, content=PROPFIND_ROOT_XML)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.mirror_ok is True
    assert report.blocking_failure is False
    assert calls["archive"] == 2  # falhou uma vez, recuperou na segunda


def test_smoke_mirror_4xx_does_not_retry(monkeypatch):
    # 4xx (que não 429) é definitivo — não adianta retry, falha na 1ª.
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(smoke.time, "sleep", lambda *_: None)
    calls = {"archive": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if "archive.org" in str(request.url):
            calls["archive"] += 1
            return httpx.Response(404)
        return httpx.Response(207, content=PROPFIND_ROOT_XML)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.mirror_ok is False
    assert report.blocking_failure is True
    assert calls["archive"] == 1  # sem retry em erro definitivo


def test_smoke_upstream_zero_snapshots(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    empty_xml = (
        b'<?xml version="1.0"?>'
        b'<d:multistatus xmlns:d="DAV:">'
        b"<d:response><d:href>/public.php/webdav/</d:href>"
        b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
        b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
        b"</d:multistatus>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PROPFIND":
            return httpx.Response(207, content=empty_xml)
        return httpx.Response(200)

    _patch_client(monkeypatch, handler)
    report = smoke.run_smoke()
    assert report.upstream_ok is False
    assert "0 snapshots" in report.upstream_detail
