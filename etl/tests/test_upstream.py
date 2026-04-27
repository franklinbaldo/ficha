import httpx
import pytest

from ficha_etl import upstream


PROPFIND_ROOT_XML = (
    b'<?xml version="1.0"?>'
    b'<d:multistatus xmlns:d="DAV:">'
    b"<d:response><d:href>/public.php/webdav/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2025-12/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-01/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-02/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/cnpj.tar.gz</d:href>"
    b"<d:propstat><d:prop><d:resourcetype/><d:getcontentlength>63954782749</d:getcontentlength>"
    b"<d:getcontenttype>application/gzip</d:getcontenttype>"
    b'<d:getetag>"abc"</d:getetag></d:prop>'
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"</d:multistatus>"
)


PROPFIND_MONTH_XML = (
    b'<?xml version="1.0"?>'
    b'<d:multistatus xmlns:d="DAV:">'
    b"<d:response><d:href>/public.php/webdav/2026-04/</d:href>"
    b"<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>"
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-04/Empresas0.zip</d:href>"
    b"<d:propstat><d:prop><d:resourcetype/>"
    b"<d:getcontentlength>518166309</d:getcontentlength>"
    b"<d:getcontenttype>application/zip</d:getcontenttype>"
    b'<d:getetag>"e1"</d:getetag></d:prop>'
    b"<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
    b"<d:response><d:href>/public.php/webdav/2026-04/Cnaes.zip</d:href>"
    b"<d:propstat><d:prop><d:resourcetype/>"
    b"<d:getcontentlength>22078</d:getcontentlength>"
    b"<d:getcontenttype>application/zip</d:getcontenttype>"
    b'<d:getetag>"e2"</d:getetag></d:prop>'
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


def test_default_base_url():
    assert upstream.DEFAULT_BASE_URL == "https://arquivos.receitafederal.gov.br"


def test_base_url_env_override(monkeypatch):
    monkeypatch.setenv(upstream.ENV_BASE_URL, "https://example.test")
    assert upstream.base_url() == "https://example.test"


def test_webdav_url():
    assert (
        upstream.webdav_url() == "https://arquivos.receitafederal.gov.br/public.php/webdav/"
    )
    assert upstream.webdav_url("2026-04", "Empresas0.zip") == (
        "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-04/Empresas0.zip"
    )


def test_known_tokens_present():
    assert "YggdBLfdninEJX9" in upstream.KNOWN_TOKENS


def test_files_for_month_count_and_urls():
    files = upstream.files_for_month("XYZ", "2026-04")
    assert len(files) == 37
    assert all(
        f.url.startswith(
            "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-04/"
        )
        for f in files
    )


def test_files_for_month_invalid_month():
    with pytest.raises(ValueError):
        upstream.files_for_month("XYZ", "bad")


def test_discover_token_uses_env(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.delenv(upstream.ENV_BASE_URL, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        if "Basic" in request.headers.get("authorization", ""):
            return httpx.Response(207, content=PROPFIND_ROOT_XML)
        return httpx.Response(401)

    _patch_client(monkeypatch, handler)
    token = upstream.discover_token()
    assert token == "FROM_ENV"


def test_discover_token_falls_back_to_known(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        # First known token works.
        return httpx.Response(207, content=PROPFIND_ROOT_XML)

    _patch_client(monkeypatch, handler)
    token = upstream.discover_token()
    assert token == upstream.KNOWN_TOKENS[0]


def test_discover_token_raises_when_all_fail(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    _patch_client(monkeypatch, handler)
    with pytest.raises(upstream.NoTokenError):
        upstream.discover_token()


def test_list_snapshots(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(207, content=PROPFIND_ROOT_XML)

    _patch_client(monkeypatch, handler)
    months = upstream.list_snapshots("TOK")
    assert months == ["2025-12", "2026-01", "2026-02"]


def test_list_files(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(207, content=PROPFIND_MONTH_XML)

    _patch_client(monkeypatch, handler)
    files = upstream.list_files("TOK", "2026-04")
    names = [f.name for f in files]
    assert names == ["Cnaes.zip", "Empresas0.zip"]
    cnaes = next(f for f in files if f.name == "Cnaes.zip")
    assert cnaes.size == 22078
    assert cnaes.content_type == "application/zip"
    assert cnaes.etag == "e2"


def test_list_files_invalid_month():
    with pytest.raises(ValueError):
        upstream.list_files("TOK", "not-a-month")


def test_file_url():
    url = upstream.file_url("TOK", "2026-04", "Empresas0.zip")
    assert url == (
        "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-04/Empresas0.zip"
    )
