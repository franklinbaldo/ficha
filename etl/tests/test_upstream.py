import httpx
import pytest

from ficha_etl import upstream


def test_share_root_url():
    assert upstream.share_root_url("ABC") == "https://arquivos.receitafederal.gov.br/s/ABC"


def test_file_url():
    assert (
        upstream.file_url("ABC", "Empresas0.zip")
        == "https://arquivos.receitafederal.gov.br/s/ABC/download?path=%2F&files=Empresas0.zip"
    )


def test_files_in_share_count_and_urls():
    files = upstream.files_in_share("XYZ")
    assert len(files) == 37
    assert all("/s/XYZ/download?path=%2F&files=" in f.url for f in files)


def test_discover_token_uses_env_first(monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV_TOKEN")
    # Mocked client should never be called.
    transport = httpx.MockTransport(lambda r: pytest.fail("unexpected HTTP request"))
    client = httpx.Client(transport=transport)
    result = upstream.discover_token(client=client)
    assert result.source == "env"
    assert result.token == "FROM_ENV_TOKEN"


def test_discover_token_falls_back_to_known(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        # First known token works, second never asked.
        if upstream.KNOWN_TOKENS[0] in str(request.url):
            return httpx.Response(200)
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = upstream.discover_token(client=client)
    assert result.source == "known"
    assert result.token == upstream.KNOWN_TOKENS[0]


def test_discover_token_scrapes_landing_page_when_known_fail(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)
    scraped_token = "ScRaPeDtOkN42"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if upstream.RFB_LANDING_PAGE in url:
            html = (
                f'<html>...<a href="https://arquivos.receitafederal.gov.br/s/{scraped_token}">'
                "download</a>...</html>"
            )
            return httpx.Response(200, text=html)
        if scraped_token in url:
            return httpx.Response(200)
        # Known tokens all 404.
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = upstream.discover_token(client=client)
    assert result.source == "scrape"
    assert result.token == scraped_token


def test_discover_token_raises_when_all_strategies_fail(monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        if upstream.RFB_LANDING_PAGE in str(request.url):
            return httpx.Response(200, text="<html>no tokens here</html>")
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(upstream.NoTokenFoundError):
        upstream.discover_token(client=client)
