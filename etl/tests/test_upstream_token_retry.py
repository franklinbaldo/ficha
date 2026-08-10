import httpx
import pytest

from ficha_etl import upstream


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_discover_token_retries_transient_transport(monkeypatch):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectTimeout("transient timeout", request=request)
        return httpx.Response(207)

    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(upstream, "KNOWN_TOKENS", ())
    monkeypatch.setattr(upstream.time, "sleep", lambda _seconds: None)

    with _client(handler) as client:
        assert upstream.discover_token(client=client) == "FROM_ENV"
    assert calls == 2


def test_discover_token_exhausts_transport_retries(monkeypatch):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("still unavailable", request=request)

    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(upstream, "KNOWN_TOKENS", ())
    monkeypatch.setattr(upstream.time, "sleep", lambda _seconds: None)

    with _client(handler) as client, pytest.raises(upstream.NoTokenError):
        upstream.discover_token(client=client)
    assert calls == upstream._TOKEN_PROBE_RETRIES


def test_discover_token_does_not_retry_non_207(monkeypatch):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401)

    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV")
    monkeypatch.setattr(upstream, "KNOWN_TOKENS", ())

    with _client(handler) as client, pytest.raises(upstream.NoTokenError):
        upstream.discover_token(client=client)
    assert calls == 1


def test_discover_token_deduplicates_env_and_known_candidate(monkeypatch):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(403)

    monkeypatch.setenv(upstream.ENV_VAR, "SAME_TOKEN")
    monkeypatch.setattr(upstream, "KNOWN_TOKENS", ("SAME_TOKEN",))

    with _client(handler) as client, pytest.raises(upstream.NoTokenError):
        upstream.discover_token(client=client)
    assert calls == 1
