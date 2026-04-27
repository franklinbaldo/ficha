import httpx
import pytest

from ficha_etl import fetcher, upstream


PAYLOAD = b"x" * 1024


def _patch_client(monkeypatch, handler):
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched)


# -----------------------------------------------------------------------------
# LocalCacheFetcher
# -----------------------------------------------------------------------------


def test_local_cache_hit(tmp_path):
    month_dir = tmp_path / "2026-04"
    month_dir.mkdir()
    f = month_dir / "Empresas0.zip"
    f.write_bytes(b"data")
    fc = fetcher.LocalCacheFetcher(cache_dir=tmp_path, month="2026-04")
    assert fc.get("Empresas0.zip") == f


def test_local_cache_miss_when_file_absent(tmp_path):
    fc = fetcher.LocalCacheFetcher(cache_dir=tmp_path, month="2026-04")
    assert fc.get("Empresas0.zip") is None


def test_local_cache_miss_when_zero_size(tmp_path):
    month_dir = tmp_path / "2026-04"
    month_dir.mkdir()
    (month_dir / "Empresas0.zip").touch()  # zero bytes
    fc = fetcher.LocalCacheFetcher(cache_dir=tmp_path, month="2026-04")
    assert fc.get("Empresas0.zip") is None


# -----------------------------------------------------------------------------
# IAMirrorFetcher
# -----------------------------------------------------------------------------


def test_ia_mirror_404_returns_none(tmp_path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    _patch_client(monkeypatch, handler)
    fc = fetcher.IAMirrorFetcher(month="2026-04", cache_dir=tmp_path)
    assert fc.get("Empresas0.zip") is None


def test_ia_mirror_200_downloads(tmp_path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": str(len(PAYLOAD))})
        return httpx.Response(200, content=PAYLOAD, headers={"content-length": str(len(PAYLOAD))})

    _patch_client(monkeypatch, handler)
    fc = fetcher.IAMirrorFetcher(month="2026-04", cache_dir=tmp_path)
    path = fc.get("Empresas0.zip")
    assert path is not None
    assert path.read_bytes() == PAYLOAD
    assert path == tmp_path / "2026-04" / "Empresas0.zip"


def test_ia_mirror_network_error_returns_none(tmp_path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    _patch_client(monkeypatch, handler)
    fc = fetcher.IAMirrorFetcher(month="2026-04", cache_dir=tmp_path)
    assert fc.get("Empresas0.zip") is None


# -----------------------------------------------------------------------------
# UpstreamFetcher
# -----------------------------------------------------------------------------


def test_upstream_downloads(tmp_path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        # Confirma que Auth header está presente.
        assert "Authorization" in request.headers
        return httpx.Response(200, content=PAYLOAD, headers={"content-length": str(len(PAYLOAD))})

    _patch_client(monkeypatch, handler)
    fc = fetcher.UpstreamFetcher(token="TOK", month="2026-04", cache_dir=tmp_path)
    path = fc.get("Empresas0.zip")
    assert path is not None
    assert path.read_bytes() == PAYLOAD


def test_upstream_failure_returns_none(tmp_path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    _patch_client(monkeypatch, handler)
    fc = fetcher.UpstreamFetcher(token="TOK", month="2026-04", cache_dir=tmp_path)
    assert fc.get("Empresas0.zip") is None


# -----------------------------------------------------------------------------
# ChainedFetcher
# -----------------------------------------------------------------------------


class _StubFetcher:
    def __init__(self, name: str, result):
        self.name = name
        self._result = result
        self.called = False

    def get(self, filename: str):
        self.called = True
        return self._result


def test_chain_uses_first_hit(tmp_path):
    p = tmp_path / "x.zip"
    p.write_bytes(b"data")
    a = _StubFetcher("a", p)
    b = _StubFetcher("b", None)
    chain = fetcher.ChainedFetcher(fetchers=[a, b])
    assert chain.get("x.zip") == p
    assert a.called
    assert not b.called  # short-circuited


def test_chain_falls_through_to_second(tmp_path):
    p = tmp_path / "x.zip"
    p.write_bytes(b"data")
    a = _StubFetcher("a", None)
    b = _StubFetcher("b", p)
    chain = fetcher.ChainedFetcher(fetchers=[a, b])
    assert chain.get("x.zip") == p
    assert a.called
    assert b.called


def test_chain_raises_when_all_miss(tmp_path):
    a = _StubFetcher("a", None)
    b = _StubFetcher("b", None)
    chain = fetcher.ChainedFetcher(fetchers=[a, b])
    with pytest.raises(FileNotFoundError, match="a, b"):
        chain.get("x.zip")


# -----------------------------------------------------------------------------
# default_chain
# -----------------------------------------------------------------------------


def test_default_chain_with_upstream(tmp_path, monkeypatch):
    monkeypatch.setenv(upstream.ENV_VAR, "FROM_ENV_TOKEN")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            207,
            content=b'<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>/public.php/webdav/</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>',
        )

    _patch_client(monkeypatch, handler)
    chain = fetcher.default_chain("2026-04", cache_dir=tmp_path)
    names = [f.name for f in chain.fetchers]
    assert names == ["local", "ia", "rfb"]


def test_default_chain_skips_upstream_when_no_token(tmp_path, monkeypatch):
    monkeypatch.delenv(upstream.ENV_VAR, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    _patch_client(monkeypatch, handler)
    chain = fetcher.default_chain("2026-04", cache_dir=tmp_path)
    names = [f.name for f in chain.fetchers]
    assert names == ["local", "ia"]


def test_default_chain_explicit_no_upstream(tmp_path):
    chain = fetcher.default_chain("2026-04", cache_dir=tmp_path, include_upstream=False)
    names = [f.name for f in chain.fetchers]
    assert names == ["local", "ia"]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename,expected_kind",
    [
        ("Empresas0.zip", "empresas"),
        ("Empresas9.zip", "empresas"),
        ("Estabelecimentos3.zip", "estabelecimentos"),
        ("Socios0.zip", "socios"),
        ("Simples.zip", "simples"),
        ("Cnaes.zip", "cnaes"),
        ("Naturezas.zip", "naturezas"),
        ("Qualificacoes.zip", "qualificacoes"),
    ],
)
def test_kind_for_filename(filename: str, expected_kind: str):
    assert fetcher._kind_for_filename(filename) == expected_kind
