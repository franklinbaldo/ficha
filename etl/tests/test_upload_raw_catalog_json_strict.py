"""Reject ambiguous IA raw inventory JSON before RFB GET or IA PUT."""

from __future__ import annotations

import httpx
import pytest

from ficha_etl import upload


def _mock_catalog(monkeypatch: pytest.MonkeyPatch, payload: bytes, status: int = 200) -> None:
    """Run the real HTTPX JSON parser through a local MockTransport."""
    original_client = httpx.Client

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "archive.org"
        assert request.url.path == "/metadata/ficha-2026-07"
        return httpx.Response(status, content=payload, request=request)

    transport = httpx.MockTransport(respond)

    def client_factory(**kwargs):
        return original_client(transport=transport, **kwargs)

    monkeypatch.setattr(upload.httpx, "Client", client_factory)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"files":[],"files":[{"name":"raw/Empresas0.zip","size":"151"}]}',
        b'{"files":[{"name":"raw/Empresas0.zip","name":"raw/Empresas1.zip","size":"151"}]}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"0","size":"151"}]}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151","size":"151"}]}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"irrelevant":NaN}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"irrelevant":Infinity}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"irrelevant":-Infinity}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"nested":{"flag":1,"flag":1}}',
    ],
)
def test_ambiguous_ia_json_cannot_authorize_any_egress(monkeypatch, payload):
    _mock_catalog(monkeypatch, payload)

    def forbid(*args, **kwargs):
        pytest.fail("RFB GET or IA PUT attempted after ambiguous IA catalog JSON")

    monkeypatch.setattr(upload.upstream, "discover_token", forbid)
    monkeypatch.setattr(upload, "_stream_one_zip_with_retry", forbid)

    with pytest.raises(RuntimeError, match="IA raw metadata: invalid JSON"):
        upload.stream_raw_zips_to_ia("2026-07", access_key="A", secret_key="S")


def test_unambiguous_ia_json_preserves_skip_observation(monkeypatch):
    _mock_catalog(
        monkeypatch,
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"},'
        b'{"name":"raw/Empresas1.zip","size":"0"},'
        b'{"name":"cnpjs.parquet","size":"100"}]}',
    )
    assert upload._existing_raw_files_on_ia("ficha-2026-07") == {"raw/Empresas0.zip"}


@pytest.mark.parametrize("status, expected", [(404, "empty"), (403, "access denied")])
def test_strict_ia_json_preserves_http_status_contract(monkeypatch, status, expected):
    _mock_catalog(monkeypatch, b"", status=status)
    if status == 404:
        assert upload._existing_raw_files_on_ia("ficha-2026-07") == set()
    else:
        with pytest.raises(RuntimeError, match=expected):
            upload._existing_raw_files_on_ia("ficha-2026-07")
