"""Reject ambiguous IA raw inventory JSON before RFB GET or IA PUT."""

from __future__ import annotations

import httpx
import pytest

from ficha_etl import upload


def _mock_catalog(
    monkeypatch: pytest.MonkeyPatch, payload: bytes, status: int = 200
) -> list[tuple[str, str]]:
    """Run the real HTTPX JSON parser through a local MockTransport."""
    original_client = httpx.Client
    seen: list[tuple[str, str]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url)))
        return httpx.Response(status, content=payload, request=request)

    transport = httpx.MockTransport(respond)

    def client_factory(**kwargs):
        return original_client(transport=transport, **kwargs)

    monkeypatch.setattr(upload.httpx, "Client", client_factory)
    return seen


def _require_catalog_get(seen):
    if seen != [("GET", "https://archive.org/metadata/ficha-2026-07")]:
        pytest.fail(f"unexpected catalog requests: {seen!r}")


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
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"irrelevant":1e999}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"irrelevant":-1e999}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"nested":{"flag":-1e999}}',
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"}],"nested":{"flag":1,"flag":1}}',
    ],
)
def test_ambiguous_ia_json_is_refused_by_observational_catalog(monkeypatch, payload):
    seen = _mock_catalog(monkeypatch, payload)

    def forbid(*args, **kwargs):
        pytest.fail("RFB GET or IA PUT attempted after ambiguous IA catalog JSON")

    monkeypatch.setattr(upload.upstream, "discover_token", forbid)
    monkeypatch.setattr(upload, "_stream_one_zip_with_retry", forbid)

    with pytest.raises(RuntimeError, match="IA raw metadata: invalid JSON") as caught:
        upload._existing_raw_files_on_ia("ficha-2026-07")
    if not isinstance(caught.value.__cause__, ValueError):
        pytest.fail("JSON validation cause not preserved")
    _require_catalog_get(seen)


def test_unambiguous_ia_json_preserves_skip_observation(monkeypatch):
    seen = _mock_catalog(
        monkeypatch,
        b'{"files":[{"name":"raw/Empresas0.zip","size":"151"},'
        b'{"name":"raw/Empresas1.zip","size":"0"},'
        b'{"name":"cnpjs.parquet","size":"100"}],"finite":1.25}',
    )
    assert upload._existing_raw_files_on_ia("ficha-2026-07") == {"raw/Empresas0.zip"}
    _require_catalog_get(seen)


@pytest.mark.parametrize("status, expected", [(404, "empty"), (403, "access denied")])
def test_strict_ia_json_preserves_http_status_contract(monkeypatch, status, expected):
    seen = _mock_catalog(monkeypatch, b"", status=status)
    if status == 404:
        assert upload._existing_raw_files_on_ia("ficha-2026-07") == set()
    else:
        with pytest.raises(RuntimeError, match=expected):
            upload._existing_raw_files_on_ia("ficha-2026-07")
    _require_catalog_get(seen)


@pytest.mark.parametrize("error_type", [upload.httpx.ConnectError, upload.httpx.ReadTimeout])
def test_catalog_rejects_metadata_transport_failure(monkeypatch, error_type):
    """Transport failure gives no evidence that an IA item is absent."""

    seen = []
    failure = error_type("boom")

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            seen.append(("GET", url))
            raise failure

    def forbid(*args, **kwargs):
        pytest.fail("RFB GET or IA PUT after IA metadata transport failure")

    monkeypatch.setattr(upload.httpx, "Client", _FakeClient)
    monkeypatch.setattr(upload.upstream, "discover_token", forbid)
    monkeypatch.setattr(upload, "_stream_one_zip_with_retry", forbid)
    with pytest.raises(RuntimeError, match="IA raw metadata: fetch failed") as caught:
        upload._existing_raw_files_on_ia("ficha-2026-04")
    if caught.value.__cause__ is not failure:
        pytest.fail("transport failure cause not preserved")
    if seen != [("GET", "https://archive.org/metadata/ficha-2026-04")]:
        pytest.fail(f"unexpected catalog requests: {seen!r}")
