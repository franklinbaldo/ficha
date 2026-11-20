"""Guard wire-byte identity when mirroring RFB ZIPs to IA, without network."""

from __future__ import annotations

import gzip
import io
import zipfile

import httpx
import pytest

from ficha_etl import upload


def _zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as handle:
        handle.writestr("Estabelecimentos0.csv", "cnpj;logradouro\n123;RUA ALFA\n")
    return buffer.getvalue()


def _run_stream(
    monkeypatch, *, payload: bytes, encoding: str | None, expected_put: bool, seen=None
):
    if seen is None:
        seen = {}
    seen.update(put_count=0, body=b"", length=None, header=None)
    original_client = httpx.Client

    def respond(request: httpx.Request) -> httpx.Response:
        seen["header"] = request.headers.get("accept-encoding")
        headers = {"Content-Length": str(len(payload))}
        if encoding is not None:
            headers["Content-Encoding"] = encoding
        return httpx.Response(
            200,
            headers=headers,
            stream=httpx.ByteStream(payload),
            request=request,
        )

    transport = httpx.MockTransport(respond)

    def client_factory(**kwargs):
        return original_client(transport=transport, **kwargs)

    def spy_put(identifier, remote_name, body_iter, *, content_length, **kwargs):
        seen["put_count"] += 1
        seen["body"] = b"".join(body_iter)
        seen["length"] = content_length
        assert identifier == "offline-item"
        assert remote_name == "raw/Estabelecimentos0.zip"
        assert len(seen["body"]) == int(content_length)

    monkeypatch.setattr(upload.httpx, "Client", client_factory)
    monkeypatch.setattr(upload, "_ia_s3_put", spy_put)
    upload._stream_one_zip(
        type("Spec", (), {"name": "Estabelecimentos0.zip"})(),
        rfb_token="offline-token",
        month="2026-07",
        identifier="offline-item",
        access_key="offline-access",
        secret_key="offline-secret",
        is_first=False,
    )
    assert seen["put_count"] == int(expected_put)
    return seen


@pytest.mark.parametrize("header", [None, "identity"])
def test_raw_zip_identity_response_preserves_exact_octets(monkeypatch, header):
    raw = _zip_bytes()
    seen = _run_stream(monkeypatch, payload=raw, encoding=header, expected_put=True)
    assert seen["header"] == "identity"
    assert seen["body"] == raw
    assert seen["length"] == str(len(raw))


@pytest.mark.parametrize("header", ["gzip", "GZIP", "gzip, identity", "br"])
def test_raw_zip_rejects_content_encoding_before_any_put(monkeypatch, header):
    compressed = gzip.compress(_zip_bytes())
    seen = {}
    with pytest.raises(RuntimeError, match="Content-Encoding"):
        _run_stream(monkeypatch, payload=compressed, encoding=header, expected_put=False, seen=seen)
    assert seen["put_count"] == 0
    assert seen["header"] == "identity"
