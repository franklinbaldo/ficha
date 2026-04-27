"""Tests for the download module.

We mock the HTTP layer with httpx.MockTransport so tests never touch the network.
"""

from __future__ import annotations

import httpx
import pytest

from ficha_etl import download
from ficha_etl.sources import RemoteFile

PAYLOAD = b"x" * 5000  # 5kB fixture payload


def _file() -> RemoteFile:
    return RemoteFile(
        name="Empresas0.zip",
        url="https://rfb.example.test/2026-01/Empresas0.zip",
        kind="empresas",
    )


def _serve_full(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        content=PAYLOAD,
        headers={"content-length": str(len(PAYLOAD))},
    )


def test_downloads_full_file(tmp_path):
    client = httpx.Client(transport=httpx.MockTransport(_serve_full))
    result = download.download_one(_file(), tmp_path, client=client)
    assert result.size_bytes == len(PAYLOAD)
    assert result.resumed is False
    assert result.path.read_bytes() == PAYLOAD


def test_resumes_partial_download(tmp_path):
    """When a partial file exists locally, we send Range and the server
    returns 206 with the missing tail."""
    file = _file()
    target = tmp_path / file.name
    prefix = PAYLOAD[:2000]
    target.write_bytes(prefix)

    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["range"] = request.headers.get("range", "")
        tail = PAYLOAD[2000:]
        return httpx.Response(
            206,
            content=tail,
            headers={
                "content-range": f"bytes 2000-{len(PAYLOAD) - 1}/{len(PAYLOAD)}",
                "content-length": str(len(tail)),
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = download.download_one(file, tmp_path, client=client)

    assert captured["range"] == "bytes=2000-"
    assert result.resumed is True
    assert result.size_bytes == len(PAYLOAD)
    assert target.read_bytes() == PAYLOAD


def test_restarts_when_server_ignores_range(tmp_path):
    """Some servers reply 200 to a Range request — we must overwrite, not append."""
    file = _file()
    target = tmp_path / file.name
    target.write_bytes(b"garbage" * 100)

    client = httpx.Client(transport=httpx.MockTransport(_serve_full))
    result = download.download_one(file, tmp_path, client=client)
    assert result.size_bytes == len(PAYLOAD)
    assert target.read_bytes() == PAYLOAD


def test_retries_then_succeeds(tmp_path):
    attempts: list[int] = []

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            raise httpx.ConnectError("simulated network blip")
        return _serve_full(request)

    client = httpx.Client(transport=httpx.MockTransport(flaky))
    result = download.download_one(_file(), tmp_path, client=client, max_attempts=4)
    assert result.size_bytes == len(PAYLOAD)
    assert len(attempts) == 3


def test_raises_after_max_attempts(tmp_path):
    def always_fails(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nope")

    client = httpx.Client(transport=httpx.MockTransport(always_fails))
    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
        download.download_one(_file(), tmp_path, client=client, max_attempts=2)


def test_size_mismatch_is_an_error(tmp_path):
    def short(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=PAYLOAD[:100],
            headers={"content-length": str(len(PAYLOAD))},  # lies
        )

    client = httpx.Client(transport=httpx.MockTransport(short))
    with pytest.raises(RuntimeError, match="downloaded"):
        download.download_one(_file(), tmp_path, client=client, max_attempts=1)


def test_download_all_runs_sequentially(tmp_path):
    files = [RemoteFile(name=f"f{i}.zip", url=f"https://x/{i}", kind="cnaes") for i in range(3)]
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return _serve_full(request)

    # download_all opens its own client; bypass by patching httpx.Client briefly.
    transport = httpx.MockTransport(handler)
    original_client = httpx.Client

    def patched(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return original_client(*args, **kwargs)

    import httpx as _httpx

    _httpx.Client = patched  # type: ignore[assignment]
    try:
        results = download.download_all(files, tmp_path)
    finally:
        _httpx.Client = original_client  # type: ignore[assignment]

    assert len(results) == 3
    assert len(seen) == 3
    assert all(r.size_bytes == len(PAYLOAD) for r in results)
