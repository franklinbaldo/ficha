"""Offline-only regression for the dormant legacy raw-cache upload helper.

A callable code path is not evidence of a production invocation.
The sentinel replaces the IA client: no network access is made.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from ficha_etl import upload
from ficha_etl.sources import canonical_inventory


class LegacyRawEgressReached(AssertionError):
    """Historical cache helper reached the raw IA write boundary."""


@pytest.mark.xfail(
    strict=True,
    raises=LegacyRawEgressReached,
    reason="#433/F15: legacy raw-cache helper lacks global custody admission",
)
def test_legacy_cache_helper_refuses_partial_raw_without_any_ia_egress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One well-formed ZIP out of 37 cannot authorize any raw write."""
    specs = list(canonical_inventory())
    if len(specs) != 37 or len({spec.name for spec in specs}) != 37:
        raise AssertionError("canonical inventory fixture changed")
    source = tmp_path / "2026-07" / specs[0].name
    source.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("source.csv", "cnpj;value\n1;A\n")

    def reject_sink(_identifier: str, **kwargs: object) -> None:
        files = kwargs.get("files")
        if not isinstance(files, dict) or list(files) != [f"raw/{specs[0].name}"]:
            raise AssertionError("unexpected fake sink arguments")
        raise LegacyRawEgressReached("legacy helper reached ia.upload with just 1/37 raw ZIPs")

    monkeypatch.setattr(upload.ia, "upload", reject_sink)

    # A future fail-closed guard can refuse the partial lot by raising.
    # No-op silence and unguarded IA calls are not accepted as passing.
    with pytest.raises((RuntimeError, ValueError, FileNotFoundError)):
        upload.upload_raw_zips("2026-07", tmp_path, access_key="fake", secret_key="fake")


class LegacyRawRetryReached(AssertionError):
    """A second fake raw PUT was attempted after an ambiguous 503."""


@pytest.mark.xfail(
    strict=True,
    raises=LegacyRawRetryReached,
    reason="#433/F15: dormant retry helper has no UNKNOWN journal or shared admission",
)
def test_legacy_raw_retry_reissues_changed_body_after_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise real _stream_one_zip_with_retry with an offline HTTPX RFB source.

    The fake first PUT consumes A and reports 503; a new GET serves B, with
    the same remote name and size but a different digest. No socket is opened.
    This is an importable legacy helper, not a demonstrated production caller.
    """
    import hashlib
    import io
    from types import SimpleNamespace

    import httpx

    def zip_body(value: bytes) -> bytes:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr(
                zipfile.ZipInfo("source.csv", date_time=(1980, 1, 1, 0, 0, 0)),
                value,
            )
        return output.getvalue()

    source_a = zip_body(b"row=A")
    source_b = zip_body(b"row=B")
    assert len(source_a) == len(source_b)
    assert hashlib.sha256(source_a).digest() != hashlib.sha256(source_b).digest()

    source_url = "https://rfb.invalid/offline/Empresas0.zip"
    source_gets: list[str] = []
    fake_put_bodies: list[bytes] = []

    def rfb_handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET" or str(request.url) != source_url:
            raise AssertionError("unexpected offline upstream request")
        source_gets.append(str(request.url))
        body = source_a if len(source_gets) == 1 else source_b
        return httpx.Response(200, stream=httpx.ByteStream(body), headers={"Content-Length": str(len(body))})

    transport = httpx.MockTransport(rfb_handler)
    original_client = httpx.Client
    monkeypatch.setattr(
        upload.httpx, "Client", lambda **_kwargs: original_client(transport=transport)
    )
    monkeypatch.setattr(upload.upstream, "file_url", lambda *_args: source_url)
    monkeypatch.setattr(upload.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(upload, "_RETRIES", 2)

    def fake_put(
        identifier: str,
        remote_name: str,
        chunks,
        *,
        content_length: str,
        **_kwargs: object,
    ) -> None:
        if (identifier, remote_name) != ("ficha-2026-07", "raw/Empresas0.zip"):
            raise AssertionError("wrong fake remote key")
        body = b"".join(chunks)
        if len(body) != int(content_length):
            raise AssertionError("incomplete fake request body")
        fake_put_bodies.append(body)
        if len(fake_put_bodies) == 1:
            if body != source_a:
                raise AssertionError("first fake PUT did not contain A")
            raise upload._IAS3Error(503, "https://s3.us.archive.org/offline/raw/Empresas0.zip")
        if len(fake_put_bodies) == 2 and body == source_b:
            raise LegacyRawRetryReached("ambiguous PUT(A)/503 followed by PUT(B) without readback")
        raise AssertionError("unexpected fake egress sequence")

    monkeypatch.setattr(upload, "_ia_s3_put", fake_put)
    spec = SimpleNamespace(name="Empresas0.zip")
    try:
        upload._stream_one_zip_with_retry(
            spec,
            rfb_token="offline-token",
            month="2026-07",
            identifier="ficha-2026-07",
            access_key="fake",
            secret_key="fake",
            is_first=True,
        )
    except upload._IAS3Error:
        # Expected fail-closed behavior: preserve UNKNOWN and never send B.
        pass
    if source_gets != [source_url] or fake_put_bodies != [source_a]:
        raise AssertionError("legacy raw retry made an unadmitted second GET or PUT")
