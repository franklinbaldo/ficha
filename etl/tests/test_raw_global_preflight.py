"""Pre-PUT 37/37 RFB raw admission: HTTP and S3 are strictly offline."""

from __future__ import annotations

import io
import struct
import zipfile
import zlib
from pathlib import Path

import httpx
import pytest

from ficha_etl import raw_preflight, upload
from ficha_etl.sources import canonical_inventory


def _equal(actual, expected) -> None:
    # Explicit failure remains live in python -O, unlike bare assert.
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def _source_zip(number: int) -> bytes:
    payload = f"DATA-{number:02d}".encode()
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(f"source-{number:02d}.csv", payload)
    return memory.getvalue()


def _offline(
    monkeypatch, *, bad_crc=False, status_37=None, bad_length=False,
    redirect_37=None, source_37=None,
):
    specs = canonical_inventory()
    _equal(len(specs), 37)
    streams = [_source_zip(i) for i in range(37)]
    if source_37 is not None:
        streams[-1] = source_37
    if bad_crc:
        source = bytearray(streams[-1])
        offset = source.index(b"DATA-36")
        source[offset] ^= 1  # same byte length and ZIP directory, invalid CRC
        streams[-1] = bytes(source)
    seen: dict[str, object] = {"gets": [], "puts": [], "received": []}
    original_client = httpx.Client
    names = {spec.name: i for i, spec in enumerate(specs)}

    def respond(request: httpx.Request):
        name = request.url.path.rsplit("/", 1)[-1]
        index = names[name]
        seen["gets"].append(name)
        if index == 36 and redirect_37 is not None and len(seen["gets"]) == 37:
            return httpx.Response(302, headers={"Location": redirect_37}, request=request)
        if index == 36 and status_37 is not None:
            return httpx.Response(status_37, request=request)
        source = streams[index]
        length = len(source) + (1 if bad_length and index == 36 else 0)
        return httpx.Response(
            200,
            headers={"Content-Length": str(length)},
            stream=httpx.ByteStream(source),
            request=request,
        )

    transport = httpx.MockTransport(respond)

    def client_factory(**kwargs):
        return original_client(transport=transport, **kwargs)

    def put_spy(identifier, remote_name, chunks, *, content_length, **kwargs):
        data = b"".join(chunks)
        _equal(identifier, "ficha-2026-07")
        _equal(len(data), int(content_length))
        # A successful final count alone would not prove temporal ordering.
        _equal(len(seen["gets"]), 37)
        seen["puts"].append(remote_name)
        seen["received"].append(data)

    monkeypatch.setattr(upload.httpx, "Client", client_factory)
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "fake-token")
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _: set())
    monkeypatch.setattr(upload, "_ia_s3_put", put_spy)
    return seen, streams


def _run(tmp_path: Path):
    upload.stream_raw_zips_to_ia(
        "2026-07", access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path
    )


def test_preflight_admits_exact_37_before_any_put(monkeypatch, tmp_path):
    seen, sources = _offline(monkeypatch)
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [f"raw/{s.name}" for s in canonical_inventory()])
    _equal(seen["received"], sources)
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize(
    "case,reason",
    [("bad_crc", "CRC"), ("status_37", "404"), ("bad_length", "length mismatch")],
)
def test_final_zip_failure_cannot_partial_publish(monkeypatch, tmp_path, case, reason):
    options = {case: 404 if case == "status_37" else True}
    seen, _ = _offline(monkeypatch, **options)
    with pytest.raises((RuntimeError, httpx.HTTPStatusError), match=reason):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize(
    "origin",
    [
        "https://other.example",
        "http://arquivos.receitafederal.gov.br",
        "https://arquivos.receitafederal.gov.br",
    ],
)
def test_redirected_final_rfb_zip_is_not_admitted(monkeypatch, tmp_path, origin):
    # Last case covers even a same-origin HTTPS redirect. Every 3xx is denied.
    last_name = canonical_inventory()[-1].name
    target = f"{origin}/public.php/webdav/2026-07/{last_name}"
    seen, _ = _offline(monkeypatch, redirect_37=target)
    with pytest.raises(RuntimeError, match="redirected RFB source"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)  # The redirect target must not be requested.
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def _oversized_source_37(compression: int, *, forge_header: bool) -> bytes:
    """Synthetic malicious ZIP: 65536 expanded bytes but claims one byte."""
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=compression) as archive:
        archive.writestr("source-36.csv", b"A" * 65536)
    if not forge_header:
        return memory.getvalue()
    raw = bytearray(memory.getvalue())
    local = raw.index(b"PK\x03\x04")
    central = raw.index(b"PK\x01\x02")
    prefix_crc = zlib.crc32(b"A")
    struct.pack_into("<I", raw, local + 14, prefix_crc)
    struct.pack_into("<I", raw, local + 22, 1)
    struct.pack_into("<I", raw, central + 16, prefix_crc)
    struct.pack_into("<I", raw, central + 24, 1)
    return bytes(raw)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_underreported_final_zip_cannot_partial_publish(monkeypatch, tmp_path, method):
    forged = _oversized_source_37(method, forge_header=True)
    # The first 36 members are seven bytes each; allow just one more.
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 36 * 7 + 1)
    seen, _ = _offline(monkeypatch, source_37=forged)
    with pytest.raises(RuntimeError, match="global uncompressed limit"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_complete_final_zip_at_actual_expansion_limit(monkeypatch, tmp_path, method):
    valid = _oversized_source_37(method, forge_header=False)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 36 * 7 + 65536)
    seen, sources = _offline(monkeypatch, source_37=valid)
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(len(seen["puts"]), 37)
    _equal(seen["received"], sources)


def test_duplicate_zip_bytes_under_two_source_names_cannot_publish(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch, source_37=_source_zip(0))
    with pytest.raises(RuntimeError, match="duplicate source SHA-256"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def test_expansion_cap_global_not_per_zip(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 35)
    with pytest.raises(RuntimeError, match="global uncompressed limit"):
        _run(tmp_path)
    _equal(seen["puts"], [])


def test_compressed_cap_global_not_per_zip(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_COMPRESSED_BYTES", 2 * len(_source_zip(0)))
    with pytest.raises(RuntimeError, match="global compressed limit"):
        _run(tmp_path)
    _equal(seen["puts"], [])


def test_canonical_duplicate_is_rejected_before_get(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    expected = canonical_inventory()
    monkeypatch.setattr(upload, "canonical_inventory", lambda: expected[:-1] + expected[:1])
    with pytest.raises(RuntimeError, match="canonical inventory"):
        _run(tmp_path)
    _equal(seen["gets"], [])
    _equal(seen["puts"], [])


def test_no_explicit_spool_denies_get_and_put(monkeypatch):
    seen, _ = _offline(monkeypatch)
    with pytest.raises(RuntimeError, match="explicit spool_dir"):
        upload.stream_raw_zips_to_ia("2026-07", access_key="x", secret_key="x")
    _equal(seen["gets"], [])
    _equal(seen["puts"], [])
